"""
Streams for source-apple-store-custom.

Streams:
  summary_sales     — Sales API, daily, per vendor
  financial_report  — Finance API, monthly, per vendor
  app_installations — Analytics API, daily, per app_id
  app_sessions      — Analytics API, daily, per app_id

Conventions (playbook §3.3):
  - All column names normalized to _Name_ via _normalize()
  - Metadata fields injected: _vendor_id_, _vendor_name_, _synced_at_
  - Lazy client (§3.1)
  - State per slice key (vendor_id or app_id)
  - Cursor stored as YYYY-MM-DD in state
"""

import logging
import re
from abc import ABC, abstractmethod
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional

import pytz
from airbyte_cdk.sources.streams import IncrementalMixin, Stream

from .client import AppStoreClient

logger = logging.getLogger("airbyte")


# ─── Normalize ────────────────────────────────────────────────────────────────

def _normalize(col: str) -> str:
    """Convert any column name to BigQuery-safe _Name_ format. (§3.3)"""
    s = re.sub(r"[^0-9a-zA-Z]+", "_", str(col)).strip("_")
    return f"_{s}_"


# ─── Metadata constants ───────────────────────────────────────────────────────

META_VENDOR_ID   = _normalize("vendor_id")    # _vendor_id_
META_VENDOR_NAME = _normalize("vendor_name")  # _vendor_name_
META_SYNCED_AT   = _normalize("synced_at")    # _synced_at_


# ─── Base Stream ──────────────────────────────────────────────────────────────

class BaseAppStoreStream(Stream, IncrementalMixin, ABC):
    """
    Base class for all Apple App Store streams.

    Lazy client pattern (§3.1): client is not created until read_records().
    State is tracked per-slice (vendor_id or app_id).
    """

    primary_key = None   # Must be set as class attribute in subclasses (§C)

    def __init__(
        self,
        key_id: str,
        issuer_id: str,
        private_key: str,
        vendors: List[Dict],
        apps: List[Dict],
        start_date: str,
        lookback_days: int,
        get_last_x_days: bool,
        timezone: str,
    ):
        super().__init__()
        self._key_id         = key_id
        self._issuer_id      = issuer_id
        self._private_key    = private_key
        self._vendors        = vendors
        self._apps           = apps            # [{app_id, app_name, bundle_id}]
        self._start_date     = start_date      # YYYY-MM-DD
        self._lookback_days  = lookback_days
        self._get_last_x_days = get_last_x_days
        self._timezone       = timezone
        self._client_obj: Optional[AppStoreClient] = None
        self._state: MutableMapping[str, Any] = {}

    # ── Lazy client ───────────────────────────────────────────────────────────

    @property
    def _client(self) -> AppStoreClient:
        if self._client_obj is None:
            self._client_obj = AppStoreClient(
                key_id=self._key_id,
                issuer_id=self._issuer_id,
                private_key=self._private_key,
            )
        return self._client_obj

    # ── IncrementalMixin state ────────────────────────────────────────────────

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        self._state = value

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _today(self) -> date:
        tz = pytz.timezone(self._timezone or "UTC")
        return datetime.now(tz).date()

    def _parse_date(self, value: str) -> Optional[date]:
        """Parse date from MM/DD/YYYY or YYYY-MM-DD."""
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y-%m"):
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                pass
        return None

    def _inject_metadata(self, record: Dict, vendor: Dict) -> Dict:
        """Normalize keys + inject _vendor_id_, _vendor_name_, _synced_at_."""
        out = {_normalize(k): v for k, v in record.items() if k is not None}
        out[META_VENDOR_ID]   = vendor["vendor_id"]
        out[META_VENDOR_NAME] = vendor["vendor_name"]
        out[META_SYNCED_AT]   = datetime.utcnow().isoformat() + "Z"
        return out

    def get_json_schema(self) -> Dict[str, Any]:
        """Normalize property names in schema to _Name_. (§3.3)"""
        schema = super().get_json_schema()
        props  = schema.get("properties", {})
        schema["properties"] = {_normalize(k): v for k, v in props.items()}
        return schema

    def _date_range(self, slice_key: str, cursor_raw: Optional[str] = None) -> Iterable[date]:
        """
        Yield dates to sync.

        get_last_x_days=True → yield last lookback_days dates regardless of cursor.
        get_last_x_days=False → yield from (cursor - lookback_days) to yesterday.
        """
        today = self._today()
        end   = today - timedelta(days=1)    # yesterday; today's data not complete

        if self._get_last_x_days:
            start = today - timedelta(days=self._lookback_days)
        else:
            saved_cursor = self._state.get(slice_key, {}).get(self.cursor_field)
            if saved_cursor:
                cursor_date = self._parse_date(saved_cursor)
                start = (cursor_date - timedelta(days=self._lookback_days)) if cursor_date else \
                        datetime.strptime(self._start_date, "%Y-%m-%d").date()
            else:
                start = datetime.strptime(self._start_date, "%Y-%m-%d").date()

        current = start
        while current <= end:
            yield current
            current += timedelta(days=1)

    def _update_state(self, slice_key: str, cursor_value: str) -> None:
        """Update state cursor for a slice (ISO format YYYY-MM-DD)."""
        current = self._state.get(slice_key, {}).get(self.cursor_field, "")
        # Store as YYYY-MM-DD
        parsed = self._parse_date(cursor_value)
        new_iso = parsed.isoformat() if parsed else cursor_value
        if not current or new_iso > current:
            self._state.setdefault(slice_key, {})[self.cursor_field] = new_iso


# ─── Summary Sales Stream ─────────────────────────────────────────────────────

class SummarySalesStream(BaseAppStoreStream):
    """
    Daily aggregated sales and download data.
    Source: GET /v1/salesReports (Sales API)
    Cursor: Begin Date (MM/DD/YYYY in report → stored as YYYY-MM-DD in state)
    Ref: https://developer.apple.com/help/app-store-connect/reference/reporting/summary-sales-report
    """

    name        = "summary_sales"
    cursor_field = _normalize("Begin Date")   # _Begin_Date_
    primary_key = [
        META_VENDOR_ID,
        _normalize("Begin Date"),             # _Begin_Date_
        _normalize("SKU"),                    # _SKU_
        _normalize("Country Code"),           # _Country_Code_
        _normalize("Device"),                 # _Device_
        _normalize("Product Type Identifier"),# _Product_Type_Identifier_
        _normalize("Subscription"),           # _Subscription_
        _normalize("Period"),                 # _Period_
        _normalize("Client"),                 # _Client_
        _normalize("Order Type"),             # _Order_Type_
    ]

    def stream_slices(self, **kwargs) -> Iterable[Mapping[str, Any]]:
        for vendor in self._vendors:
            yield {"vendor": vendor}

    def read_records(
        self,
        stream_slice: Mapping[str, Any],
        **kwargs,
    ) -> Iterable[Mapping[str, Any]]:
        vendor    = stream_slice["vendor"]
        vendor_id = vendor["vendor_id"]

        for day in self._date_range(slice_key=vendor_id):
            date_str = day.strftime("%Y-%m-%d")
            logger.info(f"[summary_sales] vendor={vendor_id} date={date_str}")

            content = self._client.fetch_sales_report(vendor_id, date_str)
            if not content:
                logger.debug(f"No data for vendor={vendor_id} date={date_str}")
                continue

            for raw in self._client.parse_gzip_tsv(content):
                record = self._inject_metadata(raw, vendor)
                # Update state cursor
                cursor_val = record.get(self.cursor_field)
                if cursor_val:
                    self._update_state(vendor_id, cursor_val)
                yield record

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> MutableMapping[str, Any]:
        vendor_id  = latest_record.get(META_VENDOR_ID)
        cursor_val = latest_record.get(self.cursor_field)
        if vendor_id and cursor_val:
            self._update_state(vendor_id, cursor_val)
        return self._state


# ─── Financial Report Stream ──────────────────────────────────────────────────

class FinancialReportStream(BaseAppStoreStream):
    """
    Monthly actual proceeds (post Apple commission + tax).
    Source: GET /v1/financeReports (Finance API)
    Cursor: Start Date (MM/DD/YYYY → stored as YYYY-MM-DD)
    Frequency: Monthly (Apple fiscal calendar)
    Ref: https://developer.apple.com/help/app-store-connect/reference/reporting/financial-report-fields
    """

    name         = "financial_report"
    cursor_field = _normalize("Start Date")   # _Start_Date_
    primary_key  = [
        META_VENDOR_ID,
        _normalize("Start Date"),             # _Start_Date_
        _normalize("Vendor Identifier"),      # _Vendor_Identifier_
        _normalize("Country of Sale"),        # _Country_of_Sale_
        _normalize("Product Type Identifier"),# _Product_Type_Identifier_
        _normalize("Sale or Return"),         # _Sale_or_Return_
        _normalize("Promo Code"),             # _Promo_Code_
    ]

    def _month_range(self, vendor_id: str) -> Iterable[str]:
        """Yield YYYY-MM strings from start_date to last complete month."""
        today      = self._today()
        last_month = (today.replace(day=1) - timedelta(days=1))  # last day of prev month

        if self._get_last_x_days:
            # For monthly: interpret lookback_days as months for simplicity
            start_month = last_month.replace(day=1)
        else:
            saved = self._state.get(vendor_id, {}).get(self.cursor_field)
            if saved:
                parsed = self._parse_date(saved)
                start_month = parsed.replace(day=1) if parsed else \
                              datetime.strptime(self._start_date, "%Y-%m-%d").date().replace(day=1)
            else:
                start_month = datetime.strptime(self._start_date, "%Y-%m-%d").date().replace(day=1)

        current = start_month
        while current <= last_month:
            yield current.strftime("%Y-%m")
            # Advance to next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1)
            else:
                current = current.replace(month=current.month + 1)

    def stream_slices(self, **kwargs) -> Iterable[Mapping[str, Any]]:
        for vendor in self._vendors:
            yield {"vendor": vendor}

    def read_records(
        self,
        stream_slice: Mapping[str, Any],
        **kwargs,
    ) -> Iterable[Mapping[str, Any]]:
        vendor    = stream_slice["vendor"]
        vendor_id = vendor["vendor_id"]

        for year_month in self._month_range(vendor_id):
            logger.info(f"[financial_report] vendor={vendor_id} month={year_month}")

            content = self._client.fetch_finance_report(vendor_id, year_month)
            if not content:
                logger.debug(f"No financial data for vendor={vendor_id} month={year_month}")
                continue

            for raw in self._client.parse_gzip_tsv(content):
                record = self._inject_metadata(raw, vendor)
                cursor_val = record.get(self.cursor_field)
                if cursor_val:
                    self._update_state(vendor_id, cursor_val)
                yield record

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> MutableMapping[str, Any]:
        vendor_id  = latest_record.get(META_VENDOR_ID)
        cursor_val = latest_record.get(self.cursor_field)
        if vendor_id and cursor_val:
            self._update_state(vendor_id, cursor_val)
        return self._state


# ─── Base Analytics Stream ────────────────────────────────────────────────────

class BaseAnalyticsStream(BaseAppStoreStream, ABC):
    """
    Base for Analytics API streams (app_installations, app_sessions).

    5-step flow per app per date:
      1. Create/reuse ONGOING request
      2. Create ONE_TIME_SNAPSHOT for backfill (once per app)
      3. Get report_id for report_name
      4. Get instance_ids for date
      5. Get segment URLs → download → parse TSV

    State per app_id:
      {
        "ongoing_request_id":  "...",
        "snapshot_request_id": "...",
        "snapshot_done":       True/False,
        "cursor":              "YYYY-MM-DD"
      }
    Ref: https://developer.apple.com/documentation/analytics-reports
    """

    cursor_field = _normalize("date")    # _date_

    @property
    @abstractmethod
    def report_name(self) -> str:
        """Analytics report name: 'APP_INSTALLS' | 'APP_SESSIONS'"""
        ...

    def _get_app_state(self, app_id: str) -> Dict[str, Any]:
        return self._state.get(app_id, {})

    def _save_app_state(self, app_id: str, **kwargs) -> None:
        self._state.setdefault(app_id, {}).update(kwargs)

    def stream_slices(self, **kwargs) -> Iterable[Mapping[str, Any]]:
        """One slice per (vendor, app). Requires apps to be pre-discovered."""
        for app in self._apps:
            # Use first vendor as metadata carrier (account-level analytics)
            vendor = self._vendors[0] if self._vendors else {"vendor_id": "", "vendor_name": ""}
            yield {"app": app, "vendor": vendor}

    def read_records(
        self,
        stream_slice: Mapping[str, Any],
        **kwargs,
    ) -> Iterable[Mapping[str, Any]]:
        app    = stream_slice["app"]
        vendor = stream_slice["vendor"]
        app_id = app["app_id"]

        app_state           = self._get_app_state(app_id)
        ongoing_request_id  = app_state.get("ongoing_request_id")
        snapshot_request_id = app_state.get("snapshot_request_id")
        snapshot_done       = app_state.get("snapshot_done", False)

        for day in self._date_range(slice_key=app_id):
            date_str = day.strftime("%Y-%m-%d")
            logger.info(f"[{self.name}] app={app_id} date={date_str}")

            records, ongoing_request_id, snapshot_request_id, snapshot_done = \
                self._client.fetch_analytics_data(
                    app_id=app_id,
                    report_name=self.report_name,
                    date=date_str,
                    ongoing_request_id=ongoing_request_id,
                    snapshot_request_id=snapshot_request_id,
                    snapshot_done=snapshot_done,
                )

            # Persist analytics request IDs in state after every date
            self._save_app_state(
                app_id,
                ongoing_request_id=ongoing_request_id,
                snapshot_request_id=snapshot_request_id,
                snapshot_done=snapshot_done,
            )

            for raw in records:
                record = self._inject_metadata(raw, vendor)
                # Inject app metadata (analytics data has app_name/app_apple_id natively)
                cursor_val = record.get(self.cursor_field)
                if cursor_val:
                    self._update_state(app_id, cursor_val)
                    self._save_app_state(
                        app_id, **{self.cursor_field: self._state[app_id][self.cursor_field]}
                    )
                yield record

    def get_updated_state(
        self,
        current_stream_state: MutableMapping[str, Any],
        latest_record: Mapping[str, Any],
    ) -> MutableMapping[str, Any]:
        # State already updated in read_records; return current state
        return self._state


# ─── App Installations Stream ─────────────────────────────────────────────────

class AppInstallationsStream(BaseAnalyticsStream):
    """
    App Store install and delete activity (Standard report).
    report_name: APP_INSTALLS
    Ref: https://developer.apple.com/documentation/analytics-reports/app-installs
    """

    name        = "app_installations"
    report_name = "APP_INSTALLS"
    primary_key = [
        META_VENDOR_ID,
        _normalize("date"),             # _date_
        _normalize("app_apple_id"),     # _app_apple_id_
        _normalize("event"),            # _event_
        _normalize("source_type"),      # _source_type_
        _normalize("territory"),        # _territory_
        _normalize("device"),           # _device_
        _normalize("platform_version"), # _platform_version_
    ]


# ─── App Sessions Stream ──────────────────────────────────────────────────────

class AppSessionsStream(BaseAnalyticsStream):
    """
    Session counts and active devices (DAU proxy) per app.
    report_name: APP_SESSIONS
    Ref: https://developer.apple.com/documentation/analytics-reports/app-sessions
    """

    name        = "app_sessions"
    report_name = "APP_SESSIONS"
    primary_key = [
        META_VENDOR_ID,
        _normalize("date"),             # _date_
        _normalize("app_apple_id"),     # _app_apple_id_
        _normalize("source_type"),      # _source_type_
        _normalize("device"),           # _device_
        _normalize("territory"),        # _territory_
        _normalize("platform_version"), # _platform_version_
    ]
