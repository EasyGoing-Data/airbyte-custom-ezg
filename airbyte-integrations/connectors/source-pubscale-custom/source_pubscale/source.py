#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
# source-pubscale-custom v1.0.1
#   v1.0.1: + package_name, platform (tu app.platforms[0]); round metric 6 so le (het warning TRUNCATED)
# Pubscale (GreedyGame) Immersive Ads Reporting API -> BigQuery.
# Khung theo source-applovin-max-custom v1.8.0 (multi-account, incremental + lookback,
# cursor khong +1, clamp <= today, guard slice None).
#
# Khac AppLovin:
#   - POST JSON body, auth header X-Api-Key (khong query param)
#   - Response tra `rows` JSON truc tiep (khong tai CSV), `app` la dimension -> khong can discover app
#   - Gioi han: date_range <= 90 ngay / request, 1 request / 2 giay / key, phan trang {page, count}
#   - Metric = 0 bi API luoc bo -> connector fill 0 cho cac metric da request
#   - ad_tag KHONG dung duoc cung ad_requests / ad_responses / fill_rate / show_rate

from abc import ABC
import json
import logging
import threading
import time
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import pendulum
import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import IncrementalMixin, Stream
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams.http import HttpStream

logger = logging.getLogger("airbyte")

URL_BASE = "https://go.greedygame.com/"
REPORT_PATH = "v1/chameleon/reporting/public/signal/report/generate"

DEFAULT_LOOKBACK_DAYS = 7
DEFAULT_WINDOW_DAYS = 30          # so ngay / 1 request (API max 90)
MAX_WINDOW_DAYS = 90
DEFAULT_PAGE_SIZE = 1000
DEFAULT_TIMEZONE = "UTC"
MIN_REQUEST_INTERVAL_SEC = 2.1    # API: 1 request / 2 giay / api key

DEFAULT_DIMENSIONS = ["date", "app", "country", "placement_id", "demand_source"]
DEFAULT_METRICS = [
    "ad_requests", "ad_responses", "impressions", "clicks",
    "revenue", "ecpm", "fill_rate", "show_rate", "click_rate",
]
ALLOWED_DIMENSIONS = {"date", "app", "country", "placement_id", "ad_tag", "demand_source", "sdk_v"}
ALLOWED_METRICS = set(DEFAULT_METRICS)
AD_TAG_INCOMPATIBLE_METRICS = {"ad_requests", "ad_responses", "fill_rate", "show_rate"}


def _to_int(value: Any, default: int) -> int:
    """Cast config value to int. UI may send '7' as string. (dong nhat apple-store-custom)"""
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _to_bool(value: Any, default: bool = False) -> bool:
    """Cast config value to bool. UI may send 'false' as string. (dong nhat apple-store-custom)"""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes")


def _to_list(value: Any, default: List[str]) -> List[str]:
    """Config array; UI co the gui chuoi 'a,b,c'."""
    if value is None or value == "" or value == []:
        return list(default)
    if isinstance(value, str):
        return [v.strip() for v in value.split(",") if v.strip()]
    return [str(v).strip() for v in value if str(v).strip()]


def _resolve_dimensions_metrics(config: Mapping[str, Any]) -> Tuple[List[str], List[str]]:
    dims = _to_list(config.get("dimensions"), DEFAULT_DIMENSIONS)
    mets = _to_list(config.get("metrics"), DEFAULT_METRICS)
    dims = [d for d in dims if d in ALLOWED_DIMENSIONS]
    mets = [m for m in mets if m in ALLOWED_METRICS]
    if "date" not in dims:
        dims.insert(0, "date")  # cursor bat buoc
    if "ad_tag" in dims:
        dropped = [m for m in mets if m in AD_TAG_INCOMPATIBLE_METRICS]
        if dropped:
            logger.warning(f"ad_tag khong tuong thich voi {dropped} -> bo cac metric nay.")
        mets = [m for m in mets if m not in AD_TAG_INCOMPATIBLE_METRICS]
    if not mets:
        mets = ["impressions", "revenue"]
    return dims, mets


class _RateLimiter:
    """1 request / 2s / api key. Dung chung cho check_connection + stream, thread-safe."""
    _lock = threading.Lock()
    _last_at: Dict[str, float] = {}

    @classmethod
    def wait(cls, key: str) -> None:
        with cls._lock:
            now = time.monotonic()
            last = cls._last_at.get(key)
            if last is not None:
                delta = now - last
                if delta < MIN_REQUEST_INTERVAL_SEC:
                    time.sleep(MIN_REQUEST_INTERVAL_SEC - delta)
            cls._last_at[key] = time.monotonic()


""" Base Stream """


class PubscaleStream(HttpStream, ABC):
    url_base = URL_BASE
    http_method = "POST"

    def __init__(self, config: Mapping[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def path(self, stream_state=None, stream_slice=None, next_page_token=None) -> str:
        return REPORT_PATH

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None

    def request_params(self, stream_state, stream_slice=None, next_page_token=None) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}


""" Report stream (incremental, multi-account) """


class PubscaleReport(PubscaleStream, IncrementalMixin):
    def __init__(self, accounts: List[Dict], **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = None
        self.number_days_backward = _to_int(self.config.get("number_days_backward"), DEFAULT_LOOKBACK_DAYS)
        self.window_days = min(max(_to_int(self.config.get("window_days"), DEFAULT_WINDOW_DAYS), 1), MAX_WINDOW_DAYS)
        self.page_size = max(_to_int(self.config.get("page_size"), DEFAULT_PAGE_SIZE), 1)
        self.timezone = self.config.get("timezone") or DEFAULT_TIMEZONE
        self.get_last_X_days = _to_bool(self.config.get("get_last_X_days"), False)
        self.dimensions, self.metrics = _resolve_dimensions_metrics(self.config)
        # accounts: [{account_name, api_key}] — KHONG dua api_key vao stream_slice (slice bi log ra stdout)
        self.accounts = accounts
        self._keys_by_account = {a.get("account_name"): a.get("api_key") for a in accounts}
        self._raise_on_http_errors = True
        self._current_slice: Mapping[str, Any] = {}
        # FIX A: cursor theo TUNG account; state emit = MIN cua cac account.
        # Slice la cua so 30 ngay (khong phai 1 ngay nhu applovin) nen neu acc1 xong
        # cua so, acc2 fail -> state max toan cuc se lam acc2 mat ca cua so (> lookback).
        self._cursor_by_account: Dict[str, Any] = {}

    @property
    def name(self) -> str:
        return "pubscale_report"

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        # app -> app_id sau khi flatten
        pk = ["account_name"] + [("app_id" if d == "app" else d) for d in self.dimensions]
        return pk

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "date"

    # ---- state (giu 5 FIX cua applovin v1.7.0) ----
    @property
    def state(self) -> Mapping[str, Any]:
        # Chi phat cursor khi MOI account da co du lieu trong lan chay nay;
        # cursor = min(max_date cua tung account). Chua du -> giu cursor cu.
        names = [a.get("account_name") for a in self.accounts if a.get("account_name")]
        if names and all(n in self._cursor_by_account for n in names):
            new_cursor = min(self._cursor_by_account[n] for n in names)
            if self._cursor_value is None or new_cursor > self._cursor_value:
                self._cursor_value = new_cursor
        return {self.cursor_field: self._cursor_value.to_date_string() if self._cursor_value else None}

    @state.setter
    def state(self, value: Mapping[str, Any]):
        raw = value.get(self.cursor_field) if value else None
        if not raw:
            self._cursor_value = None
            return
        cursor = pendulum.parse(str(raw)).date()
        today = pendulum.today(self.timezone).date()
        if cursor > today:
            self.logger.info(f"Cursor {cursor} o tuong lai -> clamp ve {today}")
            cursor = today
        self._cursor_value = cursor
        self.logger.info(f"Cursor Setter {self._cursor_value}")

    # ---- slices: account x cua so ngay (<= window_days, API max 90) ----
    def stream_slices(self, stream_state=None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        slices = []
        today = pendulum.today(self.timezone).date()

        if self.get_last_X_days:
            start_date = today.subtract(days=self.number_days_backward)
        elif stream_state and self._cursor_value:
            start_date = self._cursor_value.subtract(days=self.number_days_backward)
        else:
            start_date = pendulum.parse(self.config["start_date"]).date()

        if not self.accounts:
            self.logger.warning("Khong co account nao trong config.")

        cur = start_date
        while cur <= today:
            end = min(cur.add(days=self.window_days - 1), today)
            for account in self.accounts:
                account_name = account.get("account_name")
                if not account_name:
                    continue
                slices.append({
                    "start_date": cur.to_date_string(),
                    "end_date": end.to_date_string(),
                    "account_name": account_name,
                })
            cur = end.add(days=1)

        if not slices:
            self.logger.info("Khong co slice nao (start_date > today hoac khong co account) — skip.")
        return slices

    # ---- request ----
    def request_headers(self, stream_state, stream_slice=None, next_page_token=None) -> Mapping[str, Any]:
        account_name = (stream_slice or {}).get("account_name")
        return {
            "X-Api-Key": self._keys_by_account.get(account_name, ""),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def request_body_json(self, stream_state, stream_slice=None, next_page_token=None) -> Optional[Mapping[str, Any]]:
        s = stream_slice or {}
        page = (next_page_token or {}).get("page", 1)
        return {
            "date_range": {"start_date": s.get("start_date"), "end_date": s.get("end_date")},
            "dimensions": self.dimensions,
            "metrics": self.metrics,
            # FIX C: sort xac dinh theo toan bo dimensions de phan trang on dinh
            "order_by": [{"column": d, "ascending": True} for d in self.dimensions],
            "pagination": {"page": page, "count": self.page_size},
        }

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        if response.status_code != 200:
            return None
        try:
            data = response.json()
        except ValueError:
            return None
        # API tra `has_next` (bool) + `total_rows` -> phan trang theo has_next (chinh xac nhat).
        # Fallback (neu response khong co has_next): doc tiep khi trang day (>= page_size).
        if isinstance(data, dict) and "has_next" in data:
            has_next = bool(data.get("has_next"))
        else:
            has_next = len(self._rows(response)) >= self.page_size
        if not has_next:
            return None
        try:
            body = json.loads(response.request.body or b"{}")
            page = int(body.get("pagination", {}).get("page", 1))
        except Exception:
            page = 1
        return {"page": page + 1}

    def _send(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> requests.Response:
        # Rate limit theo api key (header), truoc khi gui
        _RateLimiter.wait(request.headers.get("X-Api-Key", ""))
        return super()._send(request, request_kwargs)

    # ---- response ----
    @staticmethod
    def _rows(response: requests.Response) -> List[Mapping[str, Any]]:
        if response.status_code == 204 or not (response.content or b"").strip():
            return []
        try:
            data = response.json()
        except ValueError:
            return []
        if isinstance(data, dict):
            rows = data.get("rows") or []
        elif isinstance(data, list):
            rows = data
        else:
            rows = []
        return [r for r in rows if isinstance(r, dict)]

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None) -> Iterable[Mapping[str, Any]]:
        self._current_slice = stream_slice or {}
        self.logger.info(f"stream slice {stream_slice}")
        records = super().read_records(
            sync_mode=sync_mode, cursor_field=cursor_field, stream_slice=stream_slice, stream_state=stream_state
        )
        account_name = self._current_slice.get("account_name")
        for record in records:
            if record and record.get(self.cursor_field):
                record_cursor_value = pendulum.parse(str(record[self.cursor_field])).date()
                prev = self._cursor_by_account.get(account_name)
                self._cursor_by_account[account_name] = max(prev, record_cursor_value) if prev else record_cursor_value
            yield record
        # Slice doc xong (moi trang OK) -> account nay coi nhu da phu den end_date,
        # ke ca khi slice rong (204) de cursor van tien.
        end_raw = self._current_slice.get("end_date")
        if account_name and end_raw:
            end_date = pendulum.parse(str(end_raw)).date()
            prev = self._cursor_by_account.get(account_name)
            self._cursor_by_account[account_name] = max(prev, end_date) if prev else end_date

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info(f"Status code in Parse Response {response.status_code}")
        account_name = self._current_slice.get("account_name")
        for row in self._rows(response):
            rec: Dict[str, Any] = {}
            # dimensions
            for d in self.dimensions:
                if d == "app":
                    app = row.get("app")
                    if isinstance(app, dict):
                        rec["app_name"] = app.get("name")
                        rec["app_id"] = row.get("app_id") or app.get("id") or app.get("app_id")
                        # v1.0.1: package_name + platform tu app.platforms[0] (fallback app.engine)
                        platforms = app.get("platforms") or []
                        first = platforms[0] if platforms and isinstance(platforms[0], dict) else {}
                        rec["package_name"] = first.get("package")
                        rec["platform"] = first.get("platform") or app.get("engine")
                        if len(platforms) > 1:
                            self.logger.warning(
                                f"App {rec['app_id']} co {len(platforms)} platforms {platforms} — chi lay phan tu dau."
                            )
                    else:
                        rec["app_name"] = app if app is not None else None
                        rec["app_id"] = row.get("app_id")
                        rec["package_name"] = None
                        rec["platform"] = None
                    if rec.get("app_id") is not None:
                        rec["app_id"] = str(rec["app_id"])
                else:
                    rec[d] = row.get(d)
            # flatten `plcmnt` (chi xuat hien khi co dimension placement_id):
            # placement_id la so ("89706"), ten nguoi doc + unit_id nam trong plcmnt{}
            if "placement_id" in self.dimensions:
                plcmnt = row.get("plcmnt")
                if isinstance(plcmnt, dict):
                    rec["placement_name"] = plcmnt.get("name")
                    rec["ad_unit_id"] = plcmnt.get("unit_id")
            # metrics — API luoc bo gia tri 0 -> fill 0; round 6 so le (BigQuery NUMERIC scale 9,
            # API tra toi 16 so le -> Airbyte bao TRUNCATED; 6 la du cho revenue USD / ecpm / rate %)
            for m in self.metrics:
                v = row.get(m)
                if v is None:
                    rec[m] = 0
                elif isinstance(v, float):
                    rec[m] = round(v, 6)
                else:
                    rec[m] = v
            # passthrough cac cot la (ngoai app object)
            for k, v in row.items():
                if k not in rec and k != "app" and not isinstance(v, (dict, list)):
                    rec[k] = v
            rec["account_name"] = account_name
            yield rec

    def get_json_schema(self) -> Mapping[str, Any]:
        props: Dict[str, Any] = {"account_name": {"type": ["null", "string"]}}
        for d in self.dimensions:
            if d == "app":
                props["app_id"] = {"type": ["null", "string"]}
                props["app_name"] = {"type": ["null", "string"]}
                props["package_name"] = {"type": ["null", "string"]}
                props["platform"] = {"type": ["null", "string"]}
            elif d == "date":
                props["date"] = {"type": ["null", "string"], "format": "date"}
            elif d == "placement_id":
                props["placement_id"] = {"type": ["null", "string"]}
                props["placement_name"] = {"type": ["null", "string"]}
                props["ad_unit_id"] = {"type": ["null", "string"]}
            else:
                props[d] = {"type": ["null", "string"]}
        for m in self.metrics:
            props[m] = {"type": ["null", "number"]}
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "additionalProperties": True,
            "properties": props,
        }

    # ---- errors / retry ----
    @property
    def raise_on_http_errors(self) -> bool:
        return self._raise_on_http_errors

    @raise_on_http_errors.setter
    def raise_on_http_errors(self, value: bool):
        self._raise_on_http_errors = value

    def should_retry(self, response: requests.Response) -> bool:
        if response.status_code == 204:
            return False
        if response.status_code == 429:
            return True
        return super().should_retry(response=response)

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            try:
                return max(float(retry_after), MIN_REQUEST_INTERVAL_SEC) if retry_after else 5.0
            except ValueError:
                return 5.0
        return super().backoff_time(response)


# Source
class SourcePubscale(AbstractSource):
    @staticmethod
    def _probe(api_key: str, dims: List[str], mets: List[str]) -> requests.Response:
        """1 request nho (2 ngay gan nhat, 1 row) de validate key. 200/204 = OK."""
        today = pendulum.today(DEFAULT_TIMEZONE).date()
        body = {
            "date_range": {"start_date": today.subtract(days=2).to_date_string(), "end_date": today.to_date_string()},
            "dimensions": dims,
            "metrics": mets,
            "pagination": {"page": 1, "count": 1},
        }
        _RateLimiter.wait(api_key)
        return requests.post(
            URL_BASE + REPORT_PATH,
            headers={"X-Api-Key": api_key, "Content-Type": "application/json", "Accept": "application/json"},
            json=body,
            timeout=60,
        )

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        accounts = config.get("accounts", [])
        if not accounts:
            return False, "No accounts configured. Add at least one account."
        dims, mets = _resolve_dimensions_metrics(config)
        for account in accounts:
            account_name = account.get("account_name", "?")
            try:
                api_key = account["api_key"]
                resp = self._probe(api_key, dims, mets)
                if resp.status_code == 429:
                    time.sleep(5)
                    resp = self._probe(api_key, dims, mets)
                if resp.status_code in (200, 204):
                    logger.info(f"Account '{account_name}' OK (HTTP {resp.status_code})")
                    continue
                try:
                    err = resp.json()
                except ValueError:
                    err = resp.text[:300]
                return False, f"Account '{account_name}' failed: HTTP {resp.status_code} {err}"
            except KeyError as exc:
                return False, f"Account '{account_name}' is missing required field: {exc}"
            except Exception as e:
                return False, f"Account '{account_name}' failed: {e}"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [PubscaleReport(config=config, accounts=config.get("accounts", []))]
