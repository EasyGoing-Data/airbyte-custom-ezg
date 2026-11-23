#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
# source-liftoff-custom v1.0.2
# Liftoff Monetize (Vungle) Publisher Reporting API 2.0 -> BigQuery (doanh thu quang cao lay truc tiep tu Liftoff / Vungle).
# Khung theo source-pubscale-custom v1.0.1 / source-applovin-max-custom v1.8.0
# (multi-account, incremental + lookback, cursor theo tung account, khong +1, clamp <= today, guard slice None).
#
# Docs: https://support.vungle.com/hc/en-us/articles/211365828-Publisher-Reporting-API-2-0
#
# Khac Pubscale:
#   - GET, header `Authorization: Bearer <api key>` + `Vungle-Version: 1`
#   - Response JSON la 1 mang, ten cot co dau cach ("application id", "placement name") -> chuan hoa ve snake_case
#   - Chi co UTC; lich su toi da 6 thang; khong co du lieu -> 200 rong
#   - Khong phan trang -> chia cua so `window_days`

from abc import ABC
import csv
import io
import logging
import os
import threading
import time
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Union

import pendulum
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import IncrementalMixin, Stream
from airbyte_cdk.sources.streams.availability_strategy import AvailabilityStrategy
from airbyte_cdk.sources.streams.http import HttpStream

logger = logging.getLogger("airbyte")

# LIFTOFF_URL_BASE chi dung cho test voi server gia (integration_tests/mock_test.py)
URL_BASE = os.environ.get("LIFTOFF_URL_BASE", "https://report.api.vungle.com/")
REPORT_PATH = "ext/pub/reports/performance"

REPORT_TIMEZONE = "UTC"           # API chi bao cao theo UTC
DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_WINDOW_DAYS = 7
MAX_WINDOW_DAYS = 31
MAX_HISTORY_DAYS = 180            # API: lich su toi da 6 thang
MIN_REQUEST_INTERVAL_SEC = 1.1    # tai lieu khong ghi rate limit -> gian 1 request / giay / key
REQUEST_TIMEOUT_SEC = 180

# v1.0.2: them incentivized + adType vao mac dinh de bang co tang "ad type" (Giam doc yeu cau 16/11/2026).
DEFAULT_DIMENSIONS = ["date", "application", "placement", "country", "platform", "incentivized", "adType"]
ALLOWED_DIMENSIONS = {"date", "application", "placement", "country", "platform", "incentivized", "adType", "adSize"}
DEFAULT_AGGREGATES = ["impressions", "views", "completes", "clicks", "revenue", "ecpm"]
ALLOWED_AGGREGATES = set(DEFAULT_AGGREGATES)
INT_AGGREGATES = {"impressions", "views", "completes", "clicks"}

# dimension -> cot trong record (cot dau la cot khoa)
DIMENSION_COLUMNS = {
    "date": ["date"],
    "application": ["application_id", "application_name"],
    "placement": ["placement_id", "placement_name", "placement_reference_id"],
    "country": ["country"],
    "platform": ["platform"],
    "incentivized": ["incentivized"],
    "adType": ["ad_type"],
    "adSize": ["ad_size"],
}

# Ten cot API (sau khi chuan hoa chu thuong + `_`) -> ten cot trong record
COLUMN_ALIASES = {
    "adtype": "ad_type",
    "adsize": "ad_size",
    "view": "views",
    "complete": "completes",
    "click": "clicks",
    "impression": "impressions",
}


def _to_int(value: Any, default: int) -> int:
    """Cast config value to int. UI may send '7' as string. (dong nhat pubscale / apple-store-custom)"""
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _to_bool(value: Any, default: bool = False) -> bool:
    """Cast config value to bool. UI may send 'false' as string."""
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


def _resolve_dimensions_aggregates(config: Mapping[str, Any]) -> Tuple[List[str], List[str]]:
    dims = [d for d in _to_list(config.get("dimensions"), DEFAULT_DIMENSIONS) if d in ALLOWED_DIMENSIONS]
    aggs = [a for a in _to_list(config.get("aggregates"), DEFAULT_AGGREGATES) if a in ALLOWED_AGGREGATES]
    if "date" in dims:
        dims.remove("date")
    dims = ["date"] + dims  # cursor bat buoc, luon dung dau
    if not aggs:
        aggs = ["views", "revenue"]
    return dims, aggs


def _normalize_key(key: Any) -> str:
    k = str(key or "").strip().lower().replace(" ", "_").replace("-", "_")
    return COLUMN_ALIASES.get(k, k)


def _to_number(value: Any, as_int: bool) -> Union[int, float]:
    """Rong -> 0. So dem -> int, tien / ecpm -> float round 6 (BigQuery NUMERIC scale 9)."""
    if value is None or value == "":
        return 0
    try:
        number = float(value)
    except (ValueError, TypeError):
        return 0
    if as_int and number == int(number):
        return int(number)
    return round(number, 6)


def _parse_body(response: requests.Response) -> List[Dict[str, Any]]:
    """JSON (mang / {data|rows|results: [...]}) hoac CSV -> list dict, key da chuan hoa. Body rong -> []."""
    if response.status_code == 204:
        return []
    text = (response.text or "").lstrip("﻿").strip()
    if not text:
        return []
    rows: List[Any] = []
    if text[0] in "[{":
        data = response.json()  # JSON hong -> ValueError -> sync dung, khong coi la "khong co du lieu"
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            for key in ("data", "rows", "results"):
                if isinstance(data.get(key), list):
                    rows = data[key]
                    break
            else:
                raise ValueError(f"Response JSON khong co mang du lieu; khoa nhan duoc: {sorted(data.keys())}")
    else:
        rows = list(csv.DictReader(io.StringIO(text)))
    out = []
    for row in rows:
        if isinstance(row, dict):
            out.append({_normalize_key(k): v for k, v in row.items() if k is not None})
    return out


class _RateLimiter:
    """1 request / giay / api key. Dung chung cho check_connection + stream, thread-safe."""
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


def _auth_headers(api_key: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {api_key}", "Vungle-Version": "1", "Accept": "application/json"}


""" Base Stream """


class LiftoffStream(HttpStream, ABC):
    url_base = URL_BASE
    http_method = "GET"

    def __init__(self, config: Mapping[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def path(self, stream_state=None, stream_slice=None, next_page_token=None) -> str:
        return REPORT_PATH

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None  # API khong phan trang

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}


""" Report stream (incremental, multi-account) """


class LiftoffReport(LiftoffStream, IncrementalMixin):
    def __init__(self, accounts: List[Dict], **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = None
        self.number_days_backward = _to_int(self.config.get("number_days_backward"), DEFAULT_LOOKBACK_DAYS)
        self.window_days = min(max(_to_int(self.config.get("window_days"), DEFAULT_WINDOW_DAYS), 1), MAX_WINDOW_DAYS)
        self.get_last_X_days = _to_bool(self.config.get("get_last_X_days"), False)
        self.dimensions, self.aggregates = _resolve_dimensions_aggregates(self.config)
        # accounts: [{account_name, api_key}] — KHONG dua api_key vao stream_slice (slice bi log ra stdout)
        self.accounts = accounts
        self._keys_by_account = {a.get("account_name"): a.get("api_key") for a in accounts}
        self._raise_on_http_errors = True
        self._current_slice: Mapping[str, Any] = {}
        # Cursor theo TUNG account; state emit = MIN cua cac account (giong pubscale FIX A)
        self._cursor_by_account: Dict[str, Any] = {}

    @property
    def name(self) -> str:
        return "liftoff_report"

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ["account_name"] + [DIMENSION_COLUMNS[d][0] for d in self.dimensions]

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "date"

    # ---- state ----
    @property
    def state(self) -> Mapping[str, Any]:
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
        today = pendulum.today(REPORT_TIMEZONE).date()
        if cursor > today:
            self.logger.info(f"Cursor {cursor} o tuong lai -> clamp ve {today}")
            cursor = today
        self._cursor_value = cursor
        self.logger.info(f"Cursor Setter {self._cursor_value}")

    # ---- slices: account x cua so ngay ----
    def stream_slices(self, stream_state=None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        slices = []
        today = pendulum.today(REPORT_TIMEZONE).date()

        if self.get_last_X_days:
            start_date = today.subtract(days=self.number_days_backward)
        elif stream_state and self._cursor_value:
            start_date = self._cursor_value.subtract(days=self.number_days_backward)
        else:
            start_date = pendulum.parse(self.config["start_date"]).date()

        earliest = today.subtract(days=MAX_HISTORY_DAYS)
        if start_date < earliest:
            self.logger.warning(
                f"start {start_date} som hon lich su {MAX_HISTORY_DAYS} ngay cua API -> bat dau tu {earliest}. "
                f"Du lieu cu hon phai xin Liftoff (tech-support@vungle.com)."
            )
            start_date = earliest

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
        return _auth_headers(self._keys_by_account.get(account_name, ""))

    def request_params(self, stream_state, stream_slice=None, next_page_token=None) -> MutableMapping[str, Any]:
        s = stream_slice or {}
        return {
            "start": s.get("start_date"),
            "end": s.get("end_date"),
            "dimensions": ",".join(self.dimensions),
            "aggregates": ",".join(self.aggregates),
        }

    def request_kwargs(self, stream_state, stream_slice=None, next_page_token=None) -> Mapping[str, Any]:
        return {"timeout": REQUEST_TIMEOUT_SEC}

    def _send(self, request: requests.PreparedRequest, request_kwargs: Mapping[str, Any]) -> requests.Response:
        _RateLimiter.wait(request.headers.get("Authorization", ""))
        return super()._send(request, request_kwargs)

    # ---- response ----
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
        # Slice doc xong -> account nay coi nhu da phu den end_date, ke ca khi slice rong, de cursor van tien.
        end_raw = self._current_slice.get("end_date")
        if account_name and end_raw:
            end_date = pendulum.parse(str(end_raw)).date()
            prev = self._cursor_by_account.get(account_name)
            self._cursor_by_account[account_name] = max(prev, end_date) if prev else end_date

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info(f"Status code in Parse Response {response.status_code}")
        account_name = self._current_slice.get("account_name")
        start_raw = self._current_slice.get("start_date")
        end_raw = self._current_slice.get("end_date")
        today = pendulum.today(REPORT_TIMEZONE).date()
        key_columns = [DIMENSION_COLUMNS[d][0] for d in self.dimensions]
        dim_columns = [c for d in self.dimensions for c in DIMENSION_COLUMNS[d] if c != "date"]
        rows = _parse_body(response)
        if rows:
            # Thieu cot khoa -> PK = null -> Airbyte dedup gop nham row. Dung han thay vi nap du lieu sai.
            missing = [c for c in key_columns if c not in rows[0]]
            if missing:
                raise ValueError(
                    f"Response thieu cot khoa {missing}; cot nhan duoc: {sorted(rows[0].keys())}. "
                    f"Bo sung COLUMN_ALIASES trong source.py roi sync lai."
                )
        seen = set()
        for row in rows:
            try:
                row_date = pendulum.parse(str(row.get("date"))).date()
            except Exception:
                raise ValueError(f"Khong parse duoc date '{row.get('date')}' trong response.")
            # Chi giu ngay trong cua so da xin, khong vuot today
            if start_raw and row_date < pendulum.parse(start_raw).date():
                continue
            if end_raw and row_date > pendulum.parse(end_raw).date():
                continue
            if row_date > today:
                continue
            rec: Dict[str, Any] = {"account_name": account_name, "date": row_date.to_date_string()}
            for c in dim_columns:
                v = row.get(c)
                if isinstance(v, bool):
                    v = str(v).lower()
                rec[c] = None if v is None else str(v)
            for a in self.aggregates:
                rec[a] = _to_number(row.get(a), as_int=a in INT_AGGREGATES)
            # passthrough cot la
            for k, v in row.items():
                if k and k not in rec and not isinstance(v, (dict, list)):
                    rec[k] = v
            pk = tuple(rec.get(c) for c in ["account_name"] + key_columns)
            if pk in seen:
                # Trung khoa trong cung 1 response -> grain gia dinh sai; dedup se lam mat doanh thu.
                raise ValueError(f"Response co 2 dong trung khoa chinh {key_columns}: {pk[1:]}. Them dimension vao config.")
            seen.add(pk)
            yield rec

    def get_json_schema(self) -> Mapping[str, Any]:
        props: Dict[str, Any] = {
            "account_name": {"type": ["null", "string"]},
            "date": {"type": ["null", "string"], "format": "date"},
        }
        for d in self.dimensions:
            for c in DIMENSION_COLUMNS[d]:
                if c != "date":
                    props[c] = {"type": ["null", "string"]}
        for a in self.aggregates:
            props[a] = {"type": ["null", "number"]}
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
        if response.status_code in (408, 429):
            return True
        return super().should_retry(response=response)

    def backoff_time(self, response: requests.Response) -> Optional[float]:
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            try:
                return max(float(retry_after), MIN_REQUEST_INTERVAL_SEC) if retry_after else 10.0
            except ValueError:
                return 10.0
        return super().backoff_time(response)


# Source
class SourceLiftoff(AbstractSource):
    @staticmethod
    def _probe(api_key: str) -> requests.Response:
        """1 request nho (hom qua, chi theo date) de validate key. 200/204 = OK."""
        yesterday = pendulum.today(REPORT_TIMEZONE).date().subtract(days=1).to_date_string()
        _RateLimiter.wait(f"Bearer {api_key}")
        return requests.get(
            URL_BASE + REPORT_PATH,
            headers=_auth_headers(api_key),
            params={"start": yesterday, "end": yesterday, "dimensions": "date", "aggregates": "views,revenue"},
            timeout=REQUEST_TIMEOUT_SEC,
        )

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        accounts = config.get("accounts", [])
        if not accounts:
            return False, "No accounts configured. Add at least one account."
        for account in accounts:
            account_name = account.get("account_name", "?")
            try:
                api_key = account["api_key"]
                resp = self._probe(api_key)
                if resp.status_code == 429:
                    time.sleep(10)
                    resp = self._probe(api_key)
                if resp.status_code in (200, 204):
                    logger.info(f"Account '{account_name}' OK (HTTP {resp.status_code})")
                    continue
                # Khong dua URL / header vao thong bao loi
                return False, f"Account '{account_name}' failed: HTTP {resp.status_code} {(resp.text or '')[:300]}"
            except KeyError as exc:
                return False, f"Account '{account_name}' is missing required field: {exc}"
            except requests.RequestException as e:
                return False, f"Account '{account_name}' failed: {type(e).__name__}"
            except Exception as e:
                return False, f"Account '{account_name}' failed: {e}"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [LiftoffReport(config=config, accounts=config.get("accounts", []))]
