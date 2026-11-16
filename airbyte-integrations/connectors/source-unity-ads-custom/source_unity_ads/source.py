#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
# source-unity-ads-custom v1.0.0
# Unity Ads Monetization Stats API -> BigQuery (doanh thu quang cao lay truc tiep tu Unity).
# Khung theo source-pubscale-custom v1.0.1 / source-applovin-max-custom v1.8.0
# (multi-account, incremental + lookback, cursor theo tung account, khong +1, clamp <= today, guard slice None).
#
# Docs: https://docs.unity.com/en-us/monetization/performance-and-analytics/api/monetization-stats-api
#
# Khac Pubscale:
#   - GET, auth header `Authorization: Token <api_key>`, organization core id nam trong path
#   - Response CSV (mac dinh) hoac JSON -> parser doc ca 2
#   - Ngay cat theo 00:00 UTC, API khong co tham so timezone -> connector co dinh UTC
#   - Khong phan trang; server timeout 60s -> chia cua so `window_days`
#   - Tai lieu khong co mau response: ten cot duoc chuan hoa + alias, cot la passthrough

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

# UNITY_ADS_URL_BASE chi dung cho test voi server gia (integration_tests/mock_test.py)
URL_BASE = os.environ.get("UNITY_ADS_URL_BASE", "https://monetization.api.unity.com/")
PATH_TEMPLATE = "stats/v1/operate/organizations/{organization_id}"

REPORT_TIMEZONE = "UTC"           # API cat ngay tai 00:00 UTC, khong doi duoc
DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_WINDOW_DAYS = 7           # so ngay / 1 request (server timeout 60s)
MAX_WINDOW_DAYS = 31
MIN_REQUEST_INTERVAL_SEC = 1.1    # tai lieu khong ghi nguong 429 -> gian 1 request / giay / key
REQUEST_TIMEOUT_SEC = 120

DEFAULT_GROUP_BY = ["country", "placement", "platform", "game"]
ALLOWED_GROUP_BY = {"country", "placement", "platform", "game"}
DEFAULT_FIELDS = ["adrequest_count", "available_sum", "revenue_sum", "start_count", "view_count"]
ALLOWED_FIELDS = set(DEFAULT_FIELDS)

# groupBy -> cot trong record
GROUP_BY_COLUMNS = {
    "country": ["country"],
    "placement": ["placement"],
    "platform": ["platform"],
    "game": ["source_game_id", "source_name"],
}
GROUP_BY_PK = {"country": "country", "placement": "placement", "platform": "platform", "game": "source_game_id"}

# Ten cot API (sau khi chuan hoa) -> ten cot trong record
COLUMN_ALIASES = {
    "timestamp": "timestamp",
    "date": "timestamp",
    "time": "timestamp",
    "game": "source_game_id",
    "game_id": "source_game_id",
    "gameid": "source_game_id",
    "source_game": "source_game_id",
    "source_game_id": "source_game_id",
    "game_name": "source_name",
    "source_game_name": "source_name",
    "source_name": "source_name",
    "placement_id": "placement",
    "ad_unit_id": "placement",
    "placement": "placement",
    "country": "country",
    "platform": "platform",
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


def _resolve_group_by_fields(config: Mapping[str, Any]) -> Tuple[List[str], List[str]]:
    group_by = [g for g in _to_list(config.get("group_by"), DEFAULT_GROUP_BY) if g in ALLOWED_GROUP_BY]
    fields = [f for f in _to_list(config.get("fields"), DEFAULT_FIELDS) if f in ALLOWED_FIELDS]
    if not fields:
        fields = ["revenue_sum"]
    return group_by, fields


def _normalize_key(key: Any) -> str:
    k = str(key or "").strip().lower().replace(" ", "_").replace("-", "_")
    return COLUMN_ALIASES.get(k, k)


def _to_number(value: Any, as_int: bool) -> Union[int, float]:
    """CSV tra chuoi; rong -> 0. *_count -> int, *_sum -> float round 6 (BigQuery NUMERIC scale 9)."""
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
    """CSV (mac dinh cua API) hoac JSON (list / {data|rows|results: [...]}) -> list dict, key da chuan hoa."""
    if response.status_code == 204:
        return []
    text = (response.text or "").lstrip("﻿").strip()
    if not text:
        return []
    rows: List[Any] = []
    if text[0] in "[{":
        try:
            data = response.json()
        except ValueError:
            data = None
        if isinstance(data, list):
            rows = data
        elif isinstance(data, dict):
            for key in ("data", "rows", "results"):
                if isinstance(data.get(key), list):
                    rows = data[key]
                    break
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
    return {"Authorization": f"Token {api_key}", "Accept": "text/csv"}


""" Base Stream """


class UnityAdsStream(HttpStream, ABC):
    url_base = URL_BASE
    http_method = "GET"

    def __init__(self, config: Mapping[str, Any], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config

    @property
    def availability_strategy(self) -> Optional["AvailabilityStrategy"]:
        return None

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        return None  # API khong phan trang

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        yield {}


""" Monetization stream (incremental, multi-account) """


class UnityAdsMonetization(UnityAdsStream, IncrementalMixin):
    def __init__(self, accounts: List[Dict], **kwargs):
        super().__init__(**kwargs)
        self._cursor_value = None
        self.number_days_backward = _to_int(self.config.get("number_days_backward"), DEFAULT_LOOKBACK_DAYS)
        self.window_days = min(max(_to_int(self.config.get("window_days"), DEFAULT_WINDOW_DAYS), 1), MAX_WINDOW_DAYS)
        self.get_last_X_days = _to_bool(self.config.get("get_last_X_days"), False)
        self.group_by, self.fields = _resolve_group_by_fields(self.config)
        # accounts: [{account_name, organization_core_id, api_key}]
        # KHONG dua api_key vao stream_slice (slice bi log ra stdout)
        self.accounts = accounts
        self._keys_by_account = {a.get("account_name"): a.get("api_key") for a in accounts}
        self._org_by_account = {a.get("account_name"): str(a.get("organization_core_id") or "").strip() for a in accounts}
        self._raise_on_http_errors = True
        self._current_slice: Mapping[str, Any] = {}
        # Cursor theo TUNG account; state emit = MIN cua cac account (giong pubscale FIX A):
        # acc1 xong cua so, acc2 fail -> state khong vuot qua acc2.
        self._cursor_by_account: Dict[str, Any] = {}

    @property
    def name(self) -> str:
        return "unity_ads_monetization"

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ["account_name", "date"] + [GROUP_BY_PK[g] for g in self.group_by]

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
    def path(self, stream_state=None, stream_slice=None, next_page_token=None) -> str:
        account_name = (stream_slice or {}).get("account_name")
        return PATH_TEMPLATE.format(organization_id=self._org_by_account.get(account_name, ""))

    def request_headers(self, stream_state, stream_slice=None, next_page_token=None) -> Mapping[str, Any]:
        account_name = (stream_slice or {}).get("account_name")
        return _auth_headers(self._keys_by_account.get(account_name, ""))

    def request_params(self, stream_state, stream_slice=None, next_page_token=None) -> MutableMapping[str, Any]:
        s = stream_slice or {}
        params: Dict[str, Any] = {
            "fields": ",".join(self.fields),
            "scale": "day",
            "start": f"{s.get('start_date')}T00:00:00Z",
            "end": f"{s.get('end_date')}T23:59:59Z",
        }
        if self.group_by:
            params["groupBy"] = ",".join(self.group_by)
        return params

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
        organization_id = self._org_by_account.get(account_name)
        start_raw = self._current_slice.get("start_date")
        end_raw = self._current_slice.get("end_date")
        today = pendulum.today(REPORT_TIMEZONE).date()
        dim_columns = [c for g in self.group_by for c in GROUP_BY_COLUMNS[g]]
        rows = _parse_body(response)
        if rows:
            # Thieu cot khoa -> PK = null -> Airbyte dedup gop nham row. Dung han thay vi nap du lieu sai.
            missing = [GROUP_BY_PK[g] for g in self.group_by if GROUP_BY_PK[g] not in rows[0]]
            if missing:
                raise ValueError(
                    f"Response thieu cot khoa {missing}; cot nhan duoc: {sorted(rows[0].keys())}. "
                    f"Bo sung COLUMN_ALIASES trong source.py roi sync lai."
                )
        seen = set()
        for row in rows:
            raw_ts = row.get("timestamp")
            if not raw_ts:
                self.logger.warning(f"Row khong co timestamp, bo qua. Cot nhan duoc: {sorted(row.keys())}")
                continue
            try:
                row_date = pendulum.parse(str(raw_ts)).in_timezone(REPORT_TIMEZONE).date()
            except Exception:
                self.logger.warning(f"Khong parse duoc timestamp '{raw_ts}', bo qua row.")
                continue
            # API khong ghi ro `end` la bao gom hay khong -> chi giu ngay trong cua so da xin, khong vuot today
            if start_raw and row_date < pendulum.parse(start_raw).date():
                continue
            if end_raw and row_date > pendulum.parse(end_raw).date():
                continue
            if row_date > today:
                continue
            rec: Dict[str, Any] = {
                "account_name": account_name,
                "organization_id": organization_id,
                "date": row_date.to_date_string(),
            }
            for c in dim_columns:
                v = row.get(c)
                rec[c] = None if v is None else str(v)
            for f in self.fields:
                rec[f] = _to_number(row.get(f), as_int=f.endswith("_count"))
            # passthrough cot la
            for k, v in row.items():
                if k and k not in rec and k != "timestamp" and not isinstance(v, (dict, list)):
                    rec[k] = v
            pk = tuple(rec.get(c) for c in self.primary_key)
            if pk in seen:
                # Trung khoa trong cung 1 response -> grain gia dinh sai; dedup se lam mat doanh thu.
                raise ValueError(f"Response co 2 dong trung khoa chinh {self.primary_key}: {pk}. Kiem lai group_by / ten cot.")
            seen.add(pk)
            yield rec

    def get_json_schema(self) -> Mapping[str, Any]:
        props: Dict[str, Any] = {
            "account_name": {"type": ["null", "string"]},
            "organization_id": {"type": ["null", "string"]},
            "date": {"type": ["null", "string"], "format": "date"},
        }
        for g in self.group_by:
            for c in GROUP_BY_COLUMNS[g]:
                props[c] = {"type": ["null", "string"]}
        for f in self.fields:
            props[f] = {"type": ["null", "number"]}
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
class SourceUnityAds(AbstractSource):
    @staticmethod
    def _probe(organization_id: str, api_key: str) -> requests.Response:
        """1 request nho (hom qua, khong groupBy) de validate key + organization id. 200/204 = OK."""
        today = pendulum.today(REPORT_TIMEZONE).date()
        yesterday = today.subtract(days=1)
        _RateLimiter.wait(f"Token {api_key}")
        return requests.get(
            URL_BASE + PATH_TEMPLATE.format(organization_id=organization_id),
            headers=_auth_headers(api_key),
            params={
                "fields": "revenue_sum",
                "scale": "day",
                "start": f"{yesterday.to_date_string()}T00:00:00Z",
                "end": f"{yesterday.to_date_string()}T23:59:59Z",
            },
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
                organization_id = str(account["organization_core_id"]).strip()
                resp = self._probe(organization_id, api_key)
                if resp.status_code == 429:
                    time.sleep(10)
                    resp = self._probe(organization_id, api_key)
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
        return [UnityAdsMonetization(config=config, accounts=config.get("accounts", []))]
