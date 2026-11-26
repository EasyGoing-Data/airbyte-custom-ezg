#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
# source-meta-audience-network-custom v1.1.0
# Meta Audience Network Reporting API v2 -> BigQuery (doanh thu quang cao lay truc tiep tu Meta Audience Network).
# Khung theo source-pubscale-custom v1.0.1 / source-applovin-max-custom v1.8.0
# (multi-account, incremental + lookback, cursor theo tung account, khong +1, clamp <= today, guard slice None).
#
# Docs: https://developers.facebook.com/docs/audience-network/optimization/report-api/guide-v2
#
# Khac Pubscale:
#   - GET graph.facebook.com/<version>/<business id>/adnetworkanalytics, token gui bang header `Authorization: Bearer`
#     (khong dua vao URL) va KHONG di theo `paging.next` (URL do chua access_token) -> phan trang bang cursor `after`.
#   - Response dang dai: moi dong la 1 metric cua 1 o (time x breakdowns) -> connector gop lai thanh 1 record / o.
#     Vi 1 o co the nam o nhieu trang, moi slice doc het vao bo nho roi moi phat record -> khong dung HttpStream.
#   - NGAY THEO GIO PACIFIC (PDT / PST), API khong co tham so timezone; so theo gio chi giu 72 gio
#     -> khong doi ve UTC duoc. Record co `report_timezone` + `period_start_utc` de biet ro cua so thoi gian.
#   - O co qua it impression: doanh thu KHONG duoc tra (nam trong `omitted_results`) -> record co `revenue_omitted` = true.
#     Do tren du lieu that 26/11/2026: nguong = 100 impression / o / ngay (Meta khong cong bo); so dem luon du.
#   - v1.1.0: 3 stream CO DINH (khong con tuy chon `breakdowns`), tach cang it chieu thi cang it doanh thu bi an:
#       meta_audience_network_by_platform     platform                      (thay 100% doanh thu -> so chuan)
#       meta_audience_network_by_app          property x platform           (thay ~92%)
#       meta_audience_network_by_app_country  property x platform x country (thay ~44%, impression du 100%)
#     Phan doanh thu bi an = so by_platform - tong phan thay duoc; phan bo o buoc dbt, khong lam trong connector.
#   - Toi da 8 ngay / request dong bo, limit toi da 2000 dong, du lieu giu 540 ngay, 250 query / phut.
#   - Can long-lived USER access token (Facebook Login, quyen read_audience_network_insights);
#     token system user / token tao trong Business Manager bi API tu choi.

import json
import logging
import os
import threading
import time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union

import pendulum
import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import IncrementalMixin, Stream

logger = logging.getLogger("airbyte")

# META_AN_URL_BASE / META_AN_RETRY_BASE_SEC chi dung cho test voi server gia (integration_tests/mock_test.py)
URL_BASE = os.environ.get("META_AN_URL_BASE", "https://graph.facebook.com/")
RETRY_BASE_SEC = float(os.environ.get("META_AN_RETRY_BASE_SEC", "5"))

DEFAULT_API_VERSION = "v27.0"
REPORT_TIMEZONE = "America/Los_Angeles"   # API cat ngay theo gio Pacific
DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_WINDOW_DAYS = 7                   # API: toi da 8 ngay / request dong bo
MAX_WINDOW_DAYS = 7
MAX_HISTORY_DAYS = 539                    # API: du lieu giu 540 ngay
PAGE_LIMIT = 2000                         # API: limit toi da 2000 voi request dong bo
MAX_PAGES = 2000
MIN_REQUEST_INTERVAL_SEC = 0.3            # API: 250 query / phut
REQUEST_TIMEOUT_SEC = 120
MAX_ATTEMPTS = 5
TRANSIENT_ERROR_CODES = {1, 2, 4, 17, 341, 613}   # loi tam thoi / rate limit cua Graph API

METRIC_PREFIX = "fb_ad_network_"
DEFAULT_METRICS = [
    "fb_ad_network_request", "fb_ad_network_filled_request", "fb_ad_network_imp",
    "fb_ad_network_click", "fb_ad_network_revenue",
]
ALLOWED_METRICS = set(DEFAULT_METRICS) | {
    "fb_ad_network_cpm", "fb_ad_network_ctr", "fb_ad_network_fill_rate", "fb_ad_network_show_rate",
    "fb_ad_network_bidding_request", "fb_ad_network_bidding_response",
    "fb_ad_network_bidding_bid_rate", "fb_ad_network_bidding_win_rate",
}
COUNT_METRICS = {"request", "filled_request", "imp", "click", "bidding_request", "bidding_response"}
REVENUE_METRIC = "revenue"

# Stream co dinh: ten -> breakdowns (thu tu = thu tu cot trong khoa chinh). `property` = ID app tren Meta.
STREAM_BREAKDOWNS = [
    ("meta_audience_network_by_platform", ["platform"]),
    ("meta_audience_network_by_app", ["property", "platform"]),
    ("meta_audience_network_by_app_country", ["property", "platform", "country"]),
]


class MetaApiError(Exception):
    """Loi goi API — thong bao KHONG chua URL / access_token."""


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


def _resolve_metrics(config: Mapping[str, Any]) -> List[str]:
    metrics = [m for m in _to_list(config.get("metrics"), DEFAULT_METRICS) if m in ALLOWED_METRICS]
    if not metrics:
        metrics = list(DEFAULT_METRICS)
    if config.get("breakdowns"):
        # Config tao tu v1.0.0 con truong nay: v1.1.0 dung 3 stream co dinh, bo qua.
        logger.warning("`breakdowns` trong config khong con dung tu v1.1.0 (3 stream co dinh) -> bo qua.")
    return metrics


def _metric_column(metric: str) -> str:
    return metric[len(METRIC_PREFIX):] if metric.startswith(METRIC_PREFIX) else metric


def _to_number(value: Any, as_int: bool) -> Optional[Union[int, float]]:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (ValueError, TypeError):
        return None
    if as_int and number == int(number):
        return int(number)
    return round(number, 6)


class _RateLimiter:
    """250 query / phut -> gian 0.3 giay / token. Dung chung cho check_connection + stream, thread-safe."""
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


def _describe_graph_error(status: int, body: Any) -> Tuple[str, Optional[int]]:
    """Rut gon loi Graph API. Tra (thong bao, error code)."""
    err = body.get("error") if isinstance(body, dict) else None
    if not isinstance(err, dict):
        return f"HTTP {status}", None
    code = err.get("code")
    message = str(err.get("message") or "")[:300]
    text = f"HTTP {status} code={code} subcode={err.get('error_subcode')} type={err.get('type')}: {message}"
    if code == 190:
        text += " | Goi y: access token het han hoac khong hop le -> tao lai long-lived user token."
    if "Facebook Login" in message:
        text += " | Goi y: API khong nhan token system user / Business Manager; can user token qua Facebook Login."
    try:
        return text, int(code) if code is not None else None
    except (ValueError, TypeError):
        return text, None


def call_graph_api(business_id: str, access_token: str, api_version: str, params: Mapping[str, Any]) -> Dict[str, Any]:
    """
    GET /<version>/<business id>/adnetworkanalytics. Token gui bang header (khong nam trong URL).
    Retry: loi mang, HTTP 429 / 5xx, error code tam thoi cua Graph. Loi khac -> MetaApiError, khong retry.
    """
    url = f"{URL_BASE.rstrip('/')}/{api_version}/{str(business_id).strip()}/adnetworkanalytics"
    headers = {"Authorization": f"Bearer {access_token}", "Accept": "application/json"}
    last_error = "unknown"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        _RateLimiter.wait(str(business_id))
        retriable = False
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=REQUEST_TIMEOUT_SEC)
        except requests.RequestException as exc:
            last_error = f"network error {type(exc).__name__}"  # khong dua str(exc) vi co URL
            retriable = True
        else:
            try:
                body = resp.json()
            except ValueError:
                body = None
            if resp.status_code == 200 and isinstance(body, dict) and "error" not in body:
                return body
            text, code = _describe_graph_error(resp.status_code, body)
            if resp.status_code == 429 or resp.status_code >= 500 or code in TRANSIENT_ERROR_CODES:
                last_error = text
                retriable = True
            else:
                raise MetaApiError(text)
        if retriable and attempt < MAX_ATTEMPTS:
            wait = RETRY_BASE_SEC * (2 ** (attempt - 1))
            logger.info(f"Meta API {last_error[:120]} — thu lai lan {attempt + 1}/{MAX_ATTEMPTS} sau {wait:.0f}s")
            time.sleep(wait)
    raise MetaApiError(f"Het {MAX_ATTEMPTS} lan thu: {last_error}")


""" Report stream (incremental, multi-account) """


class MetaAudienceNetworkStream(Stream, IncrementalMixin):
    def __init__(self, config: Mapping[str, Any], accounts: List[Dict], stream_name: str, breakdowns: List[str],
                 metrics: List[str], **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self._name = stream_name
        self.breakdowns = list(breakdowns)
        self.metrics = list(metrics)
        self._cursor_value = None
        self.api_version = str(self.config.get("graph_api_version") or DEFAULT_API_VERSION).strip()
        self.number_days_backward = _to_int(self.config.get("number_days_backward"), DEFAULT_LOOKBACK_DAYS)
        self.window_days = min(max(_to_int(self.config.get("window_days"), DEFAULT_WINDOW_DAYS), 1), MAX_WINDOW_DAYS)
        self.get_last_X_days = _to_bool(self.config.get("get_last_X_days"), False)
        # accounts: [{account_name, business_id, access_token}]
        # KHONG dua access_token vao stream_slice (slice bi log ra stdout)
        self.accounts = accounts
        self._creds_by_account = {a.get("account_name"): (a.get("business_id"), a.get("access_token")) for a in accounts}
        # Cursor theo TUNG account; state emit = MIN cua cac account (giong pubscale FIX A)
        self._cursor_by_account: Dict[str, Any] = {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ["account_name", "date"] + list(self.breakdowns)

    @property
    def cursor_field(self) -> Union[str, List[str]]:
        return "date"

    @property
    def source_defined_cursor(self) -> bool:
        return True

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

    # ---- slices: account x cua so ngay (<= 7 ngay Pacific, khong lui qua 539 ngay) ----
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
            self.logger.warning(f"start {start_date} som hon gioi han 540 ngay cua API -> bat dau tu {earliest}.")
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

    # ---- read ----
    def _read_slice(self, account_name: str, start_raw: str, end_raw: str) -> List[Dict[str, Any]]:
        business_id, access_token = self._creds_by_account.get(account_name, (None, None))
        start_date = pendulum.parse(start_raw).date()
        end_date = pendulum.parse(end_raw).date()
        today = pendulum.today(REPORT_TIMEZONE).date()
        base_params: Dict[str, Any] = {
            "metrics": json.dumps(self.metrics),
            "since": start_date.to_date_string(),
            "until": end_date.add(days=1).to_date_string(),   # `until` khong bao gom voi aggregation theo ngay
            "aggregation_period": "day",
            "ordering_column": "time",
            "ordering_type": "ascending",
            "limit": PAGE_LIMIT,
        }
        if self.breakdowns:
            base_params["breakdowns"] = json.dumps(self.breakdowns)

        # (time, gia tri breakdown) -> record; 1 o co the nam rai rac o nhieu trang
        cells: Dict[Tuple, Dict[str, Any]] = {}
        after: Optional[str] = None
        for _ in range(MAX_PAGES):
            params = dict(base_params)
            if after:
                params["after"] = after
            body = call_graph_api(business_id, access_token, self.api_version, params)
            page_rows = 0
            for block in body.get("data") or []:
                if not isinstance(block, dict):
                    continue
                for kind in ("results", "omitted_results"):
                    for item in block.get(kind) or []:
                        if not isinstance(item, dict):
                            continue
                        page_rows += 1
                        bd = {b.get("key"): b.get("value") for b in (item.get("breakdowns") or []) if isinstance(b, dict)}
                        key = (item.get("time"),) + tuple(bd.get(b) for b in self.breakdowns)
                        cell = cells.setdefault(key, {"_time": item.get("time"), "_bd": bd, "_omitted": set(), "_values": {}})
                        column = _metric_column(str(item.get("metric")))
                        if column in cell["_values"] or column in cell["_omitted"]:
                            # Trung (time, breakdowns, metric): thieu breakdown hoac phan trang lap -> gop se sai so.
                            raise MetaApiError(f"trung o (time, breakdowns, metric): {key} / {column}")
                        if kind == "results":
                            cell["_values"][column] = item.get("value")
                        else:
                            cell["_omitted"].add(column)
            paging = body.get("paging") or {}
            after = (paging.get("cursors") or {}).get("after")
            if not paging.get("next") or not after or page_rows == 0:
                break
        else:
            raise MetaApiError(f"qua {MAX_PAGES} trang, dung de tranh vong lap vo han")

        recs: List[Dict[str, Any]] = []
        for key, cell in cells.items():
            try:
                period_start = pendulum.parse(str(cell["_time"]))
            except Exception:
                raise MetaApiError(f"khong parse duoc time '{cell['_time']}'")
            row_date = period_start.in_timezone(REPORT_TIMEZONE).date()
            if row_date < start_date or row_date > end_date or row_date > today:
                continue  # chi giu ngay trong cua so da xin
            rec: Dict[str, Any] = {
                "account_name": account_name,
                "business_id": str(business_id).strip(),
                "date": row_date.to_date_string(),
                "report_timezone": REPORT_TIMEZONE,
                "period_start_utc": period_start.in_timezone("UTC").to_iso8601_string(),
            }
            for b in self.breakdowns:
                v = cell["_bd"].get(b)
                rec[b] = None if v is None else str(v)
            for metric in self.metrics:
                column = _metric_column(metric)
                if column in cell["_values"]:
                    rec[column] = _to_number(cell["_values"][column], as_int=column in COUNT_METRICS)
                elif column in cell["_omitted"]:
                    rec[column] = None
                else:
                    rec[column] = 0 if column in COUNT_METRICS else None   # API khong tra dong cho metric nay
            rec["revenue_omitted"] = REVENUE_METRIC in cell["_omitted"]
            recs.append(rec)
        pks = {tuple(r.get(c) for c in self.primary_key) for r in recs}
        if len(pks) != len(recs):
            raise MetaApiError(f"{len(recs) - len(pks)} dong trung khoa chinh {self.primary_key}")
        return recs

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None) -> Iterable[Mapping[str, Any]]:
        s = stream_slice or {}
        account_name = s.get("account_name")
        start_raw, end_raw = s.get("start_date"), s.get("end_date")
        if not account_name or not start_raw or not end_raw:
            return
        self.logger.info(f"stream slice {stream_slice}")
        try:
            recs = self._read_slice(account_name, start_raw, end_raw)
        except MetaApiError as exc:
            raise MetaApiError(f"[{self.name}] Account '{account_name}' {start_raw}..{end_raw}: {exc}") from None
        for rec in recs:
            record_cursor_value = pendulum.parse(rec["date"]).date()
            prev = self._cursor_by_account.get(account_name)
            self._cursor_by_account[account_name] = max(prev, record_cursor_value) if prev else record_cursor_value
            yield rec
        # Slice doc xong (moi trang OK) -> account nay coi nhu da phu den end_date, ke ca khi slice rong.
        end_date = pendulum.parse(str(end_raw)).date()
        prev = self._cursor_by_account.get(account_name)
        self._cursor_by_account[account_name] = max(prev, end_date) if prev else end_date

    def get_json_schema(self) -> Mapping[str, Any]:
        props: Dict[str, Any] = {
            "account_name": {"type": ["null", "string"]},
            "business_id": {"type": ["null", "string"]},
            "date": {"type": ["null", "string"], "format": "date"},
            "report_timezone": {"type": ["null", "string"]},
            "period_start_utc": {"type": ["null", "string"]},
            "revenue_omitted": {"type": ["null", "boolean"]},
        }
        for b in self.breakdowns:
            props[b] = {"type": ["null", "string"]}
        for metric in self.metrics:
            props[_metric_column(metric)] = {"type": ["null", "number"]}
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "additionalProperties": True,
            "properties": props,
        }


# Source
class SourceMetaAudienceNetwork(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        accounts = config.get("accounts", [])
        if not accounts:
            return False, "No accounts configured. Add at least one account."
        api_version = str(config.get("graph_api_version") or DEFAULT_API_VERSION).strip()
        today = pendulum.today(REPORT_TIMEZONE).date()
        for account in accounts:
            account_name = account.get("account_name", "?")
            try:
                # 1 request nho (hom qua, 1 metric, khong breakdown) de validate token + business id
                call_graph_api(account["business_id"], account["access_token"], api_version, {
                    "metrics": json.dumps(["fb_ad_network_imp"]),
                    "since": today.subtract(days=1).to_date_string(),
                    "until": today.to_date_string(),
                    "aggregation_period": "day",
                    "limit": 1,
                })
                logger.info(f"Account '{account_name}' OK")
            except KeyError as exc:
                return False, f"Account '{account_name}' is missing required field: {exc}"
            except MetaApiError as exc:
                return False, f"Account '{account_name}' failed: {exc}"
            except Exception as exc:
                return False, f"Account '{account_name}' failed: {type(exc).__name__}"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        accounts = config.get("accounts", [])
        metrics = _resolve_metrics(config)
        return [
            MetaAudienceNetworkStream(config=config, accounts=accounts, stream_name=stream_name,
                                      breakdowns=breakdowns, metrics=metrics)
            for stream_name, breakdowns in STREAM_BREAKDOWNS
        ]
