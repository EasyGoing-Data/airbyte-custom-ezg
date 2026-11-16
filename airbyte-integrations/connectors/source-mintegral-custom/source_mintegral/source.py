#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
# source-mintegral-custom v1.0.1
# Mintegral Reporting API (publisher) -> BigQuery (doanh thu quang cao lay truc tiep tu Mintegral).
# Khung theo source-pubscale-custom v1.0.1 / source-applovin-max-custom v1.8.0
# (multi-account, incremental + lookback, cursor theo tung account, khong +1, clamp <= today, guard slice None).
#
# Docs: https://cdn-adn-https.rayjump.com/cdn-adn/reporting_api/MintegralRA.html?v=3.0
#
# Khac Pubscale:
#   - GET, xac thuc bang query param: skey + time + sign = md5(SECRET + md5(time)), time hop le +-300 giay
#     -> skey / sign nam trong URL, nen KHONG dung HttpStream (loi cua HttpStream in ca URL ra log);
#        connector tu goi requests + retry va chi bao loi da bo URL.
#   - Toi da 7 ngay / request, chi lui duoc 60 ngay
#   - `timezone` mac dinh cua API la +8 -> connector luon gui timezone=0 (UTC)
#   - Phan trang page / limit (toi da 10000), response {code, data: {total_page, lists: [...]}}
#   - `date` tra ve dang so 20190506 -> doi thanh 2019-05-06

import hashlib
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

# MINTEGRAL_URL / MINTEGRAL_RETRY_BASE_SEC chi dung cho test voi server gia (integration_tests/mock_test.py)
REPORT_URL = os.environ.get("MINTEGRAL_URL", "https://api.mintegral.com/reporting/v2/data")
RETRY_BASE_SEC = float(os.environ.get("MINTEGRAL_RETRY_BASE_SEC", "5"))

REPORT_TIMEZONE = "UTC"
API_TIMEZONE_OFFSET = 0           # tham so `timezone` cua API: 0 = GMT+00:00
DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_WINDOW_DAYS = 7           # API: toi da 7 ngay / request
MAX_WINDOW_DAYS = 7
MAX_HISTORY_DAYS = 60             # API: chi lay duoc du lieu trong 60 ngay gan nhat
DEFAULT_PAGE_SIZE = 10000
MAX_PAGE_SIZE = 10000
MAX_PAGES = 1000                  # chan vong lap vo han neu API tra total_page sai
MIN_REQUEST_INTERVAL_SEC = 1.1    # tai lieu khong ghi rate limit -> gian 1 request / giay / skey
REQUEST_TIMEOUT_SEC = 120
MAX_ATTEMPTS = 5
SLICE_READ_ATTEMPTS = 3           # so lan doc lai 1 slice khi phan trang khong nhat quan (lech total / trung PK)

DEFAULT_GROUP_BY = ["date", "app_id", "placement_id", "unit_id", "country", "platform"]
ALLOWED_GROUP_BY = {"date", "app_id", "placement_id", "unit_id", "country", "platform", "bidding_type"}

# group_by -> cot dimension API tra kem
GROUP_BY_COLUMNS = {
    "date": ["date"],
    "app_id": ["app_id", "app_name", "app_package"],
    "placement_id": ["placement_id", "placement_name"],
    "unit_id": ["unit_id", "unit_name", "ad_format"],
    "country": ["country"],
    "platform": ["platform"],
    "bidding_type": ["bidding_type"],
}
INT_METRICS = ["filled", "request", "impression", "click", "hb_load", "hb_load_filled"]
FLOAT_METRICS = ["est_revenue", "ecpm", "fill_rate", "ctr"]
# API chi tra 2 metric nay khi group_by co `bidding_type` -> chi khai trong schema khi do
# (16/11/2026: grain mac dinh khong co, khai san thi bang dich co 2 cot toan null).
BIDDING_ONLY_METRICS = {"hb_load", "hb_load_filled"}


class MintegralApiError(Exception):
    """Loi goi API — thong bao KHONG chua URL / skey / sign."""


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


def _resolve_group_by(config: Mapping[str, Any]) -> List[str]:
    group_by = [g for g in _to_list(config.get("group_by"), DEFAULT_GROUP_BY) if g in ALLOWED_GROUP_BY]
    if "date" in group_by:
        group_by.remove("date")
    return ["date"] + group_by  # cursor bat buoc, luon dung dau


def _sign(secret: str, timestamp: int) -> str:
    """sign = md5(SECRET + md5(time))"""
    inner = hashlib.md5(str(timestamp).encode("utf-8")).hexdigest()
    return hashlib.md5((str(secret) + inner).encode("utf-8")).hexdigest()


def _format_date(value: Any) -> Optional[str]:
    """20190506 (int / str) hoac 2019-05-06 -> '2019-05-06'."""
    if value is None or value == "":
        return None
    s = str(value).strip()
    if len(s) == 8 and s.isdigit():
        return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
    try:
        return pendulum.parse(s).date().to_date_string()
    except Exception:
        return None


class _RateLimiter:
    """1 request / giay / skey. Dung chung cho check_connection + stream, thread-safe."""
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


def call_report_api(skey: str, secret: str, params: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Goi Reporting API, tra ve object `data`. Ky lai (time + sign) o MOI lan thu vi `time` chi hop le +-300 giay.
    Retry: loi mang, HTTP 429, HTTP 5xx. Loi khac (HTTP 4xx, code != ok) -> MintegralApiError, khong retry.
    """
    last_error = "unknown"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        _RateLimiter.wait(str(skey))
        timestamp = int(time.time())
        query = dict(params)
        query.update({"skey": skey, "time": timestamp, "sign": _sign(secret, timestamp)})
        retriable = False
        try:
            resp = requests.get(REPORT_URL, params=query, timeout=REQUEST_TIMEOUT_SEC)
        except requests.RequestException as exc:
            last_error = f"network error {type(exc).__name__}"  # khong dua str(exc) vi co URL
            retriable = True
        else:
            if resp.status_code == 429 or resp.status_code >= 500:
                last_error = f"HTTP {resp.status_code}"
                retriable = True
            elif resp.status_code != 200:
                raise MintegralApiError(f"HTTP {resp.status_code} {(resp.text or '')[:300]}")
            else:
                try:
                    body = resp.json()
                except ValueError:
                    raise MintegralApiError(f"Response khong phai JSON: {(resp.text or '')[:200]}")
                if not isinstance(body, dict):
                    raise MintegralApiError("Response JSON khong phai object")
                code = str(body.get("code", "")).strip().lower()
                if code != "ok":
                    msg = body.get("msg") or body.get("message") or body.get("data")
                    raise MintegralApiError(f"API code={body.get('code')} msg={str(msg)[:300]}")
                data = body.get("data")
                return data if isinstance(data, dict) else {"lists": data if isinstance(data, list) else []}
        if retriable and attempt < MAX_ATTEMPTS:
            wait = RETRY_BASE_SEC * (2 ** (attempt - 1))
            logger.info(f"Mintegral API {last_error} — thu lai lan {attempt + 1}/{MAX_ATTEMPTS} sau {wait:.0f}s")
            time.sleep(wait)
    raise MintegralApiError(f"Het {MAX_ATTEMPTS} lan thu: {last_error}")


""" Report stream (incremental, multi-account) """


class MintegralReport(Stream, IncrementalMixin):
    def __init__(self, config: Mapping[str, Any], accounts: List[Dict], **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self._cursor_value = None
        self.number_days_backward = _to_int(self.config.get("number_days_backward"), DEFAULT_LOOKBACK_DAYS)
        self.window_days = min(max(_to_int(self.config.get("window_days"), DEFAULT_WINDOW_DAYS), 1), MAX_WINDOW_DAYS)
        self.page_size = min(max(_to_int(self.config.get("page_size"), DEFAULT_PAGE_SIZE), 1), MAX_PAGE_SIZE)
        self.get_last_X_days = _to_bool(self.config.get("get_last_X_days"), False)
        self.group_by = _resolve_group_by(self.config)
        # accounts: [{account_name, skey, secret}] — KHONG dua skey / secret vao stream_slice (slice bi log ra stdout)
        self.accounts = accounts
        self._creds_by_account = {a.get("account_name"): (a.get("skey"), a.get("secret")) for a in accounts}
        # Cursor theo TUNG account; state emit = MIN cua cac account (giong pubscale FIX A)
        self._cursor_by_account: Dict[str, Any] = {}

    @property
    def name(self) -> str:
        return "mintegral_report"

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ["account_name"] + list(self.group_by)

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

    # ---- slices: account x cua so ngay (<= 7 ngay, khong lui qua 60 ngay) ----
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
                f"start {start_date} som hon gioi han {MAX_HISTORY_DAYS} ngay cua API -> bat dau tu {earliest}. "
                f"Du lieu truoc {earliest} KHONG lay duoc qua API."
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

    # ---- read ----
    def _build_record(self, row: Mapping[str, Any], account_name: str) -> Optional[Dict[str, Any]]:
        row_date = _format_date(row.get("date"))
        if not row_date:
            self.logger.warning(f"Row khong co date hop le, bo qua. Cot nhan duoc: {sorted(row.keys())}")
            return None
        rec: Dict[str, Any] = {"account_name": account_name, "date": row_date}
        for g in self.group_by:
            for c in GROUP_BY_COLUMNS[g]:
                if c == "date":
                    continue
                v = row.get(c)
                rec[c] = None if v is None else str(v)
        for m in INT_METRICS:
            if m in row:
                try:
                    rec[m] = int(float(row[m])) if row[m] not in (None, "") else 0
                except (ValueError, TypeError):
                    rec[m] = 0
        for m in FLOAT_METRICS:
            if m in row:
                try:
                    rec[m] = round(float(row[m]), 6) if row[m] not in (None, "") else 0
                except (ValueError, TypeError):
                    rec[m] = 0
        # passthrough cot la
        for k, v in row.items():
            if k not in rec and not isinstance(v, (dict, list)):
                rec[k] = v
        return rec

    def _read_slice(self, skey: str, secret: str, base_params: Mapping[str, Any], account_name: str) -> Tuple[List[Dict[str, Any]], str]:
        """Doc moi trang cua 1 slice. Tra (records, problem); problem != '' nghia la phan trang khong nhat quan."""
        recs: List[Dict[str, Any]] = []
        raw_count = 0
        total: Optional[int] = None
        page = 1
        while page <= MAX_PAGES:
            params = dict(base_params)
            params["page"] = page
            data = call_report_api(skey, secret, params)
            rows = [r for r in (data.get("lists") or []) if isinstance(r, dict)]
            if page == 1:
                if data.get("total") not in (None, ""):
                    total = _to_int(data.get("total"), -1)
                if rows:
                    # Thieu cot khoa -> PK = null -> Airbyte dedup gop nham row. Dung han thay vi nap du lieu sai.
                    missing = [g for g in self.group_by if g not in rows[0]]
                    if missing:
                        raise MintegralApiError(f"Response thieu cot khoa {missing}; cot nhan duoc: {sorted(rows[0].keys())}")
            raw_count += len(rows)
            for row in rows:
                rec = self._build_record(row, account_name)
                if rec is not None:
                    recs.append(rec)
            total_page = _to_int(data.get("total_page"), 0)
            if total_page:
                if page >= total_page:
                    break
            elif len(rows) < self.page_size:  # API khong tra total_page -> dung khi trang chua day
                break
            page += 1
        if total is not None and total >= 0 and raw_count != total:
            return recs, f"doc duoc {raw_count} dong, API bao total={total}"
        keys = {tuple(rec.get(k) for k in self.primary_key) for rec in recs}
        if len(keys) != len(recs):
            return recs, f"{len(recs) - len(keys)} dong trung khoa chinh"
        return recs, ""

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None) -> Iterable[Mapping[str, Any]]:
        s = stream_slice or {}
        account_name = s.get("account_name")
        start_raw, end_raw = s.get("start_date"), s.get("end_date")
        if not account_name or not start_raw or not end_raw:
            return
        self.logger.info(f"stream slice {stream_slice}")
        skey, secret = self._creds_by_account.get(account_name, (None, None))
        base_params = {
            "start": start_raw.replace("-", ""),
            "end": end_raw.replace("-", ""),
            "timezone": API_TIMEZONE_OFFSET,
            "group_by": ",".join(self.group_by),
            "limit": self.page_size,
        }
        # Doc het cac trang cua slice vao bo nho roi moi phat record: API khong co order_by nen phan trang
        # co the trung / thieu dong neu du lieu doi giua chung -> doi chieu voi `total` + PK unique, lech thi doc lai.
        slice_label = f"Account '{account_name}' {start_raw}..{end_raw}"
        recs: List[Dict[str, Any]] = []
        for attempt in range(1, SLICE_READ_ATTEMPTS + 1):
            try:
                recs, problem = self._read_slice(skey, secret, base_params, account_name)
            except MintegralApiError as exc:
                raise MintegralApiError(f"{slice_label}: {exc}") from None
            if not problem:
                break
            if attempt == SLICE_READ_ATTEMPTS:
                raise MintegralApiError(f"{slice_label}: phan trang khong nhat quan sau {attempt} lan doc ({problem})")
            self.logger.warning(f"{slice_label}: {problem} — doc lai slice (lan {attempt + 1}/{SLICE_READ_ATTEMPTS})")
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
            "date": {"type": ["null", "string"], "format": "date"},
        }
        for g in self.group_by:
            for c in GROUP_BY_COLUMNS[g]:
                if c != "date":
                    props[c] = {"type": ["null", "string"]}
        for m in INT_METRICS + FLOAT_METRICS:
            if m in BIDDING_ONLY_METRICS and "bidding_type" not in self.group_by:
                continue
            props[m] = {"type": ["null", "number"]}
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "additionalProperties": True,
            "properties": props,
        }


# Source
class SourceMintegral(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        accounts = config.get("accounts", [])
        if not accounts:
            return False, "No accounts configured. Add at least one account."
        yesterday = pendulum.today(REPORT_TIMEZONE).date().subtract(days=1).format("YYYYMMDD")
        for account in accounts:
            account_name = account.get("account_name", "?")
            try:
                skey, secret = account["skey"], account["secret"]
                # 1 request nho (hom qua, group_by=date, 1 row) de validate skey + secret
                call_report_api(skey, secret, {
                    "start": yesterday, "end": yesterday, "timezone": API_TIMEZONE_OFFSET,
                    "group_by": "date", "page": 1, "limit": 1,
                })
                logger.info(f"Account '{account_name}' OK")
            except KeyError as exc:
                return False, f"Account '{account_name}' is missing required field: {exc}"
            except MintegralApiError as exc:
                return False, f"Account '{account_name}' failed: {exc}"
            except Exception as exc:
                return False, f"Account '{account_name}' failed: {type(exc).__name__}"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [MintegralReport(config=config, accounts=config.get("accounts", []))]
