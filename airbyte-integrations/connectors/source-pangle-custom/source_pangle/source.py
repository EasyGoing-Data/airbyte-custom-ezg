#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#
# source-pangle-custom v1.0.0
# Pangle Reporting API 2.0 -> BigQuery (doanh thu quang cao lay truc tiep tu Pangle).
# Khung theo source-pubscale-custom v1.0.1 / source-applovin-max-custom v1.8.0
# (multi-account, incremental + lookback, cursor theo tung account, khong +1, clamp <= today, guard slice None).
#
# Docs: https://www.pangleglobal.com/integration/reporting-api-v2
#
# Khac Pubscale:
#   - GET, xac thuc bang chu ky: sign = md5("k1=v1&k2=v2..." (khoa xep alphabet, bo gia tri rong) + security_key)
#     -> user_id / role_id / sign nam trong URL, nen KHONG dung HttpStream (loi cua HttpStream in ca URL ra log);
#        connector tu goi requests + retry va chi bao loi da bo URL. security_key khong bao gio duoc gui di.
#   - Moi request chi lay 1 ngay (`date`); `timestamp` lech qua 10 phut bi tu choi -> ky lai moi lan thu
#   - Mac dinh cua API la UTC+8 va CNY -> connector luon gui time_zone=0 (UTC), currency mac dinh usd
#   - Gioi han 2 request / giay; 1 bao cao toi da 100.000 dong (qua thi bi cat)
#   - Response {"Code": "100", "Message": "", "Data": {"<date>": [ ... ]}}

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

# PANGLE_URL / PANGLE_RETRY_BASE_SEC chi dung cho test voi server gia (integration_tests/mock_test.py)
REPORT_URL = os.environ.get("PANGLE_URL", "https://open-api.pangleglobal.com/union_pangle/open/api/rt/income")
RETRY_BASE_SEC = float(os.environ.get("PANGLE_RETRY_BASE_SEC", "5"))

REPORT_TIMEZONE = "UTC"
API_TIME_ZONE = 0                 # tham so `time_zone`: 0 = UTC+0 (mac dinh cua API la 8)
API_VERSION = "2.0"
SIGN_TYPE = "MD5"
DEFAULT_CURRENCY = "usd"
ALLOWED_CURRENCIES = {"usd", "cny"}
DEFAULT_LOOKBACK_DAYS = 14
DEFAULT_WINDOW_DAYS = 7           # so ngay / 1 slice (moi ngay 1 request); state luu sau moi slice
MAX_WINDOW_DAYS = 31
MIN_REQUEST_INTERVAL_SEC = 0.6    # API: toi da 2 request / giay
REQUEST_TIMEOUT_SEC = 120
MAX_ATTEMPTS = 5
MAX_REPORT_ROWS = 100000          # API cat bao cao o 100.000 dong

CODE_SUCCESS = "100"
CODE_NO_DATA = "PD0004"
CODE_QPS_LIMIT = "106"

KEY_COLUMNS = ["date", "app_id", "ad_slot_id", "region"]   # grain mac dinh cua API: ngay x ad placement x quoc gia
STRING_COLUMNS = ["region", "app_id", "app_name", "package_name", "ad_slot_id", "code_name", "media_name", "os", "currency"]
INT_COLUMNS = ["ad_slot_type", "time_zone", "request", "return", "show", "click", "use_mediation", "bidding_type",
               "ad_request", "response", "app_code_type"]
FLOAT_COLUMNS = ["fill_rate", "click_rate", "revenue", "ecpm", "ad_fill_rate", "ad_impression_rate"]


class PangleApiError(Exception):
    """Loi goi API — thong bao KHONG chua URL / sign / security_key."""


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


def _sign(params: Mapping[str, Any], security_key: str) -> str:
    """sign = md5("k1=v1&k2=v2..." + security_key); khoa xep alphabet, bo qua gia tri rong (theo code mau cua Pangle)."""
    parts = [f"{k}={v}" for k, v in sorted(params.items(), key=lambda kv: kv[0]) if v != ""]
    return hashlib.md5(("&".join(parts) + str(security_key)).encode("utf-8")).hexdigest()


class _RateLimiter:
    """Toi da 2 request / giay / user_id. Dung chung cho check_connection + stream, thread-safe."""
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


def call_income_api(user_id: str, role_id: str, security_key: str, report_date: str, currency: str) -> List[Dict[str, Any]]:
    """
    Goi rt/income cho 1 ngay, tra ve danh sach dong cua ngay do ([] neu khong co du lieu).
    Ky lai (timestamp + sign) o MOI lan thu. Retry: loi mang, HTTP 429 / 5xx, Code 106 (qua 2 QPS).
    Loi khac (Code != 100 / PD0004) -> PangleApiError, khong retry.
    """
    last_error = "unknown"
    for attempt in range(1, MAX_ATTEMPTS + 1):
        _RateLimiter.wait(str(user_id))
        params: Dict[str, Any] = {
            "user_id": str(user_id).strip(),
            "role_id": str(role_id).strip(),
            "version": API_VERSION,
            "sign_type": SIGN_TYPE,
            "timestamp": int(time.time()),
            "date": report_date,
            "time_zone": API_TIME_ZONE,
            "currency": currency,
        }
        query = dict(params)
        query["sign"] = _sign(params, security_key)
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
                raise PangleApiError(f"HTTP {resp.status_code} {(resp.text or '')[:300]}")
            else:
                try:
                    body = resp.json()
                except ValueError:
                    raise PangleApiError(f"Response khong phai JSON: {(resp.text or '')[:200]}")
                if not isinstance(body, dict):
                    raise PangleApiError("Response JSON khong phai object")
                code = str(body.get("Code", body.get("code", ""))).strip()
                message = str(body.get("Message", body.get("message", "")) or "")[:300]
                if code == CODE_NO_DATA:
                    return []
                if code == CODE_QPS_LIMIT:
                    last_error = "Code 106 (qua gioi han 2 QPS)"
                    retriable = True
                elif code != CODE_SUCCESS:
                    raise PangleApiError(f"API Code={code} Message={message}")
                else:
                    data = body.get("Data", body.get("data"))
                    if not data:
                        return []
                    if not isinstance(data, dict):
                        raise PangleApiError("Truong Data khong phai object {date: [...]}")
                    rows: List[Dict[str, Any]] = []
                    for day, items in data.items():
                        for item in items or []:
                            if isinstance(item, dict):
                                row = dict(item)
                                row.setdefault("date", day)
                                rows.append(row)
                    return rows
        if retriable and attempt < MAX_ATTEMPTS:
            wait = RETRY_BASE_SEC * (2 ** (attempt - 1))
            logger.info(f"Pangle API {last_error} — thu lai lan {attempt + 1}/{MAX_ATTEMPTS} sau {wait:.0f}s")
            time.sleep(wait)
    raise PangleApiError(f"Het {MAX_ATTEMPTS} lan thu: {last_error}")


""" Report stream (incremental, multi-account) """


class PangleReport(Stream, IncrementalMixin):
    def __init__(self, config: Mapping[str, Any], accounts: List[Dict], **kwargs):
        super().__init__(**kwargs)
        self.config = config
        self._cursor_value = None
        self.number_days_backward = _to_int(self.config.get("number_days_backward"), DEFAULT_LOOKBACK_DAYS)
        self.window_days = min(max(_to_int(self.config.get("window_days"), DEFAULT_WINDOW_DAYS), 1), MAX_WINDOW_DAYS)
        self.get_last_X_days = _to_bool(self.config.get("get_last_X_days"), False)
        currency = str(self.config.get("currency") or DEFAULT_CURRENCY).strip().lower()
        self.currency = currency if currency in ALLOWED_CURRENCIES else DEFAULT_CURRENCY
        # accounts: [{account_name, user_id, role_id, security_key}]
        # KHONG dua security_key vao stream_slice (slice bi log ra stdout)
        self.accounts = accounts
        self._creds_by_account = {
            a.get("account_name"): (a.get("user_id"), a.get("role_id"), a.get("security_key")) for a in accounts
        }
        # Cursor theo TUNG account; state emit = MIN cua cac account (giong pubscale FIX A)
        self._cursor_by_account: Dict[str, Any] = {}

    @property
    def name(self) -> str:
        return "pangle_report"

    @property
    def primary_key(self) -> Optional[Union[str, List[str], List[List[str]]]]:
        return ["account_name"] + KEY_COLUMNS

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

    # ---- slices: account x cua so ngay (moi ngay trong cua so la 1 request) ----
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

    # ---- read ----
    def _build_record(self, row: Mapping[str, Any], account_name: str, report_date: str) -> Dict[str, Any]:
        rec: Dict[str, Any] = {"account_name": account_name, "date": report_date}
        for c in STRING_COLUMNS:
            if c in row:
                rec[c] = None if row[c] is None else str(row[c])
        for c in INT_COLUMNS:
            if c in row:
                try:
                    rec[c] = int(float(row[c])) if row[c] not in (None, "") else 0
                except (ValueError, TypeError):
                    rec[c] = 0
        for c in FLOAT_COLUMNS:
            if c in row:
                try:
                    rec[c] = round(float(row[c]), 6) if row[c] not in (None, "") else 0
                except (ValueError, TypeError):
                    rec[c] = 0
        # passthrough cot la
        for k, v in row.items():
            if k not in rec and not isinstance(v, (dict, list)):
                rec[k] = v
        return rec

    def _read_day(self, account_name: str, report_date: str) -> List[Dict[str, Any]]:
        user_id, role_id, security_key = self._creds_by_account.get(account_name, (None, None, None))
        rows = call_income_api(user_id, role_id, security_key, report_date, self.currency)
        if not rows:
            return []
        if len(rows) >= MAX_REPORT_ROWS:
            raise PangleApiError(
                f"bao cao co {len(rows)} dong, cham gioi han {MAX_REPORT_ROWS} cua API -> co the da bi cat, dung de khong nap thieu"
            )
        # Thieu cot khoa -> PK = null -> Airbyte dedup gop nham row. Dung han thay vi nap du lieu sai.
        missing = [c for c in KEY_COLUMNS if c != "date" and c not in rows[0]]
        if missing:
            raise PangleApiError(f"Response thieu cot khoa {missing}; cot nhan duoc: {sorted(rows[0].keys())}")
        recs = []
        seen = set()
        for row in rows:
            row_date = str(row.get("date") or report_date)[:10]
            if row_date != report_date:
                continue  # chi giu dung ngay da xin
            rec = self._build_record(row, account_name, report_date)
            pk = tuple(rec.get(c) for c in KEY_COLUMNS)
            if pk in seen:
                # Trung khoa trong cung 1 ngay -> grain gia dinh sai; dedup se lam mat doanh thu.
                raise PangleApiError(f"2 dong trung khoa chinh {KEY_COLUMNS}: {pk}. Can bo sung cot vao khoa chinh.")
            seen.add(pk)
            recs.append(rec)
        return recs

    def read_records(self, sync_mode, cursor_field=None, stream_slice=None, stream_state=None) -> Iterable[Mapping[str, Any]]:
        s = stream_slice or {}
        account_name = s.get("account_name")
        start_raw, end_raw = s.get("start_date"), s.get("end_date")
        if not account_name or not start_raw or not end_raw:
            return
        self.logger.info(f"stream slice {stream_slice}")
        day = pendulum.parse(start_raw).date()
        end_date = pendulum.parse(end_raw).date()
        while day <= end_date:
            report_date = day.to_date_string()
            try:
                recs = self._read_day(account_name, report_date)
            except PangleApiError as exc:
                raise PangleApiError(f"Account '{account_name}' ngay {report_date}: {exc}") from None
            for rec in recs:
                yield rec
            # Ngay doc xong (ke ca rong) -> account nay da phu den ngay do.
            prev = self._cursor_by_account.get(account_name)
            self._cursor_by_account[account_name] = max(prev, day) if prev else day
            day = day.add(days=1)

    def get_json_schema(self) -> Mapping[str, Any]:
        props: Dict[str, Any] = {
            "account_name": {"type": ["null", "string"]},
            "date": {"type": ["null", "string"], "format": "date"},
        }
        for c in STRING_COLUMNS:
            props[c] = {"type": ["null", "string"]}
        for c in INT_COLUMNS + FLOAT_COLUMNS:
            props[c] = {"type": ["null", "number"]}
        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": [],
            "additionalProperties": True,
            "properties": props,
        }


# Source
class SourcePangle(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        accounts = config.get("accounts", [])
        if not accounts:
            return False, "No accounts configured. Add at least one account."
        yesterday = pendulum.today(REPORT_TIMEZONE).date().subtract(days=1).to_date_string()
        currency = str(config.get("currency") or DEFAULT_CURRENCY).strip().lower()
        if currency not in ALLOWED_CURRENCIES:
            currency = DEFAULT_CURRENCY
        for account in accounts:
            account_name = account.get("account_name", "?")
            try:
                # 1 request (hom qua) de validate user_id + role_id + security_key
                call_income_api(account["user_id"], account["role_id"], account["security_key"], yesterday, currency)
                logger.info(f"Account '{account_name}' OK")
            except KeyError as exc:
                return False, f"Account '{account_name}' is missing required field: {exc}"
            except PangleApiError as exc:
                return False, f"Account '{account_name}' failed: {exc}"
            except Exception as exc:
                return False, f"Account '{account_name}' failed: {type(exc).__name__}"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        return [PangleReport(config=config, accounts=config.get("accounts", []))]
