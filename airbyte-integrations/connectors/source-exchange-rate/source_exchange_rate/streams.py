"""Stream cho source-exchange-rate (Frankfurter).

Output LONG: mỗi dòng = 1 cặp (date, currency) -> rate.
  _date_, _base_, _currency_, _rate_, _amount_, _extracted_at_, _synced_at_

Incremental: cursor = _date_ (YYYY-MM-DD). State lưu ngày lớn nhất đã đọc.
Frankfurter chỉ trả ngày làm việc; mỗi lần sync lấy từ (cursor - lookback) tới hôm nay.
"""
from __future__ import annotations

import re
from abc import ABC
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.core import IncrementalMixin

from .frankfurter_client import FrankfurterClient

DEFAULT_START = "2024-01-01"


def _normalize(col: str) -> str:
    # Chuẩn hóa tên cột về _Name_ (BigQuery-safe). Xem playbook §3.3.
    s = re.sub(r"[^0-9a-zA-Z]+", "_", str(col)).strip("_")
    return f"_{s}_"


META_DATE         = _normalize("date")
META_BASE         = _normalize("base")
META_CURRENCY     = _normalize("currency")
META_RATE         = _normalize("rate")
META_AMOUNT       = _normalize("amount")
META_EXTRACTED_AT = _normalize("extracted_at")
META_SYNCED_AT    = _normalize("synced_at")


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class ExchangeRates(Stream, IncrementalMixin):
    name = "exchange_rates"
    primary_key = None                 # đặt ở connection UI
    cursor_field = META_DATE

    def __init__(self, base: str, symbols: Optional[str] = None,
                 start_date: Optional[str] = None, lookback_days: int = 7,
                 **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._base = (base or "USD").upper()
        self._symbols = symbols
        self._start_date = start_date or DEFAULT_START
        self._lookback_days = lookback_days
        self._client: Optional[FrankfurterClient] = None
        self._cursor_value: MutableMapping[str, Any] = {}

    @property
    def _conn(self) -> FrankfurterClient:
        # lazy: dựng client khi đọc, không phải lúc discover
        if self._client is None:
            self._client = FrankfurterClient(base=self._base, symbols=self._symbols)
        return self._client

    # ---- IncrementalMixin ----
    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._cursor_value

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        self._cursor_value = value or {}

    def get_json_schema(self) -> Mapping[str, Any]:
        schema = super().get_json_schema()
        props = schema.get("properties", {})
        schema["properties"] = {_normalize(k): v for k, v in props.items()}
        return schema

    # ---- read ----
    def _window(self, sync_mode: SyncMode) -> tuple[str, str]:
        today = date.today()
        end = today.isoformat()
        if sync_mode == SyncMode.incremental and self._cursor_value.get("date"):
            cur = date.fromisoformat(self._cursor_value["date"])
            start = (cur - timedelta(days=self._lookback_days)).isoformat()
        else:
            start = self._start_date
        return start, end

    def read_records(self, sync_mode: SyncMode, cursor_field: Optional[List[str]] = None,
                     stream_slice: Optional[Mapping[str, Any]] = None,
                     stream_state: Optional[Mapping[str, Any]] = None) -> Iterable[Mapping[str, Any]]:
        now = _iso_now()
        start, end = self._window(sync_mode)
        data = self._conn.time_series(start, end)
        amount = data.get("amount", 1.0)
        base = data.get("base", self._base)
        rates_by_date = data.get("rates", {})

        max_seen: Optional[str] = None
        for d in sorted(rates_by_date.keys()):
            for currency, rate in rates_by_date[d].items():
                yield {
                    META_DATE: d,
                    META_BASE: base,
                    META_CURRENCY: currency,
                    META_RATE: rate,
                    META_AMOUNT: amount,
                    META_EXTRACTED_AT: now,
                    META_SYNCED_AT: now,
                }
            if max_seen is None or d > max_seen:
                max_seen = d

        if max_seen is not None:
            prev = self._cursor_value.get("date")
            self._cursor_value["date"] = max(prev, max_seen) if prev else max_seen
