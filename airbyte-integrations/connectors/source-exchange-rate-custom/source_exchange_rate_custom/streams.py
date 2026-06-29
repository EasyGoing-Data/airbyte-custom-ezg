import re
import logging
from datetime import date, datetime, timedelta
from typing import Any, Iterable, Mapping, MutableMapping, Optional

from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.core import IncrementalMixin

from .oxr_client import OxrClient

logger = logging.getLogger("airbyte")

DEFAULT_START_DATE = "2024-12-31"
DEFAULT_LOOKBACK_DAYS = 3


def _normalize(col: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z]+", "_", str(col)).strip("_")
    return f"_{s}_"


META_SYNCED_AT = _normalize("synced_at")


class ExchangeRates(Stream, IncrementalMixin):
    # ── class-level attrs required by CDK ──────────────────────────────────
    primary_key = ["_date_", "_currency_"]
    cursor_field = "_date_"
    state_checkpoint_interval = 100

    def __init__(
        self,
        app_id: str,
        start_date: Optional[str],
        lookback_days: Optional[int],
        **kwargs,
    ):
        super().__init__(**kwargs)
        # store raw params — do NOT build client here (lazy §3.1)
        self._app_id = app_id
        self._start_date = start_date or DEFAULT_START_DATE
        self._lookback_days = lookback_days if lookback_days is not None else DEFAULT_LOOKBACK_DAYS
        self._client: Optional[OxrClient] = None
        self._state: MutableMapping[str, Any] = {}

    # ── lazy client ────────────────────────────────────────────────────────
    @property
    def _conn(self) -> OxrClient:
        if self._client is None:
            self._client = OxrClient(self._app_id)
        return self._client

    # ── incremental state ──────────────────────────────────────────────────
    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._state = value

    # ── slices: backfill + lookback overwrite ──────────────────────────────
    def stream_slices(
        self,
        sync_mode,
        cursor_field=None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:

        today = date.today()
        yesterday = today - timedelta(days=1)

        # determine start: use state cursor, but roll back lookback_days
        # so recent days are always re-fetched (overwrite via PK dedup)
        if stream_state and stream_state.get(self.cursor_field):
            state_date = datetime.strptime(
                stream_state[self.cursor_field], "%Y-%m-%d"
            ).date()
            # roll back lookback_days from state to ensure overwrite window
            effective_start = state_date - timedelta(days=self._lookback_days)
        else:
            effective_start = datetime.strptime(self._start_date, "%Y-%m-%d").date()

        # clamp to start_date (never go earlier than configured start)
        configured_start = datetime.strptime(self._start_date, "%Y-%m-%d").date()
        effective_start = max(effective_start, configured_start)

        current = effective_start
        while current <= yesterday:
            yield {"date": current.strftime("%Y-%m-%d")}
            current += timedelta(days=1)

    # ── record reading ─────────────────────────────────────────────────────
    def read_records(
        self,
        sync_mode,
        cursor_field=None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:

        date_str = stream_slice["date"]
        synced_at = datetime.utcnow().isoformat()

        data = self._conn.get_historical(date_str)
        self._conn.throttle()

        rates: dict = data.get("rates", {})
        base: str = data.get("base", "USD")
        timestamp: str = str(data.get("timestamp", ""))

        for currency, rate in rates.items():
            record = {
                "_base_": base,
                "_date_": date_str,
                "_currency_": str(currency),
                "_rate_": str(rate),
                "_timestamp_": timestamp,
                META_SYNCED_AT: synced_at,
            }
            yield record

        # update cursor state to this date
        if self._state.get(self.cursor_field, "") < date_str:
            self._state[self.cursor_field] = date_str

    # ── schema ─────────────────────────────────────────────────────────────
    def get_json_schema(self) -> Mapping[str, Any]:
        schema = super().get_json_schema()
        props = schema.get("properties", {})
        schema["properties"] = {_normalize(k): v for k, v in props.items()}
        return schema
