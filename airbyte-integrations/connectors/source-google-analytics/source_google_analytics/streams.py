"""
streams.py — Dynamic GA4Stream: mỗi report trong config = 1 stream = 1 bảng BQ.

Tuân thủ playbook EasyGoing Data + Airbyte CDK chuẩn:
- Lazy client §3.1 — credentials giữ dạng string, client tạo lúc read_records
- Normalize tên cột _Name_ §3.3
- Metadata chuẩn §4
- Incremental cursor = _date_ §1
- additionalProperties: true — schema dynamic §1
- Mọi giá trị string — raw passthrough §1
- stream_slices per property — checkpoint state sau mỗi property (CDK)
- state_checkpoint_interval — checkpoint mỗi 100 records (CDK)
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Any, Dict, Iterable, Iterator, List, Mapping, MutableMapping, Optional, Tuple

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import IncrementalMixin, Stream

from .ga4_client import GA4Client

# ---------------------------------------------------------------------------
# Normalize tên cột về _Name_ — BQ safe §3.3
# ---------------------------------------------------------------------------

def _normalize(col: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z]+", "_", str(col)).strip("_")
    return f"_{s}_"


# Metadata constants (đã normalize sẵn)
META_PROPERTY_ID   = "_property_id_"
META_PROPERTY_NAME = "_property_name_"
META_PACKAGE_NAME  = "_package_name_"
META_SYNCED_AT     = "_synced_at_"

CURSOR_FIELD_RAW = "date"                        # tên GA4 trả về
CURSOR_FIELD     = _normalize(CURSOR_FIELD_RAW)  # "_date_"



# ---------------------------------------------------------------------------
# CohortStream — cohort retention report
# ---------------------------------------------------------------------------

class CohortStream(Stream, IncrementalMixin):
    """
    Cohort stream — firstSessionDate cohort, D0→D{cohort_range}.
    Lazy client, normalize _Name_, metadata chuẩn.
    """

    state_checkpoint_interval = 100
    primary_key = None  # set trong __init__

    def __init__(
        self,
        credentials_json: str,
        report_name: str,
        dimensions: List[str],
        metrics: List[str],
        start_date: str,
        cohort_range: int,
        properties: List[Dict[str, str]],
        number_days_backward: int,
        get_last_x_days: bool,
    ):
        super().__init__()
        self._credentials_json = credentials_json
        self._report_name      = report_name
        self._dimensions       = dimensions  # extra dims ngoài cohort/cohortNthDay
        self._metrics          = metrics
        self._start_date       = start_date
        self._cohort_range     = cohort_range
        self._properties       = properties
        self._lookback         = number_days_backward
        self._last_x_days      = get_last_x_days
        self._state: Dict[str, str] = {}

        # PK = property_id + cohort + cohortNthDay + extra dimensions
        fixed = [_normalize("cohort"), _normalize("cohortNthDay")]
        extra = [_normalize(d) for d in dimensions if d not in ("cohort", "cohortNthDay")]
        self.primary_key = [META_PROPERTY_ID] + fixed + extra

    @property
    def name(self) -> str:
        return self._report_name

    @property
    def cursor_field(self) -> str:
        return _normalize("cohort")  # "_cohort_" — ngày cohort (YYYYMMDD)

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._state = dict(value)

    def get_json_schema(self) -> Mapping[str, Any]:
        props: Dict[str, Any] = {}
        props[META_PROPERTY_ID]   = {"type": ["null", "string"]}
        props[META_PROPERTY_NAME] = {"type": ["null", "string"]}
        props[META_PACKAGE_NAME]  = {"type": ["null", "string"]}
        props[META_SYNCED_AT]     = {"type": ["null", "string"]}

        # cohort + cohortNthDay luôn có
        for col in ["cohort", "cohortNthDay"] + self._dimensions + self._metrics:
            props[_normalize(col)] = {"type": ["null", "string"]}

        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": True,
            "properties": props,
        }

    def stream_slices(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        for prop in self._properties:
            yield {
                "property_id":   prop["property_id"],
                "property_name": prop["property_name"],
                "package_name":  prop.get("package_name", ""),
            }

    def _start(self, property_id: str) -> str:
        if self._last_x_days:
            return (date.today() - timedelta(days=self._lookback)).strftime("%Y-%m-%d")
        saved = self._state.get(property_id)
        if saved:
            saved_date = datetime.strptime(saved, "%Y%m%d").date()
            start = (saved_date - timedelta(days=self._lookback)).strftime("%Y-%m-%d")
            return max(start, self._start_date)
        return self._start_date

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:

        if not stream_slice:
            return

        property_id   = stream_slice["property_id"]
        property_name = stream_slice["property_name"]
        package_name  = stream_slice["package_name"]
        synced_at     = datetime.utcnow().isoformat() + "Z"

        client = GA4Client(self._credentials_json)

        rows = client.fetch_cohort(
            property_id=property_id,
            dimensions=self._dimensions,
            metrics=self._metrics,
            start_date=self._start(property_id),
            cohort_range=self._cohort_range,
        )

        max_cohort: Optional[str] = self._state.get(property_id)

        for row in rows:
            cohort_val = row.get("cohort", "")
            record: Dict[str, Any] = {_normalize(k): str(v) for k, v in row.items()}
            record[META_PROPERTY_ID]   = property_id
            record[META_PROPERTY_NAME] = property_name
            record[META_PACKAGE_NAME]  = package_name
            record[META_SYNCED_AT]     = synced_at

            if cohort_val and (max_cohort is None or cohort_val > max_cohort):
                max_cohort = cohort_val

            yield record

        if max_cohort:
            self._state[property_id] = max_cohort

class GA4Stream(Stream, IncrementalMixin):
    """
    Dynamic stream — tên stream, dimensions, metrics đều từ config.
    Lazy client: GA4Client chỉ khởi tạo lúc read_records() chạy.

    stream_slices: mỗi slice = 1 property → state checkpoint sau mỗi property.
    state_checkpoint_interval: checkpoint mỗi 100 records trong 1 slice.
    """

    primary_key = None  # set trong __init__ theo dimensions

    # Checkpoint state sau mỗi 100 records — CDK chuẩn
    state_checkpoint_interval = 100

    def __init__(
        self,
        credentials_json: str,
        report_name: str,
        dimensions: List[str],
        metrics: List[str],
        start_date: str,
        properties: List[Dict[str, str]],
        number_days_backward: int,
        get_last_x_days: bool,
    ):
        super().__init__()
        # Giữ string — KHÔNG parse / dựng client ở đây (lazy §3.1)
        self._credentials_json = credentials_json
        self._report_name      = report_name
        self._dimensions       = list(dict.fromkeys(["date"] + dimensions))  # date luôn đầu
        self._metrics          = metrics
        self._start_date       = start_date
        self._properties       = properties
        self._lookback         = number_days_backward
        self._last_x_days      = get_last_x_days
        self._state: Dict[str, str] = {}

        # PK = property_id + tất cả dimensions (đã normalize)
        self.primary_key = [META_PROPERTY_ID] + [_normalize(d) for d in self._dimensions]

    # --- Stream identity ---

    @property
    def name(self) -> str:
        return self._report_name

    # --- IncrementalMixin ---

    @property
    def cursor_field(self) -> str:
        return CURSOR_FIELD

    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._state

    @state.setter
    def state(self, value: MutableMapping[str, Any]):
        self._state = dict(value)

    # --- Schema dynamic §3.3 + additionalProperties: true §1 ---

    def get_json_schema(self) -> Mapping[str, Any]:
        """
        Schema hoàn toàn dynamic — build từ dimensions + metrics trong config.
        Normalize tên cột về _Name_. additionalProperties: true để pipeline không vỡ.
        """
        props: Dict[str, Any] = {}

        # Metadata
        props[META_PROPERTY_ID]   = {"type": ["null", "string"]}
        props[META_PROPERTY_NAME] = {"type": ["null", "string"]}
        props[META_PACKAGE_NAME]  = {"type": ["null", "string"]}
        props[META_SYNCED_AT]     = {"type": ["null", "string"]}

        # Dimensions + metrics — tất cả string (raw passthrough §1)
        for col in self._dimensions + self._metrics:
            props[_normalize(col)] = {"type": ["null", "string"]}

        return {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": True,
            "properties": props,
        }

    # --- stream_slices: mỗi slice = 1 property (CDK chuẩn) ---

    def stream_slices(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        """
        Mỗi property = 1 slice.
        Sau khi đọc xong 1 slice, CDK tự emit AirbyteStateMessage → state được lưu.
        Nếu sync fail ở property thứ N, lần sau tiếp tục từ property N, không đọc lại 1..N-1.
        """
        for prop in self._properties:
            yield {
                "property_id":   prop["property_id"],
                "property_name": prop["property_name"],
                "package_name":  prop.get("package_name", ""),
            }

    # --- Date range logic ---

    def _date_range(self, property_id: str) -> Tuple[str, str]:
        end = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

        if self._last_x_days:
            start = (date.today() - timedelta(days=self._lookback)).strftime("%Y-%m-%d")
            return start, end

        saved = self._state.get(property_id)
        if saved:
            saved_date = datetime.strptime(saved, "%Y%m%d").date()
            start = (saved_date - timedelta(days=self._lookback)).strftime("%Y-%m-%d")
            if start < self._start_date:
                start = self._start_date
        else:
            start = self._start_date

        return start, end

    # --- read_records: đọc từ 1 slice (1 property) ---

    def read_records(
        self,
        sync_mode: SyncMode,
        cursor_field: Optional[List[str]] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        stream_state: Optional[Mapping[str, Any]] = None,
    ) -> Iterable[Mapping[str, Any]]:

        if not stream_slice:
            return

        property_id   = stream_slice["property_id"]
        property_name = stream_slice["property_name"]
        package_name  = stream_slice["package_name"]
        synced_at     = datetime.utcnow().isoformat() + "Z"

        start, end = self._date_range(property_id)

        # Lazy client (§3.1)
        client = GA4Client(self._credentials_json)

        rows = client.fetch(
            property_id=property_id,
            dimensions=self._dimensions,
            metrics=self._metrics,
            start_date=start,
            end_date=end,
        )

        max_date: Optional[str] = self._state.get(property_id)

        for row in rows:
            # Đọc cursor từ tên GỐC trước khi normalize
            raw_date = row.get(CURSOR_FIELD_RAW, "")

            # Normalize tất cả key về _Name_
            record: Dict[str, Any] = {
                _normalize(k): str(v) for k, v in row.items()
            }

            # Chèn metadata
            record[META_PROPERTY_ID]   = property_id
            record[META_PROPERTY_NAME] = property_name
            record[META_PACKAGE_NAME]  = package_name
            record[META_SYNCED_AT]     = synced_at

            # Update cursor state
            if raw_date and (max_date is None or raw_date > max_date):
                max_date = raw_date

            yield record

        # Cập nhật state sau khi xong slice (1 property)
        if max_date:
            self._state[property_id] = max_date
