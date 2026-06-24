"""Stream definitions for source-google-play-console.

Đọc Google Play bulk reports từ GCS bucket pubsite_prod_* của từng store.

Grain
-----
  Per-app monthly CSV (package trong tên file): installs_overview, ratings, reviews -> utf-16
  Account-level monthly zipped CSV (package là 1 cột): estimated_sales, earnings -> utf-8-sig

Incremental
-----------
  cursor = _modified_at_ (blob.updated). State theo từng store_id.
  Mỗi sync chỉ đọc blob có updated >= (cursor_store - lookback_days).

Primary key: để None -> user chọn ở connection setup (UI).
  LƯU Ý: earnings/estimated_sales phải chọn PK gồm _row_number, nếu không sẽ gộp mất giao dịch.
"""
from __future__ import annotations

import csv
import io
import os
import re
from abc import ABC
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional

from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources.streams.core import IncrementalMixin

from .gcs_client import GCSClient

# Mọi tên cột (gốc lẫn metadata) đều chuẩn hóa về dạng _Tên_Sạch_ :
# giữ chữ hoa/thường, thay ký tự không phải [0-9a-zA-Z] bằng "_", gộp "_", bọc 2 đầu.
# Đảm bảo hợp lệ BigQuery (không dấu cách/ngoặc/%, không trùng tiền tố cấm _FILE_ ...).
def _normalize(col: str) -> str:
    s = re.sub(r"[^0-9a-zA-Z]+", "_", str(col)).strip("_")
    return f"_{s}_"


# Tên metadata SAU chuẩn hóa (connector chèn vào mỗi dòng).
META_STORE_ID     = _normalize("store_id")        # _store_id_
META_APP_ID       = _normalize("app_id")          # _app_id_
META_REPORT_MONTH = _normalize("report_month")    # _report_month_
META_SOURCE_FILE  = _normalize("source_file")     # _source_file_
META_ROW_NUMBER   = _normalize("row_number")      # _row_number_
META_MODIFIED_AT  = _normalize("modified_at")     # _modified_at_
META_SYNCED_AT    = _normalize("synced_at")       # _synced_at_

METADATA_FIELDS = [
    META_STORE_ID, META_APP_ID, META_REPORT_MONTH, META_SOURCE_FILE,
    META_ROW_NUMBER, META_MODIFIED_AT, META_SYNCED_AT,
]


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


class GooglePlayGCSStream(Stream, IncrementalMixin, ABC):
    primary_key: Optional[Any] = None
    cursor_field: str = META_MODIFIED_AT

    # ---- subclass contract ----
    report_prefix: str = ""
    filename_regex: str = ""
    encoding: str = "utf-16"
    is_zip: bool = False
    app_id_source: str = "filename"      # "filename" | "column"
    app_id_column: Optional[str] = None

    def __init__(self, service_account: str, stores: List[Mapping[str, str]],
                 start_date: Optional[str] = None, lookback_days: int = 28, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._service_account = service_account
        self._gcs_client: Optional[GCSClient] = None  # tạo lazy ở lần đọc đầu
        self._stores = stores
        self._start_date = start_date          # "YYYY-MM" hoặc None
        self._lookback_days = lookback_days
        self._cursor_value: MutableMapping[str, Any] = {}
        self._rx = re.compile(self.filename_regex)

    @property
    def _gcs(self) -> GCSClient:
        # Chỉ khởi tạo client (parse SA) khi thật sự cần đọc data, không phải lúc discover.
        if self._gcs_client is None:
            self._gcs_client = GCSClient(self._service_account)
        return self._gcs_client

    def get_json_schema(self) -> Mapping[str, Any]:
        # Schema json khai tên cột GỐC (dễ đọc/đối chiếu); ở đây normalize tên property
        # về _Tên_Sạch_ để KHỚP với key của row trong read_records.
        schema = super().get_json_schema()
        props = schema.get("properties", {})
        schema["properties"] = {_normalize(k): v for k, v in props.items()}
        return schema

    # ---- IncrementalMixin ----
    @property
    def state(self) -> MutableMapping[str, Any]:
        return self._cursor_value

    @state.setter
    def state(self, value: MutableMapping[str, Any]) -> None:
        self._cursor_value = value or {}

    # ---- helpers ----
    def _threshold(self, store_id: str) -> Optional[datetime]:
        """Mốc thời gian tối thiểu của blob.updated cần đọc (đã trừ lookback)."""
        cur = self._cursor_value.get(store_id)
        if not cur:
            return None
        return datetime.fromisoformat(cur) - timedelta(days=self._lookback_days)

    def _passes_start_date(self, yyyymm: str) -> bool:
        if not self._start_date:
            return True
        return yyyymm >= self._start_date.replace("-", "")

    # ---- read ----
    def read_records(self, sync_mode: SyncMode, cursor_field: Optional[List[str]] = None,
                     stream_slice: Optional[Mapping[str, Any]] = None,
                     stream_state: Optional[Mapping[str, Any]] = None) -> Iterable[Mapping[str, Any]]:
        now = _iso(datetime.now(timezone.utc))

        for store in self._stores:
            store_id = store["store_id"]
            bucket = store["bucket"]
            threshold = self._threshold(store_id) if sync_mode == SyncMode.incremental else None
            max_seen: Optional[datetime] = None

            blobs = [b for b in self._gcs.list_blobs(bucket, self.report_prefix)
                     if self._rx.search(os.path.basename(b.name))]
            blobs.sort(key=lambda b: b.updated)

            for blob in blobs:
                m = self._rx.search(os.path.basename(blob.name))
                yyyymm = m.group("yyyymm")
                if not self._passes_start_date(yyyymm):
                    continue
                if threshold is not None and blob.updated < threshold:
                    continue

                file_modified = _iso(blob.updated)
                app_from_name = m.groupdict().get("package")

                text = self._gcs.download_text(bucket, blob.name, self.encoding, self.is_zip)
                reader = csv.DictReader(io.StringIO(text))
                for i, row in enumerate(reader, start=1):
                    # đọc app_id từ tên cột GỐC trước khi normalize
                    if self.app_id_source == "column":
                        app_id = row.get(self.app_id_column)
                    else:
                        app_id = app_from_name
                    # chuẩn hóa tên mọi cột nguồn -> _Tên_Sạch_ (bỏ cột key None)
                    out = {_normalize(k): v for k, v in row.items() if k is not None}
                    # chèn metadata (tên đã ở dạng bọc)
                    out.update({
                        META_STORE_ID: store_id,
                        META_APP_ID: app_id,
                        META_REPORT_MONTH: yyyymm,
                        META_SOURCE_FILE: f"gs://{bucket}/{blob.name}",
                        META_ROW_NUMBER: i,
                        META_MODIFIED_AT: file_modified,
                        META_SYNCED_AT: now,
                    })
                    yield out

                if max_seen is None or blob.updated > max_seen:
                    max_seen = blob.updated

            if max_seen is not None:
                prev = self._cursor_value.get(store_id)
                new = _iso(max_seen)
                self._cursor_value[store_id] = max(prev, new) if prev else new


# ---- grain mixins ----
class MonthlyPerAppCsvStream(GooglePlayGCSStream, ABC):
    encoding = "utf-16"
    is_zip = False
    app_id_source = "filename"


class MonthlyAccountZipStream(GooglePlayGCSStream, ABC):
    encoding = "utf-8-sig"
    is_zip = True
    app_id_source = "column"
    app_id_column = "Package ID"


# ---- concrete streams ----
class EstimatedSales(MonthlyAccountZipStream):
    name = "estimated_sales"
    report_prefix = "sales/"
    filename_regex = r"^salesreport_(?P<yyyymm>\d{6})\.zip$"


class Earnings(MonthlyAccountZipStream):
    name = "earnings"
    report_prefix = "earnings/"
    filename_regex = r"^earnings_(?P<yyyymm>\d{6})_[\d-]+\.zip$"


class InstallsOverview(MonthlyPerAppCsvStream):
    name = "installs_overview"
    report_prefix = "stats/installs/"
    filename_regex = r"^installs_(?P<package>.+?)_(?P<yyyymm>\d{6})_overview\.csv$"


class InstallsCountry(MonthlyPerAppCsvStream):
    name = "installs_country"
    report_prefix = "stats/installs/"
    filename_regex = r"^installs_(?P<package>.+?)_(?P<yyyymm>\d{6})_country\.csv$"


class Ratings(MonthlyPerAppCsvStream):
    name = "ratings"
    report_prefix = "stats/ratings/"
    filename_regex = r"^ratings_(?P<package>.+?)_(?P<yyyymm>\d{6})_overview\.csv$"


class Reviews(MonthlyPerAppCsvStream):
    name = "reviews"
    report_prefix = "reviews/"
    filename_regex = r"^reviews_(?P<package>.+?)_(?P<yyyymm>\d{6})\.csv$"