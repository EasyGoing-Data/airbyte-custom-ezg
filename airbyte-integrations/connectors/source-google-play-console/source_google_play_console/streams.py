"""Stream definitions for source-google-play-console.

Đọc Google Play bulk reports từ GCS bucket pubsite_prod_* của từng store.

Grain
-----
  Per-app monthly CSV (package trong tên file): installs_overview, ratings, reviews -> utf-16
  Account-level monthly zipped CSV (package là 1 cột): estimated_sales, earnings -> utf-8-sig

Incremental
-----------
  cursor = _file_modified_at (blob.updated). State theo từng store_id.
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

METADATA_FIELDS = [
    "store_id", "app_id", "_report_month", "_source_file",
    "_row_number", "_file_modified_at", "_synced_at",
]


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


class GooglePlayGCSStream(Stream, IncrementalMixin, ABC):
    primary_key: Optional[Any] = None
    cursor_field: str = "_file_modified_at"

    # ---- subclass contract ----
    report_prefix: str = ""
    filename_regex: str = ""
    encoding: str = "utf-16"
    is_zip: bool = False
    app_id_source: str = "filename"      # "filename" | "column"
    app_id_column: Optional[str] = None

    def __init__(self, gcs_client: GCSClient, stores: List[Mapping[str, str]],
                 start_date: Optional[str] = None, lookback_days: int = 28, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._gcs = gcs_client
        self._stores = stores
        self._start_date = start_date          # "YYYY-MM" hoặc None
        self._lookback_days = lookback_days
        self._cursor_value: MutableMapping[str, Any] = {}
        self._rx = re.compile(self.filename_regex)

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
                    row = {k: v for k, v in row.items() if k is not None}
                    if self.app_id_source == "column":
                        app_id = row.get(self.app_id_column)
                    else:
                        app_id = app_from_name
                    row.update({
                        "store_id": store_id,
                        "app_id": app_id,
                        "_report_month": yyyymm,
                        "_source_file": f"gs://{bucket}/{blob.name}",
                        "_row_number": i,
                        "_file_modified_at": file_modified,
                        "_synced_at": now,
                    })
                    yield row

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


class Ratings(MonthlyPerAppCsvStream):
    name = "ratings"
    report_prefix = "stats/ratings/"
    filename_regex = r"^ratings_(?P<package>.+?)_(?P<yyyymm>\d{6})_overview\.csv$"


class Reviews(MonthlyPerAppCsvStream):
    name = "reviews"
    report_prefix = "reviews/"
    filename_regex = r"^reviews_(?P<package>.+?)_(?P<yyyymm>\d{6})\.csv$"
