"""
ga4_client.py — Wrapper quanh google-analytics-data SDK.

Một client = một SA credentials (dùng chung cho tất cả property).
Tách riêng khỏi stream để dễ test và lazy init.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
    CohortSpec,
    Cohort,
    CohortsRange,
)
from google.oauth2 import service_account

logger = logging.getLogger("airbyte")

GA4_SCOPES = ["https://www.googleapis.com/auth/analytics.readonly"]

# Retry config — không hardcode số lần, đọc từ constant
MAX_RETRIES = 5
BACKOFF_BASE = 2  # seconds
PAGE_SIZE = 10_000


def _make_client(credentials_json: str) -> BetaAnalyticsDataClient:
    info = json.loads(credentials_json)
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=GA4_SCOPES
    )
    return BetaAnalyticsDataClient(credentials=creds)


def _run_with_backoff(fn, *args, **kwargs):
    """Exponential backoff khi gặp quota / server error."""
    for attempt in range(MAX_RETRIES):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            err = str(e).lower()
            retryable = any(x in err for x in (
                "quota", "resource_exhausted", "429", "500", "503", "rateLimitExceeded"
            ))
            if retryable and attempt < MAX_RETRIES - 1:
                wait = BACKOFF_BASE ** attempt
                logger.warning(
                    f"GA4 API error (attempt {attempt + 1}/{MAX_RETRIES}): {e}. "
                    f"Retry sau {wait}s..."
                )
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f"GA4 API vẫn lỗi sau {MAX_RETRIES} lần retry")


class GA4Client:
    """
    Client GA4 Data API — dùng chung 1 instance cho tất cả property.
    Lazy init: client chỉ được tạo khi fetch() được gọi lần đầu.
    """

    def __init__(self, credentials_json: str):
        self._credentials_json = credentials_json
        self._client: Optional[BetaAnalyticsDataClient] = None

    @property
    def client(self) -> BetaAnalyticsDataClient:
        if self._client is None:
            self._client = _make_client(self._credentials_json)
        return self._client

    def fetch(
        self,
        property_id: str,
        dimensions: List[str],
        metrics: List[str],
        start_date: str,
        end_date: str,
    ) -> List[Dict[str, Any]]:
        """
        Kéo data từ GA4 runReport với pagination tự động.
        Trả về list of dict — tên key là tên gốc GA4 (chưa normalize).
        """
        # Đảm bảo 'date' luôn có trong dimensions
        dims = list(dict.fromkeys(["date"] + dimensions))  # dedup, giữ thứ tự

        rows = []
        offset = 0

        while True:
            req = RunReportRequest(
                property=f"properties/{property_id}",
                dimensions=[Dimension(name=d) for d in dims],
                metrics=[Metric(name=m) for m in metrics],
                date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
                offset=offset,
                limit=PAGE_SIZE,
                keep_empty_rows=False,
            )

            response = _run_with_backoff(self.client.run_report, req)

            dim_headers = [h.name for h in response.dimension_headers]
            met_headers = [h.name for h in response.metric_headers]

            for row in response.rows:
                record: Dict[str, Any] = {}
                for i, dv in enumerate(row.dimension_values):
                    record[dim_headers[i]] = dv.value
                for i, mv in enumerate(row.metric_values):
                    record[met_headers[i]] = mv.value
                rows.append(record)

            fetched = offset + len(response.rows)
            if fetched >= response.row_count:
                break
            offset = fetched

        return rows

    def fetch_cohort(
        self,
        property_id: str,
        dimensions: List[str],
        metrics: List[str],
        start_date: str,
        cohort_range: int,
    ) -> List[Dict[str, Any]]:
        """
        Kéo cohort report — dùng CohortSpec thay vì DateRange thông thường.
        Cohort by firstSessionDate, retention D0 → D{cohort_range}.
        """
        # Build cohort date ranges — mỗi ngày trong start_date là 1 cohort
        cohort_spec = CohortSpec(
            cohorts=[
                Cohort(
                    name="cohort",
                    dimension="firstSessionDate",
                    date_range=DateRange(start_date=start_date, end_date="today"),
                )
            ],
            cohorts_range=CohortsRange(
                granularity=CohortsRange.Granularity.DAILY,
                start_offset=0,
                end_offset=cohort_range,
            ),
        )

        # cohort + cohortNthDay luôn bắt buộc
        fixed_dims = ["cohort", "cohortNthDay"]
        all_dims = fixed_dims + [d for d in dimensions if d not in fixed_dims]

        rows = []
        offset = 0

        while True:
            from google.analytics.data_v1beta.types import RunReportRequest as RR
            req = RR(
                property=f"properties/{property_id}",
                dimensions=[Dimension(name=d) for d in all_dims],
                metrics=[Metric(name=m) for m in metrics],
                cohort_spec=cohort_spec,
                offset=offset,
                limit=PAGE_SIZE,
            )

            response = _run_with_backoff(self.client.run_report, req)

            dim_headers = [h.name for h in response.dimension_headers]
            met_headers = [h.name for h in response.metric_headers]

            for row in response.rows:
                record: Dict[str, Any] = {}
                for i, dv in enumerate(row.dimension_values):
                    record[dim_headers[i]] = dv.value
                for i, mv in enumerate(row.metric_values):
                    record[met_headers[i]] = mv.value
                rows.append(record)

            fetched = offset + len(response.rows)
            if fetched >= response.row_count:
                break
            offset = fetched

        return rows
    def check(self, property_id: str) -> bool:
        """Test kết nối — kéo 1 row sessions hôm qua."""
        yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
        req = RunReportRequest(
            property=f"properties/{property_id}",
            dimensions=[Dimension(name="date")],
            metrics=[Metric(name="sessions")],
            date_ranges=[DateRange(start_date=yesterday, end_date=yesterday)],
            limit=1,
        )
        _run_with_backoff(self.client.run_report, req)
        return True
