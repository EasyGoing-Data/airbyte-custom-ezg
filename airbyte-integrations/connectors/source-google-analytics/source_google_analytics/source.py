"""
source.py — AbstractSource: check_connection() + streams()

streams() truyền config string xuống stream — KHÔNG truyền object client (lazy §3.1).
"""
from __future__ import annotations

import logging
from typing import Any, List, Mapping, Optional, Tuple

from airbyte_cdk.sources import AbstractSource

from .ga4_client import GA4Client
from .streams import GA4Stream, CohortStream

logger = logging.getLogger("airbyte")


class SourceGoogleAnalytics(AbstractSource):

    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        try:
            credentials_json = config["credentials"]["credentials_json"]
            properties = config["list_properties_name_and_id_as_dict"]

            if not properties:
                return False, "Cần ít nhất 1 property trong config."

            # Test kết nối với property đầu tiên
            client = GA4Client(credentials_json)
            client.check(properties[0]["property_id"])
            return True, None

        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List[GA4Stream]:
        """
        Tạo dynamic streams từ daily_reports config.
        KHÔNG dựng GA4Client ở đây — lazy init trong stream (§3.1).
        """
        credentials_json     = config["credentials"]["credentials_json"]
        properties           = config["list_properties_name_and_id_as_dict"]
        number_days_backward = int(config.get("number_days_backward") or 7)
        get_last_x_days      = bool(config.get("get_last_x_days") or False)
        daily_reports        = config.get("daily_reports") or []
        daily_cohort_reports = config.get("daily_cohort_reports") or []

        streams = []

        for report in daily_reports:
            streams.append(
                GA4Stream(
                    credentials_json     = credentials_json,
                    report_name          = report["report_name"],
                    dimensions           = report.get("dimensions") or [],
                    metrics              = report["metrics"],
                    start_date           = report["start_date"],
                    properties           = properties,
                    number_days_backward = number_days_backward,
                    get_last_x_days      = get_last_x_days,
                )
            )

        for report in daily_cohort_reports:
            streams.append(
                CohortStream(
                    credentials_json     = credentials_json,
                    report_name          = report["report_name"],
                    dimensions           = report.get("dimensions") or [],
                    metrics              = report["metrics"],
                    start_date           = report["start_date"],
                    cohort_range         = int(report.get("cohort_range") or 90),
                    properties           = properties,
                    number_days_backward = number_days_backward,
                    get_last_x_days      = get_last_x_days,
                )
            )

        return streams
