"""source-google-play-console — AbstractSource entrypoint."""
from __future__ import annotations

from typing import Any, List, Mapping, Optional, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .gcs_client import GCSClient
from .streams import Earnings, EstimatedSales, InstallsCountry, InstallsOverview, Ratings, Reviews


class SourceGooglePlayConsole(AbstractSource):
    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        try:
            gcs = GCSClient(config["service_account"])
        except Exception as e:  # noqa: BLE001
            return False, f"Service account JSON không hợp lệ: {e}"

        stores = config.get("stores") or []
        if not stores:
            return False, "Cần khai báo ít nhất 1 store."

        for store in stores:
            bucket = store.get("bucket")
            try:
                # Chỉ cần list được 1 object là đủ chứng minh quyền đọc bucket.
                next(iter(gcs.list_blobs(bucket, "")), None)
            except Exception as e:  # noqa: BLE001
                return False, f"Không đọc được bucket '{bucket}' (store {store.get('store_id')}): {e}"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        # KHÔNG tạo GCSClient ở đây: discover chỉ cần liệt kê stream + schema,
        # không phụ thuộc SA hợp lệ. Client được tạo lazy khi read_records chạy.
        common = dict(
            service_account=config["service_account"],
            stores=config.get("stores") or [],
            start_date=config.get("start_date"),
            lookback_days=config.get("lookback_days", 28),
        )
        return [
            EstimatedSales(**common),
            Earnings(**common),
            InstallsOverview(**common),
            InstallsCountry(**common),
            Ratings(**common),
            Reviews(**common),
        ]