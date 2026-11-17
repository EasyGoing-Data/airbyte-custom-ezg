"""
SourceAppleAppStore — AbstractSource implementation.

check_connection(): validates each vendor's JWT + discovers its apps.
streams(): returns stream instances with per-vendor lazy clients and
           apps discovered per vendor.
"""

import logging
from typing import Any, Dict, Iterator, List, Mapping, Optional, Tuple

from airbyte_cdk.models import AirbyteMessage, AirbyteStreamStatus, Type as MessageType
from airbyte_cdk.models import TraceType
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

from .client import AppStoreAuthError, AppStoreClient
from .streams import (
    AppInstallationsStream,
    AppSessionsStream,
    FinancialReportDetailStream,
    FinancialReportStream,
    SummarySalesStream,
)

logger = logging.getLogger("airbyte")

DEFAULT_LOOKBACK_DAYS   = 7
DEFAULT_GET_LAST_X_DAYS = False
DEFAULT_TIMEZONE        = "UTC"


def _to_int(value: Any, default: int) -> int:
    """Cast config value to int. Airbyte Builder UI may send '7' as string."""
    if value is None or value == "":
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default


def _to_bool(value: Any, default: bool = False) -> bool:
    """Cast config value to bool. Builder UI may send 'false' as string."""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes")


class SourceAppleAppStore(AbstractSource):

    @property
    def stop_sync_on_stream_failure(self) -> bool:
        """1 stream loi KHONG dung cac stream con lai (ghim ro, khong phu thuoc mac dinh cua CDK)."""
        return False

    def read(self, logger: logging.Logger, config: Mapping[str, Any], catalog: Any, state: Any = None) -> Iterator[AirbyteMessage]:
        """Chay moi stream; stream nao loi thi ghi CANH BAO neu ro ten, cac stream khac van chay het.

        Job van ket thuc `failed` neu co stream loi (CDK nem loi tong sau khi da chay xong
        cac stream con lai) de loi khong bi im lang; du lieu va state cua stream chay tot da duoc phat ra.
        """
        finished: Dict[str, str] = {}
        try:
            for message in super().read(logger, config, catalog, state):
                if (message.type == MessageType.TRACE and message.trace
                        and message.trace.type == TraceType.STREAM_STATUS and message.trace.stream_status):
                    status = message.trace.stream_status
                    if status.status in (AirbyteStreamStatus.COMPLETE, AirbyteStreamStatus.INCOMPLETE):
                        finished[status.stream_descriptor.name] = status.status.value
                        if status.status == AirbyteStreamStatus.INCOMPLETE:
                            logger.warning(
                                f"WARNING stream '{status.stream_descriptor.name}' failed; "
                                f"continuing with the remaining streams."
                            )
                yield message
        except AirbyteTracedException:
            failed = sorted(name for name, status in finished.items() if status == AirbyteStreamStatus.INCOMPLETE.value)
            ok     = sorted(name for name, status in finished.items() if status == AirbyteStreamStatus.COMPLETE.value)
            logger.warning(
                f"WARNING sync finished with failed stream(s): {', '.join(failed) or '(unknown)'}. "
                f"Streams synced normally: {', '.join(ok) or '(none)'}."
            )
            raise

    def _discover_apps_by_vendor(
        self, vendors: List[Dict]
    ) -> Dict[str, List[Dict]]:
        """
        Discover apps for each vendor using that vendor's own credentials.
        Returns {vendor_id: [{app_id, app_name, bundle_id}]}.
        """
        apps_by_vendor: Dict[str, List[Dict]] = {}
        for vendor in vendors:
            vid = vendor["vendor_id"]
            try:
                client = AppStoreClient(
                    key_id=vendor["key_id"],
                    issuer_id=vendor["issuer_id"],
                    private_key=vendor["private_key"],
                )
                apps_by_vendor[vid] = client.list_apps()
            except Exception as exc:
                logger.warning(f"Could not discover apps for vendor {vid}: {exc}")
                apps_by_vendor[vid] = []
        return apps_by_vendor

    def _common_kwargs(
        self, config: Mapping[str, Any], apps_by_vendor: Dict[str, List[Dict]]
    ) -> Dict:
        """Common kwargs passed to every stream constructor."""
        return dict(
            vendors         = config["vendors"],
            apps_by_vendor  = apps_by_vendor,
            start_date      = config["start_date"],
            lookback_days   = _to_int(config.get("lookback_days"), DEFAULT_LOOKBACK_DAYS),
            get_last_x_days = _to_bool(config.get("get_last_x_days"), DEFAULT_GET_LAST_X_DAYS),
            timezone        = config.get("timezone") or DEFAULT_TIMEZONE,
        )

    def check_connection(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate credentials for every vendor by listing its apps.
        Each vendor uses its own key_id / issuer_id / private_key.

        Moi vendor deu duoc thu (khong dung o vendor dau tien loi).
        - Vendor bi Apple tu choi (401 / 403, AppStoreAuthError): ghi log, thu tiep
          vendor sau. Neu con it nhat 1 vendor goi duoc thi check PASS, de sync
          van chay cho cac tai khoan lanh; sync se bo qua vendor loi roi ket thuc
          o trang thai failed va neu ten vendor do (xem streams.py).
        - Moi vendor deu bi tu choi: check FAIL, liet ke tung vendor.
        - Loi khac (thieu truong cau hinh, khoa sai dinh dang, loi mang...): check
          FAIL ngay nhu truoc.
        """
        vendors = config.get("vendors", [])
        if not vendors:
            return False, "No vendors configured. Add at least one vendor."

        ok_count = 0
        rejected: List[str] = []

        for vendor in vendors:
            vid   = vendor.get("vendor_id", "?")
            vname = vendor.get("vendor_name", vid)
            try:
                client = AppStoreClient(
                    key_id=vendor["key_id"],
                    issuer_id=vendor["issuer_id"],
                    private_key=vendor["private_key"],
                )
                apps = client.list_apps()
                logger.info(f"Vendor '{vname}' ({vid}) OK — found {len(apps)} app(s).")
                ok_count += 1
            except KeyError as exc:
                return False, f"Vendor '{vname}' is missing required field: {exc}"
            except AppStoreAuthError as exc:
                message = f"Vendor '{vname}' ({vid}) failed: {exc}"
                logger.error(message)
                rejected.append(message)
            except Exception as exc:
                return False, f"Vendor '{vname}' ({vid}) failed: {exc}"

        if ok_count == 0:
            return False, " || ".join(rejected)

        if rejected:
            logger.warning(
                f"{len(rejected)}/{len(vendors)} vendor(s) rejected by Apple. Check passes so the "
                f"other {ok_count} vendor(s) keep syncing; the sync will skip the rejected "
                f"vendor(s) and end as failed until Apple access is restored."
            )

        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Returns all stream instances.
        Apps are discovered per vendor here and shared across analytics streams.
        Discovery failures are tolerated (empty app list) so `discover` works
        even with placeholder credentials.
        """
        try:
            apps_by_vendor = self._discover_apps_by_vendor(config["vendors"])
        except Exception:
            apps_by_vendor = {}

        kwargs = self._common_kwargs(config, apps_by_vendor)

        return [
            SummarySalesStream(**kwargs),
            FinancialReportStream(**kwargs),
            FinancialReportDetailStream(**kwargs),
            AppInstallationsStream(**kwargs),
            AppSessionsStream(**kwargs),
        ]