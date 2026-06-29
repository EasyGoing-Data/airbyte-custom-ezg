"""
SourceAppleAppStore — AbstractSource implementation.

check_connection(): validates each vendor's JWT + discovers its apps.
streams(): returns stream instances with per-vendor lazy clients and
           apps discovered per vendor.
"""

import logging
from typing import Any, Dict, List, Mapping, Optional, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .client import AppStoreClient
from .streams import (
    AppInstallationsStream,
    AppSessionsStream,
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
        """
        vendors = config.get("vendors", [])
        if not vendors:
            return False, "No vendors configured. Add at least one vendor."

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
            except KeyError as exc:
                return False, f"Vendor '{vname}' is missing required field: {exc}"
            except Exception as exc:
                return False, f"Vendor '{vname}' ({vid}) failed: {exc}"

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
            AppInstallationsStream(**kwargs),
            AppSessionsStream(**kwargs),
        ]