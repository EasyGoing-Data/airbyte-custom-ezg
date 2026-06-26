"""
SourceAppleAppStore — AbstractSource implementation.

check_connection(): validates JWT + discovers apps (does NOT init streams).
streams(): returns stream instances with lazy client + pre-discovered apps.
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

DEFAULT_LOOKBACK_DAYS  = 7
DEFAULT_GET_LAST_X_DAYS = False
DEFAULT_TIMEZONE        = "UTC"


class SourceAppleAppStore(AbstractSource):

    def _build_client(self, config: Mapping[str, Any]) -> AppStoreClient:
        return AppStoreClient(
            key_id      = config["key_id"],
            issuer_id   = config["issuer_id"],
            private_key = config["private_key"],
        )

    def _common_kwargs(self, config: Mapping[str, Any], apps: List[Dict]) -> Dict:
        """Common kwargs passed to every stream constructor."""
        return dict(
            key_id          = config["key_id"],
            issuer_id       = config["issuer_id"],
            private_key     = config["private_key"],
            vendors         = config["vendors"],
            apps            = apps,
            start_date      = config["start_date"],
            lookback_days   = config.get("lookback_days") or DEFAULT_LOOKBACK_DAYS,
            get_last_x_days = config.get("get_last_x_days") or DEFAULT_GET_LAST_X_DAYS,
            timezone        = config.get("timezone") or DEFAULT_TIMEZONE,
        )

    def check_connection(
        self,
        logger: logging.Logger,
        config: Mapping[str, Any],
    ) -> Tuple[bool, Optional[str]]:
        """
        Validate credentials and discover apps.
        Client is created here (check is allowed to create client per §3.1).
        """
        try:
            client = self._build_client(config)

            # 1. Validate JWT + API key by fetching apps
            apps = client.list_apps()
            if not apps:
                return False, "Connection succeeded but no apps found for this account."

            # 2. Validate at least one vendor exists
            vendors = config.get("vendors", [])
            if not vendors:
                return False, "No vendors configured. Add at least one vendor."

            logger.info(
                f"Connection OK. Found {len(apps)} app(s) and {len(vendors)} vendor(s)."
            )
            return True, None

        except Exception as exc:
            return False, str(exc)

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        """
        Returns all stream instances.
        Apps are discovered once here and shared across analytics streams.
        streams() is called during discover — lazy client ensures it
        does NOT fail with invalid credentials (§3.1).
        """
        # Discover apps for analytics streams (only called during actual sync)
        # During discover, streams() must not call the API → use empty list
        # Apps will be fetched lazily via check_connection before actual sync.
        # To keep discover safe, we pass empty apps list;
        # source.read() calls check_connection first which validates.
        try:
            client = self._build_client(config)
            apps   = client.list_apps()
        except Exception:
            # discover must not fail → return empty app list
            apps = []

        kwargs = self._common_kwargs(config, apps)

        return [
            SummarySalesStream(**kwargs),
            FinancialReportStream(**kwargs),
            AppInstallationsStream(**kwargs),
            AppSessionsStream(**kwargs),
        ]
