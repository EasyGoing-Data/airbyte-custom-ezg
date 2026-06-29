import logging
from typing import Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource

from .oxr_client import OxrClient
from .streams import ExchangeRates

logger = logging.getLogger("airbyte")


class SourceExchangeRateCustom(AbstractSource):

    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, Any]:
        try:
            client = OxrClient(config["app_id"])
            data = client.get_latest()
            if "rates" not in data:
                return False, f"Unexpected response: {data}"
            return True, None
        except Exception as e:
            return False, str(e)

    def streams(self, config: Mapping[str, Any]) -> List:
        common = dict(
            app_id=config["app_id"],
            start_date=config.get("start_date"),
            lookback_days=config.get("lookback_days"),
        )
        return [ExchangeRates(**common)]
