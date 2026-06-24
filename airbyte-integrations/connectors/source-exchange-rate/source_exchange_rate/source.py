"""source-exchange-rate (Frankfurter) — AbstractSource entrypoint."""
from __future__ import annotations

from typing import Any, List, Mapping, Optional, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream

from .frankfurter_client import FrankfurterClient
from .streams import ExchangeRates


class SourceExchangeRate(AbstractSource):
    def check_connection(self, logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        try:
            client = FrankfurterClient(
                base=(config.get("base") or "USD"),
                symbols=config.get("symbols"),
            )
            data = client.latest()
            if not data.get("rates"):
                return False, "Frankfurter trả về rỗng — kiểm tra base/symbols."
        except Exception as e:  # noqa: BLE001
            return False, f"Không gọi được Frankfurter API: {e}"
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        # KHÔNG dựng client ở đây (discover không cần). Stream tự dựng lazy.
        return [
            ExchangeRates(
                base=config.get("base") or "USD",
                symbols=config.get("symbols"),
                start_date=config.get("start_date"),
                lookback_days=config.get("lookback_days", 7),
            )
        ]
