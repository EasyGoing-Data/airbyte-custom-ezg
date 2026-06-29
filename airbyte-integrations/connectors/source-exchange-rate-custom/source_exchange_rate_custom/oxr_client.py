import time
import logging

import requests

logger = logging.getLogger("airbyte")

BASE_URL = "https://openexchangerates.org/api"
MAX_RETRIES = 5
BACKOFF_BASE = 2  # seconds
THROTTLE_SECONDS = 1  # between requests during backfill


class OxrClient:
    def __init__(self, app_id: str):
        self._app_id = app_id
        self._session = requests.Session()

    def get_historical(self, date_str: str) -> dict:
        """Fetch exchange rates for a specific date (YYYY-MM-DD). Base = USD."""
        url = f"{BASE_URL}/historical/{date_str}.json"
        return self._get(url)

    def get_latest(self) -> dict:
        """Fetch latest exchange rates. Base = USD."""
        url = f"{BASE_URL}/latest.json"
        return self._get(url)

    def _get(self, url: str) -> dict:
        params = {"app_id": self._app_id, "base": "USD"}
        last_exc = None

        for attempt in range(MAX_RETRIES):
            try:
                resp = self._session.get(url, params=params, timeout=30)

                if resp.status_code == 200:
                    return resp.json()

                if resp.status_code in (429, 500, 502, 503, 504):
                    wait = BACKOFF_BASE ** attempt
                    logger.warning(
                        f"HTTP {resp.status_code} from {url} — retry {attempt + 1}/{MAX_RETRIES} in {wait}s"
                    )
                    time.sleep(wait)
                    continue

                # 4xx non-retryable
                resp.raise_for_status()

            except requests.exceptions.RequestException as e:
                wait = BACKOFF_BASE ** attempt
                last_exc = e
                logger.warning(f"Request error: {e} — retry {attempt + 1}/{MAX_RETRIES} in {wait}s")
                time.sleep(wait)

        raise RuntimeError(
            f"Failed to fetch {url} after {MAX_RETRIES} retries. Last error: {last_exc}"
        )

    def throttle(self):
        """Call between requests during backfill to avoid burst rate limiting."""
        time.sleep(THROTTLE_SECONDS)
