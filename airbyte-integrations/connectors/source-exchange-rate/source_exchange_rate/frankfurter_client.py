"""HTTP client gọi Frankfurter API (REST public, không auth)."""
from __future__ import annotations

import time
from typing import Any, Dict, Mapping, Optional

import requests

BASE_URL = "https://api.frankfurter.dev/v1"


class FrankfurterClient:
    def __init__(self, base: str = "USD", symbols: Optional[str] = None,
                 timeout: int = 30, max_retries: int = 3) -> None:
        self._base = base.upper()
        self._symbols = symbols
        self._timeout = timeout
        self._max_retries = max_retries
        self._session = requests.Session()

    def _get(self, path: str, params: Mapping[str, Any]) -> Dict[str, Any]:
        url = f"{BASE_URL}/{path}"
        last_err: Optional[Exception] = None
        for attempt in range(self._max_retries):
            try:
                resp = self._session.get(url, params=params, timeout=self._timeout)
                resp.raise_for_status()
                return resp.json()
            except Exception as e:  # noqa: BLE001
                last_err = e
                time.sleep(2 ** attempt)   # backoff 1,2,4s
        raise RuntimeError(f"Frankfurter API lỗi sau {self._max_retries} lần thử: {last_err}")

    def latest(self) -> Dict[str, Any]:
        """Dùng cho check_connection: lấy rate mới nhất."""
        params: Dict[str, Any] = {"base": self._base}
        if self._symbols:
            params["symbols"] = self._symbols
        return self._get("latest", params)

    def time_series(self, start: str, end: str) -> Dict[str, Any]:
        """Lấy time series [start..end]. Trả về dict {amount, base, start_date, end_date, rates{date:{cur:rate}}}."""
        params: Dict[str, Any] = {"base": self._base}
        if self._symbols:
            params["symbols"] = self._symbols
        return self._get(f"{start}..{end}", params)
