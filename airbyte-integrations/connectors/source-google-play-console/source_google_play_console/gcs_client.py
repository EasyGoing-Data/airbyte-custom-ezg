"""Wrapper quanh google-cloud-storage cho Google Play reporting buckets.

Một service account đọc bucket pubsite_prod_* của mọi store. Chỉ READ.
"""
from __future__ import annotations

import io
import json
import logging
import zipfile
from typing import Any, Iterable, List

from google.cloud import storage
from google.oauth2 import service_account


# --- Tắt cảnh báo "Regional Access Boundary ... Precondition check failed" ---
class _SuppressRABFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            return "Regional Access Boundary" not in record.getMessage()
        except Exception:
            return True


def _silence_rab_warning() -> None:
    _filter = _SuppressRABFilter()
    for name in ("", "google", "google.cloud.storage", "google.api_core", "google.auth"):
        lg = logging.getLogger(name)
        lg.addFilter(_filter)
        if name.startswith("google"):
            lg.setLevel(logging.ERROR)


_silence_rab_warning()


class GCSClient:
    def __init__(self, service_account_json: str) -> None:
        info = json.loads(service_account_json)
        creds = service_account.Credentials.from_service_account_info(info)
        self._client = storage.Client(credentials=creds, project=info.get("project_id"))

    def list_blobs(self, bucket: str, prefix: str) -> Iterable[Any]:
        """Trả về các blob (có .name, .updated) dưới prefix."""
        return self._client.list_blobs(bucket, prefix=prefix)

    def download_text(
        self, bucket: str, blob_name: str, encoding: str, is_zip: bool = False
    ) -> str:
        """Tải blob -> (giải nén nếu zip) -> decode thành text.

        - stats/reviews: encoding='utf-16'
        - sales/earnings: file nằm trong .zip, CSV bên trong là 'utf-8-sig'
        Có fallback nếu encoding chỉ định thất bại.
        """
        raw = self._client.bucket(bucket).blob(blob_name).download_as_bytes()
        if is_zip:
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                names = [n for n in zf.namelist() if n.lower().endswith(".csv")] or zf.namelist()
                raw = zf.read(names[0])
        return self._decode(raw, encoding)

    @staticmethod
    def _decode(data: bytes, primary: str) -> str:
        candidates: List[str] = [primary, "utf-16", "utf-8-sig", "utf-8"]
        seen = set()
        for enc in candidates:
            if enc in seen:
                continue
            seen.add(enc)
            try:
                return data.decode(enc)
            except UnicodeDecodeError:
                continue
        return data.decode("utf-8", errors="replace")
