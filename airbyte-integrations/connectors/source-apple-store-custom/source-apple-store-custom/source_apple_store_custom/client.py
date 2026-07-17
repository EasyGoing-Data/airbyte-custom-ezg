"""
Apple App Store Connect API client.
Handles JWT auth, rate limiting, and all 3 API types:
  - Sales API  (/v1/salesReports)
  - Finance API (/v1/financeReports)
  - Analytics API (/v1/analyticsReportRequests — 5-step flow)
"""

import gzip
import io
import logging
import time
from typing import Any, Dict, Generator, List, Optional, Tuple

import jwt
import requests

logger = logging.getLogger("airbyte")

BASE_URL          = "https://api.appstoreconnect.apple.com"
JWT_AUDIENCE      = "appstoreconnect-v1"
JWT_TTL           = 1200         # 20 minutes max
RATE_LIMIT_HEADER = "X-Rate-Limit"
THROTTLE_AT       = 0.10         # slow down when < 10% quota remains
CRITICAL_AT       = 50           # hard sleep when < 50 requests remain
MAX_RETRIES       = 5
BACKOFF_BASE      = 2            # exponential backoff base (seconds)


class AppStoreClient:
    def __init__(self, key_id: str, issuer_id: str, private_key: str):
        self._key_id      = key_id
        self._issuer_id   = issuer_id
        self._private_key = (
            private_key.encode() if isinstance(private_key, str) else private_key
        )

    # ─── JWT ─────────────────────────────────────────────────────────────────

    def _generate_jwt(self) -> str:
        """Generate a fresh ES256 JWT. Called before every request (Cách A)."""
        now = int(time.time())
        payload = {
            "iss": self._issuer_id,
            "iat": now,
            "exp": now + JWT_TTL,
            "aud": JWT_AUDIENCE,
        }
        return jwt.encode(
            payload,
            self._private_key,
            algorithm="ES256",
            headers={"kid": self._key_id},
        )

    def _auth_headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self._generate_jwt()}",
            "Content-Type": "application/json",
        }

    # ─── Rate Limit ──────────────────────────────────────────────────────────

    def _parse_rate_limit(self, response: requests.Response) -> Tuple[int, int]:
        """Parse X-Rate-Limit: user-hour-lim:3500;user-hour-rem:500;"""
        header = response.headers.get(RATE_LIMIT_HEADER, "")
        parts: Dict[str, int] = {}
        for segment in header.split(";"):
            if ":" in segment:
                k, v = segment.strip().split(":", 1)
                try:
                    parts[k.strip()] = int(v.strip())
                except ValueError:
                    pass
        limit     = parts.get("user-hour-lim", 3500)
        remaining = parts.get("user-hour-rem", limit)
        return limit, remaining

    def _throttle_if_needed(self, limit: int, remaining: int) -> None:
        if remaining <= CRITICAL_AT:
            logger.warning(
                f"Rate limit critical: {remaining}/{limit} remaining. Sleeping 60s."
            )
            time.sleep(60)
        elif remaining < limit * THROTTLE_AT:
            sleep_sec = min(1800 / max(remaining, 1), 10)
            logger.info(
                f"Rate limit low ({remaining}/{limit}). Throttling {sleep_sec:.1f}s/req."
            )
            time.sleep(sleep_sec)

    # ─── HTTP helpers ─────────────────────────────────────────────────────────

    def _get(
        self,
        url: str,
        params: Optional[Dict] = None,
        allow_404: bool = False,
    ) -> requests.Response:
        """GET with retry + rate-limit handling."""
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(
                    url, headers=self._auth_headers(), params=params, timeout=60
                )
                limit, remaining = self._parse_rate_limit(resp)
                self._throttle_if_needed(limit, remaining)

                if resp.status_code == 200:
                    return resp
                if resp.status_code in (404, 410) and allow_404:
                    # 404 = no report for this date; 410 = date older than
                    # Apple's retention window (Sales API keeps ~365 days).
                    # Both mean "no data" → caller treats as empty, not error.
                    return resp
                if resp.status_code == 429:
                    wait = BACKOFF_BASE ** attempt
                    logger.warning(f"429 RATE_LIMIT_EXCEEDED. Waiting {wait}s (attempt {attempt+1}).")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()

            except requests.exceptions.RequestException as exc:
                if attempt < MAX_RETRIES - 1:
                    wait = BACKOFF_BASE ** attempt
                    logger.warning(f"Request error: {exc}. Retry in {wait}s.")
                    time.sleep(wait)
                else:
                    raise

        raise RuntimeError(f"GET failed after {MAX_RETRIES} retries: {url}")

    def _post(self, url: str, body: Dict, allow_403: bool = False) -> requests.Response:
        """POST with retry."""
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.post(
                    url, headers=self._auth_headers(), json=body, timeout=60
                )
                if resp.status_code in (200, 201):
                    return resp
                if resp.status_code == 403 and allow_403:
                    # Analytics requires Admin role to create report requests.
                    # If the key lacks it, skip gracefully instead of crashing.
                    return resp
                if resp.status_code == 429:
                    wait = BACKOFF_BASE ** attempt
                    logger.warning(f"429 RATE_LIMIT_EXCEEDED on POST. Waiting {wait}s.")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()

            except requests.exceptions.RequestException as exc:
                if attempt < MAX_RETRIES - 1:
                    wait = BACKOFF_BASE ** attempt
                    logger.warning(f"POST error: {exc}. Retry in {wait}s.")
                    time.sleep(wait)
                else:
                    raise

        raise RuntimeError(f"POST failed after {MAX_RETRIES} retries: {url}")

    # ─── Gzip TSV parser ──────────────────────────────────────────────────────

    def parse_gzip_tsv(self, content: bytes) -> List[Dict[str, Optional[str]]]:
        """
        Parse gzip TSV bytes into list of dicts.
        Đã sửa lỗi dính ký tự \r và chặn đứng Section 2 bằng dòng trống + số lượng cột.
        """
        if not content:
            return []

        for encoding in ("utf-8-sig", "utf-16"):
            try:
                with gzip.open(io.BytesIO(content), "rt", encoding=encoding) as f:
                    lines = f.readlines()
                break
            except Exception:
                continue
        else:
            logger.warning("Could not decode gzip TSV with any known encoding.")
            return []

        if not lines:
            return []

        # 👉 ĐOẠN 1: Sửa rstrip("\r\n") cho Header để cột cuối cùng không bị dính ký tự ẩn \r
        headers = lines[0].rstrip("\r\n").split("\t")
        expected_cols = len(headers)
        records: List[Dict] = []

        for line in lines[1:]:
            # 👉 ĐOẠN 2: Làm sạch \r\n ở đầu mỗi dòng dữ liệu
            clean_line = line.rstrip("\r\n")
            
            # 👉 ĐOẠN 3: Dừng hẳn khi gặp dòng trống phân cách giữa 2 Section (Cách của bạn)
            if not clean_line.strip():
                break             
            
            values = clean_line.split("\t")
            
            # 👉 ĐOẠN 4: Lớp bảo hiểm phòng hờ nếu Apple bỏ dòng trống (Lệch số cột là dừng luôn)
            if len(values) != expected_cols:
                break

            # Map dữ liệu an toàn dựa trên số lượng cột tiêu chuẩn của Header
            record = {
                headers[i]: (values[i].strip() if i < len(values) and values[i] is not None else None)
                for i in range(expected_cols)
            }
            records.append(record)

        return records

    # ─── App Discovery ────────────────────────────────────────────────────────

    def list_apps(self) -> List[Dict[str, str]]:
        """
        Discover all apps in the account (paginated).
        Returns list of {app_id, app_name, bundle_id}.
        """
        apps: List[Dict] = []
        url: Optional[str] = f"{BASE_URL}/v1/apps"
        params: Optional[Dict] = {"limit": 200, "fields[apps]": "name,bundleId"}

        while url:
            resp   = self._get(url, params=params)
            data   = resp.json()
            for item in data.get("data", []):
                attrs = item.get("attributes", {})
                apps.append({
                    "app_id":    item["id"],
                    "app_name":  attrs.get("name", ""),
                    "bundle_id": attrs.get("bundleId", ""),
                })
            url    = data.get("links", {}).get("next")
            params = None   # next URL already contains params

        logger.info(f"Discovered {len(apps)} apps in account.")
        return apps

    # ─── Sales API ────────────────────────────────────────────────────────────

    def fetch_sales_report(self, vendor_id: str, date: str) -> bytes:
        """
        Fetch daily Summary Sales report (gzip TSV).
        date: YYYY-MM-DD
        Returns raw bytes; empty b"" if no data for that date.
        Ref: https://developer.apple.com/documentation/appstoreconnectapi/get-v1-salesreports
        """
        resp = self._get(
            f"{BASE_URL}/v1/salesReports",
            params={
                "filter[vendorNumber]": vendor_id,
                "filter[reportType]":   "SALES",
                "filter[reportSubType]":"SUMMARY",
                "filter[frequency]":    "DAILY",
                "filter[reportDate]":   date,
            },
            allow_404=True,
        )
        return resp.content if resp.status_code == 200 else b""

    # ─── Finance API ──────────────────────────────────────────────────────────

    def fetch_finance_report(self, vendor_id: str, year_month: str) -> bytes:
        """
        Fetch monthly Financial report (gzip TSV).
        year_month: YYYY-MM
        Returns raw bytes; empty b"" if no data.
        Ref: https://developer.apple.com/documentation/appstoreconnectapi/get-v1-financereports
        """
        resp = self._get(
            f"{BASE_URL}/v1/financeReports",
            params={
                "filter[vendorNumber]": vendor_id,
                "filter[reportType]":   "FINANCIAL",
                "filter[regionCode]":   "ZZ",
                "filter[reportDate]":   year_month,
            },
            allow_404=True,
        )
        return resp.content if resp.status_code == 200 else b""

    # ─── Analytics API — 5-step flow ─────────────────────────────────────────

    def create_analytics_request(self, app_id: str, access_type: str) -> Optional[str]:
        """
        Step 1 — Create ONGOING or ONE_TIME_SNAPSHOT request for an app.
        access_type: "ONGOING" | "ONE_TIME_SNAPSHOT"
        Returns request_id (stable, reuse forever for ONGOING), or None if the
        API key lacks Admin role (403) — analytics streams are skipped then.
        Ref: https://developer.apple.com/documentation/appstoreconnectapi/post-v1-analyticsreportrequests
        """
        resp = self._post(
            f"{BASE_URL}/v1/analyticsReportRequests",
            body={
                "data": {
                    "type": "analyticsReportRequests",
                    "attributes": {"accessType": access_type},
                    "relationships": {
                        "app": {"data": {"type": "apps", "id": app_id}}
                    },
                }
            },
            allow_403=True,
        )
        if resp.status_code == 403:
            logger.warning(
                f"403 Forbidden creating {access_type} analytics request for app "
                f"{app_id}. The API key likely lacks Admin role — skipping "
                f"analytics streams. Sales/Finance streams are unaffected."
            )
            return None
        request_id = resp.json()["data"]["id"]
        logger.info(f"Created {access_type} request {request_id} for app {app_id}.")
        return request_id

    def get_report_id(self, request_id: str, report_name: str) -> Optional[str]:
        """
        Step 2 — Get report_id for a named report within a request.
        report_name: "APP_INSTALLS" | "APP_SESSIONS"
        Returns None if not yet available (e.g. still generating).
        """
        resp = self._get(
            f"{BASE_URL}/v1/analyticsReportRequests/{request_id}/reports",
            params={"filter[name]": report_name},
            allow_404=True,
        )
        if resp.status_code != 200:
            return None
        data = resp.json().get("data", [])
        return data[0]["id"] if data else None

    def get_instance_ids(self, report_id: str, date: str) -> List[str]:
        """
        Step 3 — Get instance IDs for a specific date (DAILY granularity).
        date: YYYY-MM-DD
        Returns empty list if no data available for that date yet.
        """
        resp = self._get(
            f"{BASE_URL}/v1/analyticsReports/{report_id}/instances",
            params={
                "filter[granularity]":    "DAILY",
                "filter[processingDate]": date,
            },
            allow_404=True,
        )
        if resp.status_code != 200:
            return []
        return [item["id"] for item in resp.json().get("data", [])]

    def get_segment_urls(self, instance_id: str) -> List[str]:
        """
        Step 4 — Get pre-signed download URLs for all segments of an instance.
        Returns empty list if none.
        """
        resp = self._get(
            f"{BASE_URL}/v1/analyticsReportInstances/{instance_id}/segments",
            allow_404=True,
        )
        if resp.status_code != 200:
            return []
        urls = []
        for seg in resp.json().get("data", []):
            url = seg.get("attributes", {}).get("url")
            if url:
                urls.append(url)
        return urls

    def download_segment(self, url: str) -> bytes:
        """
        Step 5 — Download segment from pre-signed S3 URL (no auth needed).
        Returns raw gzip bytes.
        """
        for attempt in range(MAX_RETRIES):
            try:
                resp = requests.get(url, timeout=120)
                if resp.status_code == 200:
                    return resp.content
                if attempt < MAX_RETRIES - 1:
                    time.sleep(BACKOFF_BASE ** attempt)
            except requests.exceptions.RequestException as exc:
                logger.warning(f"Download error: {exc}. Retry {attempt+1}.")
                time.sleep(BACKOFF_BASE ** attempt)
        return b""

    def fetch_analytics_data(
        self,
        app_id: str,
        report_name: str,
        date: str,
        ongoing_request_id: Optional[str],
        snapshot_request_id: Optional[str],
        snapshot_done: bool,
    ) -> Tuple[List[Dict], str, Optional[str], bool]:
        """
        Full 5-step Analytics flow for one app + date.

        Returns:
            records              — parsed rows for this date
            ongoing_request_id   — created/reused ONGOING ID
            snapshot_request_id  — created/reused SNAPSHOT ID (or None)
            snapshot_done        — True when backfill is complete
        """
        records: List[Dict] = []

        # ── Ensure ONGOING request exists ────────────────────────────────────
        if not ongoing_request_id:
            ongoing_request_id = self.create_analytics_request(app_id, "ONGOING")

        # If we still have no ONGOING request, the key lacks Admin role (403).
        # Skip analytics entirely — return empty, leave state untouched.
        if not ongoing_request_id:
            return [], None, snapshot_request_id, snapshot_done

        # ── Create ONE_TIME_SNAPSHOT for backfill (once) ─────────────────────
        if not snapshot_done and not snapshot_request_id:
            snapshot_request_id = self.create_analytics_request(
                app_id, "ONE_TIME_SNAPSHOT"
            )

        # ── Fetch from ONGOING ────────────────────────────────────────────────
        ongoing_report_id = self.get_report_id(ongoing_request_id, report_name)
        if ongoing_report_id:
            for iid in self.get_instance_ids(ongoing_report_id, date):
                for url in self.get_segment_urls(iid):
                    records.extend(self.parse_gzip_tsv(self.download_segment(url)))

        # ── Fetch from SNAPSHOT (backfill) ────────────────────────────────────
        if not snapshot_done and snapshot_request_id:
            snap_report_id = self.get_report_id(snapshot_request_id, report_name)
            if snap_report_id:
                snap_instances = self.get_instance_ids(snap_report_id, date)
                if snap_instances:
                    for iid in snap_instances:
                        for url in self.get_segment_urls(iid):
                            records.extend(
                                self.parse_gzip_tsv(self.download_segment(url))
                            )
                else:
                    # No instances for this date from snapshot → backfill complete
                    snapshot_done = True
                    logger.info(
                        f"ONE_TIME_SNAPSHOT complete for app {app_id} / {report_name}."
                    )

        return records, ongoing_request_id, snapshot_request_id, snapshot_done