"""
Test ban 0.9.4: 1 tai khoan Apple bi tu choi (401 / 403) khong chan tai khoan con lai.

Khong goi Apple that: requests.get duoc thay bang ham gia, phan biet vendor qua
header Authorization (thay _auth_headers de khong can khoa ES256 that).

Chay (tu thu muc connector):
    python -m pytest unit_tests -q
"""

import gzip
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest
import requests
from airbyte_cdk.models import (
    AirbyteMessageSerializer,
    ConfiguredAirbyteCatalogSerializer,
)
from airbyte_cdk.utils.traced_exception import AirbyteTracedException

from source_apple_store_custom import client as client_module
from source_apple_store_custom.client import AppStoreAuthError, AppStoreClient
from source_apple_store_custom.source import SourceAppleAppStore

LOGGER = logging.getLogger("airbyte")

BAD  = {"vendor_id": "111", "vendor_name": "Bad Vendor", "key_id": "BADKEY",
        "issuer_id": "issuer-bad", "private_key": "not-a-real-key"}
GOOD = {"vendor_id": "222", "vendor_name": "Good Vendor", "key_id": "GOODKEY",
        "issuer_id": "issuer-good", "private_key": "not-a-real-key"}

AGREEMENT_CODE = "FORBIDDEN.REQUIRED_AGREEMENTS_MISSING_OR_EXPIRED"


def _yesterday() -> str:
    return (datetime.now(timezone.utc).date() - timedelta(days=1)).strftime("%Y-%m-%d")


def _config(vendors):
    start = (datetime.now(timezone.utc).date() - timedelta(days=3)).strftime("%Y-%m-%d")
    return {"vendors": vendors, "start_date": start, "lookback_days": 1,
            "get_last_x_days": False, "timezone": "UTC"}


class FakeResponse:
    def __init__(self, status_code, json_body=None, content=b"", url=""):
        self.status_code = status_code
        self._json       = json_body
        self.content     = content
        self.text        = "" if json_body is None else str(json_body)
        self.headers     = {}
        self.url         = url

    def json(self):
        if self._json is None:
            raise ValueError("no json")
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.exceptions.HTTPError(
                f"{self.status_code} Client Error for url: {self.url}", response=self
            )


def _sales_tsv(date_iso: str) -> bytes:
    us_date = datetime.strptime(date_iso, "%Y-%m-%d").strftime("%m/%d/%Y")
    text = "Begin Date\tSKU\tUnits\n" f"{us_date}\tsku.one\t5\n"
    return gzip.compress(text.encode("utf-8"))


class FakeApple:
    """Gia lap Apple: vendor co key trong `rejected` luon bi 403, con lai tra du lieu."""

    def __init__(self, rejected=("BADKEY",), rejected_status=403):
        self.rejected        = set(rejected)
        self.rejected_status = rejected_status
        self.calls           = []          # [(key, path)]

    def get(self, url, headers=None, params=None, timeout=None):
        key  = (headers or {}).get("Authorization", "").replace("Bearer ", "")
        path = url.replace(client_module.BASE_URL, "")
        self.calls.append((key, path))

        if key in self.rejected:
            return FakeResponse(
                self.rejected_status,
                json_body={"errors": [{
                    "status": str(self.rejected_status),
                    "code":   AGREEMENT_CODE,
                    "title":  "A required agreement is missing or has expired.",
                    "detail": "This request requires an in-effect agreement.",
                }]},
                url=url,
            )
        if path == "/v1/apps":
            return FakeResponse(200, json_body={
                "data": [{"id": "999", "attributes": {"name": "App", "bundleId": "com.app"}}],
                "links": {},
            })
        if path == "/v1/salesReports":
            day = (params or {}).get("filter[reportDate]")
            if day == _yesterday():
                return FakeResponse(200, content=_sales_tsv(day))
            return FakeResponse(404, url=url)
        if path == "/v1/financeReports":
            return FakeResponse(404, url=url)
        raise AssertionError(f"unexpected url {url}")

    def calls_for(self, key):
        return [path for k, path in self.calls if k == key]


@pytest.fixture
def no_sleep():
    with patch.object(client_module.time, "sleep") as sleep:
        yield sleep


@pytest.fixture
def apple(no_sleep):
    fake = FakeApple()
    with patch.object(client_module.requests, "get", side_effect=fake.get), \
         patch.object(AppStoreClient, "_auth_headers",
                      lambda self: {"Authorization": f"Bearer {self._key_id}"}):
        yield fake


def _catalog(*names):
    return ConfiguredAirbyteCatalogSerializer.load({"streams": [
        {"stream": {"name": name, "json_schema": {},
                    "supported_sync_modes": ["full_refresh", "incremental"]},
         "sync_mode": "incremental", "destination_sync_mode": "append"}
        for name in names
    ]})


def _run_read(config, catalog):
    """Chay source.read() that cua CDK; tra ve (messages dang dict, exception hoac None)."""
    messages, error = [], None
    try:
        for message in SourceAppleAppStore().read(LOGGER, config, catalog, state=[]):
            messages.append(AirbyteMessageSerializer.dump(message))
    except AirbyteTracedException as exc:
        error = exc
    return messages, error


def _records(messages, stream):
    return [m["record"]["data"] for m in messages
            if m["type"] == "RECORD" and m["record"]["stream"] == stream]


def _states(messages, stream):
    return [m["state"]["stream"]["stream_state"] for m in messages
            if m["type"] == "STATE"
            and m["state"]["stream"]["stream_descriptor"]["name"] == stream]


def _statuses(messages, stream):
    return [m["trace"]["stream_status"]["status"] for m in messages
            if m["type"] == "TRACE" and m["trace"]["type"] == "STREAM_STATUS"
            and m["trace"]["stream_status"]["stream_descriptor"]["name"] == stream]


# ─── client: 401 / 403 khong retry, co ma loi cua Apple ───────────────────────

@pytest.mark.parametrize("status", [401, 403])
def test_auth_error_is_not_retried_and_carries_apple_code(no_sleep, status):
    fake = FakeApple(rejected_status=status)
    with patch.object(client_module.requests, "get", side_effect=fake.get), \
         patch.object(AppStoreClient, "_auth_headers",
                      lambda self: {"Authorization": f"Bearer {self._key_id}"}):
        client = AppStoreClient("BADKEY", "issuer", "key")
        with pytest.raises(AppStoreAuthError) as raised:
            client.list_apps()

    assert len(fake.calls) == 1                 # 0.9.3: 5 lan goi
    no_sleep.assert_not_called()
    assert raised.value.status_code == status
    assert AGREEMENT_CODE in str(raised.value)
    assert "?" not in raised.value.url          # khong kem query string


def test_server_error_is_still_retried_and_body_is_logged(no_sleep, caplog):
    calls = []

    def always_500(url, headers=None, params=None, timeout=None):
        calls.append(url)
        return FakeResponse(500, json_body={"errors": [{"code": "UNEXPECTED_ERROR"}]}, url=url)

    with patch.object(client_module.requests, "get", side_effect=always_500), \
         patch.object(AppStoreClient, "_auth_headers", lambda self: {}), \
         caplog.at_level(logging.WARNING, logger="airbyte"):
        with pytest.raises(requests.exceptions.HTTPError):
            AppStoreClient("K", "issuer", "key").list_apps()

    assert len(calls) == client_module.MAX_RETRIES
    assert "UNEXPECTED_ERROR" in caplog.text


def test_report_404_is_still_treated_as_no_data(apple):
    client = AppStoreClient("GOODKEY", "issuer", "key")
    assert client.fetch_sales_report("222", "2020-01-01") == b""


# ─── check_connection ─────────────────────────────────────────────────────────

def test_check_passes_when_one_vendor_rejected_and_tries_every_vendor(apple):
    ok, error = SourceAppleAppStore().check_connection(LOGGER, _config([BAD, GOOD]))

    assert (ok, error) == (True, None)
    assert apple.calls_for("BADKEY") == ["/v1/apps"]
    assert apple.calls_for("GOODKEY") == ["/v1/apps"]   # 0.9.3: khong duoc thu


def test_check_fails_when_every_vendor_rejected(no_sleep):
    fake = FakeApple(rejected=("BADKEY", "GOODKEY"))
    with patch.object(client_module.requests, "get", side_effect=fake.get), \
         patch.object(AppStoreClient, "_auth_headers",
                      lambda self: {"Authorization": f"Bearer {self._key_id}"}):
        ok, error = SourceAppleAppStore().check_connection(LOGGER, _config([BAD, GOOD]))

    assert ok is False
    assert "Bad Vendor" in error and "Good Vendor" in error
    assert AGREEMENT_CODE in error


def test_check_still_fails_fast_on_non_auth_error(no_sleep):
    def boom(url, headers=None, params=None, timeout=None):
        raise requests.exceptions.ConnectionError("network down")

    with patch.object(client_module.requests, "get", side_effect=boom), \
         patch.object(AppStoreClient, "_auth_headers", lambda self: {}):
        ok, error = SourceAppleAppStore().check_connection(LOGGER, _config([BAD, GOOD]))

    assert ok is False
    assert "Bad Vendor" in error and "network down" in error


def test_check_still_fails_on_missing_field(apple):
    broken = {"vendor_id": "333", "vendor_name": "No Key"}
    ok, error = SourceAppleAppStore().check_connection(LOGGER, _config([GOOD, broken]))

    assert ok is False
    assert "missing required field" in error


# ─── read: chay bang CDK that ─────────────────────────────────────────────────

STREAMS = ("summary_sales", "financial_report", "financial_report_detail")


def test_read_skips_rejected_vendor_keeps_good_vendor_and_fails_job(apple, no_sleep):
    # Vendor loi dung TRUOC, giong cau hinh that (Bang Hai Le truoc Deviloper).
    messages, error = _run_read(_config([BAD, GOOD]), _catalog(*STREAMS))

    # 1. Du lieu cua vendor lanh van ve, khong co dong nao cua vendor loi.
    sales = _records(messages, "summary_sales")
    assert len(sales) == 1
    assert sales[0]["_vendor_id_"] == "222"
    assert sales[0]["_SKU_"] == "sku.one"

    # 2. State cua vendor lanh duoc phat ra; vendor loi khong co cursor moi.
    states = _states(messages, "summary_sales")
    assert states, "phai co state cho summary_sales"
    assert states[-1] == {"222": {"_Begin_Date_": _yesterday()}}

    # 3. Ca 3 stream deu duoc chay va deu ket thuc INCOMPLETE (khong im lang).
    for stream in STREAMS:
        assert _statuses(messages, stream)[-1] == "INCOMPLETE", stream

    # 4. Job that bai, loi neu ten vendor va ma loi cua Apple.
    assert error is not None
    traces = [m["trace"]["error"]["message"] for m in messages
              if m["type"] == "TRACE" and m["trace"]["type"] == "ERROR"]
    assert len(traces) == len(STREAMS)
    assert all("Bad Vendor" in t and "(111)" in t and AGREEMENT_CODE in t for t in traces)
    assert all("Good Vendor" not in t for t in traces)
    assert all("The other 1 vendor(s) were synced." in t for t in traces)

    # 5. Vendor loi: 1 lan discovery + 1 lan moi stream, khong retry, khong sleep.
    assert len(apple.calls_for("BADKEY")) == 1 + len(STREAMS)
    no_sleep.assert_not_called()


def test_read_with_rejected_vendor_last_gives_same_result(apple):
    messages, error = _run_read(_config([GOOD, BAD]), _catalog("summary_sales"))

    assert [r["_vendor_id_"] for r in _records(messages, "summary_sales")] == ["222"]
    assert _states(messages, "summary_sales")[-1] == {"222": {"_Begin_Date_": _yesterday()}}
    assert _statuses(messages, "summary_sales")[-1] == "INCOMPLETE"
    assert error is not None


def test_read_all_vendors_healthy_completes(no_sleep):
    fake = FakeApple(rejected=())
    other = dict(GOOD, vendor_id="333", vendor_name="Other", key_id="OTHERKEY")
    with patch.object(client_module.requests, "get", side_effect=fake.get), \
         patch.object(AppStoreClient, "_auth_headers",
                      lambda self: {"Authorization": f"Bearer {self._key_id}"}):
        messages, error = _run_read(_config([GOOD, other]), _catalog(*STREAMS))

    assert error is None
    assert sorted(r["_vendor_id_"] for r in _records(messages, "summary_sales")) == ["222", "333"]
    for stream in STREAMS:
        assert _statuses(messages, stream)[-1] == "COMPLETE", stream


def test_read_all_vendors_rejected_fails_without_records(no_sleep):
    fake = FakeApple(rejected=("BADKEY", "GOODKEY"))
    with patch.object(client_module.requests, "get", side_effect=fake.get), \
         patch.object(AppStoreClient, "_auth_headers",
                      lambda self: {"Authorization": f"Bearer {self._key_id}"}):
        messages, error = _run_read(_config([BAD, GOOD]), _catalog("summary_sales"))

    assert error is not None
    assert _records(messages, "summary_sales") == []
    assert _statuses(messages, "summary_sales")[-1] == "INCOMPLETE"
    trace = [m["trace"]["error"]["message"] for m in messages
             if m["type"] == "TRACE" and m["trace"]["type"] == "ERROR"][0]
    assert "2 of 2 vendor(s)" in trace and "No vendor was synced." in trace
