"""
Test ban 0.9.4: 1 stream loi thi ghi canh bao, cac stream con lai van chay het;
va stream financial_report_detail doc du file co dong trong Transaction Date.

Chay bang source.read() that cua CDK, khong goi Apple that. Du lieu la du lieu gia.

Chay (tu thu muc connector):
    python -m pytest unit_tests -q
"""

import gzip
import logging
from unittest.mock import patch

import pytest

from source_apple_store_custom import client as client_module
from source_apple_store_custom.client import AppStoreClient

from test_finance_detail_parser import ROWS, SUMMARY, _file
from test_vendor_isolation import (
    GOOD,
    FakeApple,
    FakeResponse,
    _catalog,
    _config,
    _records,
    _run_read,
    _statuses,
    _states,
    _yesterday,
)


class FakeAppleFinance(FakeApple):
    """Nhu FakeApple, them hanh vi rieng cho /v1/financeReports loai FINANCE_DETAIL."""

    def __init__(self, detail_status=200, detail_payload=None):
        super().__init__(rejected=())
        self.detail_status  = detail_status
        self.detail_payload = detail_payload
        self.detail_served  = False

    def get(self, url, headers=None, params=None, timeout=None):
        params = params or {}
        if url.endswith("/v1/financeReports") and params.get("filter[reportType]") == "FINANCE_DETAIL":
            self.calls.append(("detail", params.get("filter[reportDate]")))
            if self.detail_status != 200:
                return FakeResponse(self.detail_status, json_body={"errors": [{"code": "UNEXPECTED_ERROR"}]}, url=url)
            if self.detail_payload is None or self.detail_served:
                return FakeResponse(404, url=url)
            self.detail_served = True                  # chi 1 ky co bao cao
            return FakeResponse(200, content=gzip.compress(self.detail_payload.encode("utf-8")))
        return super().get(url, headers=headers, params=params, timeout=timeout)


@pytest.fixture
def no_sleep():
    with patch.object(client_module.time, "sleep") as sleep:
        yield sleep


def _read_with(fake, *streams):
    with patch.object(client_module.requests, "get", side_effect=fake.get), \
         patch.object(AppStoreClient, "_auth_headers",
                      lambda self: {"Authorization": f"Bearer {self._key_id}"}):
        return _run_read(_config([GOOD]), _catalog(*streams))


def test_failing_stream_is_warned_and_remaining_streams_still_run(no_sleep, caplog):
    fake = FakeAppleFinance(detail_status=500)

    # Stream loi dung DAU catalog: cac stream sau no van phai duoc chay.
    with caplog.at_level(logging.WARNING, logger="airbyte"):
        messages, error = _read_with(fake, "financial_report_detail", "summary_sales", "financial_report")

    # 1. Stream loi ket thuc INCOMPLETE, khong co ban ghi.
    assert _statuses(messages, "financial_report_detail")[-1] == "INCOMPLETE"
    assert _records(messages, "financial_report_detail") == []

    # 2. Cac stream con lai chay binh thuong, co du lieu va state.
    assert _statuses(messages, "summary_sales")[-1] == "COMPLETE"
    assert _statuses(messages, "financial_report")[-1] == "COMPLETE"
    assert [r["_SKU_"] for r in _records(messages, "summary_sales")] == ["sku.one"]
    assert _states(messages, "summary_sales")[-1] == {"222": {"_Begin_Date_": _yesterday()}}

    # 3. Co canh bao neu ro stream loi va stream chay tot.
    assert "stream 'financial_report_detail' failed; continuing with the remaining streams" in caplog.text
    assert "sync finished with failed stream(s): financial_report_detail" in caplog.text
    assert "Streams synced normally: financial_report, summary_sales" in caplog.text

    # 4. Job van bao that bai (khong im lang).
    assert error is not None


def test_all_streams_healthy_gives_no_warning(no_sleep, caplog):
    fake = FakeAppleFinance(detail_payload=_file(ROWS, SUMMARY))

    with caplog.at_level(logging.WARNING, logger="airbyte"):
        messages, error = _read_with(fake, "summary_sales", "financial_report_detail")

    assert error is None
    assert _statuses(messages, "summary_sales")[-1] == "COMPLETE"
    assert _statuses(messages, "financial_report_detail")[-1] == "COMPLETE"
    assert "failed" not in caplog.text


def test_detail_stream_emits_every_row_including_the_one_without_transaction_date(no_sleep, caplog):
    fake = FakeAppleFinance(detail_payload=_file(ROWS, SUMMARY))

    with caplog.at_level(logging.WARNING, logger="airbyte"):
        messages, error = _read_with(fake, "financial_report_detail")

    records = _records(messages, "financial_report_detail")
    assert error is None
    assert len(records) == len(ROWS)                                  # truoc 0.9.4: 2
    assert [r["_row_number_"] for r in records] == list(range(1, len(ROWS) + 1))
    assert [r["_Country_of_Sale_"] for r in records] == ["AE", "UA", "US", "US", "US", "VN"]
    assert records[2]["_Transaction_Date_"] == "" and records[2]["_Settlement_Date_"] == "09/12/2026"
    assert all(r["_Start_Date_"] == "08/30/2026" and r["_vendor_id_"] == "222" for r in records)
    assert "without a valid Transaction Date" in caplog.text
    assert "do NOT match" not in caplog.text                          # doi soat voi summary: khop


def test_detail_stream_warns_when_rows_do_not_match_file_summary(no_sleep, caplog):
    wrong_summary = [item if item[0] != "US" else ("US", "USD", "9", "63.00") for item in SUMMARY]
    fake = FakeAppleFinance(detail_payload=_file(ROWS, wrong_summary))

    with caplog.at_level(logging.WARNING, logger="airbyte"):
        messages, error = _read_with(fake, "financial_report_detail")

    assert error is None                                              # chi canh bao, khong lam hong sync
    assert len(_records(messages, "financial_report_detail")) == len(ROWS)
    assert "do NOT match the file's own country summary in 1 group(s)" in caplog.text
    assert "US/USD: file summary quantity 9 amount 63.00, parsed rows quantity 4 amount 28.00" in caplog.text
