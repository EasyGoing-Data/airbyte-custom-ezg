"""
Test ban 0.9.4: parser FINANCE_DETAIL khong dung som khi gap dong giao dich trong
Transaction Date, va co buoc doi soat voi bang summary trong chinh file.

Du lieu trong test la du lieu gia.

Chay (tu thu muc connector):
    python -m pytest unit_tests -q
"""

import gzip
import logging

import pytest

from source_apple_store_custom.finance_detail_parser import (
    parse_finance_detail,
    parse_summary_section,
    summary_mismatches,
)

HEADER = [
    "Transaction Date", "Settlement Date", "Apple Identifier", "SKU", "Title", "Developer Name",
    "Product Type Identifier", "Country of Sale", "Quantity", "Partner Share", "Extended Partner Share",
    "Partner Share Currency", "Customer Price", "Customer Currency", "Sale or Return", "Promo Code",
    "Order Type", "Region",
]
SUMMARY_HEADER = ["Country Of Sale", "Partner Share Currency", "Quantity", "Extended Partner Share"]


def _row(date, country, currency="USD", quantity="1", share="7.00", settlement="09/12/2026", sku="com.app.pack_1"):
    # Apple ket thuc moi dong giao dich bang 1 TAB thua -> 19 truong
    return "\t".join([
        date, settlement, "111", sku, "Pack", "", "IA1", country, quantity, share, share,
        currency, "9.99", currency, "S", "", "", "", "",
    ])


def _file(rows, summary=None, blank_before_summary=False, total_rows=False):
    lines = ["Vendor Name\tTest Vendor", "Start Date\t08/30/2026", "End Date\t09/26/2026", "\t".join(HEADER)]
    lines += rows
    if blank_before_summary:
        lines.append("")
    if summary is not None:
        lines.append("\t".join(SUMMARY_HEADER))
        lines += ["\t".join(item) for item in summary]
    if total_rows:
        lines.append(f"Total_Rows\t{len(rows)}")
    return "\n".join(lines) + "\n"


ROWS = [
    _row("09/01/2026", "AE", "AED", share="26.66"),
    _row("09/02/2026", "UA"),
    _row("", "US"),                      # Apple de trong Transaction Date
    _row("09/10/2026", "US"),
    _row("09/11/2026", "US", quantity="2", share="14.00"),
    _row("09/12/2026", "VN", "VND", share="100000.00"),
]
SUMMARY = [("AE", "AED", "1", "26.66"), ("UA", "USD", "1", "7.00"),
           ("US", "USD", "4", "28.00"), ("VN", "VND", "1", "100000.00")]


def test_row_without_transaction_date_is_kept_and_does_not_stop_parsing(caplog):
    with caplog.at_level(logging.WARNING, logger="airbyte"):
        preamble, header, rows = parse_finance_detail(_file(ROWS, SUMMARY))

    assert preamble == {"Vendor Name": "Test Vendor", "Start Date": "08/30/2026", "End Date": "09/26/2026"}
    assert header == HEADER
    assert len(rows) == len(ROWS)                       # truoc 0.9.4: chi doc duoc 2 dong
    assert [r["Country of Sale"] for r in rows] == ["AE", "UA", "US", "US", "US", "VN"]
    undated = rows[2]
    assert undated["Transaction Date"] == "" and undated["Settlement Date"] == "09/12/2026"
    assert undated["Country of Sale"] == "US" and undated["Quantity"] == "1"
    assert set(undated) == set(HEADER)                  # khong lech cot
    assert "kept 1 transaction row(s) without a valid Transaction Date" in caplog.text
    assert "file line 7" in caplog.text


def test_row_with_malformed_transaction_date_is_kept():
    rows = [_row("09/01/2026", "AE"), _row("2026-09-02", "DE"), _row("09/03/2026", "FR")]
    assert [r["Country of Sale"] for r in parse_finance_detail(_file(rows, []))[2]] == ["AE", "DE", "FR"]


@pytest.mark.parametrize("blank", [False, True])
def test_summary_section_is_never_read_as_transactions(blank):
    _, _, rows = parse_finance_detail(_file(ROWS, SUMMARY, blank_before_summary=blank, total_rows=True))

    assert len(rows) == len(ROWS)
    assert all(len(r) == len(HEADER) for r in rows)
    assert "AED" not in [r["Transaction Date"] for r in rows]


def test_file_without_summary_section():
    _, _, rows = parse_finance_detail(_file(ROWS, summary=None, total_rows=True))
    assert len(rows) == len(ROWS)


def test_gzip_payload_and_crlf_line_endings():
    text = _file(ROWS, SUMMARY).replace("\n", "\r\n")
    _, _, rows = parse_finance_detail(gzip.compress(text.encode("utf-8")))

    assert len(rows) == len(ROWS)
    assert rows[-1]["Region"] == ""


def test_short_non_transaction_line_stops_parsing_but_is_reported(caplog):
    broken = ROWS[:2] + ["unexpected\tline"] + ROWS[3:]
    with caplog.at_level(logging.WARNING, logger="airbyte"):
        _, _, rows = parse_finance_detail(_file(broken, SUMMARY))

    assert len(rows) == 2
    assert "3 later line(s) still look like transactions and were NOT read" in caplog.text


def test_missing_header_raises():
    with pytest.raises(ValueError):
        parse_finance_detail("Vendor Name\tTest Vendor\nno header here\n")


# ─── doi soat voi bang summary cua chinh file ─────────────────────────────────

def test_summary_matches_when_every_row_is_read():
    payload = _file(ROWS, SUMMARY)
    _, _, rows = parse_finance_detail(payload)

    assert parse_summary_section(payload) == [dict(zip(SUMMARY_HEADER, item)) for item in SUMMARY]
    assert summary_mismatches(rows, parse_summary_section(payload)) == []


def test_summary_check_reports_rows_that_were_not_read():
    payload = _file(ROWS, SUMMARY)
    _, _, rows = parse_finance_detail(payload)

    problems = summary_mismatches(rows[:2], parse_summary_section(payload))   # nhu ban cu: dung o dong trong ngay

    assert len(problems) == 2
    assert problems[0].startswith("US/USD: file summary quantity 4 amount 28.00, parsed rows quantity 0")
    assert problems[1].startswith("VN/VND:")


def test_summary_check_reports_rows_missing_from_summary_and_tolerates_rounding():
    rows = [{"Country of Sale": "US", "Partner Share Currency": "USD", "Quantity": "3", "Extended Partner Share": "0.10"},
            {"Country of Sale": "JP", "Partner Share Currency": "JPY", "Quantity": "1", "Extended Partner Share": "500"}]
    summary = [{"Country Of Sale": "US", "Partner Share Currency": "USD", "Quantity": "3", "Extended Partner Share": "0.11"}]

    problems = summary_mismatches(rows, summary)

    assert problems == ["JP/JPY: parsed rows quantity 1 amount 500.00, not present in file summary"]


def test_summary_check_is_silent_when_file_has_no_summary():
    assert summary_mismatches([{"Country of Sale": "US", "Quantity": "1"}], []) == []
