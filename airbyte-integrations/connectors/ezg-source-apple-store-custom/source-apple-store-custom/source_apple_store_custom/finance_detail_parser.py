"""Parser cho Apple FINANCE_DETAIL report.

CANH BAO (playbook SS L): file nay KHONG phai bang phang. Layout thuc te:

    Vendor Name<TAB>Bang Le Hai          <- preamble key-value (2 cot)
    Start Date<TAB>05/31/2026
    End Date<TAB>06/27/2026
    Transaction Date<TAB>Settlement Date<TAB>...<TAB>Region   <- header that (18 cot)
    <cac dong giao dich>
    <dong trong>                          <- co the co
    Country Of Sale<TAB>Partner Share Currency<TAB>Quantity<TAB>Extended Partner Share
    <bang summary theo country>
    Total_Rows<TAB>N                      <- co the co

Parser dung o dong trong / moc section 2, de section 2 KHONG bi doc theo header
cua section 1 (loi lech cot da gap o financial_report 0.4.0).

Tu 0.9.4: dong giao dich co cot Transaction Date TRONG hoac sai dinh dang van duoc
GIU (Apple co phat hanh dong nhu vay). Truoc do parser dung han tai dong nay va bo
toan bo phan sau cua file ma khong bao gi. Xem summary_mismatches() de doi soat.
"""

from __future__ import annotations

import gzip
import logging
import re
from typing import Any, Dict, List, Tuple

logger = logging.getLogger("airbyte")

# Header cua section giao dich - dung lam moc de nhan dien
DETAIL_HEADER_FIRST_COL = "Transaction Date"

# Cac key o preamble (truoc header)
PREAMBLE_KEYS = {"Vendor Name", "Start Date", "End Date"}

# Moc bat dau section 2 / dong tong.
# LUU Y: dung so sanh CHINH XAC, KHONG dung startswith("Total")
# vi SKU/Title co the bat dau bang "Total..." (bai hoc SS M).
SECTION_BREAK_TOKENS = {"Country Of Sale", "Country of Sale", "Total_Rows"}

# Dong giao dich thuong bat dau bang Transaction Date dang MM/DD/YYYY
DATE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")

# Dong summary (section 2) co 4 cot; dong giao dich co ~18 cot.
# Dong khong co ngay hop le chi duoc coi la giao dich khi co it nhat chung nay cot.
MIN_TRANSACTION_FIELDS_FLOOR = 5

# Sai so cho phep khi so tong tien voi bang summary cua file
AMOUNT_TOLERANCE = 0.05


def _split(line: str) -> List[str]:
    """Tach TAB. Chi bo CR/LF - GIU nguyen TAB cuoi dong (cot rong o cuoi)."""
    return line.rstrip("\r\n").split("\t")


def decompress(raw: bytes) -> str:
    """Apple tra ve gzip; mot so proxy da giai nen san -> thu ca 2."""
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8-sig", errors="replace")


def _looks_like_transaction(fields: List[str], header: List[str]) -> bool:
    """Dong co hinh dang 1 giao dich (du cot, co noi dung) du cot dau khong phai ngay."""
    minimum = max(MIN_TRANSACTION_FIELDS_FLOOR, len(header) // 2)
    return len(fields) >= minimum and any(fields)


def parse_finance_detail(payload: bytes | str) -> Tuple[Dict[str, str], List[str], List[Dict[str, Any]]]:
    """Tra ve (preamble, header, rows).

    rows: list dict theo TEN COT GOC (chua normalize). Normalize o streams.py.
    Dong giao dich khong co Transaction Date hop le van nam trong rows (gia tri giu nguyen).
    """
    text = payload if isinstance(payload, str) else decompress(payload)
    lines = text.split("\n")

    preamble: Dict[str, str] = {}
    header: List[str] | None = None
    rows: List[Dict[str, Any]] = []
    undated_lines: List[int] = []      # so dong (1-based) cua giao dich khong co ngay hop le
    stopped_at: int | None = None      # chi so dong (0-based) noi parser dung

    for index, raw_line in enumerate(lines):
        fields = [f.strip() for f in _split(raw_line)]
        first = fields[0] if fields else ""

        # ---- Giai doan 1: truoc header ----
        if header is None:
            if first == DETAIL_HEADER_FIRST_COL:
                header = fields
                continue
            if first in PREAMBLE_KEYS and len(fields) >= 2:
                preamble[first] = fields[1]
            continue

        # ---- Giai doan 2: doc data, cac lop chan section 2 ----
        # Lop 1: dong trong -> het section giao dich
        if not raw_line.strip():
            stopped_at = index
            break
        # Lop 2: moc section 2 / dong tong (so sanh chinh xac)
        if first in SECTION_BREAK_TOKENS:
            stopped_at = index
            break
        # Lop 3: cot dau khong phai ngay hop le.
        #   - van co hinh dang giao dich -> GIU dong (Apple co dong trong Transaction Date)
        #   - khong -> coi nhu het section giao dich
        if not DATE_RE.match(first):
            if not _looks_like_transaction(fields, header):
                stopped_at = index
                break
            undated_lines.append(index + 1)
        # Lop 4: so cot lech -> pad neu thieu, cat neu thua (khong im lang bo dong)
        if len(fields) < len(header):
            fields = fields + [""] * (len(header) - len(fields))
        elif len(fields) > len(header):
            fields = fields[: len(header)]

        rows.append(dict(zip(header, fields)))

    if header is None:
        raise ValueError(
            f"Khong tim thay header '{DETAIL_HEADER_FIRST_COL}' trong finance detail report. "
            f"20 ky tu dau: {text[:20]!r}"
        )

    if undated_lines:
        shown = ", ".join(str(n) for n in undated_lines[:10])
        logger.warning(
            f"Finance detail: kept {len(undated_lines)} transaction row(s) without a valid "
            f"Transaction Date (file line {shown}{', ...' if len(undated_lines) > 10 else ''})."
        )

    # Parser da dung ma phia sau van con dong trong giong giao dich -> canh bao, khong im lang.
    if stopped_at is not None:
        leftover = 0
        for raw_line in lines[stopped_at + 1:]:
            fields = [f.strip() for f in _split(raw_line)]
            if fields and DATE_RE.match(fields[0]) and _looks_like_transaction(fields, header):
                leftover += 1
        if leftover:
            logger.warning(
                f"Finance detail: parsing stopped at file line {stopped_at + 1} but {leftover} "
                f"later line(s) still look like transactions and were NOT read."
            )

    return preamble, header, rows


def parse_summary_section(payload: bytes | str) -> List[Dict[str, str]]:
    """Doc RIENG bang summary theo country (section 2) de doi soat sau sync (SS L).

    KHONG day vao BigQuery - chi dung cho verify.
    """
    text = payload if isinstance(payload, str) else decompress(payload)
    header: List[str] | None = None
    out: List[Dict[str, str]] = []

    for raw_line in text.split("\n"):
        fields = [f.strip() for f in _split(raw_line)]
        first = fields[0] if fields else ""
        if header is None:
            if first in ("Country Of Sale", "Country of Sale") and len(fields) == 4:
                header = fields
            continue
        if not raw_line.strip() or first == "Total_Rows":
            break
        if len(fields) != len(header):
            break
        out.append(dict(zip(header, fields)))
    return out


def _number(value: Any) -> float:
    try:
        return float(str(value).replace(",", "").strip() or 0)
    except ValueError:
        return 0.0


def _country(row: Dict[str, Any]) -> str:
    return str(row.get("Country of Sale") or row.get("Country Of Sale") or "")


def summary_mismatches(rows: List[Dict[str, Any]], summary: List[Dict[str, str]]) -> List[str]:
    """So tong Quantity / Extended Partner Share cua rows voi bang summary trong chinh file.

    Tra ve danh sach mo ta cac nhom (country, currency) bi lech; rong = khop.
    File khong co bang summary -> khong ket luan duoc, tra ve rong.
    """
    if not summary:
        return []

    parsed: Dict[Tuple[str, str], List[float]] = {}
    for row in rows:
        key = (_country(row), str(row.get("Partner Share Currency") or ""))
        totals = parsed.setdefault(key, [0.0, 0.0])
        totals[0] += _number(row.get("Quantity"))
        totals[1] += _number(row.get("Extended Partner Share"))

    problems: List[str] = []
    for line in summary:
        key = (_country(line), str(line.get("Partner Share Currency") or ""))
        want_qty, want_amount = _number(line.get("Quantity")), _number(line.get("Extended Partner Share"))
        got_qty, got_amount = parsed.pop(key, [0.0, 0.0])
        if abs(want_qty - got_qty) > 0.5 or abs(want_amount - got_amount) > AMOUNT_TOLERANCE:
            problems.append(
                f"{key[0]}/{key[1]}: file summary quantity {want_qty:g} amount {want_amount:.2f}, "
                f"parsed rows quantity {got_qty:g} amount {got_amount:.2f}"
            )
    for key, (got_qty, got_amount) in parsed.items():
        problems.append(
            f"{key[0]}/{key[1]}: parsed rows quantity {got_qty:g} amount {got_amount:.2f}, "
            f"not present in file summary"
        )
    return problems
