"""Parser cho Apple FINANCE_DETAIL report.

CANH BAO (playbook SS L): file nay KHONG phai bang phang. Layout thuc te:

    Vendor Name<TAB>Bang Le Hai          <- preamble key-value (2 cot)
    Start Date<TAB>05/31/2026
    End Date<TAB>06/27/2026
    Transaction Date<TAB>Settlement Date<TAB>...<TAB>Region   <- header that (18 cot)
    <cac dong giao dich>
    <dong trong>
    Country Of Sale<TAB>Partner Share Currency<TAB>Quantity<TAB>Extended Partner Share
    <bang summary theo country>
    Total_Rows<TAB>N                      <- co the co

Parser dung o dong trong / dong dau tien khong phai giao dich, de section 2
KHONG bi doc theo header cua section 1 (loi lech cot da gap o financial_report 0.4.0).
"""

from __future__ import annotations

import gzip
import re
from typing import Any, Dict, List, Tuple

# Header cua section giao dich - dung lam moc de nhan dien
DETAIL_HEADER_FIRST_COL = "Transaction Date"

# Cac key o preamble (truoc header)
PREAMBLE_KEYS = {"Vendor Name", "Start Date", "End Date"}

# Moc bat dau section 2 / dong tong.
# LUU Y: dung so sanh CHINH XAC, KHONG dung startswith("Total")
# vi SKU/Title co the bat dau bang "Total..." (bai hoc SS M).
SECTION_BREAK_TOKENS = {"Country Of Sale", "Country of Sale", "Total_Rows"}

# Dong giao dich luon bat dau bang Transaction Date dang MM/DD/YYYY
DATE_RE = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")


def _split(line: str) -> List[str]:
    """Tach TAB. Chi bo CR/LF - GIU nguyen TAB cuoi dong (cot rong o cuoi)."""
    return line.rstrip("\r\n").split("\t")


def decompress(raw: bytes) -> str:
    """Apple tra ve gzip; mot so proxy da giai nen san -> thu ca 2."""
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return raw.decode("utf-8-sig", errors="replace")


def parse_finance_detail(payload: bytes | str) -> Tuple[Dict[str, str], List[str], List[Dict[str, Any]]]:
    """Tra ve (preamble, header, rows).

    rows: list dict theo TEN COT GOC (chua normalize). Normalize o streams.py.
    """
    text = payload if isinstance(payload, str) else decompress(payload)

    preamble: Dict[str, str] = {}
    header: List[str] | None = None
    rows: List[Dict[str, Any]] = []

    for raw_line in text.split("\n"):
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

        # ---- Giai doan 2: doc data, 4 lop chan section 2 ----
        # Lop 1: dong trong -> het section giao dich
        if not raw_line.strip():
            break
        # Lop 2: moc section 2 / dong tong (so sanh chinh xac)
        if first in SECTION_BREAK_TOKENS:
            break
        # Lop 3: khong phai Transaction Date hop le -> khong con la dong giao dich
        if not DATE_RE.match(first):
            break
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
