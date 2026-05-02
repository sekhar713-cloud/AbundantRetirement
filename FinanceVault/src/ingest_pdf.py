"""
PDF statement ingestion (best-effort).

PDFs are highly variable. This module extracts text with pdfplumber,
then applies broker-specific regex patterns to pull transactions from
account statements and confirmation pages.

Since PDF layouts differ by quarter and broker version, extraction may
be incomplete. Rows that can't be parsed are logged and saved to
processed/<filename>_unmatched.txt for manual review.
"""

import logging
import re
from pathlib import Path
from typing import Optional

from .config import PROCESSED
from .db import (db_conn, file_already_ingested, insert_transaction,
                 register_file, upsert_account)
from .normalize import normalize_account_type, normalize_action
from .utils import (detect_broker_from_path, file_hash, normalize_ticker,
                    parse_date, row_hash, safe_float)

log = logging.getLogger(__name__)


def _extract_text(path: Path) -> str:
    """Extract all text from PDF using pdfplumber."""
    try:
        import pdfplumber  # type: ignore
        with pdfplumber.open(str(path)) as pdf:
            pages = [p.extract_text() or "" for p in pdf.pages]
        return "\n".join(pages)
    except ImportError:
        log.error("pdfplumber not installed — run: pip install pdfplumber")
        return ""
    except Exception as e:
        log.error("PDF text extraction failed for %s: %s", path.name, e)
        return ""


def _detect_broker_from_text(text: str) -> Optional[str]:
    if re.search(r"fidelity", text, re.I):
        return "fidelity"
    if re.search(r"schwab", text, re.I):
        return "schwab"
    if re.search(r"vanguard", text, re.I):
        return "vanguard"
    return None


def _extract_account_number(text: str) -> str:
    """Try common account number patterns."""
    patterns = [
        r"Account\s+(?:Number|#)[:\s]+([A-Z0-9\-]+)",
        r"Acct\s*(?:#|No\.?)[:\s]+([A-Z0-9\-]+)",
        r"Account:\s+([A-Z0-9\-]+)",
        r"(\d{3}-\d{6})",     # Schwab format
        r"Z\d{8}",            # Fidelity format
    ]
    for pat in patterns:
        m = re.search(pat, text, re.I)
        if m:
            return m.group(1).strip()
    return "UNKNOWN"


def _extract_doc_type(text: str) -> str:
    text_lower = text.lower()
    if "trade confirmation" in text_lower:
        return "trade_confirmation"
    if "1099" in text_lower or "tax" in text_lower:
        return "tax"
    return "statement"


# ── Fidelity PDF patterns ────────────────────────────────────────────────────

_FIDELITY_TX_RE = re.compile(
    r"(\d{2}/\d{2}/\d{4})\s+"           # date
    r"([A-Z][^\d\n]{4,50?})\s+"         # action/description
    r"([A-Z]{1,5})\s+"                  # ticker
    r"([\d,]+\.?\d*)\s+"                # quantity
    r"\$([\d,]+\.?\d*)\s+"              # price
    r"\$?([\-\d,]+\.?\d*)",             # amount
    re.MULTILINE,
)


def _parse_fidelity_pdf(text: str) -> list[dict]:
    out = []
    for m in _FIDELITY_TX_RE.finditer(text):
        date_str, action_raw, ticker, qty, price, amt = m.groups()
        out.append(dict(
            trade_date=parse_date(date_str),
            action=normalize_action(action_raw.strip()),
            raw_action=action_raw.strip(),
            ticker=normalize_ticker(ticker),
            description=action_raw.strip(),
            quantity=safe_float(qty),
            price=safe_float(price),
            amount=safe_float(amt),
            commission=0.0,
            settle_date=None,
        ))
    return [r for r in out if r["trade_date"]]


# ── Schwab PDF patterns ──────────────────────────────────────────────────────

_SCHWAB_TX_RE = re.compile(
    r"(\d{2}/\d{2}/\d{4})\s+"
    r"(\d{2}/\d{2}/\d{4})?\s*"          # optional settle date
    r"([A-Z][a-zA-Z/ ]{3,40?})\s+"      # action
    r"([A-Z]{1,5})\s+"                  # ticker
    r"([\d,]+\.?\d*)\s+"                # quantity
    r"\$([\d,]+\.?\d*)\s+"              # price
    r"\$?([\-\d,]+\.?\d*)",             # amount
    re.MULTILINE,
)


def _parse_schwab_pdf(text: str) -> list[dict]:
    out = []
    for m in _SCHWAB_TX_RE.finditer(text):
        trade_d, settle_d, action_raw, ticker, qty, price, amt = m.groups()
        out.append(dict(
            trade_date=parse_date(trade_d),
            settle_date=parse_date(settle_d),
            action=normalize_action(action_raw.strip()),
            raw_action=action_raw.strip(),
            ticker=normalize_ticker(ticker),
            description=action_raw.strip(),
            quantity=safe_float(qty),
            price=safe_float(price),
            amount=safe_float(amt),
            commission=0.0,
        ))
    return [r for r in out if r["trade_date"]]


# ── Generic table extraction ─────────────────────────────────────────────────

_GENERIC_TX_RE = re.compile(
    r"(\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4})\s+"
    r"(Buy|Sell|Bought|Sold|Dividend|Interest|Reinvest[^\s]*)\s+"
    r"([A-Z]{1,6})\s+"
    r"([\d,]+\.?\d*)\s+"
    r"\$?([\d,]+\.?\d*)\s+"
    r"\$?([\-\d,]+\.?\d*)",
    re.IGNORECASE | re.MULTILINE,
)


def _parse_generic_pdf(text: str) -> list[dict]:
    out = []
    for m in _GENERIC_TX_RE.finditer(text):
        date_s, action_raw, ticker, qty, price, amt = m.groups()
        out.append(dict(
            trade_date=parse_date(date_s),
            settle_date=None,
            action=normalize_action(action_raw),
            raw_action=action_raw,
            ticker=normalize_ticker(ticker),
            description=action_raw,
            quantity=safe_float(qty),
            price=safe_float(price),
            amount=safe_float(amt),
            commission=0.0,
        ))
    return [r for r in out if r["trade_date"]]


# ── Entry point ──────────────────────────────────────────────────────────────

def ingest_pdf(path: Path) -> int:
    """Parse one PDF file and write detected transactions to the database."""
    path = Path(path).resolve()
    fhash = file_hash(path)

    with db_conn() as conn:
        if file_already_ingested(conn, fhash):
            log.info("Skipping (already ingested): %s", path.name)
            return 0

        text = _extract_text(path)
        if not text.strip():
            log.warning("No text extracted from %s — possibly a scanned image PDF", path.name)
            return 0

        broker = detect_broker_from_path(path) or _detect_broker_from_text(text) or "unknown"
        acct_number = _extract_account_number(text)
        doc_type = _extract_doc_type(text)

        log.info("Ingesting PDF %s (%s / %s)", path.name, broker, doc_type)

        # Parse transactions
        if broker == "fidelity":
            rows = _parse_fidelity_pdf(text)
        elif broker == "schwab":
            rows = _parse_schwab_pdf(text)
        else:
            rows = _parse_generic_pdf(text)

        if not rows:
            log.warning("  → No transactions found in %s (broker=%s)", path.name, broker)
            # Save raw text for manual inspection
            out_path = PROCESSED / f"{path.stem}_extracted.txt"
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(text, encoding="utf-8")
            log.info("  → Raw text saved to %s for manual review", out_path.name)

        account_id = upsert_account(conn, broker, acct_number, "", "taxable")
        file_id = register_file(conn, str(path), fhash, broker, "pdf",
                                doc_type, account_id, len(rows))

        inserted = 0
        for tx in rows:
            if not tx.get("trade_date"):
                continue
            rhash = row_hash(account_id, tx["trade_date"], tx["action"],
                             tx.get("ticker"), tx.get("quantity"), tx.get("amount"))
            ok = insert_transaction(
                conn,
                account_id=account_id,
                file_id=file_id,
                trade_date=tx["trade_date"],
                settle_date=tx.get("settle_date"),
                action=tx["action"],
                ticker=tx.get("ticker"),
                description=tx.get("description"),
                quantity=tx.get("quantity"),
                price=tx.get("price"),
                amount=tx.get("amount", 0),
                commission=tx.get("commission", 0),
                raw_action=tx.get("raw_action"),
                row_hash=rhash,
            )
            if ok:
                inserted += 1

        log.info("  → %d transactions extracted from PDF", inserted)
        return inserted
