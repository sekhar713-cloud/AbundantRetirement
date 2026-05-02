"""
CSV ingestion for Fidelity, Schwab, and Vanguard exports.

Supports:
  Fidelity  — transaction history + positions
  Schwab    — transaction history + positions
  Vanguard  — transaction history
"""

import csv
import logging
import re
from pathlib import Path
from typing import Optional

from .db import (db_conn, file_already_ingested, insert_transaction,
                 register_file, upsert_account)
from .normalize import normalize_account_type, normalize_action
from .utils import (detect_broker_from_headers, detect_broker_from_path,
                    file_hash, normalize_ticker, parse_date, row_hash,
                    safe_float)

log = logging.getLogger(__name__)


# ── CSV header sniffing ──────────────────────────────────────────────────────

def _sniff_csv(path: Path) -> tuple[list[str], list[list[str]], int]:
    """
    Return (headers, data_rows, skip_count).
    Skips non-CSV preamble lines that some brokers prepend.
    """
    with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
        raw_lines = f.readlines()

    # Find the first line that looks like a CSV header (contains a comma)
    header_idx = 0
    for i, line in enumerate(raw_lines):
        stripped = line.strip()
        if not stripped:
            continue
        if "," in stripped and not stripped.startswith('"Transactions for'):
            # Heuristic: a real header has multiple comma-separated tokens
            parts = next(csv.reader([stripped]))
            if len(parts) >= 3:
                header_idx = i
                break

    lines_to_parse = raw_lines[header_idx:]
    reader = csv.DictReader(lines_to_parse)
    rows = list(reader)
    headers = reader.fieldnames or []
    return list(headers), rows, header_idx


def _clean_headers(headers: list[str]) -> list[str]:
    return [h.strip().strip('"') for h in headers]


# ── Broker-specific parsers ─────────────────────────────────────────────────

def _parse_fidelity_transactions(rows: list[dict]) -> list[dict]:
    """
    Fidelity transaction history CSV.
    Key columns: Run Date, Action, Symbol, Security Description, Quantity, Price ($), Amount ($)
    """
    out = []
    for r in rows:
        action_raw = (r.get("Action") or "").strip()
        if not action_raw:
            continue

        ticker = normalize_ticker(r.get("Symbol") or r.get("Security Description"))
        trade_date = parse_date(r.get("Run Date") or r.get("Trade Date"))
        if not trade_date:
            continue

        qty   = safe_float(r.get("Quantity"))
        price = safe_float(r.get("Price ($)") or r.get("Price"))
        amt   = safe_float(r.get("Amount ($)") or r.get("Amount"))
        comm  = safe_float(r.get("Commission ($)") or r.get("Commission")) + \
                safe_float(r.get("Fees ($)") or r.get("Fees"))
        desc  = (r.get("Security Description") or r.get("Description") or "").strip()
        settle = parse_date(r.get("Settlement Date") or r.get("Settle Date"))

        out.append(dict(
            trade_date=trade_date, settle_date=settle,
            action=normalize_action(action_raw), raw_action=action_raw,
            ticker=ticker, description=desc,
            quantity=abs(qty) if qty else None,
            price=price if price else None,
            amount=amt, commission=comm,
        ))
    return out


def _parse_fidelity_positions(rows: list[dict], account_number: str) -> list[dict]:
    """Fidelity positions/portfolio CSV — used for unrealized gain snapshots."""
    out = []
    for r in rows:
        ticker = normalize_ticker(r.get("Symbol"))
        if not ticker or ticker in ("TOTAL", ""):
            continue
        out.append(dict(
            ticker=ticker,
            description=(r.get("Description") or "").strip(),
            quantity=safe_float(r.get("Quantity")),
            last_price=safe_float(r.get("Last Price")),
            market_value=safe_float(r.get("Current Value")),
            cost_basis=safe_float(r.get("Cost Basis Total")),
            avg_cost=safe_float(r.get("Average Cost Basis")),
            unrealized_gain=safe_float(r.get("Total Gain/Loss Dollar")),
            acct_number=account_number,
        ))
    return out


def _detect_fidelity_doc_type(headers: list[str]) -> str:
    header_set = {h.lower() for h in headers}
    if "run date" in header_set or "action" in header_set:
        return "transactions"
    if "current value" in header_set or "last price" in header_set:
        return "positions"
    return "transactions"


def _parse_schwab_transactions(rows: list[dict]) -> list[dict]:
    """
    Schwab transaction history CSV.
    Key columns: Date, Action, Symbol, Description, Quantity, Price, Fees & Comm, Amount
    """
    out = []
    for r in rows:
        action_raw = (r.get("Action") or "").strip()
        if not action_raw or action_raw.lower() in ("action", ""):
            continue

        ticker = normalize_ticker(r.get("Symbol"))
        trade_date = parse_date(r.get("Date") or r.get("Run Date"))
        if not trade_date:
            continue

        qty   = safe_float(r.get("Quantity"))
        price = safe_float(r.get("Price"))
        amt   = safe_float(r.get("Amount"))
        comm  = safe_float(r.get("Fees & Comm") or r.get("Commission"))
        desc  = (r.get("Description") or "").strip()

        out.append(dict(
            trade_date=trade_date, settle_date=None,
            action=normalize_action(action_raw), raw_action=action_raw,
            ticker=ticker, description=desc,
            quantity=abs(qty) if qty else None,
            price=price if price else None,
            amount=amt, commission=comm,
        ))
    return out


def _parse_schwab_positions(rows: list[dict]) -> list[dict]:
    """Schwab positions CSV."""
    out = []
    for r in rows:
        ticker = normalize_ticker(r.get("Symbol"))
        if not ticker or ticker.upper() in ("TOTAL", "SYMBOL"):
            continue
        out.append(dict(
            ticker=ticker,
            description=(r.get("Description") or "").strip(),
            quantity=safe_float(r.get("Quantity")),
            last_price=safe_float(r.get("Price")),
            market_value=safe_float(r.get("Market Value")),
            cost_basis=safe_float(r.get("Cost Basis")),
            avg_cost=None,
            unrealized_gain=safe_float(r.get("Gain/Loss $")),
        ))
    return out


def _parse_vanguard_transactions(rows: list[dict]) -> list[dict]:
    """
    Vanguard transaction history CSV.
    Key columns: Trade Date, Settlement Date, Transaction Type, Transaction Description,
                 Investment Name, Symbol, Shares, Share Price, Net Amount, Account Number
    """
    out = []
    for r in rows:
        action_raw = (r.get("Transaction Type") or r.get("Transaction Description") or "").strip()
        if not action_raw:
            continue

        ticker = normalize_ticker(r.get("Symbol"))
        trade_date = parse_date(r.get("Trade Date"))
        if not trade_date:
            continue

        qty   = safe_float(r.get("Shares"))
        price = safe_float(r.get("Share Price"))
        amt   = safe_float(r.get("Net Amount") or r.get("Principal Amount"))
        comm  = safe_float(r.get("Commission Fees"))
        desc  = (r.get("Investment Name") or r.get("Transaction Description") or "").strip()
        settle = parse_date(r.get("Settlement Date"))

        out.append(dict(
            trade_date=trade_date, settle_date=settle,
            action=normalize_action(action_raw), raw_action=action_raw,
            ticker=ticker, description=desc,
            quantity=abs(qty) if qty else None,
            price=price if price else None,
            amount=amt, commission=comm,
        ))
    return out


# ── Account number extraction ────────────────────────────────────────────────

def _extract_account_number_from_fidelity(raw_lines: list[str],
                                          rows: list[dict]) -> tuple[str, str]:
    """Return (account_number, account_type) from Fidelity preamble or row data."""
    for line in raw_lines[:10]:
        m = re.search(r"Account Number[:\s]+([A-Z0-9\-]+)", line, re.I)
        if m:
            return m.group(1).strip(), ""
        # Fidelity sometimes puts account info in row
    for r in rows[:5]:
        for key in ("Account Number", "Account Name"):
            val = r.get(key, "")
            if val and val.strip():
                return val.strip(), r.get("Account Type", "")
    return "UNKNOWN", ""


def _extract_account_number_from_schwab(raw_lines: list[str]) -> tuple[str, str]:
    """Schwab usually puts 'Transactions for account XXXX-1234' on line 1."""
    for line in raw_lines[:5]:
        m = re.search(r"account\s+([A-Z0-9\-]+)", line, re.I)
        if m:
            return m.group(1).strip(), ""
        m = re.search(r"(\d{4}-\d{4})", line)
        if m:
            return m.group(1), ""
    return "UNKNOWN", ""


def _extract_account_number_from_vanguard(rows: list[dict]) -> tuple[str, str]:
    for r in rows:
        acct = r.get("Account Number", "")
        if acct and acct.strip():
            return acct.strip(), r.get("Account Type", "")
    return "UNKNOWN", ""


# ── Main ingestion entry point ───────────────────────────────────────────────

def ingest_csv(path: Path) -> int:
    """
    Parse one CSV file, detect its broker and document type, and write to the database.
    Returns number of new transaction rows inserted.
    """
    path = Path(path).resolve()
    fhash = file_hash(path)

    with db_conn() as conn:
        if file_already_ingested(conn, fhash):
            log.info("Skipping (already ingested): %s", path.name)
            return 0

        # --- Sniff file structure ---
        with open(path, newline="", encoding="utf-8-sig", errors="replace") as f:
            raw_lines = f.readlines()

        headers_raw, rows, skip = _sniff_csv(path)
        headers = _clean_headers(headers_raw)

        # Map cleaned headers back onto rows
        rows_clean = []
        for r in rows:
            rows_clean.append({k.strip().strip('"'): v for k, v in r.items() if k})

        # --- Detect broker ---
        broker = detect_broker_from_path(path) or detect_broker_from_headers(headers)
        if not broker:
            log.warning("Cannot detect broker for %s — skipping", path.name)
            return 0

        log.info("Ingesting %s (%s)", path.name, broker)

        # --- Detect doc type and extract account info ---
        acct_number, acct_type_raw = "UNKNOWN", ""
        doc_type = "transactions"
        parsed_rows: list[dict] = []

        if broker == "fidelity":
            doc_type = _detect_fidelity_doc_type(headers)
            acct_number, acct_type_raw = _extract_account_number_from_fidelity(
                raw_lines, rows_clean)
            if doc_type == "transactions":
                parsed_rows = _parse_fidelity_transactions(rows_clean)
            else:
                # Positions — we store in logs but don't insert to transactions
                pos = _parse_fidelity_positions(rows_clean, acct_number)
                log.info("Positions snapshot: %d holdings found (not written to transactions)",
                         len(pos))
                # Register file, skip transaction insertion
                account_id = upsert_account(conn, broker, acct_number, "",
                                            normalize_account_type(acct_type_raw))
                register_file(conn, str(path), fhash, broker, "csv", "positions",
                              account_id, len(pos))
                return 0

        elif broker == "schwab":
            # Check if positions file (no 'Action' column or 'Date' column)
            if "Action" not in headers and "Date" not in headers:
                doc_type = "positions"
                pos = _parse_schwab_positions(rows_clean)
                log.info("Schwab positions: %d holdings (not written to transactions)", len(pos))
                acct_number, _ = _extract_account_number_from_schwab(raw_lines)
                account_id = upsert_account(conn, broker, acct_number, "", "taxable")
                register_file(conn, str(path), fhash, broker, "csv", "positions",
                              account_id, len(pos))
                return 0
            acct_number, acct_type_raw = _extract_account_number_from_schwab(raw_lines)
            parsed_rows = _parse_schwab_transactions(rows_clean)

        elif broker == "vanguard":
            acct_number, acct_type_raw = _extract_account_number_from_vanguard(rows_clean)
            parsed_rows = _parse_vanguard_transactions(rows_clean)

        # --- Upsert account ---
        acct_type = normalize_account_type(acct_type_raw or path.stem)
        account_id = upsert_account(conn, broker, acct_number, "", acct_type)

        # --- Register file ---
        file_id = register_file(conn, str(path), fhash, broker, "csv",
                                doc_type, account_id, len(parsed_rows))

        # --- Insert transactions ---
        inserted = 0
        skipped = 0
        for tx in parsed_rows:
            if tx["amount"] == 0.0 and not tx["ticker"]:
                continue  # skip empty/footer rows

            rhash = row_hash(account_id, tx["trade_date"], tx["action"],
                             tx.get("ticker"), tx.get("quantity"), tx["amount"])
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
                amount=tx["amount"],
                commission=tx.get("commission", 0),
                raw_action=tx.get("raw_action"),
                row_hash=rhash,
            )
            if ok:
                inserted += 1
            else:
                skipped += 1

        log.info("  → %d new transactions, %d duplicates skipped", inserted, skipped)
        return inserted
