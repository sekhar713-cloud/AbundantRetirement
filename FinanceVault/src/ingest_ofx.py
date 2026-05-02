"""
OFX / QFX ingestion (Fidelity, Schwab, Vanguard all export OFX).

OFX is a structured format — data is far more reliable than CSV.
Investment transactions live in <INVTRANLIST>; bank transactions in <BANKTRANLIST>.
"""

import logging
from pathlib import Path
from typing import Optional

from .db import (db_conn, file_already_ingested, insert_transaction,
                 register_file, upsert_account)
from .normalize import normalize_action
from .utils import (detect_broker_from_path, file_hash, normalize_ticker,
                    parse_date, row_hash, safe_float)

log = logging.getLogger(__name__)


def _parse_ofx_raw(path: Path) -> dict:
    """
    Parse OFX/QFX using ofxparse library.
    Returns the parsed ofxparse.Ofx object or raises.
    """
    import ofxparse  # type: ignore
    with open(path, "rb") as f:
        return ofxparse.OfxParser.parse(f)


def _ofx_action(tx_type: str, units: float) -> str:
    """Map OFX transaction types to normalized actions."""
    t = str(tx_type).upper()
    action_map = {
        "BUYMF": "BUY", "BUYSTOCK": "BUY", "BUYDEBT": "BUY", "BUYOPT": "BUY",
        "SELLMF": "SELL", "SELLSTOCK": "SELL", "SELLDEBT": "SELL", "SELLOPT": "SELL",
        "REINVEST": "REINVEST",
        "DIV": "DIVIDEND",
        "INT": "INTEREST",
        "INCOME": "DIVIDEND",
        "INVEXPENSE": "FEE",
        "JRNLFUND": "TRANSFER_IN" if units > 0 else "TRANSFER_OUT",
        "TRANSFER": "TRANSFER_IN" if units > 0 else "TRANSFER_OUT",
        "DEBIT":  "FEE",
        "CREDIT": "TRANSFER_IN",
        "DEP":    "TRANSFER_IN",
        "XFER":   "TRANSFER_IN" if units > 0 else "TRANSFER_OUT",
        "OTHER":  "OTHER",
    }
    if t in action_map:
        return action_map[t]
    # Fall back to direction
    return normalize_action(t)


def ingest_ofx(path: Path) -> int:
    """Parse one OFX/QFX file and write transactions to the database."""
    path = Path(path).resolve()
    fhash = file_hash(path)

    with db_conn() as conn:
        if file_already_ingested(conn, fhash):
            log.info("Skipping (already ingested): %s", path.name)
            return 0

        try:
            ofx = _parse_ofx_raw(path)
        except Exception as e:
            log.error("Failed to parse OFX %s: %s", path.name, e)
            return 0

        broker = detect_broker_from_path(path) or "unknown"
        log.info("Ingesting OFX %s (%s)", path.name, broker)

        inserted = 0
        file_id: Optional[int] = None

        # OFX can contain multiple accounts
        accounts = ofx.accounts if hasattr(ofx, "accounts") else [ofx.account]
        for account in accounts:
            if account is None:
                continue

            acct_num  = getattr(account, "account_id", "UNKNOWN") or "UNKNOWN"
            acct_type_raw = getattr(account, "account_type", "") or ""

            from .normalize import normalize_account_type
            acct_type = normalize_account_type(acct_type_raw)
            account_id = upsert_account(conn, broker, acct_num, "", acct_type)

            if file_id is None:
                file_id = register_file(conn, str(path), fhash, broker,
                                        path.suffix.lower().lstrip("."),
                                        "transactions", account_id, 0)

            statement = getattr(account, "statement", None)
            if statement is None:
                continue

            tx_list = getattr(statement, "transactions", []) or []

            for tx in tx_list:
                tx_type   = getattr(tx, "type", "OTHER") or "OTHER"
                units     = safe_float(getattr(tx, "units", 0))
                unit_price = safe_float(getattr(tx, "unit_price", 0))
                total      = safe_float(getattr(tx, "total", 0))
                memo       = getattr(tx, "memo", "") or ""
                name       = getattr(tx, "name", "") or memo

                # Security
                security   = getattr(tx, "security", None)
                ticker: Optional[str] = None
                if security:
                    ticker = normalize_ticker(
                        getattr(security, "ticker", None) or
                        getattr(security, "uniqueid", None)
                    )

                date_str = parse_date(str(getattr(tx, "tradeDate", "") or
                                          getattr(tx, "date", "")))
                if not date_str:
                    continue

                action = _ofx_action(tx_type, units)
                amount = total if total != 0 else (units * unit_price if units and unit_price else 0)

                rhash = row_hash(account_id, date_str, action, ticker, units, amount)

                ok = insert_transaction(
                    conn,
                    account_id=account_id,
                    file_id=file_id,
                    trade_date=date_str,
                    settle_date=parse_date(str(getattr(tx, "settleDate", "") or "")),
                    action=action,
                    ticker=ticker,
                    description=name,
                    quantity=abs(units) if units else None,
                    price=unit_price if unit_price else None,
                    amount=amount,
                    commission=safe_float(getattr(tx, "fees", 0)),
                    raw_action=tx_type,
                    row_hash=rhash,
                )
                if ok:
                    inserted += 1

        # Update row_count on the file record
        if file_id:
            conn.execute("UPDATE raw_files SET row_count=? WHERE id=?", (inserted, file_id))

        log.info("  → %d new transactions", inserted)
        return inserted
