"""
Portfolio computation: holdings, FIFO cost basis, unrealized/realized gains.

Run after ingestion to rebuild the derived tables (lots, realized_gains, dividends).
Idempotent — clears and rebuilds from the transactions table each time.
"""

import logging
from collections import defaultdict
from datetime import date
from typing import Optional

from .db import db_conn, insert_lot
from .utils import classify_asset, holding_days, safe_float

log = logging.getLogger(__name__)


# ── Dividend extraction ──────────────────────────────────────────────────────

def _rebuild_dividends(conn) -> int:
    conn.execute("DELETE FROM dividends")
    rows = conn.execute(
        """SELECT id, account_id, trade_date, ticker, description, amount
           FROM transactions
           WHERE action IN ('DIVIDEND','REINVEST')
           ORDER BY account_id, trade_date"""
    ).fetchall()

    inserted = 0
    for r in rows:
        is_reinvested = 1 if conn.execute(
            "SELECT action FROM transactions WHERE id=?", (r["id"],)
        ).fetchone()["action"] == "REINVEST" else 0

        conn.execute(
            """INSERT INTO dividends
               (account_id, ticker, description, pay_date, amount, is_reinvested, transaction_id)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (r["account_id"], r["ticker"], r["description"],
             r["trade_date"], abs(r["amount"]), is_reinvested, r["id"]),
        )
        inserted += 1
    return inserted


# ── FIFO lot matching ────────────────────────────────────────────────────────

def _apply_fifo_sell(conn, account_id: int, ticker: str,
                     sell_date: str, sell_qty: float,
                     sell_price: float, sell_tx_id: int) -> None:
    """
    Match a sell against open lots in FIFO order.
    Updates remaining_quantity on each lot and writes realized_gains rows.
    """
    open_lots = conn.execute(
        """SELECT id, buy_date, remaining_quantity, cost_per_share
           FROM lots
           WHERE account_id=? AND ticker=? AND is_open=1
           ORDER BY buy_date ASC, id ASC""",
        (account_id, ticker),
    ).fetchall()

    remaining = sell_qty
    for lot in open_lots:
        if remaining <= 0:
            break

        lot_qty  = lot["remaining_quantity"]
        matched  = min(remaining, lot_qty)
        proceeds = matched * sell_price
        basis    = matched * lot["cost_per_share"]
        gain     = proceeds - basis
        days     = holding_days(lot["buy_date"], sell_date)
        is_lt    = 1 if days > 365 else 0

        conn.execute(
            """INSERT INTO realized_gains
               (account_id, ticker, sell_date, sell_quantity, sell_price,
                proceeds, cost_basis, gain_loss, buy_date, holding_days, is_long_term,
                sell_transaction_id, lot_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (account_id, ticker, sell_date, matched, sell_price,
             proceeds, basis, gain, lot["buy_date"], days, is_lt,
             sell_tx_id, lot["id"]),
        )

        new_qty = lot_qty - matched
        if new_qty < 0.0001:   # treat tiny remainders as fully closed
            conn.execute(
                "UPDATE lots SET remaining_quantity=0, is_open=0, closed_date=? WHERE id=?",
                (sell_date, lot["id"]),
            )
        else:
            conn.execute(
                "UPDATE lots SET remaining_quantity=? WHERE id=?",
                (new_qty, lot["id"]),
            )
        remaining -= matched

    if remaining > 0.001:
        log.warning("  FIFO: %s %s — %.4f shares sold with no matching lot (short?)",
                    ticker, sell_date, remaining)


# ── Portfolio rebuild ────────────────────────────────────────────────────────

def build_portfolio(conn=None) -> dict:
    """
    Rebuild lots, realized_gains, and dividends tables from transactions.
    Returns summary stats dict.
    """
    if conn is not None:
        return _build(conn)
    with db_conn() as c:
        return _build(c)


def _build(conn) -> dict:
    log.info("Rebuilding portfolio from transactions…")

    # Clear derived tables
    conn.execute("DELETE FROM realized_gains")
    conn.execute("DELETE FROM lots")

    div_count = _rebuild_dividends(conn)
    log.info("  Dividends: %d records", div_count)

    # Pull all BUY/SELL/REINVEST transactions in chronological order
    txs = conn.execute(
        """SELECT id, account_id, trade_date, action, ticker, quantity, price, amount
           FROM transactions
           WHERE action IN ('BUY','SELL','REINVEST')
             AND ticker IS NOT NULL
             AND quantity IS NOT NULL
           ORDER BY trade_date ASC, id ASC"""
    ).fetchall()

    buy_count = sell_count = 0

    for tx in txs:
        acct = tx["account_id"]
        tk   = tx["ticker"]
        qty  = tx["quantity"]
        date_str = tx["trade_date"]
        action = tx["action"]

        # Derive price: use explicit price, else back-calculate from amount/qty
        price = tx["price"]
        if not price and qty and tx["amount"]:
            price = abs(tx["amount"]) / qty

        if action in ("BUY", "REINVEST"):
            cost_per = price or 0.0
            insert_lot(
                conn,
                account_id=acct,
                ticker=tk,
                buy_date=date_str,
                original_quantity=qty,
                remaining_quantity=qty,
                cost_per_share=cost_per,
                total_cost=qty * cost_per,
                transaction_id=tx["id"],
            )
            buy_count += 1

        elif action == "SELL":
            sell_price = price or 0.0
            _apply_fifo_sell(conn, acct, tk, date_str, qty, sell_price, tx["id"])
            sell_count += 1

    log.info("  Lots created: %d buys, %d sells matched", buy_count, sell_count)

    # Summary
    stats = conn.execute(
        """SELECT
             COUNT(DISTINCT account_id)  AS accounts,
             COUNT(*)                    AS total_tx,
             SUM(CASE WHEN action='BUY'      THEN 1 ELSE 0 END) AS buys,
             SUM(CASE WHEN action='SELL'     THEN 1 ELSE 0 END) AS sells,
             SUM(CASE WHEN action='DIVIDEND' THEN 1 ELSE 0 END) AS dividends
           FROM transactions"""
    ).fetchone()

    return dict(stats)


# ── Current holdings view ────────────────────────────────────────────────────

def get_current_holdings(conn) -> list[dict]:
    """
    Aggregate open lots into current holdings per (account, ticker).
    Returns list of dicts with: account_id, broker, ticker, quantity, avg_cost, total_cost, asset_class
    """
    rows = conn.execute(
        """SELECT l.account_id, a.broker, a.account_number, a.account_type,
                  l.ticker,
                  SUM(l.remaining_quantity)                          AS quantity,
                  SUM(l.total_cost * (l.remaining_quantity / l.original_quantity))
                      / NULLIF(SUM(l.remaining_quantity), 0)         AS avg_cost,
                  SUM(l.total_cost * (l.remaining_quantity / l.original_quantity))
                                                                     AS total_cost
           FROM lots l
           JOIN accounts a ON a.id = l.account_id
           WHERE l.is_open = 1 AND l.remaining_quantity > 0.0001
           GROUP BY l.account_id, l.ticker
           ORDER BY a.broker, a.account_number, SUM(l.total_cost) DESC"""
    ).fetchall()

    result = []
    for r in rows:
        d = dict(r)
        # Enrich with description from transactions
        desc_row = conn.execute(
            "SELECT description FROM transactions WHERE account_id=? AND ticker=? "
            "AND description IS NOT NULL LIMIT 1",
            (d["account_id"], d["ticker"]),
        ).fetchone()
        d["description"] = desc_row["description"] if desc_row else ""
        d["asset_class"] = classify_asset(d["ticker"], d.get("description"))
        result.append(d)
    return result
