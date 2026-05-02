"""
Report generation — writes CSV and plain-text summaries to reports/.

Reports:
  allocation.csv      — current holdings by asset class and account
  gains_report.csv    — realized gains/losses (YTD and all-time)
  dividend_report.csv — dividend income by ticker and year
  summary.txt         — human-readable portfolio snapshot
"""

import csv
import logging
from collections import defaultdict
from datetime import date
from pathlib import Path

from .config import REPORTS_DIR
from .db import db_conn
from .portfolio import get_current_holdings

log = logging.getLogger(__name__)


def _write_csv(path: Path, rows: list[dict], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    log.info("Wrote %s (%d rows)", path.name, len(rows))


def _fmt_dollars(v: float) -> str:
    return f"${v:,.2f}"


def _fmt_pct(v: float) -> str:
    return f"{v:.2f}%"


# ── Allocation report ────────────────────────────────────────────────────────

def report_allocation(conn, out_dir: Path = REPORTS_DIR) -> Path:
    holdings = get_current_holdings(conn)
    if not holdings:
        log.warning("No open holdings found — run 'python main.py build' first")
        return out_dir / "allocation.csv"

    total_cost = sum(h.get("total_cost") or 0 for h in holdings)

    rows = []
    for h in holdings:
        tc = h.get("total_cost") or 0
        rows.append({
            "broker":        h["broker"],
            "account_number": h["account_number"],
            "account_type":  h["account_type"],
            "ticker":        h["ticker"],
            "description":   h.get("description", ""),
            "asset_class":   h.get("asset_class", ""),
            "quantity":      f"{h.get('quantity', 0):.4f}",
            "avg_cost":      f"{h.get('avg_cost', 0):.4f}",
            "total_cost":    f"{tc:.2f}",
            "pct_of_total":  f"{tc / total_cost * 100:.2f}" if total_cost else "0.00",
        })

    path = out_dir / "allocation.csv"
    _write_csv(path, rows, list(rows[0].keys()) if rows else [])
    return path


# ── Gains report ─────────────────────────────────────────────────────────────

def report_gains(conn, out_dir: Path = REPORTS_DIR,
                 year: int = None) -> Path:
    where = ""
    params = []
    if year:
        where = "WHERE strftime('%Y', rg.sell_date) = ?"
        params = [str(year)]

    rows_db = conn.execute(
        f"""SELECT rg.*, a.broker, a.account_number, a.account_type
            FROM realized_gains rg
            JOIN accounts a ON a.id = rg.account_id
            {where}
            ORDER BY rg.sell_date DESC""",
        params,
    ).fetchall()

    rows = [dict(r) for r in rows_db]
    for r in rows:
        r["is_long_term"] = "LT" if r["is_long_term"] else "ST"
        r["gain_loss"] = f"{r['gain_loss']:.2f}"
        r["proceeds"]  = f"{r['proceeds']:.2f}"
        r["cost_basis"] = f"{r['cost_basis']:.2f}"

    path = out_dir / (f"gains_{year}.csv" if year else "gains_report.csv")
    fields = ["broker", "account_number", "account_type", "ticker", "sell_date",
              "buy_date", "holding_days", "is_long_term", "sell_quantity",
              "sell_price", "proceeds", "cost_basis", "gain_loss"]
    _write_csv(path, rows, fields)
    return path


# ── Dividend report ───────────────────────────────────────────────────────────

def report_dividends(conn, out_dir: Path = REPORTS_DIR,
                     year: int = None) -> Path:
    where = ""
    params = []
    if year:
        where = "WHERE strftime('%Y', d.pay_date) = ?"
        params = [str(year)]

    rows_db = conn.execute(
        f"""SELECT d.pay_date,
                   strftime('%Y', d.pay_date)  AS year,
                   strftime('%m', d.pay_date)  AS month,
                   a.broker, a.account_number, a.account_type,
                   d.ticker, d.description, d.amount, d.is_reinvested
            FROM dividends d
            JOIN accounts a ON a.id = d.account_id
            {where}
            ORDER BY d.pay_date DESC""",
        params,
    ).fetchall()

    rows = [dict(r) for r in rows_db]
    for r in rows:
        r["is_reinvested"] = "Yes" if r["is_reinvested"] else "No"
        r["amount"] = f"{r['amount']:.2f}"

    path = out_dir / (f"dividends_{year}.csv" if year else "dividend_report.csv")
    fields = ["year", "month", "pay_date", "broker", "account_number",
              "account_type", "ticker", "description", "amount", "is_reinvested"]
    _write_csv(path, rows, fields)
    return path


# ── Summary text report ──────────────────────────────────────────────────────

def report_summary(conn, out_dir: Path = REPORTS_DIR) -> Path:
    today = date.today().isoformat()
    holdings = get_current_holdings(conn)

    total_cost = sum(h.get("total_cost") or 0 for h in holdings)

    # By asset class
    by_class: dict[str, float] = defaultdict(float)
    by_acct:  dict[str, float] = defaultdict(float)
    for h in holdings:
        by_class[h.get("asset_class", "UNKNOWN")] += h.get("total_cost") or 0
        key = f"{h['broker']} / {h['account_number']} ({h['account_type']})"
        by_acct[key] += h.get("total_cost") or 0

    # Realized gains YTD
    ytd = conn.execute(
        "SELECT SUM(gain_loss) FROM realized_gains "
        "WHERE strftime('%Y', sell_date) = ?",
        (str(date.today().year),),
    ).fetchone()[0] or 0

    # Dividends YTD
    div_ytd = conn.execute(
        "SELECT SUM(amount) FROM dividends "
        "WHERE strftime('%Y', pay_date) = ?",
        (str(date.today().year),),
    ).fetchone()[0] or 0

    # Transaction counts
    tx_counts = conn.execute(
        "SELECT action, COUNT(*) AS cnt FROM transactions GROUP BY action ORDER BY cnt DESC"
    ).fetchall()

    lines = [
        f"Portfolio Summary — {today}",
        "=" * 60,
        "",
        f"Total Cost Basis:        {_fmt_dollars(total_cost)}",
        f"Realized Gains (YTD):    {_fmt_dollars(ytd)}",
        f"Dividend Income (YTD):   {_fmt_dollars(div_ytd)}",
        "",
        "── Asset Allocation ─────────────────────────────────────",
    ]
    for cls, val in sorted(by_class.items(), key=lambda x: -x[1]):
        pct = val / total_cost * 100 if total_cost else 0
        bar = "█" * int(pct / 2)
        lines.append(f"  {cls:<18}  {_fmt_dollars(val):>14}   {_fmt_pct(pct):>7}  {bar}")

    lines += ["", "── By Account ──────────────────────────────────────────"]
    for acct, val in sorted(by_acct.items(), key=lambda x: -x[1]):
        pct = val / total_cost * 100 if total_cost else 0
        lines.append(f"  {acct:<45}  {_fmt_dollars(val):>14}  {_fmt_pct(pct):>7}")

    lines += ["", "── Top Holdings (by cost basis) ────────────────────────"]
    top = sorted(holdings, key=lambda h: -(h.get("total_cost") or 0))[:20]
    for h in top:
        tc = h.get("total_cost") or 0
        pct = tc / total_cost * 100 if total_cost else 0
        lines.append(
            f"  {h['ticker']:<8}  {h['broker']:<10}  {h['account_type']:<16}"
            f"  qty:{h.get('quantity', 0):>12.4f}"
            f"  basis:{_fmt_dollars(tc):>14}  {_fmt_pct(pct):>7}"
        )

    lines += ["", "── Transaction Counts ──────────────────────────────────"]
    for row in tx_counts:
        lines.append(f"  {row['action']:<20}  {row['cnt']:>6}")

    text = "\n".join(lines)
    path = out_dir / "summary.txt"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    log.info("Wrote %s", path.name)
    return path


def run_all_reports(year: int = None) -> None:
    with db_conn() as conn:
        report_allocation(conn)
        report_gains(conn, year=year)
        report_dividends(conn, year=year)
        report_summary(conn)
