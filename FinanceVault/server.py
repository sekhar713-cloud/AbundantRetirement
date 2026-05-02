#!/usr/bin/env python3
"""
FinanceVault local API server.

Bridges the static portal.html to the local SQLite database and
runs the ingestion pipeline on demand.

Usage:
    python3 server.py              # default port 8765
    python3 server.py --port 9000
"""

import csv
import io
import json
import logging
import os
import subprocess
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

# Ensure src/ is importable
ROOT = Path(__file__).parent.resolve()
sys.path.insert(0, str(ROOT))

from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS

from src.config import LOGS_DIR, RAW_DIR, REPORTS_DIR, BROKERS
from src.db import db_conn, init_db
from src.portfolio import get_current_holdings

# ── App setup ─────────────────────────────────────────────────────────────────

app = Flask(__name__, static_folder=None)
CORS(app, resources={r"/api/*": {"origins": "*"}})   # local-only; no auth needed

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "server.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("vault.server")

SERVER_VERSION = "1.0"

# ── Job state (in-memory; single-user local server) ──────────────────────────

_job_lock = threading.Lock()
_job: dict = {"running": False, "command": None, "started": None, "finished": None, "rc": None}


def _run_command_bg(cmd: list[str]) -> None:
    global _job
    log.info("Starting job: %s", " ".join(cmd))
    with _job_lock:
        _job = {"running": True, "command": cmd[-1], "started": datetime.now().isoformat(),
                "finished": None, "rc": None}
    try:
        result = subprocess.run(
            cmd, cwd=str(ROOT), capture_output=False,
            stdout=open(LOGS_DIR / "financevault.log", "a"),
            stderr=subprocess.STDOUT,
            timeout=300,
        )
        rc = result.returncode
    except subprocess.TimeoutExpired:
        log.error("Job timed out: %s", cmd)
        rc = -1
    except Exception as e:
        log.error("Job failed: %s — %s", cmd, e)
        rc = -2
    finally:
        with _job_lock:
            _job["running"] = False
            _job["finished"] = datetime.now().isoformat()
            _job["rc"] = rc
        log.info("Job finished (rc=%s): %s", rc, cmd[-1] if cmd else "")


def _start_job(subcommand: str) -> bool:
    """Launch a main.py subcommand in a background thread. Returns False if already running."""
    with _job_lock:
        if _job["running"]:
            return False
    cmd = [sys.executable, str(ROOT / "main.py"), subcommand]
    t = threading.Thread(target=_run_command_bg, args=(cmd,), daemon=True)
    t.start()
    return True


# ── Helpers ───────────────────────────────────────────────────────────────────

def _db_stats() -> dict:
    try:
        with db_conn() as conn:
            return {
                "accounts":     conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0],
                "transactions": conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0],
                "open_lots":    conn.execute("SELECT COUNT(*) FROM lots WHERE is_open=1").fetchone()[0],
                "dividends":    conn.execute("SELECT COUNT(*) FROM dividends").fetchone()[0],
                "realized_gains": round(
                    conn.execute("SELECT COALESCE(SUM(gain_loss),0) FROM realized_gains").fetchone()[0], 2),
                "total_dividends": round(
                    conn.execute("SELECT COALESCE(SUM(amount),0) FROM dividends").fetchone()[0], 2),
                "last_ingest":  conn.execute(
                    "SELECT MAX(ingested_at) FROM raw_files").fetchone()[0],
            }
    except Exception as e:
        return {"error": str(e)}


def _tail_log(lines: int = 80) -> list[str]:
    log_path = LOGS_DIR / "financevault.log"
    if not log_path.exists():
        return []
    with open(log_path, "r", encoding="utf-8", errors="replace") as f:
        all_lines = f.readlines()
    return [l.rstrip() for l in all_lines[-lines:]]


# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/api/status")
def api_status():
    """Health check + DB stats + job state."""
    with _job_lock:
        job_snapshot = dict(_job)
    return jsonify({
        "ok": True,
        "version": SERVER_VERSION,
        "job": job_snapshot,
        "db": _db_stats(),
        "vault_root": str(ROOT),
    })


@app.route("/api/accounts")
def api_accounts():
    try:
        with db_conn() as conn:
            rows = conn.execute(
                """SELECT a.id, a.broker, a.account_number, a.account_type,
                          COUNT(t.id) AS tx_count,
                          MIN(t.trade_date) AS first_date,
                          MAX(t.trade_date) AS last_date
                   FROM accounts a
                   LEFT JOIN transactions t ON t.account_id = a.id
                   GROUP BY a.id ORDER BY a.broker, a.account_number"""
            ).fetchall()
        return jsonify({"accounts": [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/holdings")
def api_holdings():
    try:
        with db_conn() as conn:
            holdings = get_current_holdings(conn)
        total = sum(h.get("total_cost") or 0 for h in holdings)
        for h in holdings:
            tc = h.get("total_cost") or 0
            h["pct_of_total"] = round(tc / total * 100, 2) if total else 0
            h["quantity"]   = round(h.get("quantity") or 0, 4)
            h["avg_cost"]   = round(h.get("avg_cost") or 0, 4)
            h["total_cost"] = round(tc, 2)
        return jsonify({"holdings": holdings, "total_cost": round(total, 2)})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/dividends")
def api_dividends():
    year = request.args.get("year")
    try:
        with db_conn() as conn:
            where = "WHERE strftime('%Y', d.pay_date)=?" if year else ""
            params = [year] if year else []
            rows = conn.execute(
                f"""SELECT d.pay_date, a.broker, a.account_number, a.account_type,
                           d.ticker, d.description, d.amount, d.is_reinvested
                    FROM dividends d JOIN accounts a ON a.id = d.account_id
                    {where} ORDER BY d.pay_date DESC LIMIT 200""",
                params,
            ).fetchall()
        return jsonify({"dividends": [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/gains")
def api_gains():
    year = request.args.get("year")
    try:
        with db_conn() as conn:
            where = "WHERE strftime('%Y', rg.sell_date)=?" if year else ""
            params = [year] if year else []
            rows = conn.execute(
                f"""SELECT rg.sell_date, rg.buy_date, rg.ticker, rg.sell_quantity,
                           rg.proceeds, rg.cost_basis, rg.gain_loss,
                           rg.holding_days, rg.is_long_term,
                           a.broker, a.account_number
                    FROM realized_gains rg JOIN accounts a ON a.id = rg.account_id
                    {where} ORDER BY rg.sell_date DESC LIMIT 200""",
                params,
            ).fetchall()
        return jsonify({"gains": [dict(r) for r in rows]})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/log")
def api_log():
    lines = int(request.args.get("lines", 80))
    return jsonify({"lines": _tail_log(lines)})


@app.route("/api/run/<command>", methods=["POST"])
def api_run(command):
    if command not in ("ingest", "build", "report"):
        return jsonify({"error": "Unknown command"}), 400
    started = _start_job(command)
    if not started:
        return jsonify({"error": "A job is already running", "running": True}), 409
    return jsonify({"started": True, "command": command})


@app.route("/api/upload", methods=["POST"])
def api_upload():
    """Accept a brokerage file upload and save it to raw_downloads/<broker>/."""
    broker = request.form.get("broker", "").lower()
    if broker not in BROKERS:
        return jsonify({"error": f"broker must be one of {BROKERS}"}), 400
    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    f = request.files["file"]
    if not f.filename:
        return jsonify({"error": "Empty filename"}), 400

    dest_dir = RAW_DIR / broker
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / f.filename
    f.save(str(dest))
    log.info("Uploaded: %s → %s", f.filename, dest)
    return jsonify({"saved": str(dest), "filename": f.filename, "broker": broker})


@app.route("/api/files")
def api_files():
    """List all files in raw_downloads/ with their ingestion status."""
    with db_conn() as conn:
        ingested = {r["file_path"] for r in
                    conn.execute("SELECT file_path FROM raw_files").fetchall()}

    files = []
    for broker in BROKERS:
        folder = RAW_DIR / broker
        if not folder.exists():
            continue
        for p in sorted(folder.rglob("*")):
            if p.is_file() and not p.name.startswith("."):
                files.append({
                    "broker": broker,
                    "filename": p.name,
                    "path": str(p),
                    "size_kb": round(p.stat().st_size / 1024, 1),
                    "ingested": str(p) in ingested,
                })
    return jsonify({"files": files})


# ── Startup ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="FinanceVault local API server")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--host", default="127.0.0.1")
    args = parser.parse_args()

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    init_db()

    log.info("FinanceVault server v%s starting on http://%s:%d", SERVER_VERSION, args.host, args.port)
    log.info("Vault root: %s", ROOT)

    app.run(host=args.host, port=args.port, debug=False, threaded=True)
