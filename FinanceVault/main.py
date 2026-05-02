#!/usr/bin/env python3
"""
FinanceVault — Personal Portfolio OS
CLI: python main.py <command> [options]

Commands:
  ingest   -- parse and load brokerage files into the database
  build    -- rebuild portfolio (lots, gains, dividends) from transactions
  report   -- generate CSV and text reports
  status   -- show database statistics
  accounts -- list accounts in the database
"""

import logging
import sys
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table
from rich import print as rprint

ROOT = Path(__file__).parent.resolve()
sys.path.insert(0, str(ROOT))

from src.config import BROKERS, CSV_EXTS, LOGS_DIR, OFX_EXTS, PDF_EXTS, RAW_DIR, REPORTS_DIR
from src.db import db_conn, init_db

console = Console()


# ── Logging setup ─────────────────────────────────────────────────────────────

def setup_logging(verbose: bool = False) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    level = logging.DEBUG if verbose else logging.INFO
    fmt   = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOGS_DIR / "financevault.log", encoding="utf-8"),
    ]
    logging.basicConfig(level=level, format=fmt, handlers=handlers)


# ── File routing ──────────────────────────────────────────────────────────────

def _ingest_one(path: Path) -> int:
    from src.ingest_csv import ingest_csv
    from src.ingest_ofx import ingest_ofx
    from src.ingest_pdf import ingest_pdf

    ext = path.suffix.lower()
    if ext in CSV_EXTS:
        return ingest_csv(path)
    if ext in OFX_EXTS:
        return ingest_ofx(path)
    if ext in PDF_EXTS:
        return ingest_pdf(path)
    logging.getLogger(__name__).warning("Unsupported file type: %s", path.name)
    return 0


# ── CLI groups ────────────────────────────────────────────────────────────────

@click.group()
@click.option("--verbose", "-v", is_flag=True, help="Enable debug logging")
def cli(verbose):
    """FinanceVault — local portfolio tracking and document processing."""
    setup_logging(verbose)
    init_db()


@cli.command()
@click.option("--path", "-p", type=click.Path(exists=True),
              help="Ingest a specific file or directory")
@click.option("--broker", "-b", type=click.Choice(BROKERS + ["all"]), default="all",
              help="Limit ingestion to one broker's folder")
@click.option("--all", "all_files", is_flag=True, default=True,
              help="Ingest all files in raw_downloads/ (default)")
def ingest(path, broker, all_files):
    """Parse brokerage files and load transactions into the database."""
    log = logging.getLogger("ingest")
    total = 0

    if path:
        p = Path(path)
        if p.is_dir():
            files = list(p.rglob("*"))
        else:
            files = [p]
    else:
        # Scan raw_downloads/ — optionally filtered by broker
        if broker == "all":
            files = list(RAW_DIR.rglob("*"))
        else:
            files = list((RAW_DIR / broker).rglob("*"))

    supported = [f for f in files
                 if f.is_file() and f.suffix.lower() in (CSV_EXTS | OFX_EXTS | PDF_EXTS)]

    if not supported:
        console.print("[yellow]No supported files found.[/yellow]")
        console.print(f"Drop .csv / .ofx / .qfx / .pdf files into [bold]{RAW_DIR}[/bold]")
        return

    console.print(f"[bold]Found {len(supported)} file(s) to process…[/bold]")
    for f in supported:
        rprint(f"  [cyan]↳[/cyan] {f.relative_to(RAW_DIR) if RAW_DIR in f.parents else f.name}")
        n = _ingest_one(f)
        total += n

    console.print(f"\n[green]✓ Ingestion complete — {total} new transactions loaded[/green]")
    if total > 0:
        console.print("[dim]Run [bold]python main.py build[/bold] to recompute portfolio.[/dim]")


@cli.command()
def build():
    """Rebuild portfolio: lots, cost basis, realized gains, dividends."""
    from src.portfolio import build_portfolio

    console.print("[bold]Building portfolio from transactions…[/bold]")
    with db_conn() as conn:
        stats = build_portfolio(conn)

    tbl = Table(title="Build Complete", show_header=True, header_style="bold cyan")
    tbl.add_column("Metric")
    tbl.add_column("Value", justify="right")
    tbl.add_row("Accounts",         str(stats.get("accounts", 0)))
    tbl.add_row("Total transactions", str(stats.get("total_tx", 0)))
    tbl.add_row("Buy transactions",   str(stats.get("buys", 0)))
    tbl.add_row("Sell transactions",  str(stats.get("sells", 0)))
    tbl.add_row("Dividend events",    str(stats.get("dividends", 0)))
    console.print(tbl)
    console.print("[dim]Run [bold]python main.py report[/bold] to generate reports.[/dim]")


@cli.command()
@click.option("--year", "-y", type=int, default=None,
              help="Filter gains and dividends to a specific year")
@click.option("--open", "open_dir", is_flag=True, default=False,
              help="Open reports directory when done (macOS/Linux)")
def report(year, open_dir):
    """Generate allocation, gains, dividend, and summary reports."""
    from src.reports import run_all_reports
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    console.print("[bold]Generating reports…[/bold]")
    run_all_reports(year=year)

    tbl = Table(title="Reports Written", show_header=True, header_style="bold green")
    tbl.add_column("File")
    tbl.add_column("Path")
    for f in sorted(REPORTS_DIR.iterdir()):
        if f.is_file():
            tbl.add_row(f.name, str(f))
    console.print(tbl)

    if open_dir:
        import subprocess, platform
        cmd = {"darwin": "open", "linux": "xdg-open"}.get(platform.system().lower())
        if cmd:
            subprocess.Popen([cmd, str(REPORTS_DIR)])


@cli.command()
def status():
    """Show database statistics and file inventory."""
    with db_conn() as conn:
        accounts = conn.execute(
            "SELECT broker, account_number, account_type FROM accounts ORDER BY broker"
        ).fetchall()
        tx_total = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        files    = conn.execute("SELECT COUNT(*) FROM raw_files").fetchone()[0]
        lots     = conn.execute("SELECT COUNT(*) FROM lots WHERE is_open=1").fetchone()[0]
        gains    = conn.execute("SELECT SUM(gain_loss) FROM realized_gains").fetchone()[0] or 0
        divs     = conn.execute("SELECT SUM(amount) FROM dividends").fetchone()[0] or 0

    tbl = Table(title="FinanceVault Status", show_header=True, header_style="bold magenta")
    tbl.add_column("Metric")
    tbl.add_column("Value", justify="right")
    tbl.add_row("Files ingested",       str(files))
    tbl.add_row("Total transactions",   str(tx_total))
    tbl.add_row("Open lots",            str(lots))
    tbl.add_row("Total realized gains", f"${gains:,.2f}")
    tbl.add_row("Total dividends",      f"${divs:,.2f}")
    console.print(tbl)

    if accounts:
        a_tbl = Table(title="Accounts", show_header=True, header_style="bold cyan")
        a_tbl.add_column("Broker")
        a_tbl.add_column("Account")
        a_tbl.add_column("Type")
        for a in accounts:
            a_tbl.add_row(a["broker"], a["account_number"], a["account_type"] or "")
        console.print(a_tbl)
    else:
        console.print("[yellow]No accounts yet. Run 'python main.py ingest' to load files.[/yellow]")


@cli.command()
def accounts():
    """List all accounts with transaction counts."""
    with db_conn() as conn:
        rows = conn.execute(
            """SELECT a.broker, a.account_number, a.account_type,
                      COUNT(t.id) AS tx_count,
                      MIN(t.trade_date) AS first_date,
                      MAX(t.trade_date) AS last_date
               FROM accounts a
               LEFT JOIN transactions t ON t.account_id = a.id
               GROUP BY a.id
               ORDER BY a.broker, a.account_number"""
        ).fetchall()

    tbl = Table(title="Accounts", show_header=True, header_style="bold cyan")
    tbl.add_column("Broker")
    tbl.add_column("Account Number")
    tbl.add_column("Type")
    tbl.add_column("Transactions", justify="right")
    tbl.add_column("First Date")
    tbl.add_column("Last Date")
    for r in rows:
        tbl.add_row(r["broker"], r["account_number"], r["account_type"] or "",
                    str(r["tx_count"]), r["first_date"] or "", r["last_date"] or "")
    console.print(tbl)


@cli.command()
@click.argument("ticker")
def holding(ticker):
    """Show lot detail and cost basis for a specific ticker."""
    ticker = ticker.upper()
    with db_conn() as conn:
        lots = conn.execute(
            """SELECT l.buy_date, l.remaining_quantity, l.cost_per_share,
                      l.remaining_quantity * l.cost_per_share AS current_cost,
                      a.broker, a.account_number
               FROM lots l JOIN accounts a ON a.id = l.account_id
               WHERE l.ticker = ? AND l.is_open = 1
               ORDER BY l.buy_date ASC""",
            (ticker,),
        ).fetchall()

        if not lots:
            console.print(f"[yellow]No open lots found for {ticker}[/yellow]")
            return

        total_qty  = sum(r["remaining_quantity"] for r in lots)
        total_cost = sum(r["current_cost"] for r in lots)
        avg_cost   = total_cost / total_qty if total_qty else 0

        tbl = Table(title=f"{ticker} — Open Lots", show_header=True, header_style="bold")
        tbl.add_column("Buy Date")
        tbl.add_column("Broker / Account")
        tbl.add_column("Qty", justify="right")
        tbl.add_column("Cost/Share", justify="right")
        tbl.add_column("Total Cost", justify="right")
        for r in lots:
            tbl.add_row(
                r["buy_date"],
                f"{r['broker']} {r['account_number']}",
                f"{r['remaining_quantity']:.4f}",
                f"${r['cost_per_share']:.4f}",
                f"${r['current_cost']:,.2f}",
            )
        tbl.add_section()
        tbl.add_row("TOTAL", "", f"{total_qty:.4f}", f"${avg_cost:.4f}", f"${total_cost:,.2f}")
        console.print(tbl)


@cli.command("install-service")
@click.option("--port", default=8765, help="Port for the server (default 8765)")
def install_service(port):
    """
    Install a macOS LaunchAgent so the server auto-starts on login.

    After running this once, the server will always be running when your Mac
    is on — no terminal needed. Open portal.html and the Local Vault tab
    connects automatically.
    """
    import platform, subprocess, textwrap

    if platform.system() != "Darwin":
        console.print("[red]install-service is macOS-only. On Linux use systemd.[/red]")
        return

    python = sys.executable
    server = str(ROOT / "server.py")
    log_out = str(LOGS_DIR / "server.log")
    label = "com.financevault.server"
    plist_path = Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"

    plist_path.parent.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    plist = textwrap.dedent(f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
          "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
        <plist version="1.0">
        <dict>
            <key>Label</key>             <string>{label}</string>
            <key>ProgramArguments</key>
            <array>
                <string>{python}</string>
                <string>{server}</string>
                <string>--port</string>  <string>{port}</string>
            </array>
            <key>RunAtLoad</key>         <true/>
            <key>KeepAlive</key>         <true/>
            <key>ThrottleInterval</key>  <integer>10</integer>
            <key>StandardOutPath</key>   <string>{log_out}</string>
            <key>StandardErrorPath</key> <string>{log_out}</string>
            <key>WorkingDirectory</key>  <string>{ROOT}</string>
        </dict>
        </plist>
    """)

    plist_path.write_text(plist)
    console.print(f"[green]✓[/green] Wrote plist → {plist_path}")

    # Unload any old version first (ignore error if not loaded)
    subprocess.run(["launchctl", "unload", str(plist_path)],
                   capture_output=True)
    result = subprocess.run(["launchctl", "load", "-w", str(plist_path)],
                            capture_output=True, text=True)

    if result.returncode == 0:
        console.print("[green]✓ Service loaded — server is starting now[/green]")
        console.print(f"[dim]Listening on http://127.0.0.1:{port}[/dim]")
        console.print("[dim]Will auto-restart on every login.[/dim]")
    else:
        console.print(f"[red]launchctl load failed:[/red] {result.stderr.strip()}")
        console.print(f"[yellow]Plist written to {plist_path} — load it manually:[/yellow]")
        console.print(f"  launchctl load -w {plist_path}")

    # Also create a double-clickable .command helper (for manual start)
    cmd_path = ROOT / "Start FinanceVault.command"
    cmd_path.write_text(
        f"#!/bin/bash\ncd {ROOT}\n{python} server.py --port {port}\n"
    )
    cmd_path.chmod(0o755)
    console.print(f"[green]✓[/green] Created double-click launcher → {cmd_path.name}")


@cli.command("uninstall-service")
def uninstall_service():
    """Remove the macOS LaunchAgent (stops auto-start on login)."""
    import platform, subprocess
    if platform.system() != "Darwin":
        console.print("[red]macOS only[/red]")
        return
    label = "com.financevault.server"
    plist_path = Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"
    if not plist_path.exists():
        console.print("[yellow]Service not installed.[/yellow]")
        return
    subprocess.run(["launchctl", "unload", "-w", str(plist_path)], capture_output=True)
    plist_path.unlink()
    console.print("[green]✓ Service removed — server will no longer auto-start.[/green]")


@cli.command("service-status")
def service_status():
    """Show whether the LaunchAgent is installed and the server is reachable."""
    import platform, urllib.request
    label = "com.financevault.server"
    plist_path = Path.home() / "Library" / "LaunchAgents" / f"{label}.plist"
    installed = plist_path.exists()
    console.print(f"LaunchAgent installed: {'[green]Yes[/green]' if installed else '[yellow]No[/yellow]'}")
    if installed:
        console.print(f"Plist: {plist_path}")
    try:
        urllib.request.urlopen("http://127.0.0.1:8765/api/status", timeout=2)
        console.print("Server reachable:      [green]Yes — http://127.0.0.1:8765[/green]")
    except Exception:
        console.print("Server reachable:      [red]No — not running[/red]")
        if not installed:
            console.print("\n[dim]Run: python3 main.py install-service[/dim]")


if __name__ == "__main__":
    cli()
