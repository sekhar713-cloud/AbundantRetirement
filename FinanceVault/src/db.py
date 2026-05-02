"""SQLite database — schema, connection, and upsert helpers."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path

from .config import DB_PATH

SCHEMA_VERSION = 1

_DDL = """
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

-- One row per brokerage account
CREATE TABLE IF NOT EXISTS accounts (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    broker         TEXT    NOT NULL,
    account_number TEXT    NOT NULL,
    account_name   TEXT,
    account_type   TEXT,            -- taxable | traditional_ira | roth_ira | 401k | hsa
    created_at     TEXT    DEFAULT (datetime('now')),
    UNIQUE(broker, account_number)
);

-- Track ingested files; prevents re-processing the same content
CREATE TABLE IF NOT EXISTS raw_files (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    file_path   TEXT NOT NULL,
    file_hash   TEXT NOT NULL UNIQUE,   -- SHA-256 of file bytes
    broker      TEXT,
    file_type   TEXT,                   -- csv | ofx | qfx | pdf
    doc_type    TEXT,                   -- transactions | positions | statement | tax
    account_id  INTEGER REFERENCES accounts(id),
    ingested_at TEXT    DEFAULT (datetime('now')),
    row_count   INTEGER DEFAULT 0
);

-- Unified transaction log across all brokers
CREATE TABLE IF NOT EXISTS transactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id  INTEGER NOT NULL REFERENCES accounts(id),
    file_id     INTEGER REFERENCES raw_files(id),
    trade_date  TEXT    NOT NULL,       -- ISO YYYY-MM-DD
    settle_date TEXT,
    action      TEXT    NOT NULL,       -- normalized: BUY | SELL | DIVIDEND | REINVEST |
                                        --   TRANSFER_IN | TRANSFER_OUT | FEE | INTEREST | OTHER
    ticker      TEXT,
    description TEXT,
    quantity    REAL,
    price       REAL,
    amount      REAL    NOT NULL,       -- positive = money in, negative = money out
    commission  REAL    DEFAULT 0,
    raw_action  TEXT,                   -- original broker string before normalization
    row_hash    TEXT    UNIQUE,         -- SHA-256 of key fields; prevents duplicate rows
    created_at  TEXT    DEFAULT (datetime('now'))
);

-- Lot-level cost basis (one row per purchase lot, updated as shares are sold)
CREATE TABLE IF NOT EXISTS lots (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id         INTEGER NOT NULL REFERENCES accounts(id),
    ticker             TEXT    NOT NULL,
    buy_date           TEXT    NOT NULL,
    original_quantity  REAL    NOT NULL,
    remaining_quantity REAL    NOT NULL,
    cost_per_share     REAL    NOT NULL,
    total_cost         REAL    NOT NULL,
    transaction_id     INTEGER REFERENCES transactions(id),
    is_open            INTEGER DEFAULT 1,
    closed_date        TEXT
);

-- Realized gain/loss records (created when a lot is sold)
CREATE TABLE IF NOT EXISTS realized_gains (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id          INTEGER NOT NULL REFERENCES accounts(id),
    ticker              TEXT    NOT NULL,
    sell_date           TEXT    NOT NULL,
    sell_quantity       REAL    NOT NULL,
    sell_price          REAL    NOT NULL,
    proceeds            REAL    NOT NULL,
    cost_basis          REAL    NOT NULL,
    gain_loss           REAL    NOT NULL,
    buy_date            TEXT    NOT NULL,
    holding_days        INTEGER,
    is_long_term        INTEGER,        -- 1 if held > 365 days
    sell_transaction_id INTEGER REFERENCES transactions(id),
    lot_id              INTEGER REFERENCES lots(id)
);

-- Dividend income
CREATE TABLE IF NOT EXISTS dividends (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id     INTEGER NOT NULL REFERENCES accounts(id),
    ticker         TEXT,
    description    TEXT,
    pay_date       TEXT    NOT NULL,
    amount         REAL    NOT NULL,
    shares         REAL,
    per_share      REAL,
    is_reinvested  INTEGER DEFAULT 0,
    transaction_id INTEGER REFERENCES transactions(id)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_tx_account_date ON transactions(account_id, trade_date);
CREATE INDEX IF NOT EXISTS idx_tx_ticker       ON transactions(ticker);
CREATE INDEX IF NOT EXISTS idx_tx_action       ON transactions(action);
CREATE INDEX IF NOT EXISTS idx_lots_open       ON lots(account_id, ticker, is_open);
CREATE INDEX IF NOT EXISTS idx_gains_date      ON realized_gains(sell_date);
CREATE INDEX IF NOT EXISTS idx_div_date        ON dividends(pay_date);
"""


def get_connection(db_path: Path = DB_PATH) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def db_conn(db_path: Path = DB_PATH):
    conn = get_connection(db_path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db(db_path: Path = DB_PATH) -> None:
    """Create schema if not present; idempotent."""
    with db_conn(db_path) as conn:
        conn.executescript(_DDL)
        row = conn.execute("SELECT version FROM schema_version").fetchone()
        if row is None:
            conn.execute("INSERT INTO schema_version VALUES (?)", (SCHEMA_VERSION,))


def upsert_account(conn: sqlite3.Connection, broker: str, account_number: str,
                   account_name: str = "", account_type: str = "") -> int:
    conn.execute(
        """INSERT INTO accounts (broker, account_number, account_name, account_type)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(broker, account_number) DO UPDATE SET
             account_name = excluded.account_name,
             account_type = CASE WHEN excluded.account_type != '' THEN excluded.account_type
                                 ELSE account_type END""",
        (broker, account_number, account_name, account_type),
    )
    return conn.execute(
        "SELECT id FROM accounts WHERE broker=? AND account_number=?",
        (broker, account_number),
    ).fetchone()["id"]


def file_already_ingested(conn: sqlite3.Connection, file_hash: str) -> bool:
    return conn.execute(
        "SELECT 1 FROM raw_files WHERE file_hash=?", (file_hash,)
    ).fetchone() is not None


def register_file(conn: sqlite3.Connection, file_path: str, file_hash: str,
                  broker: str, file_type: str, doc_type: str,
                  account_id: int, row_count: int) -> int:
    conn.execute(
        """INSERT INTO raw_files (file_path, file_hash, broker, file_type, doc_type,
                                  account_id, row_count)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(file_hash) DO NOTHING""",
        (file_path, file_hash, broker, file_type, doc_type, account_id, row_count),
    )
    return conn.execute(
        "SELECT id FROM raw_files WHERE file_hash=?", (file_hash,)
    ).fetchone()["id"]


def insert_transaction(conn: sqlite3.Connection, **kwargs) -> bool:
    """Insert one transaction; silently skip if row_hash already exists. Returns True if inserted."""
    try:
        conn.execute(
            """INSERT INTO transactions
               (account_id, file_id, trade_date, settle_date, action, ticker,
                description, quantity, price, amount, commission, raw_action, row_hash)
               VALUES (:account_id, :file_id, :trade_date, :settle_date, :action, :ticker,
                       :description, :quantity, :price, :amount, :commission, :raw_action, :row_hash)""",
            kwargs,
        )
        return True
    except sqlite3.IntegrityError:
        return False


def insert_lot(conn: sqlite3.Connection, **kwargs) -> int:
    cur = conn.execute(
        """INSERT INTO lots
           (account_id, ticker, buy_date, original_quantity, remaining_quantity,
            cost_per_share, total_cost, transaction_id)
           VALUES (:account_id, :ticker, :buy_date, :original_quantity, :remaining_quantity,
                   :cost_per_share, :total_cost, :transaction_id)""",
        kwargs,
    )
    return cur.lastrowid
