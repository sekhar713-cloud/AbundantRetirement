"""Shared utilities: hashing, date parsing, number cleaning, asset classification."""

import hashlib
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional

from .config import ASSET_CLASS_KEYWORDS, ASSET_CLASS_MAP


# ── Hashing ──────────────────────────────────────────────────────────────────

def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def row_hash(*fields) -> str:
    payload = "|".join(str(f) for f in fields)
    return hashlib.sha256(payload.encode()).hexdigest()


# ── Number parsing ────────────────────────────────────────────────────────────

def safe_float(value, default: float = 0.0) -> float:
    if value is None:
        return default
    s = str(value).strip().replace(",", "").replace("$", "").replace("(", "-").replace(")", "")
    if s in ("", "--", "N/A", "n/a", "nan"):
        return default
    try:
        return float(s)
    except ValueError:
        return default


# ── Date parsing ──────────────────────────────────────────────────────────────

_DATE_FORMATS = [
    "%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y",
    "%m/%d/%y", "%Y%m%d", "%B %d, %Y",
    "%b %d, %Y", "%d-%b-%Y",
]


def parse_date(value: Optional[str]) -> Optional[str]:
    """Return ISO YYYY-MM-DD or None."""
    if not value:
        return None
    value = str(value).strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    # Try dateutil as last resort
    try:
        from dateutil import parser as du
        return du.parse(value, dayfirst=False).date().isoformat()
    except Exception:
        return None


def holding_days(buy_date_str: str, sell_date_str: str) -> int:
    try:
        b = date.fromisoformat(buy_date_str)
        s = date.fromisoformat(sell_date_str)
        return (s - b).days
    except Exception:
        return 0


# ── Ticker normalization ──────────────────────────────────────────────────────

def normalize_ticker(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    t = re.sub(r"[^A-Za-z0-9.]", "", str(raw).strip()).upper()
    return t if t else None


# ── Asset classification ──────────────────────────────────────────────────────

def classify_asset(ticker: Optional[str], description: Optional[str] = None) -> str:
    if ticker:
        tk = ticker.upper().strip()
        if tk in ASSET_CLASS_MAP:
            return ASSET_CLASS_MAP[tk]
    if description:
        desc_lower = description.lower()
        for keyword, cls in ASSET_CLASS_KEYWORDS.items():
            if keyword in desc_lower:
                return cls
    return "US_EQUITY"   # default assumption for unknown equity tickers


# ── Broker detection ─────────────────────────────────────────────────────────

def detect_broker_from_path(path: Path) -> Optional[str]:
    """Infer broker from folder name in the path."""
    parts = [p.lower() for p in path.parts]
    for broker in ("fidelity", "schwab", "vanguard"):
        if broker in parts:
            return broker
    return None


def detect_broker_from_headers(headers: list[str]) -> Optional[str]:
    """Infer broker from CSV column names."""
    headers_lower = {h.lower().strip() for h in headers}
    if "run date" in headers_lower or "security description" in headers_lower:
        return "fidelity"
    if "fees & comm" in headers_lower or "fees & comm" in headers_lower:
        return "schwab"
    if "transaction type" in headers_lower and "investment name" in headers_lower:
        return "vanguard"
    return None


def detect_file_type(path: Path) -> str:
    return path.suffix.lower().lstrip(".")
