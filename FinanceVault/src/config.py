"""Central configuration — all paths and constants derived from VAULT_ROOT."""

from pathlib import Path

VAULT_ROOT  = Path(__file__).parent.parent.resolve()
DB_PATH     = VAULT_ROOT / "database" / "portfolio.db"
RAW_DIR     = VAULT_ROOT / "raw_downloads"
PROCESSED   = VAULT_ROOT / "processed"
REPORTS_DIR = VAULT_ROOT / "reports"
LOGS_DIR    = VAULT_ROOT / "logs"

BROKERS = ["fidelity", "schwab", "vanguard"]

# Recognized file extensions per parser
CSV_EXTS = {".csv"}
OFX_EXTS = {".ofx", ".qfx"}
PDF_EXTS = {".pdf"}

# Tickers → asset class for allocation reports.
# Extend freely; any unknown ticker falls back to pattern matching on description.
ASSET_CLASS_MAP: dict[str, str] = {
    # Avantis 5-fund target
    "AVLV": "US_EQUITY",   "AVUV": "US_EQUITY",
    "AVDE": "INTL_EQUITY", "AVDV": "INTL_EQUITY", "AVES": "INTL_EQUITY",
    # US broad market
    "VTI": "US_EQUITY",  "ITOT": "US_EQUITY", "SCHB": "US_EQUITY",
    "FSKAX": "US_EQUITY", "SWTSX": "US_EQUITY",
    # US large cap
    "VOO": "US_EQUITY", "IVV": "US_EQUITY", "SPY": "US_EQUITY",
    "FXAIX": "US_EQUITY", "SWPPX": "US_EQUITY",
    # US small/mid
    "VB": "US_EQUITY", "IJR": "US_EQUITY", "VO": "US_EQUITY",
    # International developed
    "VXUS": "INTL_EQUITY", "VEA": "INTL_EQUITY", "IEFA": "INTL_EQUITY",
    "SCHF": "INTL_EQUITY",
    # Emerging markets
    "VWO": "INTL_EQUITY", "IEMG": "INTL_EQUITY", "EEM": "INTL_EQUITY",
    # Bonds
    "BND": "BOND",  "AGG": "BOND",  "VBTLX": "BOND", "SCHZ": "BOND",
    "FBND": "BOND", "TLT": "BOND",  "SHY": "BOND",
    # Cash / money market
    "SPAXX": "CASH", "FDRXX": "CASH", "FCASH": "CASH",
    "SWVXX": "CASH", "VMFXX": "CASH", "VMMXX": "CASH",
    # REITs
    "VNQ": "REIT", "SCHH": "REIT",
}

# Keywords in descriptions that hint at asset class (lowercase)
ASSET_CLASS_KEYWORDS: dict[str, str] = {
    "money market": "CASH",
    "treasury": "BOND",
    "bond": "BOND",
    "fixed income": "BOND",
    "international": "INTL_EQUITY",
    "emerging": "INTL_EQUITY",
    "reit": "REIT",
    "real estate": "REIT",
}
