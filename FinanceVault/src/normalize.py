"""Normalize raw broker strings into unified action codes and account types."""

import re

# Map raw broker action strings to canonical actions.
# Add patterns as you encounter new ones.
_ACTION_PATTERNS: list[tuple[re.Pattern, str]] = [
    # BUY variants
    (re.compile(r"^(buy|bought|purchase|reinvestment of dividend|reinvest dividend|"
                r"dividend reinvest|reinvestment|you bought)", re.I), "BUY"),

    # SELL variants
    (re.compile(r"^(sell|sold|you sold|redemption|exchange out)", re.I), "SELL"),

    # DIVIDEND (cash, not reinvested)
    (re.compile(r"^(dividend|qualified dividend|ordinary dividend|non-qualified dividend|"
                r"div|dividend received|short-term capital gain|long-term capital gain|"
                r"capital gain distribution)", re.I), "DIVIDEND"),

    # INTEREST
    (re.compile(r"^(interest|interest earned|money market interest|margin interest credit|"
                r"bank interest)", re.I), "INTEREST"),

    # TRANSFER IN
    (re.compile(r"^(electronic funds transfer received|transfer of assets received|"
                r"journaled shares|acats received|transfer in|transferred in|"
                r"direct deposit|contribution)", re.I), "TRANSFER_IN"),

    # TRANSFER OUT
    (re.compile(r"^(electronic funds transfer|transfer of assets sent|acats sent|"
                r"transfer out|transferred out|withdrawal|distribution)", re.I), "TRANSFER_OUT"),

    # FEE
    (re.compile(r"^(fee|advisory fee|management fee|service charge|commission|"
                r"margin interest|short sale interest)", re.I), "FEE"),

    # REINVEST (same as BUY but explicitly reinvestment — kept separate for reporting)
    (re.compile(r"reinvest", re.I), "REINVEST"),
]


def normalize_action(raw: str) -> str:
    raw = str(raw).strip()
    for pattern, action in _ACTION_PATTERNS:
        if pattern.search(raw):
            return action
    return "OTHER"


# Account type keywords → canonical type
_ACCT_TYPE_MAP: dict[str, str] = {
    "roth ira":      "roth_ira",
    "roth":          "roth_ira",
    "traditional":   "traditional_ira",
    "trad ira":      "traditional_ira",
    "rollover ira":  "traditional_ira",
    "ira":           "traditional_ira",
    "401k":          "401k",
    "401(k)":        "401k",
    "403b":          "403b",
    "403(b)":        "403b",
    "hsa":           "hsa",
    "brokerage":     "taxable",
    "individual":    "taxable",
    "joint":         "taxable",
    "trust":         "taxable",
}


def normalize_account_type(raw: str) -> str:
    if not raw:
        return "taxable"
    raw_lower = raw.lower()
    for keyword, acct_type in _ACCT_TYPE_MAP.items():
        if keyword in raw_lower:
            return acct_type
    return "taxable"
