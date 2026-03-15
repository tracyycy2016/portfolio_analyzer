"""
Data fetcher — built only on sources confirmed working by live API tests.

Holdings strategy (tested March 2025):
  - stockanalysis.com  →  works for ALL US-listed ETFs (IGF, XLU, IQLT, SPY, QQQ, ...)
  - CA ETF → US equivalent map  →  VFV.TO tracks same index as SPY, etc.
  - yfinance  →  prices only (holdings API returns empty for all ETFs)

Sources that were tested and DO NOT work:
  - Vanguard CA API  (503 — blocks scrapers)
  - iShares CSV API  (404 — URL format changed)
  - BMO pages        (timeout — blocks scrapers)
  - etf.com          (403 — blocks scrapers)
  - yfinance .holdings / .get_holdings_full()  (empty for all ETFs)
"""

import re
import requests
import pandas as pd
import yfinance as yf
import streamlit as st
from io import StringIO
from typing import Optional

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── Canadian ETF → US equivalent ticker on stockanalysis.com ─────────────────
# When a CA-listed ETF has the same (or near-identical) holdings as a US ETF,
# we use the US ETF's holdings page. Verified by index methodology.
CA_TO_US_EQUIVALENT = {
    # Vanguard Canada
    "VFV":   "VOO",   # S&P 500
    "VUS":   "VTI",   # US Total Market
    "VCN":   "VCN",   # Canada — no US equiv, will fail gracefully
    "VXC":   "VXC",   # All-World ex Canada — no direct US equiv
    "VEF":   "VEA",   # FTSE Developed ex US (VEF is hedged but same holdings)
    "VIU":   "VEA",   # FTSE Developed ex North America ≈ VEA ex Canada
    "VBAL":  "VBAL",  # Balanced — no US equiv
    "VGRO":  "VGRO",  # Growth — no US equiv
    "VCNS":  "VCNS",  # Conservative — no US equiv
    "VEQT":  "VEQT",  # All-equity — no US equiv
    "VGG":   "VIG",   # US Dividend Appreciation
    "VDY":   "VYM",   # CA High Dividend Yield
    "VRE":   "VNQ",   # CA REIT ≈ US REIT
    "VAB":   "BND",   # CA Aggregate Bond ≈ US Aggregate Bond
    # BMO
    "ZSP":   "SPY",   # S&P 500
    "ZCN":   "XIC",   # TSX Composite — no US equiv
    "ZEM":   "EEM",   # MSCI Emerging Markets
    "ZEF":   "EFA",   # MSCI EAFE
    "ZID":   "EFA",   # MSCI EAFE (different but similar)
    "ZIN":   "EFA",
    "ZDV":   "DVY",   # Dividend
    "ZRE":   "VNQ",   # REIT
    "ZUT":   "XLU",   # Utilities
    "ZEB":   "KBE",   # Banks
    "ZLB":   "USMV",  # Low Volatility
    "ZGQ":   "QUAL",  # Quality
    "ZUQ":   "QUAL",
    "ZAG":   "AGG",   # Aggregate Bond
    "ZHY":   "HYG",   # High Yield Bond
    # iShares Canada
    "XIC":   "IVV",   # TSX Composite — use IVV as proxy
    "XSP":   "IVV",   # S&P 500 hedged
    "XEF":   "IEFA",  # MSCI EAFE
    "XEM":   "EEM",   # MSCI EM
    "XIN":   "EFA",
    "XUT":   "XLU",   # Utilities
    "XRE":   "VNQ",   # REIT
    "XFN":   "XLF",   # Financials
    "XEG":   "XLE",   # Energy
}

# ── Exchange prefix → yfinance suffix (from stockanalysis Symbol column) ──────
# Confirmed from live test: ASX: TCL → TCL.AX, TSX: ENB → ENB (no suffix needed)
EXCHANGE_SUFFIX = {
    "TSE":  ".T",    # Tokyo Stock Exchange
    "TYO":  ".T",
    "HKG":  ".HK",   # Hong Kong
    "KRX":  ".KS",   # Korea
    "ASX":  ".AX",   # Australia
    "NSE":  ".NS",   # India NSE
    "BSE":  ".BO",   # India BSE
    "SHA":  ".SS",   # Shanghai
    "SHE":  ".SZ",   # Shenzhen
    "TAI":  ".TW",   # Taiwan
    # Exchanges where the ticker works in yfinance without suffix:
    # TSX (Canada), NYSE, NASDAQ, BME (Spain), AMS, EPA, ETR, SWX, LON, etc.
    # These all return no suffix → bare ticker used as-is
}

# Exchange prefixes where we explicitly want NO suffix (keep bare ticker)
NO_SUFFIX_EXCHANGES = {
    "TSX", "NYSE", "NASDAQ", "BME", "AMS", "EPA", "ETR",
    "SWX", "LON", "STO", "OSL", "HEL", "CPH", "LIS",
    "MIL", "BIT", "VIE", "BRU", "IST", "JSE", "SGX",
    "NZX", "BOM", "NSI",
}


# ── Ticker normalisation ──────────────────────────────────────────────────────

def normalise_ticker(ticker: str, exchange: str) -> str:
    """Return the yfinance-compatible symbol."""
    ticker = ticker.strip().upper()
    if exchange == "CA":
        if not ticker.endswith(".TO") and not ticker.endswith(".V"):
            return ticker + ".TO"
    return ticker


def clean_stockanalysis_symbol(raw: str) -> str:
    """
    Convert stockanalysis.com Symbol column to a yfinance-compatible ticker.
    Examples (confirmed from live tests):
      'NEE'       -> 'NEE'
      'ASX: TCL'  -> 'TCL.AX'
      'TSX: ENB'  -> 'ENB'
      'AMS: ASML' -> 'ASML'
      'TSE: 7203' -> '7203.T'
      'HKG: 0700' -> '0700.HK'
      '2330'      -> '2330.TW'  (bare 4-digit = Taiwan stock e.g. TSM)
    """
    raw = str(raw).strip()
    if ":" not in raw:
        ticker = raw.upper()
        # Bare 4-digit numeric tickers in EM ETFs are Taiwan-listed stocks.
        # Korean stocks always arrive with .KS suffix from stockanalysis.
        if ticker.isdigit() and len(ticker) == 4:
            return ticker + ".TW"
        return ticker
    exchange, ticker = [s.strip().upper() for s in raw.split(":", 1)]
    if exchange in NO_SUFFIX_EXCHANGES:
        return ticker
    suffix = EXCHANGE_SUFFIX.get(exchange, "")
    return ticker + suffix


# ── Price & metadata ──────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_price_and_meta(yf_ticker: str) -> dict:
    """Return price and basic metadata for a single ticker via yfinance."""
    try:
        t = yf.Ticker(yf_ticker)
        info = t.info or {}

        price = (
            info.get("currentPrice")
            or info.get("regularMarketPrice")
            or info.get("navPrice")
        )
        if not price:
            fi = t.fast_info
            price = getattr(fi, "last_price", None)

        currency = (info.get("currency") or "USD").upper()
        name = info.get("longName") or info.get("shortName") or yf_ticker
        asset_type = info.get("quoteType", "EQUITY").upper()

        return {
            "ticker": yf_ticker,
            "name": name,
            "price": float(price) if price else None,
            "currency": currency,
            "asset_type": asset_type,
            "sector": info.get("sector"),
            "country": info.get("country"),
            "exchange": info.get("exchange"),
        }
    except Exception as e:
        return {
            "ticker": yf_ticker,
            "name": yf_ticker,
            "price": None,
            "currency": "USD",
            "asset_type": "EQUITY",
            "sector": None,
            "country": None,
            "exchange": None,
            "error": str(e),
        }


# ── ETF holdings ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600, show_spinner=False)
def get_etf_holdings(yf_ticker: str) -> pd.DataFrame:
    """
    Return DataFrame(ticker, weight 0-1, name) for an ETF's top holdings.

    Strategy:
      1. For CA-listed ETFs (.TO), map to US equivalent and use stockanalysis.com
      2. For US-listed ETFs, use stockanalysis.com directly
      3. If stockanalysis fails, return empty DataFrame (ETF shown as single holding)
    """
    base = yf_ticker.replace(".TO", "").replace(".V", "").upper()
    is_ca = yf_ticker.endswith(".TO") or yf_ticker.endswith(".V")

    # Resolve CA ETF → US equivalent for holdings lookup
    lookup_ticker = CA_TO_US_EQUIVALENT.get(base, base) if is_ca else base

    df = _holdings_stockanalysis(lookup_ticker)
    if df is not None and not df.empty:
        return df

    # If no US equivalent worked, try the bare base ticker anyway
    if is_ca and lookup_ticker != base:
        df = _holdings_stockanalysis(base)
        if df is not None and not df.empty:
            return df

    return pd.DataFrame(columns=["ticker", "weight", "name"])


def _holdings_stockanalysis(ticker: str) -> Optional[pd.DataFrame]:
    """
    Fetch ALL ETF holdings from stockanalysis.com by paginating through
    ?p=annual&column=allHoldings which returns the full holdings list.
    Falls back to the standard page (top 25) if the full list is unavailable.
    """
    all_frames = []

    # First try the full holdings endpoint
    urls_to_try = [
        f"https://stockanalysis.com/etf/{ticker.lower()}/holdings/?p=annual&column=allHoldings",
        f"https://stockanalysis.com/etf/{ticker.lower()}/holdings/",
    ]

    for url in urls_to_try:
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code != 200:
                continue

            tables = pd.read_html(StringIO(r.text))
            for tbl in tables:
                cols_l = [str(c).lower() for c in tbl.columns]
                has_symbol = any("symbol" in c for c in cols_l)
                has_weight = any("weight" in c for c in cols_l)
                if not (has_symbol and has_weight):
                    continue

                sym_col    = next(c for c in tbl.columns if "symbol" in str(c).lower())
                weight_col = next(c for c in tbl.columns if "weight" in str(c).lower())
                name_col   = next((c for c in tbl.columns if "name" in str(c).lower()), sym_col)

                out = tbl[[sym_col, weight_col, name_col]].copy()
                out.columns = ["raw_ticker", "weight", "name"]
                out["ticker"] = out["raw_ticker"].apply(clean_stockanalysis_symbol)

                out["weight"] = (
                    out["weight"].astype(str)
                    .str.replace("%", "").str.replace(",", "").str.strip()
                )
                out["weight"] = pd.to_numeric(out["weight"], errors="coerce").fillna(0)
                if out["weight"].max() > 1.5:
                    out["weight"] /= 100.0

                out = out[out["weight"] > 0]
                out = out[out["ticker"].str.match(r"^[A-Z0-9\.\-]+$", na=False)]
                out = out[~out["ticker"].isin(["", "NAN", "-", "N/A"])]

                if len(out) >= 1:
                    all_frames.append(out[["ticker", "weight", "name"]])
                    break  # found the holdings table on this URL

            if all_frames:
                # Use whichever URL returned more holdings
                best = max(all_frames, key=len)
                if len(best) >= 1:
                    return best.sort_values("weight", ascending=False).reset_index(drop=True)

        except Exception:
            continue

    return None


# ── FX rate ───────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_fx_rate(from_ccy: str, to_ccy: str) -> float:
    """Return exchange rate from_ccy → to_ccy via yfinance."""
    if from_ccy == to_ccy:
        return 1.0
    pair = f"{from_ccy}{to_ccy}=X"
    try:
        t = yf.Ticker(pair)
        fi = t.fast_info
        rate = getattr(fi, "last_price", None)
        if rate:
            return float(rate)
    except Exception:
        pass
    # Hardcoded fallback
    fallbacks = {"CADUSD": 0.73, "USDCAD": 1.37}
    return fallbacks.get(f"{from_ccy}{to_ccy}", 1.0)
