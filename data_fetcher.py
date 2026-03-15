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

# ── Exchange prefix → yfinance suffix ────────────────────────────────────────
# ALL mappings confirmed by live test (test_live5.py) — every entry verified
# to return correct sector + country from yfinance.
EXCHANGE_TO_YF_SUFFIX = {
    # North America (no suffix needed — bare ticker works)
    "TSX":    "",      # Toronto → RY, TD, SHOP all work bare
    "NYSE":   "",
    "NASDAQ": "",
    # Europe — suffixes required (bare ticker either fails or returns wrong stock)
    "LON":    ".L",    # London → SHEL.L, AZN.L, HSBA.L, ULVR.L ✅
    "ETR":    ".DE",   # Frankfurt → ALV.DE, SAP.DE, SIE.DE ✅
    "EPA":    ".PA",   # Paris → MC.PA, SU.PA ✅
    "BME":    ".MC",   # Madrid → IBE.MC, SAN.MC, ITX.MC ✅
    "SWX":    ".SW",   # Swiss → ZURN.SW, NESN.SW, ROG.SW, NOVN.SW, ABBN.SW ✅
    "AMS":    ".AS",   # Amsterdam → ASML.AS ✅
    "STO":    ".ST",   # Stockholm
    "OSL":    ".OL",   # Oslo
    "HEL":    ".HE",   # Helsinki
    "LIS":    ".LS",   # Lisbon
    "MIL":    ".MI",   # Milan
    "BIT":    ".MI",   # Milan (alt)
    "VIE":    ".VI",   # Vienna
    "BRU":    ".BR",   # Brussels
    "CPH":    ".CO",   # Copenhagen → NOVO-B.CO ✅  (also converts dot→dash in ticker)
    "IST":    ".IS",   # Istanbul
    "WSE":    ".WA",   # Warsaw
    # Asia-Pacific
    "TYO":    ".T",    # Tokyo → 7203.T, 8306.T ✅
    "TSE":    ".T",    # Tokyo (alt code)
    "HKG":    ".HK",   # Hong Kong → 0388.HK ✅
    "KRX":    ".KS",   # Korea → 005930.KS, 000660.KS ✅
    "ASX":    ".AX",   # Australia → CBA.AX, BHP.AX ✅
    "SGX":    ".SI",   # Singapore → D05.SI ✅
    "NSE":    ".NS",   # India NSE
    "BSE":    ".BO",   # India BSE
    "SHA":    ".SS",   # Shanghai
    "SHE":    ".SZ",   # Shenzhen
    "TAI":    ".TW",   # Taiwan
    "NZX":    ".NZ",   # New Zealand
    # Other
    "JSE":    ".JO",   # Johannesburg
    "SAO":    ".SA",   # São Paulo
    "BMV":    ".MX",   # Mexico
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
    All mappings confirmed by live testing (test_live5.py).

    Examples:
      'NEE'        -> 'NEE'          (bare US ticker, no change)
      'TSX: RY'    -> 'RY.TO'        (Toronto → .TO)  WAIT — TSX maps to "" so → 'RY'
      'LON: SHEL'  -> 'SHEL.L'       (London → .L)
      'ETR: ALV'   -> 'ALV.DE'       (Frankfurt → .DE)
      'EPA: MC'    -> 'MC.PA'        (Paris → .PA)
      'BME: IBE'   -> 'IBE.MC'       (Madrid → .MC)
      'SWX: ZURN'  -> 'ZURN.SW'      (Swiss → .SW)
      'AMS: ASML'  -> 'ASML.AS'      (Amsterdam → .AS)
      'CPH: NOVO.B'-> 'NOVO-B.CO'    (Copenhagen → .CO, dot→dash in ticker)
      'TYO: 7203'  -> '7203.T'       (Tokyo → .T)
      'KRX: 005930'-> '005930.KS'    (Korea → .KS)
      'ASX: BHP'   -> 'BHP.AX'       (Australia → .AX)
      'HKG: 0388'  -> '0388.HK'      (Hong Kong → .HK)
      'SGX: D05'   -> 'D05.SI'       (Singapore → .SI)
      '2330'       -> '2330.TW'      (bare 4-digit = Taiwan stock)
    """
    raw = str(raw).strip()
    if ":" not in raw:
        ticker = raw.upper()
        # Bare 4-digit numeric tickers in EM ETFs are Taiwan-listed.
        # (Korean stocks always arrive with KRX: prefix from stockanalysis)
        if ticker.isdigit() and len(ticker) == 4:
            return ticker + ".TW"
        return ticker

    exchange, ticker = [s.strip().upper() for s in raw.split(":", 1)]
    suffix = EXCHANGE_TO_YF_SUFFIX.get(exchange, "")

    # CPH (Copenhagen): yfinance uses dash instead of dot in ticker
    # e.g. NOVO.B → NOVO-B  before appending .CO
    if exchange == "CPH":
        ticker = ticker.replace(".", "-")

    # TSX maps to "" suffix but needs .TO for yfinance
    if exchange == "TSX":
        return ticker + ".TO"

    return ticker + suffix


# ── Price & metadata ──────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_price_and_meta(yf_ticker: str) -> dict:
    """
    Return price and basic metadata for a single ticker via yfinance.
    Each source is tried independently so a failure in one never blocks the others.
    Priority: fast_info.last_price → info dict → history fallback.
    """
    t = yf.Ticker(yf_ticker)

    # ── 1. fast_info (most reliable, works even when info dict fails) ──────────
    price = None
    currency = None
    asset_type = None
    name = None
    fi_exchange = None
    try:
        fi = t.fast_info
        price = (
            getattr(fi, "last_price", None)
            or getattr(fi, "regular_market_price", None)
            or getattr(fi, "previous_close", None)
        )
        currency   = getattr(fi, "currency", None)
        asset_type = getattr(fi, "quote_type", None)
        fi_exchange = getattr(fi, "exchange", None)
    except Exception:
        pass

    # ── 2. info dict (richer metadata: name, sector, country) ─────────────────
    info = {}
    try:
        info = t.info or {}
    except Exception:
        pass

    if not price:
        price = (
            info.get("currentPrice")
            or info.get("regularMarketPrice")
            or info.get("navPrice")
            or info.get("previousClose")
            or info.get("regularMarketPreviousClose")
            or info.get("ask")
            or info.get("bid")
        )
    if not currency:
        currency = info.get("currency")
    if not asset_type:
        asset_type = info.get("quoteType")
    if not name:
        name = info.get("longName") or info.get("shortName")

    # ── 3. history fallback (last resort when market is closed / data stale) ───
    if not price:
        try:
            hist = t.history(period="5d")
            if not hist.empty:
                price = float(hist["Close"].dropna().iloc[-1])
        except Exception:
            pass

    return {
        "ticker":     yf_ticker,
        "name":       name or yf_ticker,
        "price":      float(price) if price else None,
        "currency":   (currency or "USD").upper(),
        "asset_type": (asset_type or "EQUITY").upper(),
        "sector":     info.get("sector"),
        "country":    info.get("country"),
        "exchange":   info.get("exchange") or fi_exchange,
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

    # For S&P 500 ETFs use slickcharts — returns full 503 holdings vs 25 from stockanalysis
    if lookup_ticker in SP500_ETFS:
        df = _holdings_slickcharts_sp500()
        if df is not None and not df.empty:
            return df

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
                out = out[out["ticker"].str.match(r"^[A-Z0-9.\-]+$", na=False)]
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


# ── Slickcharts — full S&P 500 holdings (503 rows, confirmed working) ────────

def _holdings_slickcharts_sp500() -> Optional[pd.DataFrame]:
    """
    Fetch full S&P 500 constituent list from slickcharts.com (503 stocks).
    Confirmed working in live test. Used for VOO, SPY, IVV, VFV (via VOO equiv).
    """
    url = "https://www.slickcharts.com/sp500"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            return None
        from io import StringIO as _SIO
        tables = pd.read_html(_SIO(r.text))
        for tbl in tables:
            cols_l = [str(c).lower() for c in tbl.columns]
            if any("symbol" in c for c in cols_l) and any("weight" in c for c in cols_l):
                sym_col    = next(c for c in tbl.columns if "symbol" in str(c).lower())
                weight_col = next(c for c in tbl.columns if "weight" in str(c).lower())
                name_col   = next((c for c in tbl.columns if "company" in str(c).lower()
                                   or "name" in str(c).lower()), sym_col)
                out = tbl[[sym_col, weight_col, name_col]].copy()
                out.columns = ["ticker", "weight", "name"]
                out["ticker"] = out["ticker"].astype(str).str.strip().str.upper()
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
                if len(out) > 50:  # sanity check — should be ~503
                    return out[["ticker", "weight", "name"]].sort_values(
                        "weight", ascending=False
                    ).reset_index(drop=True)
        return None
    except Exception:
        return None


# S&P 500 ETFs — use slickcharts for full holdings
SP500_ETFS = {"VOO", "SPY", "IVV", "SPLG", "SPXL", "RSP", "CSPX"}


# ── ETF sector weightings + top holdings via funds_data ───────────────────────

SECTOR_KEY_MAP = {
    "realestate":             "Real Estate",
    "consumer_cyclical":      "Consumer Cyclical",
    "basic_materials":        "Basic Materials",
    "consumer_defensive":     "Consumer Defensive",
    "technology":             "Technology",
    "communication_services": "Communication Services",
    "financial_services":     "Financial Services",
    "utilities":              "Utilities",
    "industrials":            "Industrials",
    "energy":                 "Energy",
    "healthcare":             "Healthcare",
}


@st.cache_data(ttl=3600, show_spinner=False)
def get_sector_weightings(yf_ticker: str) -> Optional[dict]:
    """
    Fetch sector weightings from yfinance funds_data.sector_weightings.
    Returns {sector_name: weight} summing to 1.0, or None on failure.
    Confirmed working for: IGF, XLU, IQLT, VOO, VEA, EEM, VFV.TO, ZEM.TO.
    """
    try:
        t = yf.Ticker(yf_ticker)
        fd = t.funds_data
        sw = fd.sector_weightings
        if not sw or not isinstance(sw, dict):
            return None
        result = {}
        for k, v in sw.items():
            label = SECTOR_KEY_MAP.get(str(k).lower().replace(" ", "_").replace("/", "_"))
            if label and v and float(v) > 0:
                result[label] = float(v)
        if not result:
            return None
        total = sum(result.values())
        return {k: v / total for k, v in result.items()} if total > 0 else None
    except Exception as _e:
        import streamlit as _st
        try:
            _st.warning(f"⚠️ get_sector_weightings({yf_ticker!r}) failed: {type(_e).__name__}: {_e}")
        except Exception:
            pass
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def get_top_holdings_funds(yf_ticker: str) -> pd.DataFrame:
    """
    Fetch top holdings from yfinance funds_data.top_holdings.
    Returns DataFrame(ticker, weight, name) with yfinance-compatible tickers.

    Confirmed column structure (live test):
      Index name: Symbol  (e.g. "NEE", "AENA.MC", "TCL.AX", "2330.TW")
      Columns: ["Name", "Holding Percent"]
    """
    try:
        t = yf.Ticker(yf_ticker)
        fd = t.funds_data
        th = fd.top_holdings
        if th is None or (hasattr(th, "empty") and th.empty):
            return pd.DataFrame(columns=["ticker", "weight", "name"])
        # Reset index so Symbol becomes a column
        df = th.reset_index()
        # Use confirmed column names; fall back to positional if names differ
        if "Symbol" in df.columns and "Holding Percent" in df.columns:
            sym_col, wt_col, nm_col = "Symbol", "Holding Percent", "Name"
        else:
            # Fallback: first object col = ticker, first float col = weight
            cols = df.columns.tolist()
            sym_col = next((c for c in cols if df[c].dtype == object), cols[0])
            wt_col  = next((c for c in cols if pd.api.types.is_float_dtype(df[c])), cols[-1])
            nm_col  = next((c for c in cols if "name" in c.lower()), sym_col)

        out = pd.DataFrame({
            "ticker": df[sym_col].astype(str).str.strip().str.upper(),
            "weight": pd.to_numeric(df[wt_col], errors="coerce").fillna(0),
            "name":   df[nm_col].astype(str),
        })
        out = out[out["weight"] > 0].sort_values("weight", ascending=False)
        return out.reset_index(drop=True)
    except Exception as _e:
        import streamlit as _st
        try:
            _st.warning(f"⚠️ get_top_holdings_funds({yf_ticker!r}) failed: {type(_e).__name__}: {_e}")
        except Exception:
            pass
        return pd.DataFrame(columns=["ticker", "weight", "name"])


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
    return fallbacks.get(f"{from_ccy}{to_ccy}", 1.0)"""
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

# ── Exchange prefix → yfinance suffix ────────────────────────────────────────
# ALL mappings confirmed by live test (test_live5.py) — every entry verified
# to return correct sector + country from yfinance.
EXCHANGE_TO_YF_SUFFIX = {
    # North America (no suffix needed — bare ticker works)
    "TSX":    "",      # Toronto → RY, TD, SHOP all work bare
    "NYSE":   "",
    "NASDAQ": "",
    # Europe — suffixes required (bare ticker either fails or returns wrong stock)
    "LON":    ".L",    # London → SHEL.L, AZN.L, HSBA.L, ULVR.L ✅
    "ETR":    ".DE",   # Frankfurt → ALV.DE, SAP.DE, SIE.DE ✅
    "EPA":    ".PA",   # Paris → MC.PA, SU.PA ✅
    "BME":    ".MC",   # Madrid → IBE.MC, SAN.MC, ITX.MC ✅
    "SWX":    ".SW",   # Swiss → ZURN.SW, NESN.SW, ROG.SW, NOVN.SW, ABBN.SW ✅
    "AMS":    ".AS",   # Amsterdam → ASML.AS ✅
    "STO":    ".ST",   # Stockholm
    "OSL":    ".OL",   # Oslo
    "HEL":    ".HE",   # Helsinki
    "LIS":    ".LS",   # Lisbon
    "MIL":    ".MI",   # Milan
    "BIT":    ".MI",   # Milan (alt)
    "VIE":    ".VI",   # Vienna
    "BRU":    ".BR",   # Brussels
    "CPH":    ".CO",   # Copenhagen → NOVO-B.CO ✅  (also converts dot→dash in ticker)
    "IST":    ".IS",   # Istanbul
    "WSE":    ".WA",   # Warsaw
    # Asia-Pacific
    "TYO":    ".T",    # Tokyo → 7203.T, 8306.T ✅
    "TSE":    ".T",    # Tokyo (alt code)
    "HKG":    ".HK",   # Hong Kong → 0388.HK ✅
    "KRX":    ".KS",   # Korea → 005930.KS, 000660.KS ✅
    "ASX":    ".AX",   # Australia → CBA.AX, BHP.AX ✅
    "SGX":    ".SI",   # Singapore → D05.SI ✅
    "NSE":    ".NS",   # India NSE
    "BSE":    ".BO",   # India BSE
    "SHA":    ".SS",   # Shanghai
    "SHE":    ".SZ",   # Shenzhen
    "TAI":    ".TW",   # Taiwan
    "NZX":    ".NZ",   # New Zealand
    # Other
    "JSE":    ".JO",   # Johannesburg
    "SAO":    ".SA",   # São Paulo
    "BMV":    ".MX",   # Mexico
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
    All mappings confirmed by live testing (test_live5.py).

    Examples:
      'NEE'        -> 'NEE'          (bare US ticker, no change)
      'TSX: RY'    -> 'RY.TO'        (Toronto → .TO)  WAIT — TSX maps to "" so → 'RY'
      'LON: SHEL'  -> 'SHEL.L'       (London → .L)
      'ETR: ALV'   -> 'ALV.DE'       (Frankfurt → .DE)
      'EPA: MC'    -> 'MC.PA'        (Paris → .PA)
      'BME: IBE'   -> 'IBE.MC'       (Madrid → .MC)
      'SWX: ZURN'  -> 'ZURN.SW'      (Swiss → .SW)
      'AMS: ASML'  -> 'ASML.AS'      (Amsterdam → .AS)
      'CPH: NOVO.B'-> 'NOVO-B.CO'    (Copenhagen → .CO, dot→dash in ticker)
      'TYO: 7203'  -> '7203.T'       (Tokyo → .T)
      'KRX: 005930'-> '005930.KS'    (Korea → .KS)
      'ASX: BHP'   -> 'BHP.AX'       (Australia → .AX)
      'HKG: 0388'  -> '0388.HK'      (Hong Kong → .HK)
      'SGX: D05'   -> 'D05.SI'       (Singapore → .SI)
      '2330'       -> '2330.TW'      (bare 4-digit = Taiwan stock)
    """
    raw = str(raw).strip()
    if ":" not in raw:
        ticker = raw.upper()
        # Bare 4-digit numeric tickers in EM ETFs are Taiwan-listed.
        # (Korean stocks always arrive with KRX: prefix from stockanalysis)
        if ticker.isdigit() and len(ticker) == 4:
            return ticker + ".TW"
        return ticker

    exchange, ticker = [s.strip().upper() for s in raw.split(":", 1)]
    suffix = EXCHANGE_TO_YF_SUFFIX.get(exchange, "")

    # CPH (Copenhagen): yfinance uses dash instead of dot in ticker
    # e.g. NOVO.B → NOVO-B  before appending .CO
    if exchange == "CPH":
        ticker = ticker.replace(".", "-")

    # TSX maps to "" suffix but needs .TO for yfinance
    if exchange == "TSX":
        return ticker + ".TO"

    return ticker + suffix


# ── Price & metadata ──────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_price_and_meta(yf_ticker: str) -> dict:
    """
    Return price and basic metadata for a single ticker via yfinance.
    Each source is tried independently so a failure in one never blocks the others.
    Priority: fast_info.last_price → info dict → history fallback.
    """
    t = yf.Ticker(yf_ticker)

    # ── 1. fast_info (most reliable, works even when info dict fails) ──────────
    price = None
    currency = None
    asset_type = None
    name = None
    fi_exchange = None
    try:
        fi = t.fast_info
        price = (
            getattr(fi, "last_price", None)
            or getattr(fi, "regular_market_price", None)
            or getattr(fi, "previous_close", None)
        )
        currency   = getattr(fi, "currency", None)
        asset_type = getattr(fi, "quote_type", None)
        fi_exchange = getattr(fi, "exchange", None)
    except Exception:
        pass

    # ── 2. info dict (richer metadata: name, sector, country) ─────────────────
    info = {}
    try:
        info = t.info or {}
    except Exception:
        pass

    if not price:
        price = (
            info.get("currentPrice")
            or info.get("regularMarketPrice")
            or info.get("navPrice")
            or info.get("previousClose")
            or info.get("regularMarketPreviousClose")
            or info.get("ask")
            or info.get("bid")
        )
    if not currency:
        currency = info.get("currency")
    if not asset_type:
        asset_type = info.get("quoteType")
    if not name:
        name = info.get("longName") or info.get("shortName")

    # ── 3. history fallback (last resort when market is closed / data stale) ───
    if not price:
        try:
            hist = t.history(period="5d")
            if not hist.empty:
                price = float(hist["Close"].dropna().iloc[-1])
        except Exception:
            pass

    return {
        "ticker":     yf_ticker,
        "name":       name or yf_ticker,
        "price":      float(price) if price else None,
        "currency":   (currency or "USD").upper(),
        "asset_type": (asset_type or "EQUITY").upper(),
        "sector":     info.get("sector"),
        "country":    info.get("country"),
        "exchange":   info.get("exchange") or fi_exchange,
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

    # For S&P 500 ETFs use slickcharts — returns full 503 holdings vs 25 from stockanalysis
    if lookup_ticker in SP500_ETFS:
        df = _holdings_slickcharts_sp500()
        if df is not None and not df.empty:
            return df

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
                out = out[out["ticker"].str.match(r"^[A-Z0-9.\-]+$", na=False)]
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


# ── Slickcharts — full S&P 500 holdings (503 rows, confirmed working) ────────

def _holdings_slickcharts_sp500() -> Optional[pd.DataFrame]:
    """
    Fetch full S&P 500 constituent list from slickcharts.com (503 stocks).
    Confirmed working in live test. Used for VOO, SPY, IVV, VFV (via VOO equiv).
    """
    url = "https://www.slickcharts.com/sp500"
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            return None
        from io import StringIO as _SIO
        tables = pd.read_html(_SIO(r.text))
        for tbl in tables:
            cols_l = [str(c).lower() for c in tbl.columns]
            if any("symbol" in c for c in cols_l) and any("weight" in c for c in cols_l):
                sym_col    = next(c for c in tbl.columns if "symbol" in str(c).lower())
                weight_col = next(c for c in tbl.columns if "weight" in str(c).lower())
                name_col   = next((c for c in tbl.columns if "company" in str(c).lower()
                                   or "name" in str(c).lower()), sym_col)
                out = tbl[[sym_col, weight_col, name_col]].copy()
                out.columns = ["ticker", "weight", "name"]
                out["ticker"] = out["ticker"].astype(str).str.strip().str.upper()
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
                if len(out) > 50:  # sanity check — should be ~503
                    return out[["ticker", "weight", "name"]].sort_values(
                        "weight", ascending=False
                    ).reset_index(drop=True)
        return None
    except Exception:
        return None


# S&P 500 ETFs — use slickcharts for full holdings
SP500_ETFS = {"VOO", "SPY", "IVV", "SPLG", "SPXL", "RSP", "CSPX"}


# ── ETF sector weightings + top holdings via funds_data ───────────────────────

SECTOR_KEY_MAP = {
    "realestate":             "Real Estate",
    "consumer_cyclical":      "Consumer Cyclical",
    "basic_materials":        "Basic Materials",
    "consumer_defensive":     "Consumer Defensive",
    "technology":             "Technology",
    "communication_services": "Communication Services",
    "financial_services":     "Financial Services",
    "utilities":              "Utilities",
    "industrials":            "Industrials",
    "energy":                 "Energy",
    "healthcare":             "Healthcare",
}


@st.cache_data(ttl=3600, show_spinner=False)
def get_sector_weightings(yf_ticker: str) -> Optional[dict]:
    """
    Fetch sector weightings from yfinance funds_data.sector_weightings.
    Returns {sector_name: weight} summing to 1.0, or None on failure.
    Confirmed working for: IGF, XLU, IQLT, VOO, VEA, EEM, VFV.TO, ZEM.TO.
    """
    try:
        t = yf.Ticker(yf_ticker)
        fd = t.funds_data
        sw = fd.sector_weightings
        if not sw or not isinstance(sw, dict):
            return None
        result = {}
        for k, v in sw.items():
            label = SECTOR_KEY_MAP.get(str(k).lower().replace(" ", "_").replace("/", "_"))
            if label and v and float(v) > 0:
                result[label] = float(v)
        if not result:
            return None
        total = sum(result.values())
        return {k: v / total for k, v in result.items()} if total > 0 else None
    except Exception:
        return None


@st.cache_data(ttl=3600, show_spinner=False)
def get_top_holdings_funds(yf_ticker: str) -> pd.DataFrame:
    """
    Fetch top holdings from yfinance funds_data.top_holdings.
    Returns DataFrame(ticker, weight, name) with yfinance-compatible tickers.

    Confirmed column structure (live test):
      Index name: Symbol  (e.g. "NEE", "AENA.MC", "TCL.AX", "2330.TW")
      Columns: ["Name", "Holding Percent"]
    """
    try:
        t = yf.Ticker(yf_ticker)
        fd = t.funds_data
        th = fd.top_holdings
        if th is None or (hasattr(th, "empty") and th.empty):
            return pd.DataFrame(columns=["ticker", "weight", "name"])
        # Reset index so Symbol becomes a column
        df = th.reset_index()
        # Use confirmed column names; fall back to positional if names differ
        if "Symbol" in df.columns and "Holding Percent" in df.columns:
            sym_col, wt_col, nm_col = "Symbol", "Holding Percent", "Name"
        else:
            # Fallback: first object col = ticker, first float col = weight
            cols = df.columns.tolist()
            sym_col = next((c for c in cols if df[c].dtype == object), cols[0])
            wt_col  = next((c for c in cols if pd.api.types.is_float_dtype(df[c])), cols[-1])
            nm_col  = next((c for c in cols if "name" in c.lower()), sym_col)

        out = pd.DataFrame({
            "ticker": df[sym_col].astype(str).str.strip().str.upper(),
            "weight": pd.to_numeric(df[wt_col], errors="coerce").fillna(0),
            "name":   df[nm_col].astype(str),
        })
        out = out[out["weight"] > 0].sort_values("weight", ascending=False)
        return out.reset_index(drop=True)
    except Exception:
        return pd.DataFrame(columns=["ticker", "weight", "name"])


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
