"""
Data fetcher: prices, ETF holdings, and metadata.

Holdings source priority per provider:
  1. Vanguard Canada API         → VFV, VEF, VIU, VAB, VCN, etc.
  2. iShares CSV API (BlackRock) → IGF, XLU, IQLT, XIC, etc.
  3. BMO ETF pages               → ZEM, ZGLD, ZAG, ZCN, etc.
  4. Invesco Canada              → QQC, etc.
  5. yfinance holdings           → general US ETF fallback
  6. etf.com scrape              → US ETFs final fallback
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
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

# ─── Ticker normalisation ────────────────────────────────────────────────────


def normalise_ticker(ticker: str, exchange: str) -> str:
    """Return the yfinance-compatible symbol."""
    ticker = ticker.strip().upper()
    if exchange == "CA":
        if not ticker.endswith(".TO") and not ticker.endswith(".V"):
            return ticker + ".TO"
    return ticker


# ─── Price & metadata ────────────────────────────────────────────────────────


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


# ─── Provider sets ───────────────────────────────────────────────────────────

VANGUARD_CA_TICKERS = {
    "VFV", "VEF", "VIU", "VAB", "VCN", "VRE", "VXC", "VBG",
    "VBAL", "VGRO", "VCNS", "VEQT", "VUS", "VBU", "VDY", "VGG",
    "VHY", "VSB", "VLB", "VAH", "VVO",
}

BMO_CA_TICKERS = {
    "ZEM", "ZGLD", "ZAG", "ZCN", "ZSP", "ZID", "ZIN", "ZEF",
    "ZDV", "ZRE", "ZLB", "ZMI", "ZUB", "ZFH", "ZDB", "ZFS",
    "ZST", "ZIC", "ZGQ", "ZUQ", "ZMID", "ZBA", "ZEB", "ZUT",
    "ZBAL", "ZGRO", "ZCON", "ZESG", "ZWS", "ZWH", "ZWB", "ZWC",
    "ZWU", "ZWK", "ZWP", "ZWG", "ZHY",
}

ISHARES_US_PRODUCT_IDS = {
    "IGF":  "239731", "IQLT": "272554", "IVV":  "239726", "IWM":  "239710",
    "AGG":  "239458", "EFA":  "239623", "EEM":  "239637", "IEMG": "264623",
    "IEFA": "264618", "ITOT": "239724", "ACWI": "239600", "TLT":  "239454",
    "LQD":  "239566", "HYG":  "239565", "QUAL": "301857", "MTUM": "301838",
    "USMV": "264623", "IBB":  "239699",
}

ISHARES_CA_PRODUCT_IDS = {
    "XIC": "251897", "XSP": "251895", "XIN": "251899", "XEF": "264624",
    "XEM": "251901", "XBB": "251854", "XFN": "251905", "XEG": "251908",
    "XUT": "251914", "XRE": "251910", "XDIV": "273004", "XGD": "251903",
}

# Vanguard Canada fund IDs
VANGUARD_CA_FUND_IDS = {
    "VFV":  "9563", "VEF":  "9557", "VIU":  "9558", "VAB":  "9552",
    "VCN":  "9553", "VRE":  "9562", "VXC":  "9564", "VBAL": "9570",
    "VGRO": "9571", "VCNS": "9569", "VEQT": "9572", "VUS":  "9554",
    "VBU":  "9555", "VDY":  "9556", "VGG":  "9559", "VSB":  "9560",
    "VLB":  "9561", "VBG":  "9573",
}


# ─── Master ETF holdings dispatcher ─────────────────────────────────────────


@st.cache_data(ttl=3600, show_spinner=False)
def get_etf_holdings(yf_ticker: str) -> pd.DataFrame:
    """
    Return DataFrame(ticker, weight 0-1, name) for an ETF's top holdings.
    Tries multiple sources in priority order.
    """
    base = yf_ticker.replace(".TO", "").replace(".V", "").upper()
    is_ca = yf_ticker.endswith(".TO") or yf_ticker.endswith(".V")

    # 1. Vanguard Canada
    if base in VANGUARD_CA_TICKERS:
        df = _holdings_vanguard_ca(base)
        if df is not None and not df.empty:
            return df

    # 2. iShares Canada
    if base in ISHARES_CA_PRODUCT_IDS:
        df = _holdings_ishares_csv(base, canada=True)
        if df is not None and not df.empty:
            return df

    # 3. iShares US
    if base in ISHARES_US_PRODUCT_IDS:
        df = _holdings_ishares_csv(base, canada=False)
        if df is not None and not df.empty:
            return df

    # 4. BMO
    if base in BMO_CA_TICKERS:
        df = _holdings_bmo(base)
        if df is not None and not df.empty:
            return df

    # 5. yfinance (works for many US ETFs like SPY, QQQ, XLU, etc.)
    df = _holdings_yfinance(yf_ticker)
    if df is not None and not df.empty:
        return df

    # 6. etf.com scrape (US-listed only)
    if not is_ca:
        df = _holdings_etfcom(base)
        if df is not None and not df.empty:
            return df

    return pd.DataFrame(columns=["ticker", "weight", "name"])


# ─── Source 1: Vanguard Canada API ──────────────────────────────────────────


def _holdings_vanguard_ca(base: str) -> Optional[pd.DataFrame]:
    """
    Vanguard Canada provides portfolio data via an advisor API.
    Returns top holdings with weights.
    """
    fund_id = VANGUARD_CA_FUND_IDS.get(base)
    if not fund_id:
        return None

    # Try the portfolio composition endpoint
    urls_to_try = [
        f"https://www.vanguard.ca/en/advisor/api/fund/{fund_id}/portfolio-data?lang=en",
        f"https://www.vanguard.ca/en/individual/api/fund/{fund_id}/portfolio-data?lang=en",
    ]

    for url in urls_to_try:
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code != 200:
                continue
            data = r.json()
            rows = _parse_vanguard_ca_json(data)
            if rows:
                df = pd.DataFrame(rows)
                df = df[df["weight"] > 0].sort_values("weight", ascending=False)
                return df.head(100)
        except Exception:
            continue

    # Fallback: scrape the Vanguard Canada fund page
    return _holdings_vanguard_ca_page(base, fund_id)


def _parse_vanguard_ca_json(data: dict) -> list:
    """Recursively search Vanguard CA JSON for holdings arrays."""
    rows = []

    def _recurse(obj):
        if isinstance(obj, list):
            for item in obj:
                if isinstance(item, dict):
                    ticker = (
                        item.get("ticker") or item.get("tradingSymbol")
                        or item.get("stockTicker") or item.get("cusip") or ""
                    )
                    name = (
                        item.get("holdingName") or item.get("name")
                        or item.get("securityName") or ticker
                    )
                    wt = (
                        item.get("percentWeight") or item.get("weight")
                        or item.get("marketValuePct") or item.get("percentOfFund") or 0
                    )
                    try:
                        wt = float(str(wt).replace("%", "").replace(",", "")) / 100
                    except Exception:
                        wt = 0
                    if ticker and len(ticker) <= 10 and wt > 0:
                        rows.append({
                            "ticker": str(ticker).strip().upper(),
                            "weight": wt,
                            "name": str(name),
                        })
                    else:
                        _recurse(item)
        elif isinstance(obj, dict):
            for v in obj.values():
                _recurse(v)

    _recurse(data)
    return rows


def _holdings_vanguard_ca_page(base: str, fund_id: str) -> Optional[pd.DataFrame]:
    """Scrape Vanguard Canada fund page for holdings table."""
    urls = [
        f"https://www.vanguard.ca/en/advisor/products/products-group/etfs/{base}#portfolio",
        f"https://www.vanguard.ca/en/individual/products/products-group/etfs/{base}#portfolio",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code != 200:
                continue
            tables = pd.read_html(r.text)
            for tbl in tables:
                result = _normalise_holdings_table(tbl)
                if result is not None and not result.empty:
                    return result
        except Exception:
            continue
    return None


# ─── Source 2: iShares / BlackRock CSV API ───────────────────────────────────


def _holdings_ishares_csv(base: str, canada: bool) -> Optional[pd.DataFrame]:
    """
    BlackRock exposes a CSV holdings download for every iShares ETF.
    The URL pattern uses a fixed timestamp token + product ID.
    """
    if canada:
        product_id = ISHARES_CA_PRODUCT_IDS.get(base)
        base_url = "https://www.blackrock.com/ca/investors/en/products"
    else:
        product_id = ISHARES_US_PRODUCT_IDS.get(base)
        base_url = "https://www.ishares.com/us/products"

    if not product_id:
        return None

    csv_url = f"{base_url}/{product_id}/1467271812596.ajax?tab=holdings&fileType=csv"

    try:
        r = requests.get(csv_url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            return None
        return _parse_ishares_csv_content(r.text)
    except Exception:
        return None


def _parse_ishares_csv_content(text: str) -> Optional[pd.DataFrame]:
    """Parse iShares CSV — skipping metadata rows at the top."""
    lines = text.splitlines()

    # Find the header row (contains "Ticker" or "Name" + "Weight")
    header_idx = None
    for i, line in enumerate(lines):
        parts = [p.strip().strip('"') for p in line.split(",")]
        if len(parts) >= 3:
            lower_parts = [p.lower() for p in parts]
            has_ticker = any("ticker" in p or "symbol" in p for p in lower_parts)
            has_weight = any("weight" in p for p in lower_parts)
            if has_ticker and has_weight:
                header_idx = i
                break

    if header_idx is None:
        return None

    try:
        csv_content = "\n".join(lines[header_idx:])
        df = pd.read_csv(StringIO(csv_content), on_bad_lines="skip")
        df.columns = [str(c).strip().strip('"') for c in df.columns]

        ticker_col = next(
            (c for c in df.columns if c.lower() in ("ticker", "symbol", "sedol")), None
        )
        weight_col = next(
            (c for c in df.columns if "weight" in c.lower()), None
        )
        name_col = next(
            (c for c in df.columns if "name" in c.lower()), None
        )

        if not ticker_col or not weight_col:
            return None

        out = df[[ticker_col, weight_col]].copy()
        out.columns = ["ticker", "weight"]
        out["name"] = df[name_col] if name_col else out["ticker"]

        out["ticker"] = out["ticker"].astype(str).str.strip().str.upper()
        out["weight"] = (
            out["weight"].astype(str)
            .str.replace("%", "").str.replace(",", "").str.strip()
        )
        out["weight"] = pd.to_numeric(out["weight"], errors="coerce").fillna(0)
        if out["weight"].max() > 1.5:
            out["weight"] = out["weight"] / 100.0

        out = out[out["weight"] > 0]
        out = out[out["ticker"].str.match(r"^[A-Z0-9\.\-]+$", na=False)]
        out = out[~out["ticker"].isin(["", "NAN", "CASH", "-", "N/A", "USD", "CAD"])]
        return out[["ticker", "weight", "name"]].sort_values("weight", ascending=False).head(100)
    except Exception:
        return None


# ─── Source 3: BMO ETFs ──────────────────────────────────────────────────────


def _holdings_bmo(base: str) -> Optional[pd.DataFrame]:
    """Fetch BMO ETF holdings from BMO's ETF page."""
    urls = [
        f"https://www.bmo.com/en-ca/main/personal/investments/etfs/{base.lower()}/",
        f"https://www.bmo.com/main/personal/investments/etfs/{base.lower()}/",
    ]
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                continue
            tables = pd.read_html(r.text)
            for tbl in tables:
                result = _normalise_holdings_table(tbl)
                if result is not None and not result.empty:
                    return result
        except Exception:
            continue

    # Fallback: try Morningstar data for BMO funds
    return _holdings_bmo_morningstar(base)


# Morningstar fund IDs for BMO ETFs (Morningstar assigned IDs)
BMO_MORNINGSTAR_IDS = {
    "ZEM":  "F00000T4KU", "ZSP":  "F00000T4KX", "ZCN":  "F00000T4KW",
    "ZAG":  "F00000T4KT", "ZEF":  "F00000T4L4", "ZID":  "F00000T4L2",
    "ZDV":  "F00000T4KY", "ZRE":  "F00000T4L0", "ZUT":  "F00000T4L1",
    "ZLB":  "F00000T4KZ", "ZEB":  "F00000T4L8", "ZGQ":  "F00000YGTS",
    "ZUQ":  "F00000YGTT",
}


def _holdings_bmo_morningstar(base: str) -> Optional[pd.DataFrame]:
    """Try Morningstar's portfolio data API for BMO ETF."""
    fund_id = BMO_MORNINGSTAR_IDS.get(base)
    if not fund_id:
        return None
    try:
        url = (
            f"https://lt.morningstar.com/api/rest.svc/klr5zyak8x/security/cef/portfolio"
            f"?id={fund_id}&idType=msid&languageId=en&locale=en&itype=2&securityType=FO"
        )
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code == 200:
            data = r.json()
            rows = []
            for item in data.get("topHoldings", []):
                ticker = item.get("ticker") or item.get("externalId") or ""
                name = item.get("holdingName") or ticker
                wt = item.get("weighting") or 0
                try:
                    wt = float(wt) / 100
                except Exception:
                    wt = 0
                if ticker and wt > 0:
                    rows.append({"ticker": ticker.upper(), "weight": wt, "name": name})
            if rows:
                return pd.DataFrame(rows).sort_values("weight", ascending=False)
    except Exception:
        pass
    return None


# ─── Source 4: yfinance ──────────────────────────────────────────────────────


def _holdings_yfinance(yf_ticker: str) -> Optional[pd.DataFrame]:
    """Use yfinance to get ETF holdings. Works well for US ETFs (SPY, QQQ, XLU, etc.)."""
    try:
        t = yf.Ticker(yf_ticker)
        holders = None

        # Try multiple attribute names (yfinance API changed across versions)
        for method in ["get_holdings_full", "holdings"]:
            try:
                attr = getattr(t, method, None)
                if attr is None:
                    continue
                holders = attr() if callable(attr) else attr
                if holders is not None and not (hasattr(holders, "empty") and holders.empty):
                    break
            except Exception:
                continue

        if holders is None or (hasattr(holders, "empty") and holders.empty):
            return None

        df = holders.reset_index() if hasattr(holders, "reset_index") else pd.DataFrame(holders)
        col_map = {}
        for c in df.columns:
            cl = str(c).lower()
            if any(k in cl for k in ["symbol", "ticker"]):
                col_map[c] = "ticker"
            elif any(k in cl for k in ["weight", "pct", "percent"]):
                col_map[c] = "weight"
            elif any(k in cl for k in ["name", "description"]):
                col_map[c] = "name"
        df = df.rename(columns=col_map)

        if "ticker" not in df.columns:
            return None
        if "weight" not in df.columns:
            df["weight"] = 1.0 / max(len(df), 1)
        if "name" not in df.columns:
            df["name"] = df["ticker"]

        df["weight"] = pd.to_numeric(df["weight"], errors="coerce").fillna(0)
        if df["weight"].max() > 1.5:
            df["weight"] = df["weight"] / 100.0
        df = df[df["weight"] > 0].sort_values("weight", ascending=False)
        return df[["ticker", "weight", "name"]].head(100)
    except Exception:
        return None


# ─── Source 5: etf.com scrape ────────────────────────────────────────────────


def _holdings_etfcom(base_ticker: str) -> Optional[pd.DataFrame]:
    """Scrape holdings table from etf.com (US ETFs only)."""
    try:
        url = f"https://www.etf.com/{base_ticker}"
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code != 200:
            return None
        tables = pd.read_html(r.text)
        for tbl in tables:
            result = _normalise_holdings_table(tbl)
            if result is not None and not result.empty:
                return result
    except Exception:
        pass
    return None


# ─── Shared helper ────────────────────────────────────────────────────────────


def _normalise_holdings_table(tbl: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Try to extract (ticker, weight, name) from an arbitrary HTML table.
    Returns None if the table doesn't look like a holdings table.
    """
    cols_lower = [str(c).lower() for c in tbl.columns]
    has_ticker = any("ticker" in c or "symbol" in c for c in cols_lower)
    has_weight = any("weight" in c or "alloc" in c or "%" in c for c in cols_lower)
    if not (has_ticker and has_weight):
        return None

    try:
        col_sym = next(
            c for c in tbl.columns
            if "ticker" in str(c).lower() or "symbol" in str(c).lower()
        )
        col_wt = next(
            c for c in tbl.columns
            if "weight" in str(c).lower() or "%" in str(c) or "alloc" in str(c).lower()
        )
        col_nm = next(
            (c for c in tbl.columns if "name" in str(c).lower()), col_sym
        )
        df = tbl[[col_sym, col_wt, col_nm]].copy()
        df.columns = ["ticker", "weight", "name"]
        df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
        df["weight"] = (
            df["weight"].astype(str)
            .str.replace("%", "").str.replace(",", "").str.strip()
        )
        df["weight"] = pd.to_numeric(df["weight"], errors="coerce").fillna(0)
        if df["weight"].max() > 1.5:
            df["weight"] = df["weight"] / 100.0
        df = df[df["weight"] > 0]
        df = df[df["ticker"].str.match(r"^[A-Z0-9\.\-]+$", na=False)]
        df = df[~df["ticker"].isin(["", "NAN", "CASH", "-", "N/A"])]
        if len(df) < 3:
            return None
        return df[["ticker", "weight", "name"]].sort_values("weight", ascending=False).head(100)
    except Exception:
        return None


# ─── FX rate ─────────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def get_fx_rate(from_ccy: str, to_ccy: str) -> float:
    """Return exchange rate from_ccy → to_ccy."""
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
    fallbacks = {"CADUSD": 0.73, "USDCAD": 1.37}
    return fallbacks.get(f"{from_ccy}{to_ccy}", 1.0)
