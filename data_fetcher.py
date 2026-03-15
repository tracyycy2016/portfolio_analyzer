"""
Data fetcher: prices, ETF holdings, and metadata via yfinance + fallback sources.
"""

import yfinance as yf
import requests
import pandas as pd
import time
import streamlit as st
from functools import lru_cache
from typing import Optional

# ─── Ticker normalisation ────────────────────────────────────────────────────

def normalise_ticker(ticker: str, exchange: str) -> str:
    """Return the yfinance-compatible symbol."""
    ticker = ticker.strip().upper()
    if exchange == "CA":
        if not ticker.endswith(".TO") and not ticker.endswith(".V"):
            return ticker + ".TO"
    return ticker

# ─── Price fetching ──────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def get_price_and_meta(yf_ticker: str) -> dict:
    """Return price (USD) and basic metadata for a single ticker."""
    try:
        t = yf.Ticker(yf_ticker)
        info = t.info or {}

        price = (
            info.get("currentPrice")
            or info.get("regularMarketPrice")
            or info.get("navPrice")
        )

        # fallback to fast_info
        if not price:
            fi = t.fast_info
            price = getattr(fi, "last_price", None)

        currency = (info.get("currency") or "USD").upper()
        name = info.get("longName") or info.get("shortName") or yf_ticker
        asset_type = info.get("quoteType", "EQUITY").upper()  # EQUITY / ETF / MUTUALFUND

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


# ─── ETF holdings ────────────────────────────────────────────────────────────

@st.cache_data(ttl=3600)
def get_etf_holdings(yf_ticker: str) -> pd.DataFrame:
    """
    Return a DataFrame with columns: ticker, weight (0-1), name
    for the top holdings of an ETF.
    Tries yfinance first, then etf.com scraper for US ETFs.
    """
    df = _holdings_yfinance(yf_ticker)
    if df is not None and len(df) > 0:
        return df

    # Fallback: etf.com for US-listed ETFs (no .TO suffix)
    base = yf_ticker.replace(".TO", "").replace(".V", "")
    df2 = _holdings_etfcom(base)
    if df2 is not None and len(df2) > 0:
        return df2

    return pd.DataFrame(columns=["ticker", "weight", "name"])


def _holdings_yfinance(yf_ticker: str) -> Optional[pd.DataFrame]:
    try:
        t = yf.Ticker(yf_ticker)
        holders = t.get_holdings_full()
        if holders is None or holders.empty:
            holders = t.holdings  # older yfinance attr
        if holders is None or (hasattr(holders, "empty") and holders.empty):
            return None

        df = holders.reset_index()
        # yfinance columns vary by version; normalise
        col_map = {}
        for c in df.columns:
            cl = c.lower()
            if "symbol" in cl or "ticker" in cl:
                col_map[c] = "ticker"
            elif "weight" in cl or "pct" in cl or "percent" in cl:
                col_map[c] = "weight"
            elif "name" in cl or "description" in cl:
                col_map[c] = "name"
        df = df.rename(columns=col_map)

        if "ticker" not in df.columns:
            return None
        if "weight" not in df.columns:
            df["weight"] = 1.0 / len(df)
        if "name" not in df.columns:
            df["name"] = df["ticker"]

        df["weight"] = pd.to_numeric(df["weight"], errors="coerce").fillna(0)
        # Normalise if weights look like percentages (>1)
        if df["weight"].max() > 1.5:
            df["weight"] = df["weight"] / 100.0
        df = df[df["weight"] > 0].sort_values("weight", ascending=False)
        return df[["ticker", "weight", "name"]].head(100)
    except Exception:
        return None


def _holdings_etfcom(base_ticker: str) -> Optional[pd.DataFrame]:
    """Scrape holdings from etf.com (US ETFs only, best-effort)."""
    try:
        url = f"https://www.etf.com/{base_ticker}"
        headers = {"User-Agent": "Mozilla/5.0"}
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return None
        tables = pd.read_html(resp.text)
        for tbl in tables:
            cols = [str(c).lower() for c in tbl.columns]
            if any("symbol" in c or "ticker" in c for c in cols) and any("weight" in c for c in cols):
                col_sym = next(c for c in tbl.columns if "symbol" in str(c).lower() or "ticker" in str(c).lower())
                col_wt = next(c for c in tbl.columns if "weight" in str(c).lower())
                col_nm = next((c for c in tbl.columns if "name" in str(c).lower()), col_sym)
                df = tbl[[col_sym, col_wt, col_nm]].copy()
                df.columns = ["ticker", "weight", "name"]
                df["weight"] = df["weight"].astype(str).str.replace("%", "").str.strip()
                df["weight"] = pd.to_numeric(df["weight"], errors="coerce").fillna(0) / 100.0
                df = df[df["weight"] > 0].sort_values("weight", ascending=False)
                return df.head(100)
    except Exception:
        pass
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
    # Hardcoded fallback (approximate)
    fallbacks = {"CADUSD": 0.73, "USDCAD": 1.37}
    key = f"{from_ccy}{to_ccy}"
    return fallbacks.get(key, 1.0)
