"""
Portfolio engine — two separate pipelines:

Pipeline A (Sector exposure):
  Uses funds_data.sector_weightings from yfinance — 100% coverage, no unanalysed gap.
  Each ETF contributes its full sector breakdown weighted by its portfolio value.

Pipeline B (Top holdings + Market/Country exposure):
  Uses funds_data.top_holdings (top 10 per ETF, correct yfinance tickers).
  Market/country inferred from yfinance metadata on those holdings.
  Unanalysed portion shown honestly for market/country tabs.
"""

import pandas as pd
import yfinance as yf
import streamlit as st
from typing import List, Dict, Optional
from data_fetcher import (get_price_and_meta, get_fx_rate, normalise_ticker,
                          get_etf_holdings, get_sector_weightings,
                          get_top_holdings_funds, get_etf_market_weights,
                          SECTOR_KEY_MAP)


# SECTOR_KEY_MAP imported from data_fetcher

# ── Commodity detection ───────────────────────────────────────────────────────

COMMODITY_TICKERS = {
    "ZGLD", "ZGLD.TO", "IAU", "GLD", "SLV", "PDBC", "DJP",
    "USO", "UNG", "BNO", "DBO", "CPER", "PALL", "PPLT",
    "COMT", "FTGC", "CMDY",
}


def is_commodity(ticker: str, name: str = "") -> bool:
    base = ticker.upper().replace(".TO", "").replace(".V", "")
    if base in COMMODITY_TICKERS:
        return True
    name_l = (name or "").lower()
    if any(k in name_l for k in ["gold", "silver", "oil", "bullion", "commodity",
                                   "platinum", "palladium", "copper", "natural gas"]):
        if "etf" in name_l or "fund" in name_l:
            return True
    return False


# ── Market bucket classification ──────────────────────────────────────────────

DEVELOPED_NA = {"United States", "Canada"}
DEVELOPED_NON_NA = {
    "United Kingdom", "Germany", "France", "Netherlands", "Switzerland",
    "Sweden", "Denmark", "Norway", "Finland", "Spain", "Italy", "Belgium",
    "Austria", "Portugal", "Ireland", "Luxembourg", "New Zealand",
    "Japan", "Australia", "Singapore", "Hong Kong", "Israel", "South Korea",
}
EMERGING = {
    "China", "India", "Brazil", "Mexico", "Taiwan", "South Africa",
    "Russia", "Indonesia", "Thailand", "Malaysia", "Philippines",
    "Vietnam", "Turkey", "Egypt", "Saudi Arabia", "United Arab Emirates",
    "Qatar", "Kuwait", "Greece", "Czech Republic", "Hungary", "Poland",
    "Chile", "Colombia", "Peru", "Pakistan",
}

EXCHANGE_TO_BUCKET = {
    "NMS": "Developed Market (North America)", "NYQ": "Developed Market (North America)",
    "NGM": "Developed Market (North America)", "NCM": "Developed Market (North America)",
    "ASE": "Developed Market (North America)", "PCX": "Developed Market (North America)",
    "BATS": "Developed Market (North America)", "NYA": "Developed Market (North America)",
    "ARCX": "Developed Market (North America)", "XNAS": "Developed Market (North America)",
    "XNYS": "Developed Market (North America)", "NASDAQ": "Developed Market (North America)",
    "NYSE": "Developed Market (North America)", "TOR": "Developed Market (North America)",
    "TSX": "Developed Market (North America)", "TSXV": "Developed Market (North America)",
    "XTSE": "Developed Market (North America)",
    "LSE": "Developed Market (Non-North America)", "PAR": "Developed Market (Non-North America)",
    "FRA": "Developed Market (Non-North America)", "AMS": "Developed Market (Non-North America)",
    "STO": "Developed Market (Non-North America)", "MCE": "Developed Market (Non-North America)",
    "MIL": "Developed Market (Non-North America)", "OSL": "Developed Market (Non-North America)",
    "VIE": "Developed Market (Non-North America)", "ZRH": "Developed Market (Non-North America)",
    "HEL": "Developed Market (Non-North America)", "LIS": "Developed Market (Non-North America)",
    "BRU": "Developed Market (Non-North America)", "CPH": "Developed Market (Non-North America)",
    "TKS": "Developed Market (Non-North America)", "OSA": "Developed Market (Non-North America)",
    "ASX": "Developed Market (Non-North America)", "SGX": "Developed Market (Non-North America)",
    "HKG": "Developed Market (Non-North America)", "KSC": "Developed Market (Non-North America)",
    "KOE": "Developed Market (Non-North America)",
    "SHH": "Emerging Market", "SHZ": "Emerging Market",
    "TAI": "Emerging Market", "NSI": "Emerging Market", "BSE": "Emerging Market",
    "SAO": "Emerging Market", "BMV": "Emerging Market",
}

SUFFIX_TO_BUCKET = {
    ".TO": "Developed Market (North America)", ".V": "Developed Market (North America)",
    ".AX": "Developed Market (Non-North America)", ".T": "Developed Market (Non-North America)",
    ".HK": "Developed Market (Non-North America)", ".KS": "Developed Market (Non-North America)",
    ".L": "Developed Market (Non-North America)", ".DE": "Developed Market (Non-North America)",
    ".PA": "Developed Market (Non-North America)", ".MC": "Developed Market (Non-North America)",
    ".SW": "Developed Market (Non-North America)", ".AS": "Developed Market (Non-North America)",
    ".CO": "Developed Market (Non-North America)", ".ST": "Developed Market (Non-North America)",
    ".OL": "Developed Market (Non-North America)", ".MI": "Developed Market (Non-North America)",
    ".SI": "Developed Market (Non-North America)", ".NZ": "Developed Market (Non-North America)",
    ".TW": "Emerging Market", ".NS": "Emerging Market", ".BO": "Emerging Market",
    ".SS": "Emerging Market", ".SZ": "Emerging Market",
}


def infer_market(row: pd.Series) -> str:
    yf_t    = str(row.get("yf_ticker") or row.get("ticker") or "")
    exch    = str(row.get("exchange") or "")
    country = str(row.get("country") or "")

    # Country is the most reliable classifier — a Dutch company listed on NASDAQ
    # is still Non-North America exposure. Check country FIRST.
    if country in DEVELOPED_NA:
        return "Developed Market (North America)"
    if country in DEVELOPED_NON_NA:
        return "Developed Market (Non-North America)"
    if country in EMERGING:
        return "Emerging Market"

    # Fall back to exchange code (for stocks with no country metadata)
    bucket = EXCHANGE_TO_BUCKET.get(exch.upper())
    if bucket:
        return bucket

    # Fall back to yfinance ticker suffix
    for suffix, bucket in SUFFIX_TO_BUCKET.items():
        if yf_t.endswith(suffix):
            return bucket

    # Bare alphabetic ticker with no suffix → assume North America
    if yf_t and "." not in yf_t and yf_t.isalpha() and len(yf_t) <= 5:
        return "Developed Market (North America)"

    return "Other"


# Sector + holdings functions imported from data_fetcher

# ── Main engine ───────────────────────────────────────────────────────────────

def build_portfolio(positions: List[Dict], display_ccy: str = "USD") -> Dict:
    errors = []
    position_rows = []
    usd_cad = get_fx_rate("USD", "CAD")
    cad_usd = get_fx_rate("CAD", "USD")

    def to_display(amount: float, from_ccy: str) -> float:
        if from_ccy == display_ccy:
            return amount
        return amount * (cad_usd if display_ccy == "USD" else usd_cad)

    # ── Step 1: Value positions ────────────────────────────────────────────────
    for pos in positions:
        raw_ticker = pos["ticker"].strip().upper()
        yf_ticker  = normalise_ticker(raw_ticker, pos["exchange"])
        units      = int(pos["units"])
        meta       = get_price_and_meta(yf_ticker)

        if meta.get("price") is None:
            errors.append(f"Could not fetch price for {raw_ticker} ({yf_ticker}). Skipping.")
            continue

        value_native  = meta["price"] * units
        value_display = to_display(value_native, meta["currency"])
        position_rows.append({
            "ticker": raw_ticker, "yf_ticker": yf_ticker,
            "name": meta["name"], "exchange": pos["exchange"],
            "units": units, "price": meta["price"],
            "currency": meta["currency"], "value_native": value_native,
            "value_display": value_display, "asset_type": meta["asset_type"],
        })

    if not position_rows:
        return {"errors": errors}

    positions_df = pd.DataFrame(position_rows)
    total_value  = positions_df["value_display"].sum()

    # ── Pipeline A: Sector exposure ────────────────────────────────────────────
    sector_contributions = []   # [{sector, weight_contribution}]

    for _, pos in positions_df.iterrows():
        pos_weight = pos["value_display"] / total_value

        if is_commodity(pos["ticker"], pos["name"]):
            sector_contributions.append({
                "sector": "N/A (Commodity)", "weight": pos_weight
            })
            continue

        if pos["asset_type"] in ("ETF", "MUTUALFUND"):
            sw = get_sector_weightings(pos["yf_ticker"])
            if sw:
                # Handle ETF-of-ETF: if top_holdings is a single ETF holding 100%,
                # recursively get that ETF's sector weightings
                th = get_top_holdings_funds(pos["yf_ticker"])
                if len(th) == 1 and th.iloc[0]["weight"] >= 0.95:
                    underlying = th.iloc[0]["ticker"]
                    sw2 = get_sector_weightings(underlying)
                    if sw2:
                        sw = sw2
                for sector, w in sw.items():
                    sector_contributions.append({
                        "sector": sector, "weight": pos_weight * w
                    })
            else:
                sector_contributions.append({
                    "sector": "Unknown", "weight": pos_weight
                })
        else:
            # Direct stock — get sector from metadata
            meta = get_price_and_meta(pos["yf_ticker"])
            sector = meta.get("sector") or "Unknown"
            sector_contributions.append({"sector": sector, "weight": pos_weight})

    by_sector_df = pd.DataFrame(sector_contributions)
    by_sector = (
        by_sector_df.groupby("sector")["weight"].sum()
        .reset_index().rename(columns={"weight": "weight"})
        .sort_values("weight", ascending=False)
    )
    by_sector["value_display"] = by_sector["weight"] * total_value
    by_sector["pct"] = by_sector["weight"] * 100

    # ── Pipeline A-market: Market exposure via fund description/category ────────
    market_contributions = []

    for _, pos in positions_df.iterrows():
        pos_weight = pos["value_display"] / total_value

        if is_commodity(pos["ticker"], pos["name"]):
            market_contributions.append({"market": "N/A", "weight": pos_weight})
            continue

        if pos["asset_type"] in ("ETF", "MUTUALFUND"):
            # Try direct market classification from fund description
            mw = get_etf_market_weights(pos["yf_ticker"])
            if mw is None:
                # ETF wrapper (e.g. VFV.TO→VOO): try underlying
                th = get_top_holdings_funds(pos["yf_ticker"])
                if len(th) == 1 and th.iloc[0]["weight"] >= 0.95:
                    mw = get_etf_market_weights(th.iloc[0]["ticker"])
            if mw:
                for mkt, w in mw.items():
                    market_contributions.append({"market": mkt, "weight": pos_weight * w})
            else:
                # Mixed ETF (e.g. IGF) — use top holdings to infer
                market_contributions.append({"market": "Mixed/Other", "weight": pos_weight})
        else:
            # Direct stock — infer from metadata
            meta = get_price_and_meta(pos["yf_ticker"])
            mkt = infer_market(pd.Series({
                "yf_ticker": pos["yf_ticker"],
                "exchange": meta.get("exchange"),
                "country": meta.get("country"),
            }))
            market_contributions.append({"market": mkt, "weight": pos_weight})

    market_df = pd.DataFrame(market_contributions)
    by_market_fund = (
        market_df.groupby("market")["weight"].sum()
        .reset_index().rename(columns={"weight": "weight"})
        .sort_values("weight", ascending=False)
    )
    by_market_fund["value_display"] = by_market_fund["weight"] * total_value
    by_market_fund["pct"] = by_market_fund["weight"] * 100

    # ── Pipeline B: Top holdings → Market/Country exposure ────────────────────
    all_leaves = []
    total_unanalysed_weight = 0.0

    for _, pos in positions_df.iterrows():
        pos_weight = pos["value_display"] / total_value

        if is_commodity(pos["ticker"], pos["name"]):
            all_leaves.append({
                "ticker": pos["ticker"], "yf_ticker": pos["yf_ticker"],
                "name": pos["name"], "portfolio_weight": pos_weight,
                "sector": "N/A (Commodity)", "country": "N/A",
                "exchange": None, "asset_type": pos["asset_type"],
                "source_position": pos["ticker"],
            })
            continue

        if pos["asset_type"] in ("ETF", "MUTUALFUND"):
            # Use funds_data.top_holdings first (correct tickers, up to 10)
            th = get_top_holdings_funds(pos["yf_ticker"])

            # Fall back to stockanalysis if funds_data returns empty or single ETF row
            if th.empty or (len(th) == 1 and th.iloc[0]["weight"] >= 0.95):
                underlying_ticker = th.iloc[0]["ticker"] if not th.empty else None
                if underlying_ticker:
                    # ETF-of-ETF: use underlying's top_holdings
                    th2 = get_top_holdings_funds(underlying_ticker)
                    if not th2.empty:
                        th = th2
                else:
                    th = pd.DataFrame(columns=["ticker", "weight", "name"])

            if th.empty:
                th = get_etf_holdings(pos["yf_ticker"])

            if not th.empty:
                captured = min(1.0, th["weight"].sum())
                unanalysed = max(0.0, 1.0 - captured)
                total_unanalysed_weight += pos_weight * unanalysed
                leaf_scale = captured

                for _, leaf in th.iterrows():
                    leaf_meta = get_price_and_meta(leaf["ticker"])
                    all_leaves.append({
                        "ticker":           leaf["ticker"],
                        "yf_ticker":        leaf["ticker"],
                        "name":             leaf.get("name") or leaf_meta.get("name", leaf["ticker"]),
                        "portfolio_weight": pos_weight * leaf["weight"] * leaf_scale,
                        "sector":           leaf_meta.get("sector"),
                        "country":          leaf_meta.get("country"),
                        "exchange":         leaf_meta.get("exchange"),
                        "asset_type":       leaf_meta.get("asset_type", "EQUITY"),
                        "source_position":  pos["ticker"],
                    })
            else:
                # No holdings at all — treat as leaf
                all_leaves.append({
                    "ticker": pos["ticker"], "yf_ticker": pos["yf_ticker"],
                    "name": pos["name"], "portfolio_weight": pos_weight,
                    "sector": None, "country": None,
                    "exchange": None, "asset_type": pos["asset_type"],
                })
        else:
            meta = get_price_and_meta(pos["yf_ticker"])
            all_leaves.append({
                "ticker": pos["ticker"], "yf_ticker": pos["yf_ticker"],
                "name": pos["name"], "portfolio_weight": pos_weight,
                "sector": meta.get("sector"), "country": meta.get("country"),
                "exchange": meta.get("exchange"), "asset_type": pos["asset_type"],
                "source_position": pos["ticker"],
            })

    # Collapse duplicates
    # Keep individual leaf rows (don't collapse by ticker) so source_position is preserved
    # for the breakdown tables. Collapse only for the aggregated views.
    leaves_df = pd.DataFrame(all_leaves)
    if "source_position" not in leaves_df.columns:
        leaves_df["source_position"] = ""

    # Add unanalysed row if needed
    if total_unanalysed_weight > 0.001:
        leaves_df = pd.concat([leaves_df, pd.DataFrame([{
            "ticker": "UNANALYSED", "yf_ticker": "UNANALYSED",
            "name": "Unanalysed Holdings", "portfolio_weight": total_unanalysed_weight,
            "sector": "Unanalysed", "country": "Unanalysed",
            "exchange": None, "asset_type": "OTHER",
        }])], ignore_index=True)

    leaves_df["value_display"] = leaves_df["portfolio_weight"] * total_value

    # Infer market bucket
    def classify_market(row):
        if row["ticker"] == "UNANALYSED":
            return "Unanalysed"
        if is_commodity(row["ticker"], row.get("name", "")):
            return "N/A"
        return infer_market(row)

    leaves_df["market"] = leaves_df.apply(classify_market, axis=1)

    # Roll-up market and country
    def rollup(df, col):
        grp = (
            df.groupby(col)["portfolio_weight"].sum()
            .reset_index().rename(columns={"portfolio_weight": "weight"})
            .sort_values("weight", ascending=False)
        )
        grp["value_display"] = grp["weight"] * total_value
        grp["pct"] = grp["weight"] * 100
        return grp

    by_market  = rollup(leaves_df, "market")
    by_country = rollup(leaves_df, "country")

    # Top 50 stocks — aggregate duplicate tickers across positions, keep source_position
    stock_df = leaves_df[leaves_df["ticker"] != "UNANALYSED"].copy()
    # Collapse duplicate tickers (same stock held by multiple ETFs) for aggregate view
    by_stock_agg = (
        stock_df.groupby("ticker")
        .agg(
            name=("name", "first"),
            weight=("portfolio_weight", "sum"),
            value_display=("value_display", "sum"),
            sector=("sector", "first"),
            country=("country", "first"),
            market=("market", "first"),
            source_position=("source_position", lambda x: ", ".join(sorted(set(x)))),
        )
        .reset_index()
        .sort_values("weight", ascending=False)
        .head(50)
        .assign(pct=lambda d: d["weight"] * 100)
    )
    by_stock = by_stock_agg

    return {
        "positions_detail": positions_df,
        "all_leaves":       leaves_df,
        "by_market":        by_market_fund,   # from fund description (100% coverage)
        "by_market_detail": by_market,        # from top holdings (partial, for breakdown table)
        "by_country":       by_country,
        "by_sector":        by_sector,
        "by_stock":         by_stock,
        "total_value":      total_value,
        "display_ccy":      display_ccy,
        "errors":           errors,
    }
