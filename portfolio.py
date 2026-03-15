"""
Portfolio engine: takes user positions, resolves all holdings to stocks,
and produces exposure breakdowns by market, country, sector, and stock.
"""

import pandas as pd
from typing import List, Dict
from data_fetcher import get_price_and_meta, get_fx_rate, normalise_ticker
from etf_resolver import resolve_holdings, aggregate_leaf_holdings


# ─── Exchange → Market mapping ────────────────────────────────────────────────

EXCHANGE_TO_MARKET = {
    # US
    "NMS": "US", "NYQ": "US", "NGM": "US", "NCM": "US",
    "ASE": "US", "PCX": "US", "BATS": "US", "CBOE": "US",
    "NYA": "US", "ARCX": "US", "XNAS": "US", "XNYS": "US",
    "NASDAQ": "US", "NYSE": "US",
    # Canada
    "TOR": "Canada", "TSX": "Canada", "TSXV": "Canada",
    "XTSE": "Canada", "CVE": "Canada",
    # Europe
    "LSE": "Europe", "PAR": "Europe", "FRA": "Europe", "AMS": "Europe",
    "STO": "Europe", "MCE": "Europe", "MIL": "Europe", "OSL": "Europe",
    "VIE": "Europe", "ZRH": "Europe", "HEL": "Europe", "LIS": "Europe",
    "BRU": "Europe", "DUB": "Europe",
    # Asia-Pacific
    "TKS": "Japan", "OSA": "Japan",
    "SHH": "China", "SHZ": "China", "HKG": "Hong Kong",
    "KSC": "South Korea", "KOE": "South Korea",
    "TAI": "Taiwan",
    "NSI": "India", "BSE": "India",
    "ASX": "Australia",
    "SGX": "Singapore",
    # Other
    "SAO": "Brazil", "BMV": "Mexico",
}

COUNTRY_TO_MARKET = {
    "United States": "US",
    "Canada": "Canada",
    "United Kingdom": "Europe",
    "Germany": "Europe",
    "France": "Europe",
    "Netherlands": "Europe",
    "Switzerland": "Europe",
    "Sweden": "Europe",
    "Denmark": "Europe",
    "Norway": "Europe",
    "Finland": "Europe",
    "Spain": "Europe",
    "Italy": "Europe",
    "Belgium": "Europe",
    "Austria": "Europe",
    "Portugal": "Europe",
    "Ireland": "Europe",
    "Japan": "Japan",
    "China": "China",
    "Hong Kong": "Hong Kong",
    "South Korea": "South Korea",
    "Taiwan": "Taiwan",
    "India": "India",
    "Australia": "Australia",
    "Singapore": "Singapore",
    "Brazil": "Brazil",
    "Mexico": "Mexico",
}


def infer_market(row: pd.Series) -> str:
    exch = str(row.get("exchange") or "")
    country = str(row.get("country") or "")
    mkt = EXCHANGE_TO_MARKET.get(exch.upper())
    if mkt:
        return mkt
    mkt = COUNTRY_TO_MARKET.get(country)
    if mkt:
        return mkt
    # Fallback: .TO → Canada
    if str(row.get("yf_ticker", "")).endswith(".TO"):
        return "Canada"
    return "Other"


# ─── Main engine ──────────────────────────────────────────────────────────────

def build_portfolio(positions: List[Dict], display_ccy: str = "USD") -> Dict:
    """
    positions: list of { ticker, exchange (US|CA), units }
    display_ccy: 'USD' or 'CAD'

    Returns dict with keys:
        positions_detail  – per-position breakdown
        all_leaves        – full resolved leaf holdings DataFrame
        by_market         – exposure by market
        by_country        – exposure by country
        by_sector         – exposure by sector
        by_stock          – top-50 stocks
        total_value       – portfolio total in display_ccy
        errors            – list of error messages
    """
    errors = []
    position_rows = []

    usd_cad = get_fx_rate("USD", "CAD")
    cad_usd = get_fx_rate("CAD", "USD")

    def to_display(amount: float, from_ccy: str) -> float:
        if from_ccy == display_ccy:
            return amount
        if display_ccy == "USD":
            return amount * cad_usd
        return amount * usd_cad

    # ── Step 1: value each top-level position ─────────────────────────────────
    for pos in positions:
        raw_ticker = pos["ticker"].strip().upper()
        exchange = pos["exchange"]
        units = float(pos["units"])

        yf_ticker = normalise_ticker(raw_ticker, exchange)
        meta = get_price_and_meta(yf_ticker)

        if meta.get("price") is None:
            errors.append(f"Could not fetch price for {raw_ticker} ({yf_ticker}). Skipping.")
            continue

        price = meta["price"]
        ccy = meta["currency"]
        value_native = price * units
        value_display = to_display(value_native, ccy)

        position_rows.append(
            {
                "ticker": raw_ticker,
                "yf_ticker": yf_ticker,
                "name": meta["name"],
                "exchange": exchange,
                "units": units,
                "price": price,
                "currency": ccy,
                "value_native": value_native,
                "value_display": value_display,
                "asset_type": meta["asset_type"],
            }
        )

    if not position_rows:
        return {"errors": errors}

    positions_df = pd.DataFrame(position_rows)
    total_value = positions_df["value_display"].sum()

    # ── Step 2: resolve every position to leaf holdings ───────────────────────
    all_leaves = []

    for _, pos in positions_df.iterrows():
        pos_weight = pos["value_display"] / total_value  # fraction of portfolio
        leaves = resolve_holdings(pos["yf_ticker"], parent_weight=1.0)

        if leaves:
            agg = aggregate_leaf_holdings(leaves)
            # Scale leaf weights by position's portfolio share
            for _, leaf in agg.iterrows():
                all_leaves.append(
                    {
                        "ticker": leaf["ticker"],
                        "yf_ticker": leaf["yf_ticker"],
                        "name": leaf["name"],
                        "portfolio_weight": pos_weight * leaf["weight"],
                        "currency": leaf["currency"],
                        "sector": leaf["sector"],
                        "country": leaf["country"],
                        "exchange": leaf["exchange"],
                        "asset_type": leaf["asset_type"],
                    }
                )
        else:
            # Treat top-level position itself as the leaf
            all_leaves.append(
                {
                    "ticker": pos["ticker"],
                    "yf_ticker": pos["yf_ticker"],
                    "name": pos["name"],
                    "portfolio_weight": pos_weight,
                    "currency": pos["currency"],
                    "sector": None,
                    "country": None,
                    "exchange": None,
                    "asset_type": pos["asset_type"],
                }
            )

    leaves_df = pd.DataFrame(all_leaves)

    # Collapse duplicate tickers
    leaves_df = (
        leaves_df.groupby("ticker")
        .agg(
            name=("name", "first"),
            yf_ticker=("yf_ticker", "first"),
            portfolio_weight=("portfolio_weight", "sum"),
            currency=("currency", "first"),
            sector=("sector", "first"),
            country=("country", "first"),
            exchange=("exchange", "first"),
            asset_type=("asset_type", "first"),
        )
        .reset_index()
    )

    # Normalise weights to sum to 1
    total_w = leaves_df["portfolio_weight"].sum()
    if total_w > 0:
        leaves_df["portfolio_weight"] = leaves_df["portfolio_weight"] / total_w

    leaves_df["value_display"] = leaves_df["portfolio_weight"] * total_value
    leaves_df["market"] = leaves_df.apply(infer_market, axis=1)

    # ── Step 3: roll-up breakdowns ────────────────────────────────────────────
    def rollup(col: str) -> pd.DataFrame:
        grp = (
            leaves_df.groupby(col)["portfolio_weight"]
            .sum()
            .reset_index()
            .rename(columns={"portfolio_weight": "weight"})
            .sort_values("weight", ascending=False)
        )
        grp["value_display"] = grp["weight"] * total_value
        grp["pct"] = grp["weight"] * 100
        return grp

    by_market = rollup("market")
    by_country = rollup("country")
    by_sector = rollup("sector")

    by_stock = (
        leaves_df[["ticker", "name", "portfolio_weight", "value_display", "sector", "country", "market"]]
        .sort_values("portfolio_weight", ascending=False)
        .head(50)
        .rename(columns={"portfolio_weight": "weight"})
        .assign(pct=lambda d: d["weight"] * 100)
    )

    return {
        "positions_detail": positions_df,
        "all_leaves": leaves_df,
        "by_market": by_market,
        "by_country": by_country,
        "by_sector": by_sector,
        "by_stock": by_stock,
        "total_value": total_value,
        "display_ccy": display_ccy,
        "errors": errors,
    }
