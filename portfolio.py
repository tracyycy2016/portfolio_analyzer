"""
Portfolio engine: takes user positions, resolves all holdings to stocks,
and produces exposure breakdowns by market, country, sector, and stock.
"""

import pandas as pd
from typing import List, Dict
from data_fetcher import get_price_and_meta, get_fx_rate, normalise_ticker
from etf_resolver import resolve_holdings, aggregate_leaf_holdings


# ── Commodity tickers (no sector/country/market data meaningful) ──────────────
# These are physical commodity ETFs — their "holdings" are commodities, not stocks.
COMMODITY_TICKERS = {
    "ZGLD", "ZGLD.TO", "IAU", "GLD", "SLV", "PDBC", "DJP",
    "USO", "UNG", "BNO", "DBO", "CPER", "PALL", "PPLT",
    "COMT", "FTGC", "CMDY",
}


# ── Market classification ──────────────────────────────────────────────────────
# New 4-bucket system:
#   Developed Market (North America)      = US + Canada
#   Developed Market (Non-North America)  = Europe + Japan + Australia + other DM
#   Emerging Market                       = China, India, Brazil, EM Asia, etc.
#   Other / N/A

DEVELOPED_NA = {
    "United States", "Canada",
}

DEVELOPED_NON_NA = {
    "United Kingdom", "Germany", "France", "Netherlands", "Switzerland",
    "Sweden", "Denmark", "Norway", "Finland", "Spain", "Italy", "Belgium",
    "Austria", "Portugal", "Ireland", "Luxembourg", "New Zealand",
    "Japan", "Australia", "Singapore", "Hong Kong", "Israel",
    "South Korea",   # MSCI classifies Korea as developed
}

EMERGING = {
    "China", "India", "Brazil", "Mexico", "Taiwan", "South Africa",
    "Russia", "Indonesia", "Thailand", "Malaysia", "Philippines",
    "Vietnam", "Turkey", "Egypt", "Saudi Arabia", "United Arab Emirates",
    "Qatar", "Kuwait", "Greece", "Czech Republic", "Hungary", "Poland",
    "Chile", "Colombia", "Peru", "Pakistan",
}

# Exchange code → market bucket
EXCHANGE_TO_BUCKET = {
    # Developed NA
    "NMS": "Developed Market (North America)",
    "NYQ": "Developed Market (North America)",
    "NGM": "Developed Market (North America)",
    "NCM": "Developed Market (North America)",
    "ASE": "Developed Market (North America)",
    "PCX": "Developed Market (North America)",
    "BATS": "Developed Market (North America)",
    "CBOE": "Developed Market (North America)",
    "NYA": "Developed Market (North America)",
    "ARCX": "Developed Market (North America)",
    "XNAS": "Developed Market (North America)",
    "XNYS": "Developed Market (North America)",
    "NASDAQ": "Developed Market (North America)",
    "NYSE": "Developed Market (North America)",
    "TOR": "Developed Market (North America)",
    "TSX": "Developed Market (North America)",
    "TSXV": "Developed Market (North America)",
    "XTSE": "Developed Market (North America)",
    "CVE": "Developed Market (North America)",
    # Developed Non-NA
    "LSE": "Developed Market (Non-North America)",
    "PAR": "Developed Market (Non-North America)",
    "FRA": "Developed Market (Non-North America)",
    "AMS": "Developed Market (Non-North America)",
    "STO": "Developed Market (Non-North America)",
    "MCE": "Developed Market (Non-North America)",
    "MIL": "Developed Market (Non-North America)",
    "OSL": "Developed Market (Non-North America)",
    "VIE": "Developed Market (Non-North America)",
    "ZRH": "Developed Market (Non-North America)",
    "HEL": "Developed Market (Non-North America)",
    "LIS": "Developed Market (Non-North America)",
    "BRU": "Developed Market (Non-North America)",
    "DUB": "Developed Market (Non-North America)",
    "CPH": "Developed Market (Non-North America)",
    "TKS": "Developed Market (Non-North America)",
    "OSA": "Developed Market (Non-North America)",
    "ASX": "Developed Market (Non-North America)",
    "SGX": "Developed Market (Non-North America)",
    "HKG": "Developed Market (Non-North America)",
    "KSC": "Developed Market (Non-North America)",
    "KOE": "Developed Market (Non-North America)",
    # Emerging
    "SHH": "Emerging Market",
    "SHZ": "Emerging Market",
    "TAI": "Emerging Market",
    "NSI": "Emerging Market",
    "BSE": "Emerging Market",
    "SAO": "Emerging Market",
    "BMV": "Emerging Market",
}

# yfinance suffix → market bucket fallback
SUFFIX_TO_BUCKET = {
    ".TO": "Developed Market (North America)",
    ".V":  "Developed Market (North America)",
    ".AX": "Developed Market (Non-North America)",
    ".T":  "Developed Market (Non-North America)",
    ".HK": "Developed Market (Non-North America)",
    ".KS": "Developed Market (Non-North America)",   # Korea = developed
    ".TW": "Emerging Market",
    ".NS": "Emerging Market",
    ".BO": "Emerging Market",
    ".SS": "Emerging Market",
    ".SZ": "Emerging Market",
}


def infer_market(row: pd.Series) -> str:
    """Map a leaf holding to one of the 4 market buckets."""
    yf_t = str(row.get("yf_ticker") or row.get("ticker") or "")
    exch  = str(row.get("exchange") or "")
    country = str(row.get("country") or "")

    # 1. Exchange code (most reliable when present)
    bucket = EXCHANGE_TO_BUCKET.get(exch.upper())
    if bucket:
        return bucket

    # 2. Country name
    if country in DEVELOPED_NA:
        return "Developed Market (North America)"
    if country in DEVELOPED_NON_NA:
        return "Developed Market (Non-North America)"
    if country in EMERGING:
        return "Emerging Market"

    # 3. yfinance ticker suffix fallback
    for suffix, bucket in SUFFIX_TO_BUCKET.items():
        if yf_t.endswith(suffix):
            return bucket

    # 4. Bare US ticker (no suffix, no country) → assume North America
    if yf_t and "." not in yf_t and yf_t.isalpha() and len(yf_t) <= 5:
        return "Developed Market (North America)"

    return "Other"


def is_commodity(ticker: str, asset_type: str, name: str) -> bool:
    """Return True if this holding is a commodity/bullion with no equity metadata."""
    t = ticker.upper().replace(".TO", "").replace(".V", "")
    if t in COMMODITY_TICKERS:
        return True
    name_l = (name or "").lower()
    commodity_keywords = ["gold", "silver", "oil", "commodity", "bullion",
                          "platinum", "palladium", "copper", "natural gas",
                          "energy commodity"]
    if any(k in name_l for k in commodity_keywords) and asset_type == "ETF":
        return True
    return False


# ── Metadata enrichment ───────────────────────────────────────────────────────

def enrich_missing_metadata(leaves_df: pd.DataFrame) -> pd.DataFrame:
    """
    For rows with missing sector/country/exchange, fetch from yfinance.
    Batches lookups to avoid redundant calls.
    Only fetches for non-commodity equity-type holdings.
    """
    needs_enrich = leaves_df[
        (leaves_df["sector"].isna() | leaves_df["country"].isna()) &
        ~leaves_df.apply(
            lambda r: is_commodity(r["ticker"], r.get("asset_type",""), r.get("name","")), axis=1
        )
    ]["yf_ticker"].unique()

    if len(needs_enrich) == 0:
        return leaves_df

    enriched = {}
    for yf_t in needs_enrich:
        meta = get_price_and_meta(yf_t)
        if meta.get("sector") or meta.get("country"):
            enriched[yf_t] = {
                "sector":   meta.get("sector"),
                "country":  meta.get("country"),
                "exchange": meta.get("exchange"),
            }

    if not enriched:
        return leaves_df

    def _apply_enrichment(row):
        yf_t = row["yf_ticker"]
        if yf_t in enriched:
            if pd.isna(row["sector"]) or row["sector"] is None:
                row["sector"] = enriched[yf_t]["sector"]
            if pd.isna(row["country"]) or row["country"] is None:
                row["country"] = enriched[yf_t]["country"]
            if pd.isna(row["exchange"]) or row["exchange"] is None:
                row["exchange"] = enriched[yf_t]["exchange"]
        return row

    return leaves_df.apply(_apply_enrichment, axis=1)


# ── Main engine ───────────────────────────────────────────────────────────────

def build_portfolio(positions: List[Dict], display_ccy: str = "USD") -> Dict:
    """
    positions: list of { ticker, exchange (US|CA), units }
    display_ccy: 'USD' or 'CAD'
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
        units = int(pos["units"])

        yf_ticker = normalise_ticker(raw_ticker, exchange)
        meta = get_price_and_meta(yf_ticker)

        if meta.get("price") is None:
            errors.append(f"Could not fetch price for {raw_ticker} ({yf_ticker}). Skipping.")
            continue

        price = meta["price"]
        ccy = meta["currency"]
        value_native = price * units
        value_display = to_display(value_native, ccy)

        position_rows.append({
            "ticker":       raw_ticker,
            "yf_ticker":    yf_ticker,
            "name":         meta["name"],
            "exchange":     exchange,
            "units":        units,
            "price":        price,
            "currency":     ccy,
            "value_native": value_native,
            "value_display":value_display,
            "asset_type":   meta["asset_type"],
        })

    if not position_rows:
        return {"errors": errors}

    positions_df = pd.DataFrame(position_rows)
    total_value = positions_df["value_display"].sum()

    # ── Step 2: resolve every position to leaf holdings ───────────────────────
    all_leaves = []

    for _, pos in positions_df.iterrows():
        pos_weight = pos["value_display"] / total_value
        leaves = resolve_holdings(pos["yf_ticker"], parent_weight=1.0)

        if leaves:
            agg = aggregate_leaf_holdings(leaves)
            for _, leaf in agg.iterrows():
                all_leaves.append({
                    "ticker":           leaf["ticker"],
                    "yf_ticker":        leaf["yf_ticker"],
                    "name":             leaf["name"],
                    "portfolio_weight": pos_weight * leaf["weight"],
                    "currency":         leaf["currency"],
                    "sector":           leaf["sector"],
                    "country":          leaf["country"],
                    "exchange":         leaf["exchange"],
                    "asset_type":       leaf["asset_type"],
                })
        else:
            all_leaves.append({
                "ticker":           pos["ticker"],
                "yf_ticker":        pos["yf_ticker"],
                "name":             pos["name"],
                "portfolio_weight": pos_weight,
                "currency":         pos["currency"],
                "sector":           None,
                "country":          None,
                "exchange":         None,
                "asset_type":       pos["asset_type"],
            })

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

    # ── Step 3: enrich missing sector/country from yfinance ──────────────────
    leaves_df = enrich_missing_metadata(leaves_df)

    # ── Step 4: apply commodity N/A and infer market ──────────────────────────
    def apply_commodity_na(row):
        if is_commodity(row["ticker"], row.get("asset_type", ""), row.get("name", "")):
            row["sector"]  = "N/A (Commodity)"
            row["country"] = "N/A"
            row["market"]  = "N/A"
        else:
            row["market"] = infer_market(row)
        return row

    leaves_df = leaves_df.apply(apply_commodity_na, axis=1)

    # ── Step 5: roll-up breakdowns ────────────────────────────────────────────
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

    by_market  = rollup("market")
    by_country = rollup("country")
    by_sector  = rollup("sector")

    by_stock = (
        leaves_df[[
            "ticker", "name", "portfolio_weight", "value_display",
            "sector", "country", "market"
        ]]
        .sort_values("portfolio_weight", ascending=False)
        .head(50)
        .rename(columns={"portfolio_weight": "weight"})
        .assign(pct=lambda d: d["weight"] * 100)
    )

    return {
        "positions_detail": positions_df,
        "all_leaves":       leaves_df,
        "by_market":        by_market,
        "by_country":       by_country,
        "by_sector":        by_sector,
        "by_stock":         by_stock,
        "total_value":      total_value,
        "display_ccy":      display_ccy,
        "errors":           errors,
    }
