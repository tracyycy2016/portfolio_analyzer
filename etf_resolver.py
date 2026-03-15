"""
ETF resolver: recursively unrolls ETF holdings down to individual stocks.
Handles ETF-of-ETF nesting up to a configurable depth.
"""

import pandas as pd
import streamlit as st
from typing import Dict, List, Tuple
from data_fetcher import get_price_and_meta, get_etf_holdings, normalise_ticker

MAX_DEPTH = 3          # max recursion levels for ETF-of-ETF
MIN_WEIGHT = 0.0001    # ignore holdings below 0.01%


@st.cache_data(ttl=3600, show_spinner=False)
def resolve_holdings(
    yf_ticker: str,
    parent_weight: float = 1.0,
    depth: int = 0,
    _visited: Tuple[str, ...] = (),
) -> List[Dict]:
    """
    Recursively expand ETF holdings.
    Returns a flat list of dicts:
        { ticker, yf_ticker, name, weight, currency, sector, country, exchange, asset_type }
    where weight is the effective fractional exposure within the parent.
    """
    if depth > MAX_DEPTH or yf_ticker in _visited:
        return []

    meta = get_price_and_meta(yf_ticker)
    asset_type = meta.get("asset_type", "EQUITY")

    # If it's a stock (or we can't determine), return leaf node
    if asset_type not in ("ETF", "MUTUALFUND") or depth == MAX_DEPTH:
        return [
            {
                "ticker": yf_ticker.replace(".TO", "").replace(".V", ""),
                "yf_ticker": yf_ticker,
                "name": meta.get("name", yf_ticker),
                "weight": parent_weight,
                "currency": meta.get("currency", "USD"),
                "sector": meta.get("sector"),
                "country": meta.get("country"),
                "exchange": meta.get("exchange"),
                "asset_type": asset_type,
                "price": meta.get("price"),
            }
        ]

    holdings_df = get_etf_holdings(yf_ticker)

    if holdings_df.empty:
        # Can't resolve holdings — treat ETF itself as a leaf
        return [
            {
                "ticker": yf_ticker.replace(".TO", "").replace(".V", ""),
                "yf_ticker": yf_ticker,
                "name": meta.get("name", yf_ticker),
                "weight": parent_weight,
                "currency": meta.get("currency", "USD"),
                "sector": meta.get("sector"),
                "country": meta.get("country"),
                "exchange": meta.get("exchange"),
                "asset_type": asset_type,
                "price": meta.get("price"),
            }
        ]

    results = []
    total_weight = holdings_df["weight"].sum()
    if total_weight <= 0:
        total_weight = 1.0

    visited_new = _visited + (yf_ticker,)

    for _, row in holdings_df.iterrows():
        child_ticker = str(row["ticker"]).strip().upper()
        if not child_ticker or child_ticker in ("", "NAN"):
            continue
        effective_weight = parent_weight * (row["weight"] / total_weight)
        if effective_weight < MIN_WEIGHT:
            continue

        # Determine if child is US or CA listed (best-guess from suffix)
        child_yf = child_ticker
        child_meta = get_price_and_meta(child_yf)
        child_type = child_meta.get("asset_type", "EQUITY")

        if child_type in ("ETF", "MUTUALFUND"):
            sub = resolve_holdings(
                child_yf,
                effective_weight,
                depth + 1,
                visited_new,
            )
            results.extend(sub)
        else:
            results.append(
                {
                    "ticker": child_ticker,
                    "yf_ticker": child_yf,
                    "name": row.get("name", child_ticker),
                    "weight": effective_weight,
                    "currency": child_meta.get("currency", "USD"),
                    "sector": child_meta.get("sector"),
                    "country": child_meta.get("country"),
                    "exchange": child_meta.get("exchange"),
                    "asset_type": child_type,
                    "price": child_meta.get("price"),
                }
            )

    return results


def aggregate_leaf_holdings(leaf_list: List[Dict]) -> pd.DataFrame:
    """
    Collapse duplicate tickers by summing weights.
    Returns DataFrame sorted by weight desc.
    """
    if not leaf_list:
        return pd.DataFrame()

    df = pd.DataFrame(leaf_list)
    agg = (
        df.groupby("ticker")
        .agg(
            name=("name", "first"),
            yf_ticker=("yf_ticker", "first"),
            weight=("weight", "sum"),
            currency=("currency", "first"),
            sector=("sector", "first"),
            country=("country", "first"),
            exchange=("exchange", "first"),
            asset_type=("asset_type", "first"),
            price=("price", "first"),
        )
        .reset_index()
        .sort_values("weight", ascending=False)
    )
    return agg
