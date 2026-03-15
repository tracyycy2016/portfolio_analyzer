"""
Investment Portfolio Analyzer
Streamlit app — run with: streamlit run app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Portfolio Analyzer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────────
st.markdown(
    """
<style>
@import url('https://fonts.googleapis.com/css2?family=DM+Serif+Display&family=DM+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif;
}

h1, h2, h3 { font-family: 'DM Serif Display', serif; }

/* Sidebar */
[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0f172a 0%, #1e293b 100%);
    border-right: 1px solid #334155;
}
/* Make sidebar content scrollable when many positions added */
[data-testid="stSidebar"] > div:first-child {
    overflow-y: auto !important;
}
[data-testid="stSidebar"] * { color: #e2e8f0 !important; }
[data-testid="stSidebar"] .stTextInput input,
[data-testid="stSidebar"] .stNumberInput input,
[data-testid="stSidebar"] .stSelectbox select {
    background: #1e293b !important;
    border: 1px solid #475569 !important;
    color: #f1f5f9 !important;
    border-radius: 6px;
}

/* Cards */
.metric-card {
    background: linear-gradient(135deg, #1e293b, #0f172a);
    border: 1px solid #334155;
    border-radius: 12px;
    padding: 20px 24px;
    margin-bottom: 12px;
}
.metric-card h3 { color: #94a3b8; font-size: 0.8rem; text-transform: uppercase;
                  letter-spacing: 1.5px; margin: 0 0 6px; font-family: 'DM Sans', sans-serif; font-weight: 500; }
.metric-card .value { color: #f1f5f9; font-size: 1.8rem; font-weight: 600;
                       font-family: 'DM Serif Display', serif; }

/* Tables */
.stDataFrame { border-radius: 10px; overflow: hidden; }
thead tr th { background: #1e293b !important; color: #94a3b8 !important;
              font-size: 0.78rem !important; text-transform: uppercase !important;
              letter-spacing: 1px !important; }

/* Progress bars used for weight viz */
.weight-bar-bg { background: #1e293b; border-radius: 4px; height: 8px; }
.weight-bar    { background: linear-gradient(90deg, #3b82f6, #8b5cf6);
                 border-radius: 4px; height: 8px; }

/* Section headers */
.section-header {
    font-family: 'DM Serif Display', serif;
    font-size: 1.4rem;
    color: #f1f5f9;
    padding: 12px 0 4px;
    border-bottom: 2px solid #334155;
    margin-bottom: 20px;
}
/* Error/warning boxes */
.warn-box {
    background: #1c1917;
    border-left: 3px solid #f59e0b;
    padding: 10px 16px;
    border-radius: 0 8px 8px 0;
    color: #fde68a;
    font-size: 0.85rem;
}
/* Tabs */
.stTabs [data-baseweb="tab-list"] { gap: 8px; }
.stTabs [data-baseweb="tab"] {
    border-radius: 8px 8px 0 0;
    padding: 8px 20px;
    background: #1e293b;
    color: #94a3b8;
}
.stTabs [aria-selected="true"] {
    background: linear-gradient(135deg, #3b82f6, #8b5cf6) !important;
    color: #fff !important;
}
</style>
""",
    unsafe_allow_html=True,
)


# ── Helpers ────────────────────────────────────────────────────────────────────

CCY_SYMBOL = {"USD": "$", "CAD": "C$"}

def fmt_money(val: float, ccy: str) -> str:
    sym = CCY_SYMBOL.get(ccy, "$")
    if abs(val) >= 1_000_000:
        return f"{sym}{val/1_000_000:,.2f}M"
    if abs(val) >= 1_000:
        return f"{sym}{val:,.0f}"
    return f"{sym}{val:,.2f}"

def fmt_pct(val: float) -> str:
    return f"{val:.2f}%"

CHART_COLORS = [
    "#3b82f6", "#8b5cf6", "#10b981", "#f59e0b", "#ef4444",
    "#06b6d4", "#ec4899", "#84cc16", "#f97316", "#6366f1",
    "#14b8a6", "#e879f9", "#fb923c", "#a3e635", "#38bdf8",
]

def donut_chart(labels, values, title: str):
    # Build parallel lists so colors always align with labels
    pairs = list(zip(list(labels), list(values)))
    MUTED = "#334155"
    MUTED_LABELS = {"unanalysed", "n/a", "n/a (commodity)"}
    color_list = []
    ci = 0
    for lbl, _ in pairs:
        if str(lbl).lower() in MUTED_LABELS:
            color_list.append(MUTED)
        else:
            color_list.append(CHART_COLORS[ci % len(CHART_COLORS)])
            ci += 1
    label_list = [p[0] for p in pairs]
    value_list = [p[1] for p in pairs]
    # Only show percent label if slice is large enough to read
    textinfo = "percent"
    # Build index map so we can re-sort back after Plotly inevitably sorts
    # The only reliable way: customdata carries our intended color
    fig = go.Figure(
        go.Pie(
            labels=label_list,
            values=value_list,
            hole=0.55,
            textinfo="percent",
            textfont_size=11,
            insidetextorientation="radial",
            marker=dict(colors=color_list, line=dict(color="#0f172a", width=2)),
            hovertemplate="<b>%{label}</b><br>%{percent:.1%} (%{value:,.0f})<extra></extra>",
            sort=False,
            direction="clockwise",
        )
    )
    fig.update_layout(
        title=dict(text=title, font=dict(family="DM Serif Display", size=16, color="#f1f5f9")),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#94a3b8", family="DM Sans"),
        legend=dict(
            orientation="v",
            x=1.02, y=0.5,
            font=dict(size=11),
            bgcolor="rgba(0,0,0,0)",
        ),
        margin=dict(l=10, r=10, t=40, b=10),
        height=380,
    )
    return fig


def bar_chart(labels, values, title: str, ccy: str, orientation="h"):
    sym = CCY_SYMBOL.get(ccy, "$")
    fig = go.Figure(
        go.Bar(
            x=values if orientation == "h" else labels,
            y=labels if orientation == "h" else values,
            orientation=orientation,
            marker=dict(
                color=CHART_COLORS[: len(labels)],
                line=dict(color="rgba(0,0,0,0)"),
            ),
            hovertemplate=f"<b>%{{y if orientation=='h' else %{{x}}}}</b><br>{sym}%{{x if orientation=='h' else %{{y}}:,.0f}}<extra></extra>",
        )
    )
    fig.update_layout(
        title=dict(text=title, font=dict(family="DM Serif Display", size=16, color="#f1f5f9")),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#94a3b8", family="DM Sans"),
        xaxis=dict(showgrid=False, color="#475569"),
        yaxis=dict(showgrid=True, gridcolor="#1e293b", color="#475569"),
        margin=dict(l=10, r=10, t=40, b=10),
        height=360,
    )
    return fig


def exposure_table(df: pd.DataFrame, weight_col: str, value_col: str,
                   label_col: str, ccy: str) -> pd.DataFrame:
    display = df[[label_col, weight_col, value_col]].copy()
    display[weight_col] = display[weight_col].apply(lambda x: f"{x*100:.2f}%")
    display[value_col] = display[value_col].apply(lambda x: fmt_money(x, ccy))
    display.columns = [label_col.title(), "Weight %", f"Value ({ccy})"]
    return display.reset_index(drop=True)


# ── Sidebar — position input ───────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📈 Portfolio Analyzer")
    st.markdown("---")

    display_ccy = st.selectbox(
        "Display currency",
        ["USD", "CAD"],
        help="All values will be converted to this currency",
    )

    st.markdown("### Add Positions")
    st.caption("Enter ticker, listing exchange, and number of units.")

    # ── Initialise session state ────────────────────────────────────────────
    if "positions" not in st.session_state:
        st.session_state.positions = [{"ticker": "", "exchange": "US", "units": 0}]
    if "csv_loaded_name" not in st.session_state:
        st.session_state.csv_loaded_name = None

    # ── CSV upload ──────────────────────────────────────────────────────────
    with st.expander("📂 Upload CSV to pre-populate", expanded=False):
        st.caption("CSV format: `ticker,exchange,units`  (exchange must be US or CA)")
        uploaded = st.file_uploader(
            "Choose CSV file", type="csv",
            label_visibility="collapsed", key="csv_upload"
        )
        # Only process when a NEW file is uploaded (avoid re-processing on every rerun)
        if uploaded is not None and uploaded.name != st.session_state.csv_loaded_name:
            try:
                import io as _io
                raw_bytes = uploaded.read()
                # Try UTF-8 with BOM strip, fall back to latin-1
                try:
                    raw_text = raw_bytes.decode("utf-8-sig").strip()
                except Exception:
                    raw_text = raw_bytes.decode("latin-1").strip()
                df_csv = pd.read_csv(_io.StringIO(raw_text))
                df_csv.columns = [c.strip().lower() for c in df_csv.columns]
                required = {"ticker", "exchange", "units"}
                if required.issubset(set(df_csv.columns)):
                    new_positions = []
                    skipped = 0
                    for _, row in df_csv.iterrows():
                        try:
                            t = str(row["ticker"]).strip().upper()
                            e = str(row["exchange"]).strip().upper()
                            u_raw = str(row["units"]).strip()
                            if not t or t in ("NAN", "TICKER") or not u_raw or u_raw == "NAN":
                                skipped += 1
                                continue
                            u = int(float(u_raw))
                            if t and e in ("US", "CA") and u > 0:
                                new_positions.append({"ticker": t, "exchange": e, "units": u})
                            else:
                                skipped += 1
                        except Exception:
                            skipped += 1
                    if new_positions:
                        # Explicitly SET each widget key to the new value.
                        # Just deleting keys is not enough — Streamlit may
                        # render the widget before the deletion propagates.
                        for i, p in enumerate(new_positions):
                            st.session_state[f"ticker_{i}"] = p["ticker"]
                            st.session_state[f"exch_{i}"]   = p["exchange"]
                            st.session_state[f"units_{i}"]  = p["units"]
                        # Delete any leftover keys beyond the new list length
                        for i in range(len(new_positions), len(new_positions) + 20):
                            for prefix in ("ticker_", "exch_", "units_"):
                                st.session_state.pop(f"{prefix}{i}", None)
                        st.session_state.positions = new_positions
                        st.session_state.csv_loaded_name = uploaded.name
                        if skipped:
                            st.warning(f"Loaded {len(new_positions)} positions ({skipped} rows skipped).")
                    else:
                        st.error("No valid rows found. Check ticker, exchange (US/CA), and units.")
                else:
                    missing = required - set(df_csv.columns)
                    st.error(f"Missing columns: {missing}. Required: ticker, exchange, units")
            except Exception as e:
                st.error(f"Could not parse CSV: {e}")

        if st.session_state.csv_loaded_name:
            st.success(f"✓ Loaded from {st.session_state.csv_loaded_name} — {len(st.session_state.positions)} positions")

    # ── Position rows ────────────────────────────────────────────────────────
    def add_row():
        st.session_state.positions.append({"ticker": "", "exchange": "US", "units": 0})

    def remove_row(i):
        st.session_state.positions.pop(i)
        # Clear widget keys for removed row and shift down
        for key in list(st.session_state.keys()):
            if key.startswith(("ticker_", "exch_", "units_")):
                del st.session_state[key]

    for i, pos in enumerate(st.session_state.positions):
        c1, c2, c3, c4 = st.columns([3, 2, 3, 1])
        with c1:
            pos["ticker"] = st.text_input(
                "Ticker", value=pos["ticker"], key=f"ticker_{i}",
                placeholder="e.g. VFV", label_visibility="collapsed"
            )
        with c2:
            pos["exchange"] = st.selectbox(
                "Exchange", ["US", "CA"], key=f"exch_{i}",
                index=0 if pos["exchange"] == "US" else 1,
                label_visibility="collapsed"
            )
        with c3:
            pos["units"] = st.number_input(
                "Units", value=int(pos.get("units", 0)),
                min_value=0, step=1, key=f"units_{i}",
                label_visibility="collapsed"
            )
        with c4:
            if st.button("✕", key=f"del_{i}", help="Remove"):
                remove_row(i)
                st.rerun()

    st.button("＋ Add row", on_click=add_row, use_container_width=True)
    st.markdown("---")
    analyze = st.button("🔍 Analyze Portfolio", type="primary", use_container_width=True)

    st.markdown("---")
    st.caption(
        "Prices via Yahoo Finance · ETF holdings via yfinance & etf.com · "
        "Refresh prices every 5 min · Holdings cache 1 hr"
    )


# ── Main area ──────────────────────────────────────────────────────────────────
st.markdown(
    "<h1 style='font-family:DM Serif Display,serif;color:#f1f5f9;margin-bottom:4px;'>"
    "Portfolio Exposure Analyzer</h1>"
    "<p style='color:#64748b;font-size:0.9rem;margin-bottom:32px;'>"
    "See through every ETF layer — stocks, sectors, countries, and markets.</p>",
    unsafe_allow_html=True,
)

if not analyze:
    st.markdown(
        """
        <div style='background:linear-gradient(135deg,#1e293b,#0f172a);
             border:1px solid #334155;border-radius:16px;padding:48px 40px;text-align:center;'>
        <div style='font-size:3rem;margin-bottom:16px;'>📊</div>
        <h2 style='font-family:DM Serif Display,serif;color:#f1f5f9;margin-bottom:8px;'>
            Ready to analyze your portfolio</h2>
        <p style='color:#64748b;max-width:480px;margin:0 auto;'>
            Add your positions in the sidebar — tickers, exchange (US or CA),
            and number of units — then click <strong style='color:#3b82f6'>Analyze Portfolio</strong>.
            ETFs are automatically unrolled to their underlying holdings.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.stop()


# ── Run analysis ───────────────────────────────────────────────────────────────
valid_positions = [
    p for p in st.session_state.positions
    if p["ticker"].strip() and float(p.get("units", 0)) > 0
]

if not valid_positions:
    st.warning("Please add at least one position with a ticker and units > 0.")
    st.stop()

with st.spinner("Fetching prices and resolving ETF holdings… this may take a minute."):
    from portfolio import build_portfolio
    from data_fetcher import (get_price_and_meta, get_etf_holdings,
                              get_sector_weightings, get_top_holdings_funds)
    # Clear all caches on each run — prevents stale None results from previous failures
    get_price_and_meta.clear()
    get_sector_weightings.clear()
    get_top_holdings_funds.clear()
    result = build_portfolio(valid_positions, display_ccy)

if not result or "total_value" not in result:
    st.error("Could not build portfolio. Check your tickers and try again.")
    if result and result.get("errors"):
        for e in result["errors"]:
            st.markdown(f'<div class="warn-box">⚠ {e}</div>', unsafe_allow_html=True)
    st.stop()

# Show any warnings
for err in result.get("errors", []):
    st.markdown(f'<div class="warn-box">⚠ {err}</div>', unsafe_allow_html=True)

total_value = result["total_value"]
ccy = result["display_ccy"]
sym = CCY_SYMBOL[ccy]

positions_df = result["positions_detail"]
by_market = result["by_market"]
by_country = result["by_country"]
by_sector = result["by_sector"]
by_stock = result["by_stock"]


# ── Summary metrics ────────────────────────────────────────────────────────────
st.markdown("### Portfolio Summary")
m_cols = st.columns(4)
metrics = [
    ("Total Value", fmt_money(total_value, ccy)),
    ("Positions", str(len(positions_df))),
    ("Underlying Stocks", str(len(result["all_leaves"]))),
    ("Display Currency", ccy),
]
for col, (label, val) in zip(m_cols, metrics):
    col.markdown(
        f'<div class="metric-card"><h3>{label}</h3><div class="value">{val}</div></div>',
        unsafe_allow_html=True,
    )


# ── Tabs ───────────────────────────────────────────────────────────────────────
tab_mkt, tab_country, tab_sector, tab_stock, tab_positions, tab_diag = st.tabs(
    ["🌍 By Market", "🗺 By Country", "🏭 By Sector", "📈 Top 50 Stocks", "💼 My Positions", "🔬 Diagnostics"]
)


# ── Tab: By Market ─────────────────────────────────────────────────────────────
with tab_mkt:
    st.markdown('<div class="section-header">Exposure by Market</div>', unsafe_allow_html=True)
    if by_market.empty:
        st.info("No market data available.")
    else:
        c1, c2 = st.columns([1, 1])
        with c1:
            fig = donut_chart(by_market["market"], by_market["value_display"], "Market Allocation")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            tbl = exposure_table(by_market, "weight", "value_display", "market", ccy)
            st.dataframe(tbl, use_container_width=True, hide_index=True)
        unanalysed_pct = by_market[by_market["market"]=="Unanalysed"]["pct"].sum()
        if unanalysed_pct > 0:
            st.caption(f"⚠️ {unanalysed_pct:.1f}% of portfolio is in ETF holdings beyond the analysed top holdings — market classification not available for these.")

        # ── Per-position market breakdown ─────────────────────────────────────
        with st.expander("📋 Market breakdown by position", expanded=False):
            all_leaves = result["all_leaves"]
            mkt_rows = []
            for _, pos in positions_df.iterrows():
                pos_leaves = all_leaves[
                    (all_leaves["ticker"] != "UNANALYSED") &
                    all_leaves.apply(lambda r: pos["yf_ticker"] in str(r.get("yf_ticker","")), axis=1)
                ]
                # Simpler: filter leaves by portfolio_weight contribution
                pos_w_total = pos["value_display"] / total_value
                # Get market breakdown from the leaves that belong to this position
                # We track by matching yf_ticker prefix
                pos_ticker_base = pos["yf_ticker"].replace(".TO","").replace(".V","")
                mask = all_leaves["yf_ticker"].str.upper().str.startswith(pos_ticker_base)
                pos_leaves = all_leaves[mask | (all_leaves["yf_ticker"] == pos["yf_ticker"])]
                if pos_leaves.empty:
                    pos_leaves = all_leaves.sample(0)  # empty
                by_mkt_pos = pos_leaves.groupby("market")["portfolio_weight"].sum()
                for mkt, w in sorted(by_mkt_pos.items(), key=lambda x: -x[1]):
                    if w > 0.0001:
                        mkt_rows.append({
                            "Position": pos["ticker"],
                            "Market": mkt,
                            "% of Portfolio": f"{w*100:.2f}%",
                            f"Value ({ccy})": fmt_money(w * total_value, ccy),
                        })
            if mkt_rows:
                st.dataframe(pd.DataFrame(mkt_rows), use_container_width=True, hide_index=True)
            else:
                st.caption("Market breakdown not available — top holdings not resolved.")


# ── Tab: By Country ────────────────────────────────────────────────────────────
with tab_country:
    st.markdown('<div class="section-header">Exposure by Country</div>', unsafe_allow_html=True)
    if by_country.empty:
        st.info("No country data available.")
    else:
        df_c = by_country.dropna(subset=["country"]).copy()
        df_c["country"] = df_c["country"].fillna("Unknown")
        # Include Unanalysed/N/A in chart so pie % match table % (same denominator)
        # Show top 14 named countries + bundle rest into "Other" for readability
        known = df_c[~df_c["country"].isin(["Unanalysed","N/A","Unknown"])].head(14)
        special = df_c[df_c["country"].isin(["Unanalysed","N/A"])]
        df_c_chart = pd.concat([known, special]).copy()
        c1, c2 = st.columns([1, 1])
        with c1:
            fig = donut_chart(df_c_chart["country"], df_c_chart["value_display"], "Country Allocation")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            tbl = exposure_table(df_c, "weight", "value_display", "country", ccy)
            st.dataframe(tbl, use_container_width=True, hide_index=True)
        unanalysed_country_pct = df_c[df_c["country"].isin(["Unanalysed","N/A"])]["pct"].sum()
        if unanalysed_country_pct > 0:
            st.caption(f"⚠️ {unanalysed_country_pct:.1f}% of portfolio is in ETF holdings beyond the analysed top holdings — country not available for these.")

        # ── Per-position country breakdown ────────────────────────────────────
        with st.expander("📋 Country breakdown by position", expanded=False):
            all_leaves = result["all_leaves"]
            cty_rows = []
            for _, pos in positions_df.iterrows():
                pos_ticker_base = pos["yf_ticker"].replace(".TO","").replace(".V","")
                mask = all_leaves["yf_ticker"].str.upper().str.startswith(pos_ticker_base)
                pos_leaves = all_leaves[mask | (all_leaves["yf_ticker"] == pos["yf_ticker"])]
                by_cty_pos = pos_leaves.groupby("country")["portfolio_weight"].sum().sort_values(ascending=False)
                for country, w in by_cty_pos.items():
                    if w > 0.0001:
                        cty_rows.append({
                            "Position": pos["ticker"],
                            "Country": str(country),
                            "% of Portfolio": f"{w*100:.2f}%",
                            f"Value ({ccy})": fmt_money(w * total_value, ccy),
                        })
            if cty_rows:
                st.dataframe(pd.DataFrame(cty_rows), use_container_width=True, hide_index=True)
            else:
                st.caption("Country breakdown not available — top holdings not resolved.")


# ── Tab: By Sector ─────────────────────────────────────────────────────────────
with tab_sector:
    st.markdown('<div class="section-header">Exposure by Sector</div>', unsafe_allow_html=True)
    st.caption("Sector data sourced directly from ETF fund profiles — 100% portfolio coverage.")
    if by_sector.empty:
        st.info("No sector data available.")
    else:
        df_s = by_sector.dropna(subset=["sector"]).copy()
        df_s["sector"] = df_s["sector"].fillna("Unknown")
        c1, c2 = st.columns([1, 1])
        with c1:
            fig = donut_chart(df_s["sector"], df_s["value_display"], "Sector Allocation")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            tbl = exposure_table(df_s, "weight", "value_display", "sector", ccy)
            st.dataframe(tbl, use_container_width=True, hide_index=True)

        # ── Per-position sector breakdown ────────────────────────────────────
        with st.expander("📋 Sector breakdown by position", expanded=False):
            from data_fetcher import get_sector_weightings as _gsw, get_top_holdings_funds as _gth
            from portfolio import is_commodity
            sector_rows = []
            for _, pos in positions_df.iterrows():
                sw = None
                if is_commodity(pos["ticker"], pos["name"]):
                    sw = {"N/A (Commodity)": 1.0}
                elif pos["asset_type"] in ("ETF", "MUTUALFUND"):
                    sw = _gsw(pos["yf_ticker"])
                    if not sw:
                        th = _gth(pos["yf_ticker"])
                        if not th.empty and th.iloc[0]["weight"] >= 0.95:
                            sw = _gsw(th.iloc[0]["ticker"])
                else:
                    from data_fetcher import get_price_and_meta as _gpm
                    sec = _gpm(pos["yf_ticker"]).get("sector") or "Unknown"
                    sw = {sec: 1.0}
                pos_w = pos["value_display"] / total_value
                if sw:
                    for sector, w in sorted(sw.items(), key=lambda x: -x[1]):
                        if w > 0.001:
                            sector_rows.append({
                                "Position": pos["ticker"],
                                "Sector": sector,
                                "% of Position": f"{w*100:.1f}%",
                                "% of Portfolio": f"{w*pos_w*100:.2f}%",
                                f"Value ({ccy})": fmt_money(w * pos["value_display"], ccy),
                            })
            if sector_rows:
                st.dataframe(pd.DataFrame(sector_rows), use_container_width=True, hide_index=True)


# ── Tab: Top 50 Stocks ─────────────────────────────────────────────────────────
with tab_stock:
    st.markdown('<div class="section-header">Top Underlying Holdings</div>', unsafe_allow_html=True)
    st.caption("Shows top 10 underlying stocks per ETF from fund profile data (yfinance). Direct stock positions are shown in full.")
    if by_stock.empty:
        st.info("No stock data available.")
    else:
        # Bar chart of top 20 — single accent color
        top20 = by_stock.head(20).iloc[::-1]
        fig = go.Figure(
            go.Bar(
                x=top20["pct"],
                y=top20["ticker"],
                orientation="h",
                text=top20["pct"].apply(lambda x: f"{x:.2f}%"),
                textposition="outside",
                marker=dict(
                    color="#3b82f6",
                    line=dict(color="rgba(0,0,0,0)"),
                ),
                hovertemplate="<b>%{y}</b><br>%{x:.2f}%<extra></extra>",
            )
        )
        fig.update_layout(
            title=dict(
                text="Top 20 Holdings by Weight",
                font=dict(family="DM Serif Display", size=16, color="#f1f5f9"),
            ),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            font=dict(color="#94a3b8", family="DM Sans"),
            xaxis=dict(showgrid=True, gridcolor="#1e293b", color="#475569", ticksuffix="%"),
            yaxis=dict(showgrid=False, color="#e2e8f0"),
            margin=dict(l=10, r=60, t=40, b=10),
            height=500,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Full table
        st.markdown("#### All Top 50 Holdings")
        tbl = by_stock.copy()
        tbl["pct"] = tbl["pct"].apply(fmt_pct)
        tbl["value_display"] = tbl["value_display"].apply(lambda x: fmt_money(x, ccy))
        tbl["sector"]  = tbl["sector"].fillna("—").replace("", "—").replace("None", "—")
        tbl["country"] = tbl["country"].fillna("—").replace("", "—").replace("None", "—")
        tbl["market"]  = tbl["market"].fillna("—").replace("", "—").replace("None", "—")
        tbl = tbl.rename(
            columns={
                "ticker": "Ticker",
                "name": "Name",
                "pct": "Weight %",
                "value_display": f"Value ({ccy})",
                "sector": "Sector",
                "country": "Country",
                "market": "Market",
            }
        )[["Ticker", "Name", "Weight %", f"Value ({ccy})", "Sector", "Country", "Market"]]
        st.dataframe(tbl.reset_index(drop=True), use_container_width=True, hide_index=True)


# ── Tab: My Positions ──────────────────────────────────────────────────────────
with tab_positions:
    st.markdown('<div class="section-header">Your Input Positions</div>', unsafe_allow_html=True)
    pos_display = positions_df.copy()
    pos_display["pct_of_portfolio"] = (pos_display["value_display"] / total_value * 100).apply(fmt_pct)
    pos_display["value_display"] = pos_display["value_display"].apply(lambda x: fmt_money(x, ccy))
    pos_display["price"] = pos_display.apply(
        lambda r: fmt_money(r["price"], r["currency"]), axis=1
    )
    pos_display = pos_display.rename(
        columns={
            "ticker": "Ticker",
            "name": "Name",
            "exchange": "Listed",
            "units": "Units",
            "price": "Price",
            "asset_type": "Type",
            "value_display": f"Value ({ccy})",
            "pct_of_portfolio": "Portfolio %",
        }
    )[["Ticker", "Name", "Listed", "Units", "Price", "Type", f"Value ({ccy})", "Portfolio %"]]
    st.dataframe(pos_display.reset_index(drop=True), use_container_width=True, hide_index=True)


# ── Tab: Diagnostics ───────────────────────────────────────────────────────────
with tab_diag:
    st.markdown('<div class="section-header">ETF Holdings Resolution Diagnostics</div>', unsafe_allow_html=True)
    st.caption(
        "Sector exposure uses yfinance fund profile data (100% coverage). "
        "Top holdings and market/country exposure use the top 10 holdings from fund profiles. "
        "Analysed % = fraction of ETF's index weight covered by those top 10 holdings."
    )

    import yfinance as _yf
    from data_fetcher import CA_TO_US_EQUIVALENT, get_top_holdings_funds

    # Known total holding counts per ETF (from fund documentation)
    KNOWN_TOTAL_HOLDINGS = {
        "VOO": 503, "SPY": 503, "IVV": 503, "VFV": 503, "VFV.TO": 503,
        "VEA": 3957, "VEF": 3957, "VEF.TO": 3957,
        "VIU": 3450, "VIU.TO": 3450,
        "EEM": 1200, "ZEM": 1200, "ZEM.TO": 1200,
        "IGF": 75, "XLU": 29, "IQLT": 300,
        "VCN": 180, "VCN.TO": 180, "XIC": 240, "XIC.TO": 240,
        "QQQ": 101, "IWM": 2000, "AGG": 11000,
    }

    diag_rows = []
    for _, pos in positions_df.iterrows():
        if pos["asset_type"] in ("ETF", "MUTUALFUND"):
            base = pos["ticker"].upper()
            yf_sym = pos["yf_ticker"]

            # Determine equivalent ETF used for holdings lookup
            ca_equiv = CA_TO_US_EQUIVALENT.get(base)
            lookup_sym = ca_equiv if ca_equiv else base
            equiv_display = f"{ca_equiv} (via CA→US map)" if ca_equiv and ca_equiv != base else "—"

            # Get top holdings via funds_data
            th = get_top_holdings_funds(yf_sym)
            # Handle ETF-of-ETF (e.g. VFV.TO → top_holdings is just VOO 100%)
            if len(th) == 1 and th.iloc[0]["weight"] >= 0.95:
                underlying = th.iloc[0]["ticker"]
                equiv_display = f"{underlying} (ETF wrapper)"
                th = get_top_holdings_funds(underlying)

            n_analysed = len(th) if not th.empty else 0

            # Total holdings from lookup table or yfinance
            n_total = KNOWN_TOTAL_HOLDINGS.get(base) or KNOWN_TOTAL_HOLDINGS.get(yf_sym)
            if not n_total:
                # Try yfinance fund overview
                try:
                    _info = _yf.Ticker(yf_sym).info or {}
                    # Some ETFs expose this
                    n_total = _info.get("holdings") or _info.get("totalHoldings")
                except Exception:
                    pass
            n_total_display = str(n_total) if n_total else "—"

            # Analysed % = sum of weights of analysed holdings
            if n_analysed > 0:
                captured_w = min(1.0, th["weight"].sum())
                analysed_pct = f"{captured_w * 100:.1f}%"
            else:
                captured_w = 0.0
                analysed_pct = "0.0%"

            status = "✅ Resolved" if n_analysed >= 5 else ("⚠️ Partial" if n_analysed > 0 else "❌ No data")
            top3 = ", ".join(th["ticker"].head(3).tolist()) if n_analysed > 0 else "—"

            diag_rows.append({
                "Ticker":            pos["ticker"],
                "Equivalent Used":   equiv_display,
                "Status":            status,
                "Total Holdings":    n_total_display,
                "Analysed (top N)":  n_analysed,
                "Analysed % (MV)":   analysed_pct,
                "Top 3 Holdings":    top3,
            })

    if diag_rows:
        st.dataframe(pd.DataFrame(diag_rows), use_container_width=True, hide_index=True)
        total_pos_value = positions_df[positions_df["asset_type"].isin(["ETF","MUTUALFUND"])]["value_display"].sum()
        unanalysed_val = result["all_leaves"]
        unanalysed_val = unanalysed_val[unanalysed_val["ticker"]=="UNANALYSED"]["value_display"].sum()
        if total_pos_value > 0:
            st.markdown(
                f"**Portfolio ETF top-holdings coverage:** "
                f"{fmt_money(total_pos_value - unanalysed_val, ccy)} analysed out of "
                f"{fmt_money(total_pos_value, ccy)} in ETFs "
                f"({(1 - unanalysed_val/total_pos_value)*100:.1f}% by market value). "
                f"Sector exposure uses full fund profiles — 100% coverage regardless."
            )
    else:
        st.info("No ETFs in your portfolio to diagnose.")

    st.markdown("---")
    st.caption("Sector data: yfinance funds_data.sector_weightings · Holdings: yfinance funds_data.top_holdings · Fallback: stockanalysis.com")
