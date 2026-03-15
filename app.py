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
    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.55,
            textinfo="percent",
            textfont_size=12,
            marker=dict(colors=CHART_COLORS, line=dict(color="#0f172a", width=2)),
            hovertemplate="<b>%{label}</b><br>%{percent}<extra></extra>",
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
        height=360,
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

    # Initialise session state
    if "positions" not in st.session_state:
        st.session_state.positions = [
            {"ticker": "", "exchange": "US", "units": 0.0}
        ]

    def add_row():
        st.session_state.positions.append({"ticker": "", "exchange": "US", "units": 0.0})

    def remove_row(i):
        st.session_state.positions.pop(i)

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
                "Units", value=float(pos.get("units", 0)),
                min_value=0.0, step=1.0, key=f"units_{i}",
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
tab_mkt, tab_country, tab_sector, tab_stock, tab_positions = st.tabs(
    ["🌍 By Market", "🗺 By Country", "🏭 By Sector", "📈 Top 50 Stocks", "💼 My Positions"]
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


# ── Tab: By Country ────────────────────────────────────────────────────────────
with tab_country:
    st.markdown('<div class="section-header">Exposure by Country</div>', unsafe_allow_html=True)
    if by_country.empty:
        st.info("No country data available.")
    else:
        df_c = by_country.dropna(subset=["country"]).copy()
        df_c["country"] = df_c["country"].fillna("Unknown")
        c1, c2 = st.columns([1, 1])
        with c1:
            fig = donut_chart(
                df_c["country"].head(15),
                df_c["value_display"].head(15),
                "Top 15 Countries",
            )
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            tbl = exposure_table(df_c, "weight", "value_display", "country", ccy)
            st.dataframe(tbl, use_container_width=True, hide_index=True)


# ── Tab: By Sector ─────────────────────────────────────────────────────────────
with tab_sector:
    st.markdown('<div class="section-header">Exposure by Sector</div>', unsafe_allow_html=True)
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


# ── Tab: Top 50 Stocks ─────────────────────────────────────────────────────────
with tab_stock:
    st.markdown('<div class="section-header">Top 50 Underlying Holdings</div>', unsafe_allow_html=True)
    if by_stock.empty:
        st.info("No stock data available.")
    else:
        # Bar chart of top 20
        top20 = by_stock.head(20).iloc[::-1]
        fig = go.Figure(
            go.Bar(
                x=top20["pct"],
                y=top20["ticker"],
                orientation="h",
                text=top20["pct"].apply(lambda x: f"{x:.2f}%"),
                textposition="outside",
                marker=dict(
                    color=CHART_COLORS[: len(top20)],
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
        tbl["sector"] = tbl["sector"].fillna("—")
        tbl["country"] = tbl["country"].fillna("—")
        tbl["market"] = tbl["market"].fillna("—")
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
