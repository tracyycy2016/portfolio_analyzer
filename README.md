# Investment Portfolio Analyzer

A Streamlit app that shows your investment portfolio's true exposure across markets, countries, sectors, and individual stocks — including full ETF look-through.

## Features

- ✅ US and Canadian listed stocks & ETFs
- ✅ ETF look-through (unrolls ETF → underlying stocks, recursively)
- ✅ Exposure by Market, Country, Sector, and Top 50 Stocks
- ✅ Display in USD or CAD (live FX conversion)
- ✅ Interactive donut & bar charts
- ✅ Prices cached 5 min, holdings cached 1 hr

## Running Locally

```bash
pip install -r requirements.txt
streamlit run app.py
```

## Deploy to Streamlit Cloud

1. Push this repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Click **New app** → select your repo
4. Set **Main file path** to `app.py`
5. Click **Deploy**

No secrets or API keys required — all data is from Yahoo Finance (public).

## File Structure

```
├── app.py              # Main Streamlit UI
├── portfolio.py        # Portfolio calculation engine
├── etf_resolver.py     # Recursive ETF unwrapping
├── data_fetcher.py     # Price & holdings fetching (yfinance)
├── requirements.txt
└── README.md
```

## Supported Tickers (examples)

| Ticker | Exchange | Description |
|--------|----------|-------------|
| VFV    | CA       | Vanguard S&P 500 Index ETF (TSX) |
| VEF    | CA       | FTSE Dev AC xUS ETF CAD |
| VIU    | CA       | FTSE Dev AC x NA ETF |
| ZGLD   | CA       | BMO Gold Bullion ETF |
| ZEM    | CA       | BMO MSCI Emerging Mkts Idx ETF |
| ASML   | US       | ASML Holding NV (NASDAQ) |
| XLU    | US       | Select Sector Utilities SPDR |
| IQLT   | US       | iShares MSCI Intl Quality Factor |
| SPY    | US       | SPDR S&P 500 ETF |
| QQQ    | US       | Invesco QQQ Trust |

## Notes

- ETF holdings are sourced from yfinance (and etf.com as fallback for US ETFs).
- Not all ETFs publish full holdings via public APIs. In such cases, the ETF is shown as a single holding.
- Gold/commodity ETFs typically show as a single holding (no underlying stocks).
- Data is for informational purposes only, not financial advice.
