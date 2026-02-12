# Kalshi NYC Temperature Market Exploration

Deep analysis of Kalshi odds for the "Highest temperature in NYC today?" market.

## Market Reference

- **URL**: https://kalshi.com/markets/kxhighny/highest-temperature-in-nyc/kxhighny-26feb11
- **Event ticker**: `KXHIGHNY-26FEB11` (API uses uppercase)
- **Event**: NYC daily high temperature (Central Park / KNYC station per NWS rules)

## Structure

```
exploration/
├── README.md           # This file
├── fetch_kalshi.py     # Fetch current odds from Kalshi API
├── fetch_historical.py # Fetch historical Kalshi odds (→ data/raw/kalshi/)
├── fetch_nws_temps.py  # Fetch NWS temperatures from obhistory (→ data/raw/nws/)
├── config.py           # Market config and API settings
├── data/
│   ├── raw/
│   │   ├── kalshi/     # Kalshi API responses (event_markets, orderbooks)
│   │   └── nws/        # NWS obhistory HTML and hourly observations (all columns)
│   └── processed/      # Cleaned data for analysis
└── analysis/           # Analysis notebooks and scripts
```

## Usage

```bash
cd pred_market_src/exploration
pip install -r requirements.txt

# Fetch current odds for today's market
python fetch_kalshi.py

# Fetch historical odds (last 7 days by default)
python fetch_historical.py --days 7

# Fetch a specific date range
python fetch_historical.py --start 2026-02-01 --end 2026-02-11

# Fetch NWS hourly observations (past ~3 days; all columns)
python fetch_nws_temps.py

# Compare odds vs NWS actuals (after you have data)
python analysis/compare_odds_vs_actual.py
```

### NWS Actuals for Deep Analysis

To compare Kalshi odds with actual outcomes, create `data/processed/actuals.csv`:

```csv
date,high_f,station
2026-02-10,42,KNYC
2026-02-09,38,KNYC
```

Source: [NWS Daily Climatological Report](https://www.weather.gov/) for KNYC (Central Park). See `project_rules/weather_prediction_rules.md` for resolution rules.

## API Notes

- Kalshi API: https://api.elections.kalshi.com/trade-api/v2/ (no auth needed for market data)
- Market data: `/markets/{ticker}`, `/markets/{ticker}/orderbook`
- Event/markets: `/events/{event_ticker}` or `/markets?series_ticker=KXHIGHNY` (NYC temp has multiple contracts per day, e.g. `KXHIGHNY-26FEB11-T39`)
- Historical: Fetch past days by event ticker (e.g. `KXHIGHNY-26FEB10`) and save to `data/raw/`.
