# pred_market

A data pipeline and analysis system for building a **weather-based betting strategy on Kalshi**. The project collects Kalshi prediction market data (orderbooks, probabilities, prices) alongside authoritative weather observations (ASOS, METAR, daily high) to support informed trading on temperature-linked contracts.

---

## Overview

Kalshi offers weather prediction markets—e.g., "Will the high temperature in Chicago exceed 75°F on Feb 18?"—that resolve using **official NWS Daily Climate Report** data from designated stations (e.g., KMDW for Chicago, KNYC for New York). This project:

1. **Collects Kalshi market data** — Live orderbooks, bid/ask spreads, last prices, volume, and open interest for weather events
2. **Collects weather data** — Multiple sources aligned with Kalshi's resolution methodology
3. **Analyzes discrepancies** — Compares near-real-time proxies (ASOS 1-min, METAR) vs official daily highs to find edges
4. **Supports a betting system** — Provides the data foundation for building and backtesting strategies

---

## Data Sources

### Kalshi Market Data

| Data Type | Source | Description |
|-----------|--------|-------------|
| **Orderbook** | WebSocket (live) / REST (snapshots) | Bid/ask depth, price levels, quantities |
| **Probabilities** | Derived from orderbook | Yes bid, yes ask, last price (0–100¢) |
| **Prices** | WebSocket ticker + REST candlesticks | Real-time and historical OHLC |
| **Events & Markets** | REST API | Event series (e.g. `KXHIGHCHI`, `KXHIGHNY`), market tickers, resolution dates |

The Kalshi listener subscribes to event series (e.g. Chicago and New York daily high markets), auto-resolves to the currently open events, and records market snapshots and orderbook depth. Spike detection triggers extra snapshots when prices move sharply (e.g. ≥3¢).

### Weather Data

| Source | Resolution | Use Case |
|--------|------------|----------|
| **ASOS 1-minute** | 1-min temps (Iowa Mesonet) | Highest-resolution public data; approximates official high (≈24h delay) |
| **METAR** | Hourly + specials (Aviation Weather Center) | Near-real-time temps, wind, visibility |
| **Daily Climate (CLI)** | Official daily high/low (NWS via IEM) | **Resolution source** — same data Kalshi uses to settle contracts |

Station mapping (e.g. `KXHIGHCHI` → KMDW, `KXHIGHNY` → KNYC) is configured in the weather module. See `docs/events/` for NWS station types, rounding rules, and ASOS technical specs.

---

## Project Structure

```
pred_market/
├── pred_market_src/
│   └── collector/              # Kalshi listener, Synoptic listener, bot
│       ├── kalshi/listener.py  # Live Kalshi WebSocket listener
│       ├── synoptic/listener.py # Synoptic weather WebSocket listener
│       ├── bot/weather_bot.py  # Paper-trading weather arbitrage bot
│       ├── core/               # Config, service, storage
│       ├── markets/             # Registry, ticker resolution
│       ├── config.yaml
│       └── docker-compose.yml
├── research/
│   ├── weather/                # Historical weather fetchers (ASOS, METAR, CLI)
│   ├── run_weather.py          # Weather collection script
│   └── weather_discrepancy_analysis.py  # ASOS vs official high comparison
├── pred_market_src/exploration/
│   └── market_analysis.ipynb   # Orderbook, probabilities, charts
├── scripts/oci_collector/      # OCI deployment (Kalshi listener, Synoptic listener, bot)
└── pred_env/                   # Python virtual environment
```

---

## Quick Start

### Prerequisites

- Python 3.10+
- Kalshi API credentials (API key ID + RSA private key)
- Virtual environment: `pred_env` (see `.cursor/rules/python-venv.mdc`)

### Installation

```bash
pred_env/bin/pip install -r pred_market_src/collector/requirements.txt
```

### Configuration

1. Copy `pred_market_src/collector/.env.example` to `.env` and set:
   - `KALSHI_API_KEY_ID`
   - `KALSHI_PRIVATE_KEY_PATH`
2. Edit `pred_market_src/collector/config.yaml` for event series, collection intervals, and storage paths.

### Running Services

```bash
# Live Kalshi listener (WebSocket)
pred_env/bin/python -m pred_market_src.collector.kalshi.listener

# Live Synoptic listener
pred_env/bin/python -m pred_market_src.collector.synoptic.listener

# Weather arbitrage bot
pred_env/bin/python -m pred_market_src.collector.bot.weather_bot

# Historical weather data (ASOS, METAR, daily climate)
pred_env/bin/python research/run_weather.py
```

### Data Layout

- `data/market_snapshots/` — Market snapshots (yes_bid, yes_ask, last_price, volume, open_interest)
- `data/orderbook_snapshots/` — Orderbook depth (delta-compressed)
- `data/historical/` — Candlesticks and trades
- `data/weather_obs/` — Weather observations (ASOS, METAR, CLI)

---

## Deployment

Kalshi listener, Synoptic listener, and weather bot can run 24/7 on Oracle Cloud (OCI) using the scripts in `scripts/oci_collector/`. See `scripts/oci_collector/OCI_DEPLOYMENT.md` for launch, setup, and maintenance.

---

## Betting System Goal

The project is designed to support a **data-driven betting system** for Kalshi weather markets:

1. **Data foundation** — Continuous collection of market microstructure and weather observations
2. **Resolution alignment** — Weather data sources matched to Kalshi's NWS-based resolution
3. **Discrepancy analysis** — Tools to compare ASOS 1-min vs official CLI highs (bias, RMSE, LST day boundaries)
4. **Market analysis** — Notebooks for orderbook reconstruction, probability distributions, and stacked area charts

There is no automated order placement or execution logic; the focus is on data collection and analysis to inform manual or future automated strategies.

---

## License

See repository for license details.
