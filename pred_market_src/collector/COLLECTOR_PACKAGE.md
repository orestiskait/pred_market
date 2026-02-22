# Kalshi listener, Synoptic listener, weather bot

This module runs three services:
- **Kalshi listener** — streams Kalshi market data (orderbooks, tickers) to Parquet
- **Synoptic listener** — streams real-time weather observations to Parquet
- **Weather bot** — paper-trading weather arbitrage bot

## Package Layout

```
collector/
├── config.yaml              # Central configuration
├── tz.py                    # UTC timezone utilities
│
├── core/                    # Shared infrastructure
│   ├── config.py            # Config loading, client factories, CLI helpers
│   ├── service.py           # AsyncService lifecycle with graceful shutdown
│   └── storage.py           # Parquet I/O (market snapshots, orderbooks, synoptic)
│
├── markets/                 # Market configuration & discovery
│   ├── registry.py          # StationInfo + MarketConfig (single source of truth)
│   └── ticker.py            # Event-ticker resolution via Kalshi API
│
├── kalshi/                  # Kalshi exchange integration
│   ├── client.py            # REST client + RSA-PSS authentication
│   ├── ws.py                # WebSocket mixin (orderbook maintenance)
│   └── listener.py          # Live market data listener service
│
├── synoptic/                # Synoptic Data integration
│   ├── ws.py                # WebSocket mixin (observation parsing)
│   └── listener.py          # Synoptic listener (live weather WebSocket ingest)
│
├── bot/                     # Trading bots
│   └── weather_bot.py       # Paper-trading weather arbitrage bot
│
├── Dockerfile               # Container build
├── docker-compose.yml       # Multi-service orchestration
├── docker-entrypoint.sh     # Container entry point
└── requirements.txt
```

## Adding a New City / Market

Only **two steps** are needed:

1. **Add a `MarketConfig` entry** in `markets/registry.py`:
   ```python
   "KXHIGHSFO": MarketConfig(
       series_prefix="KXHIGHSFO",
       icao="KSFO", iata="SFO", city="San Francisco",
       tz="America/Los_Angeles", synoptic_station="KSFO1M",
   ),
   ```

2. **Add the series prefix** to `config.yaml`:
   ```yaml
   event_series:
     - "KXHIGHCHI"
     - "KXHIGHNY"
     - "KXHIGHSFO"   # ← new
   ```

That's it. All services (Kalshi listener, synoptic listener, weather bot)
automatically pick up the new market.

## Timezone Handling

Different markets settle on different local days. The `MarketConfig.tz` field
(IANA timezone string) is the single source of truth for each market's local
timezone. The `markets/ticker.py` module provides `local_date_for_market()`
for computing the correct local date when needed.

**Rule:** All timestamps stored in Parquet are UTC. Local timezones are only
used at boundaries (computing event ticker dates, displaying to humans).

## Running

### Live Kalshi listener

```bash
pred_env/bin/python -m pred_market_src.collector.kalshi.listener
pred_env/bin/python -m pred_market_src.collector.kalshi.listener --config path/to/config.yaml
```

### Live Synoptic listener

```bash
pred_env/bin/python -m pred_market_src.collector.synoptic.listener
```

### Weather arbitrage bot

```bash
# All configured markets:
pred_env/bin/python -m pred_market_src.collector.bot.weather_bot

# Specific markets only:
pred_env/bin/python -m pred_market_src.collector.bot.weather_bot --series KXHIGHCHI KXHIGHNY
```

### Historical weather data

```bash
pred_env/bin/python research/run_weather.py
```

### Docker

```bash
docker compose up -d          # Start all services
docker compose down            # Stop all services
```

## Data Storage

All Parquet files are stored under `data/` (configurable in `config.yaml`):

```
data/
├── market_snapshots/         # Kalshi market state (per-date parquet)
├── orderbook_snapshots/      # Kalshi orderbook depth (baseline + delta)
├── synoptic_ws/              # Synoptic 1-min ASOS observations
├── weather_obs/              # Historical weather fetcher output
│   ├── asos_1min/
│   ├── metar/
│   └── daily_climate/
└── weather_bot/              # Paper trade logs
    └── paper_trades.csv
```

## Configuration

### Environment Variables (`.env`)

| Variable | Description |
|---|---|
| `KALSHI_API_KEY_ID` | Kalshi API key ID |
| `KALSHI_PRIVATE_KEY_PATH` | Path to RSA private key PEM file |
| `SYNOPTIC_API_TOKEN` | Synoptic Data API token |

### `config.yaml` Sections

| Section | Description |
|---|---|
| `kalshi` | API URLs and credentials |
| `event_series` | Which markets to track (auto-resolves to today's events) |
| `collection` | Snapshot intervals, spike detection, delta compression |
| `storage` | Data directory, flush intervals |
| `synoptic` | Variables to subscribe to (stations auto-derived from registry) |
| `bot` | Trading strategy parameters |
