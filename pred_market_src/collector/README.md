# Kalshi Market Data Collector

This module fetches and stores Kalshi prediction market time-series data for analysis. It supports both **historical backfill** (candlesticks + trades via REST) and **live collection** (orderbook snapshots + market state via WebSocket).

---

## Overview

| Component | Data source | What it collects | Output |
|-----------|-------------|------------------|--------|
| **Live collector** | WebSocket | Orderbook depth, yes/no bids, volume, open interest | `market_snapshots`, `orderbook_snapshots` (parquet) |
| **Backfill** | REST API | Candlesticks (OHLC), individual trades | `historical/candlesticks`, `historical/trades` (parquet) |

Key constraint: **Kalshi does not offer historical orderbook data**. The REST API only returns the current orderbook. To build orderbook time-series, you must run the live collector and let it snapshot the orderbook over time.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│  config.yaml                                                            │
│  - API credentials (or env vars)                                        │
│  - Event tickers to track (e.g. KXHIGHCHI-26FEB11)                      │
│  - Snapshot intervals (default + time-of-day overrides)                  │
│  - Storage paths                                                        │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  collector.py (Live daemon)                                              │
│  1. REST: discover all contract tickers for configured events            │
│  2. WebSocket: subscribe to orderbook_delta + ticker for those markets  │
│  3. In-memory: maintain orderbook state from snapshots + deltas          │
│  4. Snapshot loop: at dynamic intervals, capture full state             │
│  5. Buffers: accumulate rows, flush to parquet every N seconds           │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  data/                                                                  │
│  ├── market_snapshots/     YYYY-MM-DD.parquet  (one row per snapshot)   │
│  ├── orderbook_snapshots/   YYYY-MM-DD.parquet  (one row per level)      │
│  └── historical/                                                       │
│      ├── candlesticks/     EVENT_TICKER.parquet                          │
│      └── trades/           EVENT_TICKER.parquet                          │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│  backfill.py (One-off script)                                            │
│  REST: GET candlesticks + paginated trades for each event/market         │
│  → writes directly to historical/*.parquet                               │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## File roles

| File | Purpose |
|------|---------|
| `config.yaml` | Credentials, event tickers, snapshot schedule, storage paths |
| `kalshi_client.py` | REST client + RSA-PSS auth. Generates WebSocket auth headers. |
| `storage.py` | Parquet schemas and append-friendly I/O |
| `collector.py` | Live WebSocket daemon; snapshots at configurable intervals |
| `backfill.py` | Historical REST fetcher for candlesticks and trades |
| `weather/` | Weather observation fetchers (replaces `fetch_nws_temps.py`) |
| `weather/asos_1min.py` | ASOS 1-minute temperature data via IEM (highest-resolution public data) |
| `weather/metar.py` | METAR observations via Aviation Weather Center (hourly + specials) |
| `weather/daily_climate.py` | NWS Daily Climate Report via IEM (official high — Kalshi settlement value) |
| `weather/observations.py` | Orchestrator: config-driven multi-station fetch + save |
| `run_weather.py` | Script to run daily weather collection (yesterday's data) |
| `fetch_nws_temps.py` | **Deprecated** — replaced by `weather/` module |


---

## Configuration

Edit `config.yaml` (or pass `--config path/to/config.yaml`). Set credentials via environment variables (see `.env.example`):

```bash
export KALSHI_API_KEY_ID=your-key-id
export KALSHI_PRIVATE_KEY_PATH=/path/to/key.pem
```

```yaml
kalshi:
  private_key_path: "/path/to/key.pem" # optional; KALSHI_PRIVATE_KEY_PATH overrides
  base_url: "https://api.elections.kalshi.com/trade-api/v2"
  ws_url:   "wss://api.elections.kalshi.com/trade-api/ws/v2"

events:
  - "KXHIGHCHI-26FEB11"
  - "KXHIGHNY-26FEB12"

collection:
  interval_seconds: 60          # Baseline periodic snapshot interval
  spike_threshold_cents: 3      # Immediate snapshot when price moves ≥ N cents
  spike_cooldown_seconds: 2     # Min gap between spike-triggered snapshots

storage:
  data_dir: "data"
  flush_interval_seconds: 300
```

- **events**: Uppercase event tickers. Each event has many contracts (e.g. temperature ranges).
- **interval_seconds**: Baseline snapshot interval. One snapshot per cycle regardless of activity.
- **spike_threshold_cents**: When `yes_bid`, `yes_ask`, or `last_price` moves by ≥ N cents *since the last snapshot*, take an immediate snapshot. Compared against last-snapshotted price, so cumulative moves during cooldown are never missed. Set to `0` to disable.
- **spike_cooldown_seconds**: Minimum seconds between spike-triggered snapshots (prevents burst writes when multiple contracts spike simultaneously).
- **flush_interval_seconds**: How often to write in-memory buffers to disk.

---

## Data schemas

### Live snapshots (shared `snapshot_ts`)

**market_snapshots.parquet** — one row per (timestamp, contract):

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_ts` | datetime (UTC) | When the snapshot was taken |
| `event_ticker` | string | e.g. KXHIGHCHI-26FEB11 |
| `market_ticker` | string | e.g. KXHIGHCHI-26FEB11-B34 |
| `subtitle` | string | e.g. "Between 34° and 35°F" |
| `yes_bid` | int | Best yes bid (cents) |
| `yes_ask` | int | Best yes ask (cents) |
| `last_price` | int | Last traded price |
| `volume` | int64 | Total contracts traded |
| `open_interest` | int64 | Outstanding contracts |
| `trigger` | string | "periodic" or "spike" |

**orderbook_snapshots.parquet** — one row per (timestamp, contract, side, price level):

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_ts` | datetime (UTC) | Same as market_snapshots for that cycle |
| `market_ticker` | string | Contract ticker |
| `side` | string | "yes" or "no" |
| `price_cents` | int | Price level |
| `quantity` | float | Contracts at this level |

Both tables use the same `snapshot_ts` for each snapshot cycle.

### Historical backfill

**candlesticks/** — OHLC per market:

| Column | Type |
|--------|------|
| `timestamp` | datetime (UTC) |
| `event_ticker` | string |
| `market_ticker` | string |
| `open_price`, `close_price`, `high_price`, `low_price` | float |
| `volume` | int64 |

**trades/** — Individual fills:

| Column | Type |
|--------|------|
| `timestamp` | datetime (UTC) |
| `event_ticker` | string |
| `market_ticker` | string |
| `trade_id` | string |
| `price` | int |
| `count` | int64 |
| `taker_side` | string |

---

## Running the collector

### Live (WebSocket)

```bash
pred_env/bin/python pred_market_src/collector/collector.py
```

Or with a custom config:

```bash
pred_env/bin/python pred_market_src/collector/collector.py --config /path/to/config.yaml --log-level DEBUG
```

- Runs until SIGINT (Ctrl+C) or SIGTERM.
- Flushes buffers on shutdown.
- Reconnects to WebSocket automatically on disconnect.

### Historical backfill

```bash
pred_env/bin/python pred_market_src/collector/backfill.py --start 2026-02-01 --end 2026-02-11
```

Events come from config unless overridden:

```bash
pred_env/bin/python pred_market_src/collector/backfill.py --start 2026-02-01 --events KXHIGHCHI-26FEB11 KXHIGHNY-26FEB12 --period 60
```

- `--period`: candlestick interval in minutes (1, 60, or 1440).

---

## Reading the data

```python
from pred_market_src.collector.storage import ParquetStorage
from datetime import date

s = ParquetStorage("pred_market_src/collector/data")

# All live snapshots
df_market = s.read_parquets("market")
df_ob = s.read_parquets("orderbook")

# Filter by date range
df_market = s.read_parquets(
    "market",
    start_date=date(2026, 2, 10),
    end_date=date(2026, 2, 11),
)

# Historical
df_candles = s.read_parquets("candlesticks")
df_trades = s.read_parquets("trades")
```

Or load directly with pandas:

```python
import pandas as pd

df = pd.read_parquet("pred_market_src/collector/data/market_snapshots/2026-02-11.parquet")
```

---

## Deployment

Works on **local** and **cloud** (e.g. Oracle Cloud / EC2):

- **Local**: Run `collector.py` in a terminal or background process.
- **Cloud**: Run as a systemd service or under `screen`/`tmux` so it survives SSH disconnect.

### OCI (Oracle Cloud) — first-time setup

Use the OCI CLI to launch an instance that clones from GitHub and runs the collector:

```bash
cd scripts/oci_collector
./launch.sh
```

See [scripts/oci_collector/README.md](../../scripts/oci_collector/README.md) for full instructions (credentials, SSH, systemd).

### Example systemd unit (`/etc/systemd/system/kalshi-collector.service`)

```ini
[Unit]
Description=Kalshi market data collector
After=network.target

[Service]
Type=simple
User=your-user
WorkingDirectory=/path/to/pred_market
Environment="KALSHI_API_KEY_ID=..."
Environment="KALSHI_PRIVATE_KEY_PATH=/path/to/key.pem"
ExecStart=/path/to/pred_market/pred_env/bin/python pred_market_src/collector/collector.py --config /path/to/config.yaml
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Then: `systemctl enable kalshi-collector && systemctl start kalshi-collector`.

---

## Dependencies

Install into the project venv:

```bash
pred_env/bin/pip install -r pred_market_src/collector/requirements.txt
```

Required packages: `requests`, `websockets`, `pyarrow`, `pandas`, `pyyaml`, `cryptography`.
