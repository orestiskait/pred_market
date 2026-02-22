# Backtesting Framework — Design Document

## Architecture Overview

```
services/backtest/
├── __init__.py          # Package docstring
├── data_loader.py       # Reads parquet data → SimEvent timeline
├── engine.py            # BacktestEngine + SyncEventBus + BacktestExecutionManager
└── run.py               # CLI entry point
```

### How It Replicates the Live Bot — 100%

The backtester reuses the **exact same production code** for:
- `LadderStrategy` — unchanged strategy logic
- `BaseStrategy` — unchanged event subscription pattern
- `EventBus` interface — replaced with `SyncEventBus` (same API, deterministic)
- `OrderIntent` → sweep algorithm — identical sweep logic in `BacktestExecutionManager`
- `MarketDiscoveryEvent`, `OrderbookUpdateEvent`, `WeatherObservationEvent` — same dataclasses
- `nws_observation_period()` — same NWS window calculation
- Strategy config from `config.yaml` — same parsing

The **only** differences are:
1. **SyncEventBus** instead of asyncio EventBus (synchronous = deterministic)
2. **BacktestExecutionManager** stores fills in-memory instead of writing CSV/parquet
3. **DataLoader** replays from parquet files instead of live WebSocket

---

## Data Flow

```
┌─────────────────────────────────────┐
│         Historical Parquet Data      │
│  synoptic_weather_observations/      │
│  kalshi_orderbook_snapshots/         │
│  kalshi_market_snapshots/            │
└──────────────┬──────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│          DataLoader                  │
│  • Reads parquet for date range      │
│  • Applies latency model             │
│  • Merges into unified timeline      │
│  • Ensures discovery precedes data   │
└──────────────┬───────────────────────┘
               │ list[SimEvent]
               ▼
┌──────────────────────────────────────┐
│        BacktestEngine                │
│  • Iterates timeline chronologically │
│  • Publishes typed events to bus     │
│  • Progress reporting                │
└──────────────┬───────────────────────┘
               │
               ▼
┌──────────────────────────────────────┐
│        SyncEventBus                  │
│  (same subscribe/publish API)        │
│  • Calls handlers immediately        │
│  • No asyncio.create_task            │
│  • Deterministic execution order     │
└──────┬──────────────┬────────────────┘
       │              │
       ▼              ▼
┌──────────────┐ ┌────────────────────────┐
│ LadderStrat  │ │ BacktestExecutionMgr   │
│ (UNCHANGED)  │ │ (same sweep algorithm) │
│              │ │ Captures Fill objects   │
│ Emits        │ │                        │
│ OrderIntents │ │                        │
└──────────────┘ └────────────┬───────────┘
                              │
                              ▼
                 ┌─────────────────────────┐
                 │    BacktestResult       │
                 │  • fills: list[Fill]    │
                 │  • to_dataframe()       │
                 │  • to_csv()             │
                 │  • log_summary()        │
                 └─────────────────────────┘
```

---

## Latency Modeling

### The Problem
Synoptic weather observations arrive with a **~2.5 minute delay** (mean 156s, max 255s).
The `ob_timestamp` field records when the ASOS station sampled the temperature, but the
bot only reacts when the WebSocket message arrives (`received_ts`).

### How We Handle It

| Latency Model | Timeline Ordering | Use Case |
|---|---|---|
| `actual` (default) | `received_ts` from parquet | Most realistic — exactly replicates when the bot saw each observation |
| `fixed_N` | `ob_timestamp + N seconds` | Sensitivity analysis — "what if latency were 3 minutes vs 2 minutes?" |

The `ob_timestamp` is still passed through the `WeatherObservationEvent.ob_time` field,
so the strategy's NWS window filtering (`nws_start <= ob_time <= nws_end`) works
correctly.  But the **order** in which events are processed is governed by when they
would have arrived.

---

## Data Leakage Prevention

### Invariant
> At any point during the simulation, the strategy has ONLY seen data that
> would have been available at that wall-clock time in production.

### How It's Enforced

1. **Weather events** use `received_ts` for ordering (not `ob_timestamp`)
2. **Orderbook snapshots** use `snapshot_ts` (the Kalshi WS push time)
3. **Market discovery** is synthesized from the first snapshot of each day,
   timestamped to the earliest event of that day (mimicking the bot's
   `_discover()` running before WS feeds start)
4. **Timeline is strictly ordered** — the engine iterates once, forward only
5. **SyncEventBus** calls handlers immediately — no out-of-order delivery

### What Could Still Leak (and How We Prevent It)

| Risk | Prevention |
|---|---|
| Future orderbook visible during weather trigger | OB snapshots ordered by `snapshot_ts`; weather by `received_ts` |
| Strategy seeing tomorrow's contracts | Discovery events are per-day; ladder is rebuilt on each discovery |
| Strategy accumulating stale weather history | `on_market_discovery` clears `weather_history` (production behavior) |
| NWS window miscalculation | `nws_observation_period()` uses Standard Time offset (Jan 15 trick) |

---

## Usage

### CLI
```bash
# Single day
python -m services.backtest.run --start 2026-02-21 --end 2026-02-21

# Multi-day with series filter
python -m services.backtest.run --start 2026-02-20 --end 2026-02-22 --series KXHIGHCHI

# Latency sensitivity analysis
python -m services.backtest.run --start 2026-02-21 --end 2026-02-21 --latency fixed_180

# Export results
python -m services.backtest.run --start 2026-02-20 --end 2026-02-22 --export results.csv

# Debug logging
python -m services.backtest.run --start 2026-02-21 --end 2026-02-21 --log-level DEBUG
```

### Programmatic
```python
from datetime import date
from services.core.config import load_config
from services.backtest.engine import BacktestEngine

config, config_path = load_config()
data_dir = str((config_path.parent / config["storage"]["data_dir"]).resolve())

engine = BacktestEngine(
    config=config,
    data_dir=data_dir,
    start_date=date(2026, 2, 20),
    end_date=date(2026, 2, 22),
    series_filter=["KXHIGHCHI"],
    latency_model="actual",
)

result = engine.run()

# Analysis
df = result.to_dataframe()
print(f"Total fills: {result.n_fills}")
print(f"Total cost: ${result.total_cost_cents / 100:.2f}")

# Per-strategy breakdown
for sid, fills in result.fills_by_strategy().items():
    print(f"{sid}: {len(fills)} fills")
```

---

## Adding New Strategies

The backtesting framework automatically supports any new strategy added to the bot:

1. Create the strategy class in `services/bot/strategies/`
2. Register it in `STRATEGY_CLASS_REGISTRY` (`strategy_manager.py`)
3. Add config to `bot.strategies` in `config.yaml`
4. Run the backtest — it loads all configured strategies automatically

No changes to the backtesting code are needed.

---

## Future Enhancements

### Settlement P&L Calculation
Use `iem_daily_climate` data to determine the official NWS daily high and compute
whether each fill would have profited:
- For a NO fill: profit = 100 - cost if the high stayed below the cap_strike
- For a YES fill: profit = 100 - cost if the high reached/exceeded cap_strike

### Walk-Forward Optimization
Run backtests over rolling windows to optimize strategy parameters
(`consecutive_obs`, `max_price_cents`, `max_spend_per_event`) without overfitting.

### Slippage Model
Currently the backtester assumes the full orderbook depth is available at snapshot time.
In reality, the orderbook evolves between snapshots. A slippage model could:
- Add random noise to fill prices (conservative)
- Only fill up to X% of observed depth (realistic)
- Model time-to-fill based on order queue position

### Multi-Exchange Support
The data loader and engine are designed around Kalshi's specific data format.
Extending to other platforms would require:
- Platform-specific data loaders (Polymarket, etc.)
- Adapter layer to normalize orderbook formats
- Platform-specific execution simulation
