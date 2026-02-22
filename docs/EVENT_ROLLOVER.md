# Event Rollover Strategy

How the services handle market ticker changes as days pass, without cron restarts.

---

## Problem

Weather markets (e.g. Chicago high temp) resolve on **local calendar days** per NWS convention. Kalshi lists events with strike dates (e.g. Feb 22). As midnight passes in each timezone, today's event closes and tomorrow's opens. The old approach restarted services via cron at 12:01 AM and 1:01 AM ET to force re-discovery.

**Limitations of cron:**
- Process restarts cause brief downtime
- Fixed times don't scale across many timezones (Chicago, Denver, LA, Phoenix, etc.)
- No support for pre-trading tomorrow's market when it opens (e.g. Feb 22 market on Feb 21)

---

## Solution: In-Process Periodic Re-Discovery

Instead of restarting the process, a background task **re-queries the Kalshi API** every N minutes. When the set of open events changes (e.g. after local midnight), we:

1. Update `market_tickers`, `market_info`, and related state
2. Request a WebSocket reconnect so subscriptions use the new tickers
3. Continue running without downtime

### Configuration (`config.yaml` → `event_rollover`)

| Key | Default | Description |
|-----|---------|-------------|
| `event_selection` | `"active"` | `"active"` = today's market (earliest close_time). `"next"` = strike_date ≥ today in market's local tz — enables pre-trading tomorrow's market. |
| `rediscover_interval_seconds` | 300 | How often to re-query Kalshi. 0 = disable (use cron if desired). |

### Event Selection Strategies

- **`active`**: Pick the event with earliest `close_time`. Best for same-day trading. Matches previous behavior.
- **`next`**: Pick the event whose `strike_date` is today or the next local calendar day. When Kalshi lists Feb 22 as open on Feb 21, we trade it. Uses each market's NWS-aligned timezone from `KalshiMarketConfig.tz`.

---

## Timezone Handling

Each market has an IANA timezone (e.g. `America/Chicago`, `America/New_York`) in the registry. The NWS records daily highs in **Local Standard Time** year-round (see `docs/events/kalshi_settlement_rules.md`). During DST, the "Tuesday" climate day covers 1:00 AM–12:59 AM local clock. The `next` strategy computes "today" in that timezone to select the correct event.

---

## Cron

`first_time_vm_setup.sh` **skips** cron installation by default. To restore legacy 12:01/1:01 AM restarts:

```bash
SKIP_CRON=0 ./first_time_vm_setup.sh
```

---

## Services Affected

- **Kalshi listener**: `_rediscover_loop()` runs every `rediscover_interval_seconds`
- **Weather bot**: Same loop; rebuilds ladder and reconnects Kalshi WS on change
- **Synoptic listener**: No change (station IDs are fixed per series)
