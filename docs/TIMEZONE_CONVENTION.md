# Timezone Convention

**All timestamps in this project are UTC.  No exceptions.**

---

## Rules

| Context | Convention | Example |
|---------|-----------|---------|
| Parquet schemas | `pa.timestamp("us", tz="UTC")` | `snapshot_ts`, `timestamp` |
| Weather observations | Column named `valid_utc` (timezone-aware) | `2026-02-19 15:30:00+00:00` |
| Python runtime | Always use `utc_now()` / `utc_today()` from `tz.py` | Never `datetime.now()` or `date.today()` |
| Filenames (daily parquet) | UTC date in filename | `2026-02-19.parquet`, `KMDW_2026-02-19.parquet` |
| API interactions | Timestamps as UTC epoch seconds or ISO 8601 with `+00:00`/`Z` | `1740000000` (epoch), `2026-02-19T15:30:00Z` |
| Cron / systemd | OCI VM is set to `America/New_York`, **but code never relies on system timezone** | All cron docs note the local offset |

## Forbidden Patterns

These will silently produce wrong results when the system timezone ≠ UTC:

```python
# ❌ WRONG — uses system-local time
from datetime import date, datetime
datetime.now()
date.today()

# ✅ CORRECT — always UTC
from services.tz import utc_now, utc_today
utc_now()
utc_today()
```

## When Local Time Matters

Local timezones are **only** relevant at one boundary: translating UTC observations
into the station's local calendar day for weather analysis (e.g., "what was the high
temperature in Chicago on Feb 19?").

This is handled via `StationInfo.tz` (IANA timezone string like `America/Chicago`),
and only used in analysis code — never in storage or transport.

```python
from research.weather.iem_awc_station_registry import STATION_REGISTRY  # derives from services.markets.registry

chi = STATION_REGISTRY["KXHIGHCHI"]
print(chi.tz)  # "America/Chicago"
```

## Daily Climate (CLI) Reports

The NWS Daily Climate Report has no natural UTC timestamp — it covers a full local
calendar day.  For compatibility with the base class dedup logic, we store a
synthetic `valid_utc` set to midnight UTC on the report date.  **Do not treat this
as an actual observation time.**  The meaningful column is `valid_date`.
