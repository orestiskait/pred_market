# Data Sources — Detailed Reference

This document describes every data source ingested by the pred_market system: what it is, how it is collected, how frequently it updates, how long it takes to arrive, and the exact schema stored to disk. It also describes the Kalshi weather prediction market competition and its resolution methodology.

---

## Table of Contents

1. [Kalshi Competition Objective](#1-kalshi-competition-objective)
2. [Wethr Push API (WETH)](#2-wethr-push-api-weth)
3. [METAR](#3-metar)
4. [HRRR](#4-hrrr--high-resolution-rapid-refresh)
5. [NBM](#5-nbm--national-blend-of-models)
6. [RRFS](#6-rrfs--rapid-refresh-forecast-system)
7. [Kalshi Orders (Market Snapshots)](#7-kalshi-orders--market-snapshots)
8. [Kalshi Book (Orderbook Snapshots)](#8-kalshi-book--orderbook-snapshots)
9. [Supporting / Secondary Sources](#9-supporting--secondary-sources)

---

## 1. Kalshi Competition Objective

### What Kalshi weather markets are

Kalshi offers **binary prediction markets on daily temperature extremes**. Each market asks a single yes/no question such as:

> "Will the high temperature in Chicago today exceed 75°F?"

Contracts are priced 0–100¢. A YES contract that settles true pays $1.00; a NO contract that settles false pays $0.00. All prices are in cents, so a YES bid of 72¢ implies a 72% implied probability of the event occurring.

### Active event series

| Series Ticker | City | Resolution Station | ICAO |
|---------------|------|--------------------|------|
| `KXHIGHCHI` | Chicago | Chicago Midway (CLIMDW) | KMDW |
| `KXHIGHNY` | New York City | Central Park | KNYC |
| `KXHIGHLAX` | Los Angeles | — | KLAX |
| `KXHIGHMIA` | Miami | Miami International | KMIA |
| `KXHIGHDCA` | Washington D.C. | — | KDCA |
| `KXHIGHATL` | Atlanta | — | KATL |
| `KXHIGHAUS` | Austin | Austin-Bergstrom | KAUS |

Each series generates one market per calendar day with a discrete strike (e.g., "75°F"). Markets roll over at local midnight.

### Resolution methodology

**Kalshi uses the NWS Daily Climatological Report (CLI)** to settle all temperature-high contracts. The official high is determined as follows:

1. The ASOS station records temperature continuously (2–10 second internal samples).
2. Every minute the station computes a 1-minute average (or a 2–5 second average depending on station type).
3. At approximately 01:00 local time the ASOS station issues a **Daily Summary Message (DSM)** containing the peak 2-minute or 5-minute running average for the preceding climate day.
4. The NWS incorporates that value into the **Daily Climate Report (CLI)**, published typically by ~06:00 UTC the following morning.
5. Kalshi reads the CLI and settles the market, usually the same morning.

**Important precision details:**
- The official high is in **whole degrees Fahrenheit** (rounded from Celsius internally).
- The official high uses 2-minute or 5-minute averaging, so it can be **lower** than any individual 1-minute reading you observe in real time.
- Sensor accuracy: ±0.6°C. At strike boundaries (e.g. 75°F vs 76°F), rounding ambiguity matters.
- ASOS operates internally in Celsius, then converts back to Fahrenheit for the CLI. This F→C→F round-trip can shift values by ±1–2°F in some display products.

### The Standard Time (LST) rule

NWS records climate data in **Local Standard Time (LST) year-round**, even during Daylight Saving Time. This means:

- The "climate day" boundary is midnight-to-midnight in Standard Time, not wall-clock time.
- **Example (Chicago during CDT, UTC-5 DST):** The "Feb 22" market covers the period 01:00 AM local on Feb 22 through 12:59 AM local on Feb 23.
- Consequence: when comparing real-time observations to the official daily high, always use the station's Standard Time (winter UTC) offset, not the current DST offset.

### Settlement timing

| Step | Approximate Timing |
|------|-------------------|
| Climate day ends (midnight LST) | e.g. 06:00 UTC during CDT |
| ASOS station issues DSM | ~01:00 local time (~07:00 UTC during CDT) |
| NWS publishes CLI | Typically ~06:00–08:00 UTC |
| Kalshi settles the market | Same morning after CLI is released |

The lag from climate day end to final settlement is typically **6–12 hours**.



## 2. Wethr Push API (WETH)

### What it is

**Wethr.net** is a third-party service that aggregates ASOS real-time weather data and delivers it via a **Server-Sent Events (SSE) push stream**. It provides five distinct event types from NWS ASOS stations:

| Event Type | Description |
|-----------|-------------|
| `observations` | Real-time temperature and meteorological obs |
| `dsm` | Daily Summary Message — official daily high/low (posted ~01:00 local) |
| `cli` | Daily Climate Report — NWS official confirmation |
| `new_high` | Alert: a new intraday high was set at this station |
| `new_low` | Alert: a new intraday low was set at this station |

This is the **primary real-time trading signal source**. The `new_high` / `new_low` alert events are purpose-built for monitoring whether the current intraday extreme has crossed a Kalshi strike threshold.

### Update frequency

| Event | Frequency |
|-------|-----------|
| `observations` | As reported by ASOS — roughly every 1–5 minutes per station |
| `dsm` | Once per climate day, ~01:00 local time |
| `cli` | Once per climate day, ~06:00–08:00 UTC |
| `new_high` / `new_low` | Immediately upon detection by Wethr's backend; event-driven |

### Latency

The field `received_ts` (wall-clock UTC when the SSE event arrived at the client) minus `observation_time_utc` gives the end-to-end latency from ASOS observation to client receipt. In practice this is **seconds to low single-digit minutes** for observation events.

### Storage

```
data/weather/wethr_push/
├── observations/<ICAO>_YYYY-MM-DD.parquet
├── dsm/<ICAO>_YYYY-MM-DD.parquet
├── cli/<ICAO>_YYYY-MM-DD.parquet
├── new_high/<ICAO>_YYYY-MM-DD.parquet
└── new_low/<ICAO>_YYYY-MM-DD.parquet
```

Code: `services/wethr/listener.py`, `services/wethr/sse.py`, `services/wethr/storage.py`

### Schema

#### `observations` table

| Column | Type | Description |
|--------|------|-------------|
| `station_code` | string | ICAO station identifier (e.g. `KMDW`) |
| `observation_time_utc` | datetime[UTC] | Time of the ASOS observation |
| `received_ts` | datetime[UTC] | Wall-clock time the SSE event arrived at the client |
| `product` | string | ASOS product type (e.g. `METAR`, `SPECI`) |
| `temperature_celsius` | float | Air temperature in °C |
| `temperature_fahrenheit` | float | Air temperature in °F |
| `dew_point_celsius` | float | Dew point in °C |
| `dew_point_fahrenheit` | float | Dew point in °F |
| `relative_humidity` | float | Relative humidity (%) |
| `wind_direction` | string | Wind direction (e.g. `270`, `VRB`) |
| `wind_speed_mph` | float | Wind speed in mph |
| `wind_gust_mph` | float | Wind gust in mph (null if calm) |
| `visibility_miles` | float | Visibility in statute miles |
| `altimeter_inhg` | float | Altimeter setting in inHg |
| `anomaly` | bool | Wethr flag: observation is anomalous / suspect |
| `event_id` | string | Wethr-assigned unique event ID |

#### `dsm` table

| Column | Type | Description |
|--------|------|-------------|
| `station_code` | string | ICAO station identifier |
| `for_date` | string | Climate date this DSM covers (YYYY-MM-DD in LST) |
| `received_ts` | datetime[UTC] | Wall-clock time the SSE event arrived |
| `high_f` | float | Official daily high in °F |
| `high_c` | float | Official daily high in °C |
| `high_time_utc` | string | UTC timestamp of the peak reading |
| `low_f` | float | Official daily low in °F |
| `low_c` | float | Official daily low in °C |
| `low_time_utc` | string | UTC timestamp of the minimum reading |
| `anomaly` | bool | Wethr anomaly flag |
| `event_id` | string | Wethr-assigned unique event ID |

#### `cli` table

| Column | Type | Description |
|--------|------|-------------|
| `station_code` | string | ICAO station identifier |
| `for_date` | string | Climate date (YYYY-MM-DD in LST) |
| `received_ts` | datetime[UTC] | Wall-clock time the SSE event arrived |
| `high_f` | float | NWS-confirmed official daily high in °F |
| `high_c` | float | NWS-confirmed official daily high in °C |
| `low_f` | float | NWS-confirmed official daily low in °F |
| `low_c` | float | NWS-confirmed official daily low in °C |
| `anomaly` | bool | Wethr anomaly flag |
| `event_id` | string | Wethr-assigned unique event ID |

#### `new_high` / `new_low` tables

| Column | Type | Description |
|--------|------|-------------|
| `station_code` | string | ICAO station identifier |
| `observation_time_utc` | datetime[UTC] | Time of the observation setting the new extreme |
| `received_ts` | datetime[UTC] | Wall-clock time the SSE event arrived |
| `logic` | string | Which detection algorithm triggered the alert |
| `value_f` | float | New extreme temperature in °F |
| `value_c` | float | New extreme temperature in °C |
| `prev_value_f` | float | Previous intraday extreme in °F (superseded) |
| `prev_value_c` | float | Previous intraday extreme in °C (superseded) |
| `event_id` | string | Wethr-assigned unique event ID |

---

## 3. METAR

### What it is

**METAR** (METeorological Aerodrome Report) is the standard aviation weather report format. Issued by ASOS/AWOS stations at or near airports, METARs contain synoptic-style surface observations: temperature, dew point, wind, visibility, cloud cover, and altimeter. The system collects METARs from two independent sources for redundancy and cross-validation:

- **Routine METAR:** Published at approximately :55 past each hour (e.g. 14:55Z, 15:55Z).
- **SPECI (Special):** Published whenever conditions change significantly (wind shift, visibility drops below threshold, precipitation onset, etc.). Can be issued any time
- latency is approximately 3 minutes

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `ob_time_utc` | datetime[UTC] | Time of the METAR observation |
| `station` | string | ICAO station code |
| `source` | string | `"awc_metar"` or `"nws_observations"` |
| `temp_c` | float | Air temperature in °C (integer precision from METAR body) |
| `temp_f` | float | Air temperature converted to °F |
| `raw_ob` | string | Full raw METAR string (e.g. `KMDW 221755Z 27012KT ...`) |
| `rmk` | string | METAR remarks section (contains T-group: 0.1°C precision) |
| `temp_6hr_min_c` | float | 6-hour minimum temperature in °C (synoptic hours only) |
| `temp_6hr_max_c` | float | 6-hour maximum temperature in °C (synoptic hours only) |
| `temp_24hr_min_c` | float | 24-hour minimum temperature in °C (00Z/12Z obs only) |
| `temp_24hr_max_c` | float | 24-hour maximum temperature in °C (00Z/12Z obs only) |
| `saved_ts` | datetime[UTC] | Wall-clock time this row was written to disk |
| `total_latency_s` | float | Seconds from `ob_time_utc` to `saved_ts` |

**Note on temperature precision:** The METAR body (`temp_c`) uses integer Celsius. The T-group in the remarks (e.g. `T01830102`) encodes 0.1°C precision and can be parsed from `rmk` for higher-fidelity temperature estimates.

---

## 4. HRRR — High-Resolution Rapid Refresh

### What it is

The **HRRR** (High-Resolution Rapid Refresh) is NOAA's operational convection-allowing NWP model. It provides hourly-cycle, short-range forecasts at **3 km grid spacing** over the contiguous United States. The system uses the **sub-hourly (subh) product**, which contains 15-minute interval temperature forecasts valid at 2 m above ground (`TMP:2 m above ground`).

HRRR is operationally the gold-standard short-range forecast for temperature in the continental U.S. It assimilates radar reflectivity and high-density surface observations every hour.

### Data pipeline

```
NOAA generates HRRR GRIB2 on S3
    → AWS SNS publishes notification (topic: NewHRRRObject)
        → Our SQS queue receives the message (long-poll, 20s interval)
            → Herbie downloads GRIB2 from s3://noaa-hrrr-bdp-pds (anonymous)
                → xarray/cfgrib extracts point value at station lat/lon
                    → Row saved to parquet with latency timestamps
```

- **S3 bucket:** `s3://noaa-hrrr-bdp-pds` (NOAA Big Data Program; anonymous access)
- **SNS topic ARN:** `arn:aws:sns:us-east-1:123901341784:NewHRRRObject`
- **SQS queue prefix:** `pred-market-nwp` (created/reused at startup)
- **AWS credentials:** Required only for SQS/SNS subscription; S3 downloads are anonymous.

### Update frequency

| Cycle | Forecast hours |
|-------|---------------|
| All hourly cycles (00Z–23Z) | f00–f18 |
| 00Z, 06Z, 12Z, 18Z cycles | f00–f48 |

New cycles are issued every hour. The **f00 analysis** (analysis hour, closest to "now") becomes available approximately **45–90 minutes after the cycle start time**.

### Latency

Empirically measured at ~**1.5 hours** total latency from cycle time to data saved on disk (measured 2026-02-22 at KMDW). Breakdown:
- `notification_latency_s` = time from cycle start to SNS notification arriving in SQS
- `ingest_latency_s` = time from SNS notification to data saved to disk
- `total_latency_s` = `saved_ts − cycle_utc`

### Storage

```
data/weather/nwp_realtime/hrrr/<ICAO>_YYYY-MM-DD.parquet
```

Code: `services/weather/nwp/hrrr.py`, `services/weather/nwp/base.py`, `services/weather/nwp_listener.py`

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `station` | string | ICAO station identifier |
| `city` | string | City name (from station registry) |
| `model` | string | `"hrrr"` |
| `cycle_utc` | datetime[UTC] | Model initialization time (e.g. 2026-02-22 14:00Z) |
| `forecast_minutes` | int | Minutes of forecast lead time (0 = analysis hour) |
| `valid_utc` | datetime[UTC] | Valid time of the forecast (`cycle_utc + forecast_minutes`) |
| `valid_local` | datetime[tz] | Valid time in station's local timezone |
| `tmp_2m_k` | float | 2 m air temperature in Kelvin |
| `tmp_2m_f` | float | 2 m air temperature in Fahrenheit |
| `grid_lat` | float | Grid cell latitude used for point extraction |
| `grid_lon` | float | Grid cell longitude used for point extraction |
| `notification_ts` | datetime[UTC] | When the SNS notification was received by SQS |
| `saved_ts` | datetime[UTC] | Wall-clock time this row was written to disk |
| `notification_latency_s` | float | Seconds from `cycle_utc` to `notification_ts` |
| `ingest_latency_s` | float | Seconds from `notification_ts` to `saved_ts` |
| `total_latency_s` | float | Seconds from `cycle_utc` to `saved_ts` |

---

## 5. NBM — National Blend of Models

### What it is

The **NBM** (National Blend of Models) is NOAA's statistically post-processed, bias-corrected blend of multiple NWP models including GFS, HRRR, NAM, and others. It produces both **deterministic** and **probabilistic** temperature forecasts calibrated against observed climatology.

The system ingests NBM using **Cloud-Optimized GeoTIFF (COG)** format from S3, which allows efficient point extraction without downloading the entire gridded file (HTTP range requests). The temperature variable used is `TMP:2 m above ground`.

NBM complements HRRR by:
- Incorporating multiple model consensus (reduces individual model bias)
- Providing calibrated uncertainty estimates
- Extending to longer lead times (hourly out to 36 h; 3 h/6 h intervals beyond)

### Data pipeline

```
NOAA generates NBM COG on S3
    → AWS SNS publishes notification (topic: NewNBMCOGObject)
        → Our SQS queue receives the message
            → rasterio reads COG point value from s3://noaa-nbm-pds (anonymous)
                → Row saved to parquet with latency metadata
```

- **S3 bucket:** `s3://noaa-nbm-pds` (public, anonymous)
- **SNS topic ARN:** `arn:aws:sns:us-east-1:123901341784:NewNBMCOGObject`
- **COG path pattern:** `blendv4.3/conus/YYYY/MM/DD/HH00/temp/blendv4.3_conus_temp_RUN_VALID.tif`

### Update frequency

- **Hourly cycles (00Z–23Z):** Short-range forecasts (hourly valid times out to 36 h)
- **Extended range:** 3 h and 6 h valid-time intervals beyond 36 h
- Configured `max_forecast_hour: 24` in the project (adjustable)

New cycles available approximately **45–90 minutes** after cycle start (same cadence as HRRR).

### Latency

Empirically measured at ~**1.5 hours** total latency from cycle time to data saved on disk (measured 2026-02-22 at KMDW).

### Storage

```
data/weather/nwp_realtime/nbm/<ICAO>_YYYY-MM-DD.parquet
```

Code: `services/weather/nwp/nbm_cog.py`, `services/weather/nwp_listener.py`

### Schema

Identical structure to HRRR:

| Column | Type | Description |
|--------|------|-------------|
| `station` | string | ICAO station identifier |
| `city` | string | City name |
| `model` | string | `"nbm"` |
| `cycle_utc` | datetime[UTC] | Model initialization time |
| `forecast_minutes` | int | Minutes of forecast lead time |
| `valid_utc` | datetime[UTC] | Valid time of the forecast |
| `valid_local` | datetime[tz] | Valid time in station's local timezone |
| `tmp_2m_k` | float | 2 m air temperature in Kelvin |
| `tmp_2m_f` | float | 2 m air temperature in Fahrenheit |
| `grid_lat` | float | Grid cell latitude used for point extraction |
| `grid_lon` | float | Grid cell longitude used for point extraction |
| `notification_ts` | datetime[UTC] | When the SNS notification was received |
| `saved_ts` | datetime[UTC] | Wall-clock time this row was written to disk |
| `notification_latency_s` | float | Seconds from `cycle_utc` to `notification_ts` |
| `ingest_latency_s` | float | Seconds from `notification_ts` to `saved_ts` |
| `total_latency_s` | float | Seconds from `cycle_utc` to `saved_ts` |

---

## 6. RRFS — Rapid Refresh Forecast System

### What it is

The **RRFS** (Rapid Refresh Forecast System) is NOAA's **next-generation convection-allowing model** designed to replace HRRR. Like HRRR it runs at **3 km grid spacing** over CONUS using a Lambert conformal projection, but uses a fundamentally different dynamical core and data assimilation system.

**Current status (as of early 2026):** RRFS is in prototype/pre-operational status. Data generation was paused in December 2024 for retrospective testing; historical prototype data remains available on S3. Operational launch is targeted for 2026.

RRFS produces **hourly output only** — there is no sub-hourly (15-minute) product equivalent to HRRR's `subh`. The system collects the `prslev` (pressure-level) product, control member, CONUS domain.

### Data pipeline

```
NOAA uploads RRFS GRIB2 to S3
    → AWS SNS publishes notification (topic: NewRRFSObject)
        → Our SQS queue receives the message
            → Herbie downloads GRIB2 from s3://noaa-rrfs-pds (anonymous)
                → xarray/cfgrib extracts point value at station coordinates
                    → Row saved to parquet with latency metadata
```

- **S3 bucket:** `s3://noaa-rrfs-pds` (public, anonymous)
- **SNS topic ARN:** `arn:aws:sns:us-east-1:709902155096:NewRRFSObject`
- **S3 path layout:** `rrfs_a/rrfs.YYYYMMDD/HH/rrfs.tHHz.prslev.3km.fXXX.conus.grib2`
- **Note:** The default Herbie RRFS template is outdated as of early 2026; the project monkey-patches `herbie.models.rrfs.template` to match the actual S3 layout.

### Update frequency

| Cycle | Forecast hours |
|-------|---------------|
| All hourly cycles (00Z–23Z) | f00–f18 |
| 00Z, 06Z, 12Z, 18Z cycles | f00–f60 |

### Latency

Similar to HRRR (~45–90 minutes from cycle start to availability). Real-time SNS-to-disk latency tracked via the same `notification_latency_s` / `total_latency_s` columns. Note: as of early 2026, RRFS data generation is paused; historical data is available for research.

### Storage

```
data/weather/nwp_realtime/rrfs/<ICAO>_YYYY-MM-DD.parquet
```

Code: `services/weather/nwp/rrfs.py`, `services/weather/nwp_listener.py`

### Schema

Identical structure to HRRR and NBM:

| Column | Type | Description |
|--------|------|-------------|
| `station` | string | ICAO station identifier |
| `city` | string | City name |
| `model` | string | `"rrfs"` |
| `cycle_utc` | datetime[UTC] | Model initialization time |
| `forecast_minutes` | int | Minutes of forecast lead time |
| `valid_utc` | datetime[UTC] | Valid time of the forecast |
| `valid_local` | datetime[tz] | Valid time in station's local timezone |
| `tmp_2m_k` | float | 2 m air temperature in Kelvin |
| `tmp_2m_f` | float | 2 m air temperature in Fahrenheit |
| `grid_lat` | float | Grid cell latitude used for point extraction |
| `grid_lon` | float | Grid cell longitude used for point extraction |
| `notification_ts` | datetime[UTC] | When the SNS notification was received |
| `saved_ts` | datetime[UTC] | Wall-clock time this row was written to disk |
| `notification_latency_s` | float | Seconds from `cycle_utc` to `notification_ts` |
| `ingest_latency_s` | float | Seconds from `notification_ts` to `saved_ts` |
| `total_latency_s` | float | Seconds from `cycle_utc` to `saved_ts` |

---

## 7. Kalshi Orders — Market Snapshots

### What it is

The **market snapshot** table captures the top-of-book and summary state for each Kalshi market at regular intervals. This is the primary record of **market microstructure**: bid/ask spread, last trade price, cumulative volume, and open interest.

Data flows from two channels on the Kalshi WebSocket API:
- `ticker` / `ticker_v2` messages: real-time price and volume updates
- `orderbook_delta` messages: used to keep the in-memory orderbook current, whose best levels populate the snapshot

### Transport & auth

- **WebSocket endpoint:** `wss://api.elections.kalshi.com/trade-api/ws/v2`
- **Channels subscribed:** `orderbook_delta`, `ticker`
- **Authentication:** RSA-signed JWT (API key ID + RSA private key from `~/.kalshi/`)
- **Reconnection:** Automatic via `KalshiWSMixin`

### Snapshot triggers

Snapshots are taken in two ways:

| Trigger | Condition |
|---------|-----------|
| **Periodic** | Every `interval_seconds` (default: 60 s) regardless of price activity |
| **Spike** | Immediately when any of `yes_bid`, `yes_ask`, or `last_price` moves ≥ `spike_threshold_cents` (default: 3¢) from the previously snapshot price; subject to a `spike_cooldown_seconds` (default: 2 s) to prevent flood |

### Baseline vs delta snapshots

Every `baseline_every_n_snapshots` (default: 60) snapshots, the full orderbook is written to the orderbook table (`snapshot_type = "baseline"`). Between baselines, only levels that changed since the last baseline are written (`snapshot_type = "delta"`, with `quantity = 0` indicating a removed level). This design compresses storage significantly during quiet periods.

### Event rollover

Markets are re-discovered every `rediscover_interval_seconds` (default: 300 s). When a new day's market becomes active (local midnight), the listener automatically subscribes to the new ticker without requiring a restart.

### Storage

```
data/kalshi/market_snapshots/YYYY-MM-DD.parquet
```

Code: `services/kalshi/listener.py`, `services/core/storage.py`

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_ts` | timestamp[UTC] | Wall-clock time the snapshot was taken |
| `event_ticker` | string | Event series identifier (e.g. `KXHIGHCHI-26FEB22`) |
| `market_ticker` | string | Individual contract identifier (e.g. `KXHIGHCHI-26FEB22-T75`) |
| `subtitle` | string | Human-readable description (e.g. `"High above 75"`) |
| `yes_bid` | int32 | Best YES bid price in cents (0–100) |
| `yes_ask` | int32 | Best YES ask price in cents (0–100) |
| `no_bid` | int32 | Best NO bid in cents; derived as `100 − yes_ask` when not explicitly sent |
| `no_ask` | int32 | Best NO ask in cents; derived as `100 − yes_bid` when not explicitly sent |
| `last_price` | int32 | Last traded price in cents |
| `volume` | int64 | Cumulative contracts traded since market open |
| `open_interest` | int64 | Current outstanding open contracts |
| `trigger` | string | `"periodic"` or `"spike"` — what caused this snapshot |

**Pricing notes:**
- All prices are in cents (1–99¢ for tradable contracts; 0 or 100 indicates no bid/offer).
- For a binary market: `yes_bid + no_ask = 100` and `yes_ask + no_bid = 100` (approximately, subject to spread).
- Implied probability of YES = midpoint of `(yes_bid + yes_ask) / 2`.

---

## 8. Kalshi Book — Orderbook Snapshots

### What it is

The **orderbook snapshot** table records the full **depth of market** — every price level and quantity — for each Kalshi contract. It is maintained by applying a stream of `orderbook_delta` WebSocket messages to an in-memory order book, then periodically persisting that state to parquet.

This data enables reconstruction of the full limit order book at any point in time, which is useful for:
- Computing bid-ask spread at any depth
- Measuring liquidity available at each strike
- Identifying hidden support/resistance levels
- Backtesting fill assumptions for strategy simulation

### Depth limit

`max_orderbook_depth` (default: 5) limits how many price levels are stored per side. Set to 0 for unlimited depth.

### Storage

```
data/kalshi/orderbook_snapshots/YYYY-MM-DD.parquet
```

Code: `services/kalshi/listener.py` (`_take_snapshot` method), `services/core/storage.py`

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_ts` | timestamp[UTC] | Wall-clock time the snapshot was taken |
| `market_ticker` | string | Contract identifier |
| `side` | string | `"yes"` or `"no"` |
| `price_cents` | int32 | Price level in cents (1–99) |
| `quantity` | float64 | Number of contracts resting at this price level; `0.0` means the level was removed (delta rows only) |
| `snapshot_type` | string | `"baseline"` (full book) or `"delta"` (only changed levels since last baseline) |

**Reconstruction:** To reconstruct the full order book at a given time:
1. Find the most recent `baseline` row for each `(market_ticker, side, price_cents)` before or at the target time.
2. Apply all `delta` rows between that baseline and the target time: for each `(market_ticker, side, price_cents)`, use the last-seen `quantity` (0.0 = level no longer exists).

---

## 9. Supporting / Secondary Sources

These sources are part of the project but serve as historical/research inputs rather than real-time trading signals.

### RTMA-RU — Real-Time Mesoscale Analysis (Rapid Update)

An **analysis product** (not a forecast) providing the best estimate of current surface conditions. Unlike HRRR/NBM which are forecasts, RTMA-RU assimilates observations to produce an analysis of the current atmospheric state.

| Attribute | Value |
|-----------|-------|
| Grid resolution | 2.5 km NDFD grid, CONUS |
| Temporal resolution | Every 15 minutes (00, 15, 30, 45 past each hour = 96 cycles/day) |
| Forecast hours | Analysis only (f00 — no forecast lead) |
| S3 bucket | `s3://noaa-rtma-pds` (public, anonymous) |
| Latency | ~29 minutes from valid time to availability |

RTMA-RU has the **lowest latency** of the four NWP products (~29 min vs ~90 min for HRRR/NBM), making it the most current gridded surface temperature estimate. Storage path: `data/weather/nwp_realtime/rtma_ru/<ICAO>_YYYY-MM-DD.parquet`. Schema is identical to HRRR/NBM/RRFS.

### Synoptic API — ASOS 1-minute observations (live)

Real-time ASOS 1-minute air temperature via the **Synoptic Data** API. This is the highest-frequency public surface observation product (1-minute resolution vs METAR hourly). Available via:
- **WebSocket** (streaming mode, requires streaming-enabled Synoptic token)
- **REST polling** (default; polls every 90 s, fetches last 120 minutes of observations)

Storage: `data/weather/synoptic_observations/<stid>_YYYY-MM-DD.parquet`

Schema:

| Column | Type | Description |
|--------|------|-------------|
| `received_ts` | datetime[UTC] | Wall-clock time the observation was received |
| `ob_timestamp` | datetime[UTC] | Observation valid time |
| `stid` | string | Synoptic station identifier (same as ICAO for ASOS) |
| `sensor` | string | Sensor variable name (e.g. `air_temp_set_1`) |
| `value` | float | Air temperature in °F (English units) |
| `source` | string | `"live"` (polling) or omitted (streaming) |

**Latency:** Synoptic ingests ASOS 1-minute data in near-real time; API latency is typically seconds to low single-digit minutes. However, 1-minute data from IEM (historical batch) carries an 18–36 hour lag and is used only for research.

### IEM ASOS 1-minute (historical batch)

Historical ASOS 1-minute temperature from the **Iowa Environmental Mesonet (IEM)**. Fetched via `research/download_data/iem_asos_1min.py`. Used for backtesting strategy logic and verifying against official daily highs.

- **Lag:** 18–36 hours (not suitable for same-day trading)
- **Precision:** 0.1°C or 0.1°F
- **Storage:** `data/iem_asos_1min/`

### IEM Daily Climate — NWS CLI (historical batch)

Historical NWS Daily Climate Reports retrieved from IEM's archive. This is the **same official high/low** that Kalshi uses to settle contracts. Used for backtesting, discrepancy analysis, and strategy validation.

- **Lag:** 6–12 hours after climate day ends (same as live CLI)
- **Storage:** `data/iem_daily_climate/`
- **Fetch script:** `research/download_data/iem_daily_climate.py`


## Latency Comparison Summary

| Source | Update Frequency | Access Latency | Use in Trading |
|--------|-----------------|----------------|----------------|
| **Wethr Push (observations)** | 1–5 min / station | Seconds–minutes | **Primary real-time signal** |
| **Wethr Push (new_high/new_low)** | Event-driven | Seconds | **Primary trading trigger** |
| **Synoptic ASOS 1-min (live)** | 1 min | Seconds–minutes | Real-time temperature monitor |
| **RTMA-RU** | Every 15 min | ~29 min | Near-real-time gridded analysis |
| **METAR** | Hourly + SPECI | ~1.5 hours | Cross-validation |
| **HRRR** | Hourly | ~1.5 hours | Forecast context |
| **NBM** | Hourly | ~1.5 hours | Calibrated forecast |
| **RRFS** | Hourly | ~1.5 hours | Research / future |
| **Kalshi orderbook** | Real-time (WS) | Milliseconds–seconds | Market microstructure |
| **Kalshi market snapshot** | 60 s + spikes | Seconds | Position monitoring |

---

*Last updated: 2026-02-25. Re-run `python -m research.download_data.measure_data_source_latency` to refresh empirical latency measurements.*
