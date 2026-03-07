# Kalshi Daily High Temperature — XGBoost Quantile Regression Model

> **Scope**: Historical backtesting framework, designed for future production deployment.
> **Station**: KMDW (Chicago Midway) — first station; architecture must expand trivially to all stations in `KALSHI_MARKET_REGISTRY`.
> **Algorithm**: XGBoost with `reg:quantileerror` (multi-quantile regression).

---

## Table of Contents

1. [System Architecture](#1-system-architecture)
2. [Data Sources](#2-data-sources)
3. [Target Variable (Response)](#3-target-variable-response)
4. [Training Row Selection](#4-training-row-selection)
5. [Feature Engineering](#5-feature-engineering)
6. [Latency & Future-Seeing Prevention](#6-latency--future-seeing-prevention)
7. [Row Validity Rules & Logging](#7-row-validity-rules--logging)
8. [XGBoost Model Configuration](#8-xgboost-model-configuration)
9. [Walk-Forward Cross-Validation](#9-walk-forward-cross-validation)
10. [Evaluation Metrics](#10-evaluation-metrics)
11. [From Historical Testing to Production](#11-from-historical-testing-to-production)
12. [Open Questions & Future Work](#12-open-questions--future-work)

---

## 1. System Architecture

The system has two decoupled engines:

1. **Weather Brain (ML Model)** — Predicts *how much hotter* the physical atmosphere will get before the LST climate day ends, expressed as a full probability distribution over the remaining heating delta. Uses **only weather data** as input; no Kalshi market data.

2. **Execution Brain (Trading Logic)** — Maps the Weather Brain's probabilistic forecast against every active Kalshi strike contract to find mispriced expected value (EV) and execute trades. Uses Kalshi market snapshot data (`data/kalshi/market_snapshots/`) for live bid/ask prices, spread, and volume.

**This document specifies the Weather Brain only.** The Execution Brain is documented separately. Kalshi data is used exclusively for:
- Backtesting simulated trading profit (evaluation metric)
- Live execution logic (production)

---

## 2. Data Sources

Exactly **three** data sources feed the model. No others (no Synoptic 1-min, no MADIS, no aviationweather METAR, no IEM archives).

### 2.1 Wethr Push (`data/weather/wethr_push/`)

| Product | Directory | Role |
|---------|-----------|------|
| **observations** (`ASOS-HFM`, `ASOS-HR`) | `observations/{ICAO}_{date}.parquet` | Features + training row selection |
| **cli** | `cli/{ICAO}_{date}.parquet` | Ground-truth label (Final CLI High) |
| **dsm** | `dsm/{ICAO}_{date}.parquet` | Fallback label if CLI is unavailable |
| **new_high** / **new_low** | `new_high/`, `new_low/` | Not used by the model |

**Observation product types:**

| Product Code | Description | Frequency | Temperature Precision | Used For |
|-------------|-------------|-----------|----------------------|----------|
| `ASOS-HFM` | High-frequency METAR (5-min automated) | Every 5 minutes | Integer °C → ~1.8°F precision | Rolling feature extraction only |
| `ASOS-HR` | Hourly METAR / SPECI (with T-group remarks) | Hourly + event-driven | 0.1°C → ~0.18°F precision | **Training rows** |

**Key rule**: `ASOS-HFM` rows are used to compute rolling momentum features (velocity, acceleration) but are **never** used as training observations themselves. Only `ASOS-HR` rows generate training samples.

**Observation schema** (from parquet):

| Column | Type | Description |
|--------|------|-------------|
| `station_code` | string | ICAO code (e.g. `KMDW`) |
| `observation_time_utc` | datetime[UTC] | Time of the ASOS observation |
| `received_ts` | datetime[UTC] | Wall-clock time the SSE event arrived |
| `product` | string | `ASOS-HFM` or `ASOS-HR` |
| `temperature_celsius` | int64/float | Air temperature in °C |
| `temperature_fahrenheit` | float64 | Air temperature in °F |
| `dew_point_celsius` | int64/float | Dew point in °C |
| `dew_point_fahrenheit` | float64 | Dew point in °F |
| `relative_humidity` | string/null | Relative humidity (%) — may be null for HFM |
| `wind_direction` | string | Wind direction (degrees or `VRB`) |
| `wind_speed_mph` | int64 | Wind speed in mph |
| `wind_gust_mph` | string/null | Wind gust in mph (not used as model feature) |
| `visibility_miles` | string/null | Visibility in statute miles |
| `altimeter_inhg` | string/null | Altimeter setting in inHg |
| `wethr_high_nws_f` | int64 | Wethr's running NWS high estimate in °F |
| `wethr_high_wu_f` | float64/null | Wethr's running WU high estimate |
| `anomaly` | bool | Wethr anomaly flag |
| `event_id` | string | Unique event ID |

**CLI schema** (ground-truth label):

| Column | Type | Description |
|--------|------|-------------|
| `station_code` | string | ICAO code |
| `for_date_lst` | string | Climate date (YYYY-MM-DD in LST) |
| `received_ts_utc` | datetime[UTC] | When the CLI was received |
| `high_f` | int64 | **Official NWS daily high in °F** — this is the label |
| `high_c` | float64 | Official daily high in °C |
| `low_f` | int64 | Official daily low in °F |
| `low_c` | float64 | Official daily low in °C |

### 2.2 NWP — NBM Hourly (`data/weather/nwp_realtime/nbm/`)

The **National Blend of Models** provides statistically bias-corrected, consensus temperature forecasts at hourly resolution.

| Attribute | Value |
|-----------|-------|
| Temporal resolution | Hourly lead times (60, 120, 180, …, 1380 min) |
| Cycles | Hourly (00Z–23Z) |
| Latency (P95) | **4852 seconds ≈ 80.9 minutes** |
| Variable | `tmp_2m_f` — 2m air temperature in °F |

**NBM schema** (from parquet):

| Column | Type | Description |
|--------|------|-------------|
| `station` | string | ICAO code |
| `model` | string | `"nbm"` |
| `model_version` | string | e.g. `"v4.3"` |
| `model_run_time_utc` | datetime[UTC] | Cycle initialization time |
| `lead_time_minutes` | int64 | Minutes of forecast lead (60, 120, …) |
| `forecast_target_time_utc` | datetime[UTC] | Valid time of forecast |
| `tmp_2m_f` | float64 | Forecasted 2m temp in °F |
| `tmp_2m_std_f` | float64/null | Forecasted 2m temp standard deviation in °F |
| `max_temp_p10_f_std` | float64/null | 10th percentile derived from tmp_2m_std_f |
| `max_temp_p90_f_std` | float64/null | 90th percentile derived from tmp_2m_std_f |
| `forecast_target_time_lst` | datetime | Target time in Local Standard Time |
| `forecast_target_date_lst` | string | Target LST date (YYYY-MM-DD) |
| `notification_ts_utc` | datetime[UTC] | When the SNS/backfill notification was created |
| `is_live` | bool | `True` for live-ingested, `False` for backfilled |

### 2.3 NWP — RRFS Subhourly (`data/weather/nwp_realtime/rrfs/`)

The **Rapid Refresh Forecast System** provides higher-temporal-resolution (15-minute) forecasts, complementing NBM's hourly resolution.

| Attribute | Value |
|-----------|-------|
| Temporal resolution | 15-minute lead times (15, 30, 45, 60, …) |
| Cycles | Hourly (24x daily, 00Z–23Z) |
| Latency (P95) | **7291 seconds ≈ 121.5 minutes** (estimated for backtest) |
| Variable | `tmp_2m_f` — 2m air temperature in °F |
| **Status** | Pre-operational as of early 2026; data generation paused Dec 2024. Historical prototype data available on S3. |

**RRFS schema** (from parquet): identical to NBM except no std-derived columns (`tmp_2m_std_f`, `max_temp_p10_f_std`, or `max_temp_p90_f_std`).

| Column | Type | Description |
|--------|------|-------------|
| `station` | string | ICAO code |
| `model` | string | `"rrfs"` |
| `model_run_time_utc` | datetime[UTC] | Cycle initialization time |
| `lead_time_minutes` | int64 | Minutes of forecast lead (15, 30, 45, 60, …, up to 1080) |
| `forecast_target_time_utc` | datetime[UTC] | Valid time of forecast |
| `tmp_2m_f` | float64 | Forecasted 2m temp in °F |
| `forecast_target_time_lst` | datetime | Target time in LST |
| `forecast_target_date_lst` | string | Target LST date |
| `notification_ts_utc` | datetime[UTC] | Notification timestamp |
| `is_live` | bool | Live or backfill flag |

> **⚠ RRFS availability risk**: Because RRFS data generation is paused, many historical days will have no RRFS data. When RRFS is unavailable, all RRFS features are set to `NaN` and the row is **kept** — XGBoost handles missing values natively (see [§6 Latency — Decay Weight Approach](#6-latency--future-seeing-prevention)). The `rrfs_cycle_age_minutes` decay-weight feature lets the model learn how much (or little) to trust RRFS data when it is present.

### 2.4 Kalshi Market Snapshots (`data/kalshi/market_snapshots/`)

**NOT used as model features.** Used exclusively for:
- Walk-forward backtesting: computing simulated P&L against live bid/ask at each decision point.
- Production execution logic (Execution Brain).

| Column | Type | Used For |
|--------|------|----------|
| `snapshot_ts_utc` | timestamp[UTC] | Aligning market state to observation time |
| `event_ticker` | string | Identifying which climate day's market |
| `market_ticker` | string | Identifying which strike contract |
| `subtitle` | string | Parsing strike temperature (e.g. `"High above 75"`) |
| `yes_bid` | int32 | Simulated execution price (cents) |
| `yes_ask` | int32 | Simulated execution price (cents) |
| `last_price` | int32 | Reference price |

---

## 3. Target Variable (Response)

### Definition

$$Y_t = \text{Final\_CLI\_High\_F} - \text{Custom\_Intraday\_Max}_t$$

where:
- **$\text{Final\_CLI\_High\_F}$** = `high_f` from `wethr_push/cli` for the current LST climate day. This is the NWS official daily high in whole °F. Source of truth for Kalshi settlement.
- **$\text{Custom\_Intraday\_Max}_t$** = the **Tri-Factor Max** (Smart Max) — a synthesized best estimate from three sources: ASOS-HR precision, ASOS-HFM inter-hour spikes, and DSM floor. See [§5.1 Tri-Factor Max](#51-state-of-the-day-from-wethr_push-observations).

### Interpretation

- $Y_t > 0$: The atmosphere will still get hotter. There are $Y_t$ degrees of heating remaining.
- $Y_t = 0$: The peak has already been reached. No further heating expected.
- $Y_t < 0$: **Should not happen** if the CLI high is truly the day's maximum. If observed, it indicates either (a) a rounding artifact between 5-min precision and the official high, or (b) the custom_intraday_max includes an anomalous reading. Rows with $Y_t < 0$ should be clipped to 0 and flagged.

### Climate Day Boundary

The "current climate day" is midnight-to-midnight **Local Standard Time (LST)**, ignoring DST year-round. For KMDW (America/Chicago):
- LST offset: UTC−6 (always, regardless of DST)
- Climate day boundary: 06:00 UTC to 05:59 UTC (next day)
- During CDT: 1:00 AM local to 12:59 AM local (next day)

`Custom_Intraday_Max` must be **hard-reset at midnight LST** (06:00 UTC for Chicago).

### Label Source Priority

1. **Primary**: `wethr_push/cli` → `high_f` for the LST climate day.
2. **Fallback**: `wethr_push/dsm` → `high_f` if CLI is unavailable.
3. **If neither exists**: The entire climate day is excluded from training.

---

## 4. Training Row Selection

### Which rows become training samples

A training sample is generated at **every `ASOS-HR` observation** within the LST climate day. This means:
- One sample per hourly routine METAR (typically at :53 or :55 past the hour).
- Additional samples for SPECI reports (issued on significant weather changes).
- **No samples from `ASOS-HFM`** rows — those are only used for rolling feature computation.

This gives approximately **24–30 training rows per station per climate day** (24 hourly + variable SPECIs).

### Why ASOS-HR only

`ASOS-HR` observations carry T-group remarks encoding 0.1°C precision. The `temperature_fahrenheit` values from these observations have meaningful decimal precision (e.g. 30.9°F, 30.2°F). `ASOS-HFM` observations use integer °C converted to °F, yielding values like 33.8°F that look precise but are actually rounded. Using `ASOS-HR` ensures the model is trained on high-fidelity temperature signals.

### What makes a row valid

**All** of the following must be true for a row to be included in training (see [Row Validity Rules](#7-row-validity-rules--logging) for full detail):

1. The observation has `product == "ASOS-HR"`.
2. The CLI label exists for this LST climate day.
3. Core observation features are non-NaN (`distance_from_max_f`, `dew_point_f`).
4. The computed `custom_intraday_max` is valid (at least one observation exists prior to this row in the climate day).

**NWP availability is NOT a row validity requirement.** If NBM or RRFS data is missing, the corresponding features are set to `NaN` and handled by XGBoost's native missing-value support. The `cycle_age_minutes` features act as implicit decay weights (see [§6](#6-latency--future-seeing-prevention)).

If any hard condition (1–4) fails, the row is **dropped** and a structured log entry is written (see logging section).

---

## 5. Feature Engineering

All features are computed at the observation time $t$ of each `ASOS-HR` row. The feature DataFrame is constructed from a **5-minute resampled timeline** (using all `ASOS-HFM` + `ASOS-HR` observations), but only `ASOS-HR` timestamps are selected as training rows.

### 5.1 State of the Day (from wethr_push observations)

| Feature | Description | Source |
|---------|-------------|--------|
| `custom_intraday_max_f` | Tri-Factor Max (Smart Max) — synthesized best estimate of daily high so far | Derived — see Tri-Factor Max below |
| `distance_from_max_f` | `custom_intraday_max_f - temperature_at_t` (degrees below today's observed peak) | Derived |
| `distance_from_max_ratio` | `distance_from_max_f / max(custom_intraday_max_f, 1)` — relative distance from peak | Derived |
| `hours_since_midnight_lst` | Decimal hours since midnight LST | `observation_time_utc` → LST conversion |
| `t_max_lag_minutes` | Minutes since `custom_intraday_max_f` was last set or updated | Derived — see below |
| `max_source_is_hfm` | 1 if the max was set by an inter-hour HFM spike (rounding bias possible) | Derived |
| `hfm_hr_divergence_f` | `(hfm_spike_max_c × 1.8 + 32) - hr_anchor_max_f` — size of inter-hour spike | Derived |
| `dsm_update_received` | 1 if an official intraday DSM/CLI max has been parsed today | Derived |

**Tri-Factor Max (Smart Max)** — guards against ASOS-HFM upward rounding bias:

ASOS METARs report integer °C (0.5 rounds up). A naive max over all observations inflates the peak by up to ~0.9°F when HFM rounds 25.5°C → 26°C → 78.8°F. The Smart Max synthesizes three anchors:

1. **hr_anchor_max_f**: max of `temperature_fahrenheit` from ASOS-HR only (0.1°C precision).
2. **hfm_spike_max_c**: max of `temperature_celsius` from ASOS-HFM only (high frequency, low precision).
3. **dsm_floor_max_f**: max of `high_f` from DSM rows with `received_ts_utc ≤ t` for this climate day (official NWS floor).

Algorithm: (a) If DSM floor > max(hr_anchor, hfm_lower_bound) → use DSM. (b) Else if `hfm_lower_bound_f = (hfm_spike_max_c - 0.5) × 1.8 + 32` > hr_anchor → use HFM raw (flag via `max_source_is_hfm`). (c) Else → use hr_anchor. The lower bound protects against upward rounding; XGBoost learns bias correction via `max_source_is_hfm` and `hfm_hr_divergence_f`.

**`t_max_lag_minutes` — the "Stale Max" Signal**:
This feature encodes a critical physical insight: a *stale* intraday maximum (set hours ago) is much harder to break than a *fresh* maximum (set minutes ago). When `t_max_lag_minutes` is large AND `distance_from_max_f` is also large, the temperature is falling away from an old peak — the remaining delta is likely near zero. When `t_max_lag_minutes` is small, the atmosphere is actively pushing into new highs, and further warming is plausible.

**Rationale**: These features capture *where we are in the day* and *how far current temperature is from the peak*. The `distance_from_max` and `t_max_lag_minutes` together tell the model whether the peak is "stale" (temperature falling away from peak) or "fresh" (still near peak and potentially rising).

### 5.2 Micro-Momentum (from 5-min ASOS-HFM resampled timeline)

| Feature | Description | Source |
|---------|-------------|--------|
| `velocity_15m_f` | Temp change over last 15 min (3 × 5-min intervals) | `temp(t) - temp(t-15min)` from 5-min grid |
| `velocity_30m_f` | Temp change over last 30 min | `temp(t) - temp(t-30min)` from 5-min grid |
| `velocity_60m_f` | Temp change over last 60 min | `temp(t) - temp(t-60min)` from 5-min grid |
| `acceleration_15m_f` | Rate of change of 15-min velocity | `velocity_15m(t) - velocity_15m(t-15min)` |
| `temp_volatility_30m_f` | Std dev of 5-min temps over last 30 min | Rolling std over 6 × 5-min intervals |
| `temp_volatility_60m_f` | Std dev of 5-min temps over last 60 min | Rolling std over 12 × 5-min intervals |

**Rationale**: Momentum features capture the heating/cooling rate. Acceleration detects inflection points (the moment heating starts to flatten). Volatility captures rounding-boundary oscillation — frequent bouncing between integer values signals the true temperature is sitting on a rounding edge.

### 5.3 Meteorological Context (from ASOS-HR observation at time $t$)

| Feature | Description | Source |
|---------|-------------|--------|
| `dew_point_f` | Dew point at observation time | `dew_point_fahrenheit` |
| `dew_point_depression_f` | `temperature_at_t - dew_point_f` | Derived |
| `wind_speed_mph` | Wind speed | Direct column |
| `wind_dir_sin` | `sin(wind_direction_deg × π/180)` | Encoded from `wind_direction` |
| `wind_dir_cos` | `cos(wind_direction_deg × π/180)` | Encoded from `wind_direction` |
| `altimeter_inhg` | Barometric pressure | Direct column |
| `visibility_miles` | Visibility in statute miles (proxy for cloud/fog; low = reduced solar heating) | `visibility_miles` |
| `pressure_tendency_1h_inhg` | Change in altimeter over last ~60 minutes | `altimeter(t) - altimeter(t-60min)` — falling pressure often precedes warming |

**Wind direction encoding**: If `wind_direction = "VRB"` (variable), set both `wind_dir_sin = 0` and `wind_dir_cos = 0`. If `wind_direction` is null or non-numeric, the row may be dropped (handled in validity rules).

**Rationale**: Dew point depression is a strong proxy for air mass type and cloud cover. High depression = dry, clear sky = stronger solar heating. Wind direction captures lake-breeze effects (critical for KMDW near Lake Michigan — NE winds bring cooler air). Pressure tendency captures approaching systems: falling pressure often precedes warm advection and further heating; rising pressure suggests clearing and a lower ceiling for additional warming.

### 5.4 NBM Forecast Features (from latest available NBM cycle)

For each training observation at time $t$, identify the **most recent available NBM cycle** in the dataset that satisfies the latency constraint. Prefer using the ground-truth `notification_ts_utc` (the exact time data became available) when present in the parquet; otherwise, fall back to the conservative P95 latency bound:

$$\text{latest\_nbm\_cycle} = \max \{ c \mid \text{available\_at}(c) \leq t \}$$

Where $\text{available\_at}(c) = \min(\text{notification\_ts\_utc}(c), c + 4852\text{s})$.

**Key change from strict-drop**: If the theoretically expected cycle is missing from the data, **do NOT drop the row**. Instead, use the most recent *available* cycle that satisfies the latency constraint. The `nbm_cycle_age_minutes` feature acts as a **decay weight** — XGBoost will naturally learn to trust stale NWP data less as this feature increases. If no NBM cycle at all is available for the entire climate day, set all NBM features to `NaN` (XGBoost handles missing values natively).

Extract forecast rows from the selected cycle whose `forecast_target_time_utc` falls **after** $t$ and **before midnight LST** (end of climate day):

| Feature | Description | Derivation |
|---------|-------------|------------|
| `nbm_max_remaining_f` | Highest NBM-forecasted temp for the remainder of the climate day | `max(tmp_2m_f)` where `forecast_target_time_utc > t` and same LST climate day |
| `nbm_expected_delta_f` | Model-predicted remaining heating | `nbm_max_remaining_f - custom_intraday_max_f` |
| `nbm_current_error_f` | Current model bias | `temperature_at_t - nbm_forecast_valid_at_t` (interpolated from nearest lead time) |
| `nbm_hours_of_forecast_remaining` | Hours of forecast remaining until last valid-time in climate day | `(last_forecast_target_time_utc - t).total_seconds() / 3600` |
| `nbm_cycle_age_minutes` | **Decay weight**: how old the NBM cycle is relative to $t$ | `(t - latest_nbm_cycle).total_seconds() / 60` |
| `nbm_heating_trend_f` | Max remaining forecast minus current forecast | `nbm_max_remaining_f - nbm_forecast_valid_at_t` |
| `raw_nbm_model_run_time_utc` | The actual NBM cycle time used to generate features | `latest_nbm_cycle` |

**Rationale**: `nbm_expected_delta` is the most informative single feature — it's the NWP model's own estimate of the remaining heating, which XGBoost can learn to bias-correct. `nbm_current_error` captures systematic warm/cold bias that may persist through the day. `nbm_cycle_age_minutes` is the **implicit trust decay** — at 80 minutes the cycle is fresh and highly trustworthy; at 360+ minutes the model learns to discount it.

### 5.5 RRFS Forecast Features (from latest available RRFS cycle)

Same decay-weight logic as NBM, but with RRFS-specific latency and cycle frequency:

$$\text{latest\_rrfs\_cycle} = \max \{ c \mid \text{available\_at}(c) \leq t \}$$

Where $\text{available\_at}(c) = \min(\text{notification\_ts\_utc}(c), c + 7291\text{s})$.

Note: RRFS cycles are hourly (00Z-23Z), similar to NBM. `rrfs_cycle_age_minutes` will reflect this. If no RRFS cycle is available at all, set all RRFS features to `NaN`.

| Feature | Description | Derivation |
|---------|-------------|------------|
| `rrfs_max_remaining_f` | Highest RRFS-forecasted temp for the remainder of the climate day | `max(tmp_2m_f)` where `forecast_target_time_utc > t` and same LST climate day |
| `rrfs_expected_delta_f` | RRFS-predicted remaining heating | `rrfs_max_remaining_f - custom_intraday_max_f` |
| `rrfs_current_error_f` | Current RRFS bias | `temperature_at_t - rrfs_forecast_valid_at_t` (interpolated from nearest 15-min lead) |
| `rrfs_cycle_age_minutes` | **Decay weight**: age of the RRFS cycle | `(t - latest_rrfs_cycle).total_seconds() / 60` |
| `rrfs_nbm_delta_divergence_f` | Disagreement between models on expected delta | `rrfs_expected_delta_f - nbm_expected_delta_f` |
| `nbm_rrfs_max_spread_f` | Absolute disagreement on max remaining temp | `|nbm_max_remaining_f - rrfs_max_remaining_f|` — NaN if either missing |
| `raw_rrfs_model_run_time_utc` | The actual RRFS cycle time used | `latest_rrfs_cycle` |

**Rationale**: RRFS provides complementary forecast information from a different dynamical core. The divergence between RRFS and NBM is itself informative — large disagreement signals higher forecast uncertainty. Because RRFS has 15-minute resolution (vs NBM hourly), it captures finer intra-hour heating dynamics. The `rrfs_cycle_age_minutes` decay weight indicates trust: values up to 360 minutes are utilized, beyond which the cycle may be considered fully missing.

### 5.6 Advection Gap (NWP vs. Observed Temperature Divergence)

The **advection gap** measures how much the actual atmosphere has diverged from what the NWP models forecasted for the current hour. A systematic divergence signals **warm or cold advection** — the transport of heat by horizontal wind — which NWP models sometimes underestimate, especially near coastlines, fronts, or terrain boundaries.

| Feature | Description | Derivation |
|---------|-------------|------------|
| `advection_gap_trend_60m_f` | Change in NBM advection gap over last 60 minutes | `nbm_current_error_f(t) - nbm_current_error_f(t-60min)` |
| `nwp_consensus_advection_f` | Average advection gap across both models | `mean(nbm_current_error_f, rrfs_current_error_f)` — NaN-tolerant |

> **Note**: The advection gap at time $t$ is the same as `nbm_current_error_f` / `rrfs_current_error_f` (observed temp minus NWP forecast). §5.4 and §5.5 define those; here we use them for the trend and consensus.

**Rationale**: If both NWP models predicted 72°F for this hour but the actual temperature is 75°F, the +3°F advection gap indicates a warm air mass is being advected into the station. This gap tends to persist and amplifies the remaining heating potential beyond what the NWP alone predicts. The `advection_gap_trend_60m_f` captures whether the divergence is growing (accelerating advection) or shrinking (models catching up).

### 5.7 Solar Geometry (computed from station coordinates + time)

The **solar elevation angle** is a physically-motivated feature that captures the amount of insolation (solar energy) available to heat the surface. Unlike `day_of_year` or `hours_since_midnight_lst`, solar elevation directly encodes the interaction between *time of day* and *season* — the same clock time produces very different solar angles in June vs. December.

| Feature | Description | Derivation |
|---------|-------------|------------|
| `solar_elevation_deg` | Sun elevation angle above the horizon (degrees) | Computed from station `(lat, lon)` and `observation_time_utc` using standard solar position equations |
| `solar_azimuth_deg` | Sun azimuth (0°=N, 90°=E, 180°=S, 270°=W) | Same computation |
| `hours_until_solar_noon` | Hours remaining until solar noon (max elevation) | `solar_noon_utc - observation_time_utc` (negative if past solar noon) |

**Computation**: Use the standard astronomical solar position algorithm (e.g. `pvlib.solarposition.get_solarposition()` or equivalent). Inputs are station latitude/longitude from `KALSHI_MARKET_REGISTRY` and the observation UTC timestamp. No external API needed — this is a deterministic calculation.

**Rationale**: Solar elevation is the fundamental driver of daytime surface heating. After solar noon, incoming radiation decreases monotonically, making it progressively harder for the temperature to rise. A low `solar_elevation_deg` late in the afternoon combined with a `distance_from_max > 0` strongly suggests the peak has passed. This feature is more physically meaningful than raw clock time because it accounts for seasonal variation in daylight geometry.

### 5.8 Calendar / Seasonal Features

| Feature | Description |
|---------|-------------|
| `day_of_year` | Integer 1–366, captures seasonal cycle |
| `month` | Integer 1–12, coarser seasonal signal |

**Rationale**: XGBoost cannot extrapolate — it needs explicit seasonal encoding. Day-of-year captures climatological heating patterns. No cyclical encoding needed; tree-based models handle integer splits natively.

### 5.9 Complete Feature Vector Summary

| Group | Count | Features |
|-------|-------|----------|
| State of the Day | 8 | `custom_intraday_max_f`, `distance_from_max_f`, `distance_from_max_ratio`, `hours_since_midnight_lst`, `t_max_lag_minutes`, `max_source_is_hfm`, `hfm_hr_divergence_f`, `dsm_update_received` |
| Micro-Momentum | 6 | `velocity_15m_f`, `velocity_30m_f`, `velocity_60m_f`, `acceleration_15m_f`, `temp_volatility_30m_f`, `temp_volatility_60m_f` |
| Meteorological | 8 | `dew_point_f`, `dew_point_depression_f`, `wind_speed_mph`, `wind_dir_sin`, `wind_dir_cos`, `altimeter_inhg`, `visibility_miles`, `pressure_tendency_1h_inhg` |
| NBM Forecast | 6 | `nbm_max_remaining_f`, `nbm_expected_delta_f`, `nbm_current_error_f`, `nbm_hours_of_forecast_remaining`, `nbm_cycle_age_minutes`, `nbm_heating_trend_f` |
| RRFS Forecast | 6 | `rrfs_max_remaining_f`, `rrfs_expected_delta_f`, `rrfs_current_error_f`, `rrfs_cycle_age_minutes`, `rrfs_nbm_delta_divergence_f`, `nbm_rrfs_max_spread_f` |
| Advection Gap | 2 | `advection_gap_trend_60m_f`, `nwp_consensus_advection_f` |
| Solar Geometry | 3 | `solar_elevation_deg`, `solar_azimuth_deg`, `hours_until_solar_noon` |
| Calendar | 2 | `day_of_year`, `month` |
| **Total Features** | **41** | Core model features (additional raw fields are also exported for inspection) |

---

## 6. Latency & Future-Seeing Prevention

This is the **most critical guardrail** in backtesting. Every feature must reflect only information that would have been available at observation time $t$ in production.

### 6.1 NWP Latency Model

| NWP Model | P95 Latency (Fallback) | Cycles per Day | Availability Rule |
|-----------|------------------------|----------------|-------------------|
| **NBM** | 4852s (≈ 80.9 min) | 24 (hourly) | `notification_ts_utc ≤ t` (or fallback to `cycle + 4852s`) |
| **RRFS** | 7291s (≈ 121.5 min) | 24 (hourly) | `notification_ts_utc ≤ t` (or fallback to `cycle + 7291s`) |

### 6.2 Decay Weight Approach (Use Most Recent Available, Do Not Drop)

The previous design dropped any observation where the "expected" NWP cycle was missing. **This is replaced with a decay-weight approach** that is both more robust and more data-efficient:

For each observation at time $t$:

1. **Determine the availability timestamp for each cycle**:
   - If `notification_ts_utc` is present in the data, use it as the definitive arrival time.
   - If missing (e.g., older backfill formats), use the conservative fallback: `t_arrival = cycle_utc + P95_latency`.

2. **Find the most recent available cycle** at observation time $t$:
   - NBM: `latest_nbm_cycle = max { c ∈ data | t_arrival(c) ≤ t }`
   - RRFS: `latest_rrfs_cycle = max { c ∈ data | c ≤ t_safe }`

3. **Pass `cycle_age_minutes` as a feature**:
   - `nbm_cycle_age_minutes = (t - latest_nbm_cycle).total_seconds() / 60`
   - `rrfs_cycle_age_minutes = (t - latest_rrfs_cycle).total_seconds() / 60`

4. **If no cycle at all exists** for a given model (e.g., RRFS data is completely missing for this date), set **all features for that model to `NaN`**. XGBoost handles `NaN` natively by learning optimal default split directions.

**Why this is better than strict-drop**:
- **No data waste**: A missing 16Z NBM cycle doesn't throw away the observation — we use the 15Z cycle (only slightly older) and XGBoost learns to weight it appropriately via the `cycle_age_minutes` feature.
- **Matches production reality**: In production, a flaky NWP pipeline that misses one cycle shouldn't cause the system to stop trading — it should gracefully degrade to the next-best available data.
- **Self-calibrating**: XGBoost trees can learn splits like "if `nbm_cycle_age_minutes > 200`, trust the NWP less" without any hardcoded rules.

### 6.3 Worked Example

Observation at **18:00 UTC** for KMDW:
- NBM: `t_safe = 18:00 - 80.9min = 16:39 UTC`. The 16Z cycle satisfies `16:00 ≤ 16:39` ✓. If 16Z is present → `nbm_cycle_age_minutes = 120 min`. If 16Z is **missing** but 15Z exists → use 15Z, `nbm_cycle_age_minutes = 180 min`. The row is **not dropped**.
- RRFS: `t_safe = 18:00 - 121.5min = 15:58 UTC`. The 15Z cycle satisfies `15:00 ≤ 15:58` ✓ → `rrfs_cycle_age_minutes = 180 min`.
- If absolutely no RRFS data exists for this date → all RRFS features = `NaN`. The row is **still included** in training.

### 6.4 Wethr Push Latency

Wethr push observations have near-zero latency (seconds). No latency adjustment needed — the `observation_time_utc` is the feature timestamp, and the data is available at `received_ts` (seconds later).

### 6.5 Solar Geometry Latency

Solar position features (`solar_elevation_deg`, `solar_azimuth_deg`, etc.) are computed from station coordinates and UTC time using deterministic astronomical equations. **No external data or latency involved** — these are available with zero delay at any point in time.

### 6.6 Handling Backfill vs. Live Data

The pipeline **prefers ground-truth timestamps**. 
- **Live Data**: Uses the actual `notification_ts_utc` recorded when the model cycle was first received via AWS SNS. This perfectly captures real delays, outages, and regional network latency.
- **Backfill Data**: For files where `notification_ts_utc` is missing or NaN, the pipeline automatically falls back to adding the conservative P95 constants (`cycle + 4852s` for NBM).

This hybrid approach ensures that we use the most accurate timing info available while maintaining a robust safety floor for older historical data.

---

## 7. Row Validity Rules & Logging

### 7.1 Validity Checks (executed in order)

With the decay-weight approach from §6, NWP cycle availability is **no longer a drop reason**. Missing NWP data results in `NaN` features (handled natively by XGBoost), not dropped rows.

| Check # | Condition | Drop Reason Code | Priority |
|---------|-----------|-------------------|----------|
| 1 | `product != "ASOS-HR"` | `NOT_METAR` | Pre-filter (not logged) |
| 2 | CLI label missing for this LST climate day | `NO_CLI_LABEL` | Critical — drop |
| 3 | `custom_intraday_max_f` is NaN (no prior observations in climate day) | `NO_PRIOR_OBS` | Critical — drop |
| 4 | Core observation features are NaN (`distance_from_max_f`, `dew_point_f`) | `OBS_FEATURE_NAN:{col}` | Critical — drop |
| 5 | `Y_t < 0` (target is negative) | `NEGATIVE_TARGET` | Warning — clip to 0, do NOT drop |
| 6 | No NBM cycle available at all for the climate day | `NBM_ALL_MISSING` | Warning — logged, row kept with NaN features |
| 7 | No RRFS cycle available at all for the climate day | `RRFS_ALL_MISSING` | Warning — logged, row kept with NaN features |

> **Key design principle**: Only observation-level problems (missing label, missing temperature, no prior obs) cause row drops. NWP missingness is handled gracefully via `NaN` features + decay weight.

### 7.2 Structured Drop & Warning Log

Every dropped row and every warning produces a log entry (saved to parquet alongside the training data):

| Column | Type | Description |
|--------|------|-------------|
| `station` | string | ICAO code |
| `observation_time_utc` | datetime[UTC] | The observation in question |
| `climate_date_lst` | string | LST climate day (YYYY-MM-DD) |
| `log_level` | string | `DROP` or `WARNING` |
| `reason_code` | string | From the table above |
| `detail` | string | Additional context (e.g. which feature was NaN, cycle age used, etc.) |
| `timestamp` | datetime[UTC] | When the log entry was created |

**Log storage**: `data/model/training_logs/{ICAO}_drop_log.parquet`

### 7.3 Data Quality Summary Report

After building the training set, generate a summary report:
- Total climate days attempted
- Climate days with CLI labels
- Total ASOS-HR observations considered
- Rows dropped per reason code (counts and percentages)
- Rows with NWP warnings (NBM/RRFS all-missing) — counts
- Average `nbm_cycle_age_minutes` and `rrfs_cycle_age_minutes` across training set
- Final training set size
- Date coverage gaps (missing days)

---

## 8. Inference Pipeline — From Features to Trades

The full inference pipeline has **6 stages**. Stages 1–5 are the Weather Brain; Stage 6 is the Execution Brain.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        WEATHER BRAIN                               │
│                                                                     │
│  ┌──────────┐   ┌──────────────┐   ┌──────────────┐   ┌──────────┐ │
│  │ 1. Feature│──▶│ 2. Quantile  │──▶│ 3. Monotonic │──▶│ 4. Strike│ │
│  │   Engine  │   │    Suite     │   │    Mapper    │   │  Pricing │ │
│  └──────────┘   │ (7×XGBoost)  │   │              │   └────┬─────┘ │
│                  └──────────────┘   └──────────────┘        │       │
│                                                              ▼       │
│                                                    ┌──────────────┐ │
│                                                    │ 5. Isotonic  │ │
│                                                    │  Calibrator  │ │
│                                                    └──────┬───────┘ │
└───────────────────────────────────────────────────────────┼─────────┘
                                                            ▼
                                                   ┌──────────────┐
                                                   │ 6. Execution │
                                                   │    Brain     │
                                                   └──────────────┘
```

| Stage | Component | Input | Output |
|-------|-----------|-------|--------|
| 1 | **Feature Engine** | Raw observations + NWP parquet | 41-dimensional feature vector $\mathbf{x}_t$ |
| 2 | **Quantile Suite** | Feature vector $\mathbf{x}_t$ | 7 raw quantile predictions: $\hat{q}_{0.05}, ..., \hat{q}_{0.95}$ |
| 3 | **Monotonic Mapper** | Raw quantile predictions | Monotonically sorted quantiles (ensures $\hat{q}_{0.90} \geq \hat{q}_{0.75} \geq ...$) |
| 4 | **Strike Pricing** | Sorted quantiles + active Kalshi strikes | Raw probability $P_{\text{raw}}(\text{CLI\_High} \geq S)$ per strike |
| 5 | **Isotonic Calibrator** | Raw probabilities | Calibrated probability $P_{\text{cal}}$ minimizing Log Loss |
| 6 | **Execution Brain** | Calibrated probabilities + live bid/ask | Trade signals: side, size, limit price |

### 8.1 Stage 1 — Feature Engine

See [§5 Feature Engineering](#5-feature-engineering). Produces the 41-dimensional feature vector described above.

### 8.2 Stage 2 — Quantile Suite (7 × XGBoost)

Train **one XGBoost model per quantile** (XGBoost's `reg:quantileerror` accepts a single `quantile_alpha` per model). This produces a family of models whose outputs approximate a full CDF of the remaining heating delta.

**Target quantiles**:

$$\alpha \in \{0.05,\ 0.10,\ 0.25,\ 0.50,\ 0.75,\ 0.90,\ 0.95\}$$

This gives 7 models, each predicting a different percentile of $Y_t$.

**Hyperparameters (Baseline Starting Point)**:

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| `objective` | `reg:quantileerror` | Quantile regression |
| `quantile_alpha` | Varies per model (0.05–0.95) | One model per quantile |
| `n_estimators` | 500 | Reasonable starting point; tuned via early stopping |
| `max_depth` | 6 | Prevent overfitting on small datasets |
| `learning_rate` | 0.05 | Lower rate with more trees = better generalization |
| `subsample` | 0.8 | Row subsampling for regularization |
| `colsample_bytree` | 0.8 | Column subsampling |
| `min_child_weight` | 10 | Conservative to prevent fitting noise |
| `early_stopping_rounds` | 50 | Stop when validation pinball loss plateaus |
| `tree_method` | `hist` | Fast histogram-based method |

### 8.3 Stage 3 — Monotonic Mapper

Quantile regression models are trained independently, so their raw outputs can occasionally violate monotonicity (e.g. $\hat{q}_{0.75} > \hat{q}_{0.90}$). The Monotonic Mapper enforces the physical constraint that higher quantiles must predict higher (or equal) remaining deltas:

$$\hat{q}_{0.05} \leq \hat{q}_{0.10} \leq \hat{q}_{0.25} \leq \hat{q}_{0.50} \leq \hat{q}_{0.75} \leq \hat{q}_{0.90} \leq \hat{q}_{0.95}$$

**Algorithm**: Isotonic regression (pool adjacent violators) applied to the 7 quantile predictions. This is the simplest and most principled approach:
1. If a quantile pair violates monotonicity, replace both with their weighted average.
2. Repeat until the entire sequence is non-decreasing.

**Also enforce**: All quantiles are clipped to $\geq 0$ (remaining delta cannot be negative — you can't "un-warm" the peak).

### 8.4 Stage 4 — Strike Pricing

For each active Kalshi strike at temperature $S$:

1. Compute `required_delta = S - custom_intraday_max_f`
2. Interpolate through the monotonic quantile predictions to find:
   $$P_{\text{raw}}(Y_t \geq \text{required\_delta})$$

**Interpolation method**: Piecewise linear interpolation across the 7 quantile levels. The quantile predictions define points on the inverse CDF: $(\alpha_i, \hat{q}_i)$. For a given `required_delta` $d$:
- Find the two adjacent quantile predictions $\hat{q}_j \leq d \leq \hat{q}_{j+1}$
- Interpolate: $\alpha_d = \alpha_j + (\alpha_{j+1} - \alpha_j) \cdot \frac{d - \hat{q}_j}{\hat{q}_{j+1} - \hat{q}_j}$
- Probability of exceeding the strike: $P_{\text{raw}} = 1 - \alpha_d$

**Edge cases**:
- If $d \leq \hat{q}_{0.05}$: $P_{\text{raw}} = 0.95$ (clamp — at least 95% chance of exceeding)
- If $d \geq \hat{q}_{0.95}$: $P_{\text{raw}} = 0.05$ (clamp — at most 5% chance)
- If `required_delta ≤ 0`: The strike is already exceeded, $P_{\text{raw}} = 1.0$

### 8.5 Stage 5 — Isotonic Calibrator

Raw quantile-interpolated probabilities may not be perfectly calibrated (i.e. when the model says "60% chance," the true frequency may be 55% or 65%). The **Isotonic Calibrator** adjusts the raw probabilities to minimize Log Loss on a held-out calibration set.

**Training the Calibrator**:
1. During walk-forward CV, the calibrator is fit on the **validation fold** (the most recent completed test fold), NOT on the training data.
2. Input: pairs of $(P_{\text{raw}}, y_{\text{outcome}})$ where $y_{\text{outcome}} \in \{0, 1\}$ indicates whether the strike was actually exceeded.
3. Fit `sklearn.isotonic.IsotonicRegression(y_min=0.01, y_max=0.99, out_of_bounds='clip')` mapping $P_{\text{raw}} \to P_{\text{cal}}$.

**Why Isotonic Regression**:
- It is monotonic by construction: if raw model says event A is more likely than B, the calibrated output preserves that ordering.
- It is non-parametric: no assumption about the shape of the calibration curve.
- It directly minimizes the Brier Score / closely related to minimizing Log Loss.
- It handles the typical case where quantile regression is slightly overconfident in the tails.

**Output**: For each strike, a calibrated probability $P_{\text{cal}}(\text{CLI\_High} \geq S)$ that is "fair" — meaning the model's stated probability closely matches the observed frequency.

### 8.6 Stage 6 — Execution Brain

The Execution Brain maps calibrated probabilities to trading decisions. This is **not part of the Weather Brain** but is coupled during backtesting for evaluation.

For each active Kalshi strike contract with calibrated probability $P_{\text{cal}}$:

1. **Fetch** live market prices: `yes_ask`, `yes_bid`, `no_ask`, `no_bid`
2. **Compute Expected Value**:
   $$EV_{\text{YES}} = (P_{\text{cal}} \times 100) - \text{yes\_ask}$$
   $$EV_{\text{NO}} = ((1 - P_{\text{cal}}) \times 100) - \text{no\_ask}$$
3. **Trade Decision**: If $EV > \text{Threshold}$ (e.g. ≥ 3¢), place the trade.
4. **Position Sizing**: Fixed-size (1 contract) for initial backtesting; Kelly criterion as a future optimization.

**Threshold tuning**: The EV threshold is a hyperparameter tuned during walk-forward CV to maximize risk-adjusted P&L (Sharpe ratio), not raw P&L. A higher threshold means fewer but higher-conviction trades.

---

## 9. Walk-Forward Cross-Validation

### 9.1 Procedure

Walk-forward CV simulates real-time deployment: the model only ever trains on past data and predicts on future data.

```
Day 1 ──────────────────────── Day N ──── Day N+k
│◄──── Training Window ────────►│◄── Test ──►│
│                                │            │
│   Expanding or rolling         │  Predict   │
│   window of historical data    │  & evaluate│
└────────────────────────────────┘            │
                                              │
        Then shift forward by k days ───────►
```

| Parameter | Value | Notes |
|-----------|-------|-------|
| **Minimum training size** | 30 climate days | Enough to cover ~600–900 training rows |
| **Test fold size** | 7 climate days | Approximately 1 week |
| **Step size** | 7 climate days | Non-overlapping test folds |
| **Training window** | Expanding | Uses all available historical data up to the fold boundary |
| **Refit frequency** | Every fold | Retrain the 7 quantile models from scratch |

### 9.2 Walk-Forward Fold Example

Assume 90 days of data (Day 1–90):

| Fold | Train | Test | Description |
|------|-------|------|-------------|
| 1 | Day 1–30 | Day 31–37 | Minimum training, first predictions |
| 2 | Day 1–37 | Day 38–44 | Expanding training window |
| 3 | Day 1–44 | Day 45–51 | … |
| … | … | … | … |
| 9 | Day 1–86 | Day 87–90 | Last fold (may be partial) |

### 9.3 Preventing Leakage

- **Hard date boundary**: No observation from the test period's LST climate days may appear in training.
- **NWP cycles straddling the boundary**: A cycle issued at 23Z on the last training day may have forecast target times in the test period — this is fine because in production, that cycle *would* have been available.
- **CLI labels**: The label for the last training day's climate day may only be available the following morning. This is NOT leakage — in production, we train on completed days where the CLI has been published.

---

## 10. Evaluation Metrics

### 10.1 Statistical Metrics (per quantile model)

| Metric | Description | Purpose |
|--------|-------------|---------|
| **Pinball Loss** (per α) | $L_\alpha(y, \hat{q}) = \max[\alpha(y - \hat{q}),\ (1-\alpha)(\hat{q} - y)]$ | Primary loss function; lower = better |
| **Calibration Coverage** | % of test observations where $y \leq \hat{q}_\alpha$ | Should match $\alpha$ (e.g. q90 should cover 90% of test obs) |
| **CRPS** (Continuous Ranked Probability Score) | Integral of squared difference between predicted and empirical CDF | Overall distributional accuracy |
| **Median Absolute Error** | $|y - \hat{q}_{0.50}|$ | Interpretable point-forecast accuracy |

### 10.2 Trading Profit (Primary Business Metric)

The ultimate evaluation metric is **simulated trading P&L** on the test folds. This is computed by the Execution Brain logic using the Weather Brain's predictions:

1. For each `ASOS-HR` observation in the test fold:
   a. Generate quantile predictions → CDF of remaining delta
   b. For each active Kalshi strike contract, compute `P(CLI_High ≥ strike)`
   c. Look up the market snapshot closest in time to the observation → get `yes_ask`, `yes_bid`
   d. Compute `EV_YES = (P × 100) - yes_ask` and `EV_NO = ((1-P) × 100) - no_ask`
   e. Simulate placing a trade if EV exceeds a threshold (e.g. ≥ 3¢)
2. Settle all contracts against the actual CLI high
3. Sum up P&L across all trades in the test fold

| Trading Metric | Description |
|----------------|-------------|
| **Total P&L** | Net profit/loss in cents across all simulated trades |
| **Win Rate** | % of trades that settled profitably |
| **Sharpe Ratio** | Annualized risk-adjusted return (daily P&L series) |
| **Max Drawdown** | Worst peak-to-trough decline in cumulative P&L |
| **Trades per Day** | Average number of contracts traded per climate day |
| **P&L per Trade** | Average profit per trade (edge after fees) |

### 10.3 Metric Hierarchy

1. **Primary**: Simulated Trading P&L (this is what we're optimizing for)
2. **Secondary**: CRPS and Calibration Coverage (ensures the model is well-calibrated, which is a prerequisite for profitable trading)
3. **Diagnostic**: Pinball loss per quantile, MAE, feature importance (for debugging and model development)

---

## 11. From Historical Testing to Production

The system is designed so the transition from historical backtesting to live production requires **zero changes to the feature engineering or model logic**. The only differences:

| Aspect | Historical | Production |
|--------|-----------|------------|
| **Observation source** | Loaded from parquet files | Received from Wethr Push SSE stream |
| **NWP source** | Loaded from backfill parquet | Received via SNS → SQS → parquet pipeline |
| **NWP latency** | Fixed P95 constants (4852s NBM, 7291s RRFS) | Actual `notification_ts_utc` — or same fixed constants for consistency |
| **CLI label** | Available retroactively for Y construction | Not available until next morning — model predicts without label |
| **Execution** | Simulated against historical market snapshots | Live orders via Kalshi API |
| **Model retraining** | Walk-forward from start of history | Periodic retrain (e.g. weekly) with all available data |

### Production Data Flow

```
Wethr Push SSE ──► Feature   ──► Quantile ──► Monotonic ──► Strike  ──► Isotonic ──► Execution
                   Engine        Suite        Mapper        Pricing     Calibrator    Brain
                     ▲           (7×XGB)                                                │
                     │                                                                  ▼
NWP (NBM + RRFS) ───┘                                                           Kalshi API
via SNS/SQS                                                                   (place orders)
```

### Model Serialization

- Save trained XGBoost models as `.json` (XGBoost native format) for reproducibility.
- Save the feature engineering configuration (column names, P95 latency constants, quantile alphas) as a YAML sidecar.
- Storage: `data/model/trained/{ICAO}/` with versioned subdirectories.

---

## 12. Open Questions & Future Work

### RRFS Data Availability

RRFS data generation is paused (as of early 2026). The decay-weight approach (§6.2) handles this gracefully — missing RRFS cycles result in `NaN` features, and XGBoost learns to route those observations through splits that don't depend on RRFS. After initial training, evaluate RRFS feature importance:
- If RRFS features have meaningful importance → keep them and prioritize RRFS backfill.
- If RRFS features have near-zero importance → drop them from the feature vector to reduce noise.

### Global Model

The initial model is **one model per station**. A future extension is a **global model** trained on all stations simultaneously, with `station` (or derived features like `latitude`, `longitude`, `elevation`, `distance_to_coast`) as additional features. Benefits:
- Larger training set (20× more data)
- Transfer learning across cities with similar climates
- Prerequisite: verify feature distributions are comparable across stations

### Additional Features (Future Iterations)

| Feature | Source | Why |
|---------|--------|-----|
| Cloud cover (GOES satellite) | External API | Preemptive detection of incoming shade |
| Solar radiation (W/m²) | ASOS or satellite | Distinguish solar-driven vs. advective heating |
| Precipitation flags | METAR weather codes | Evaporative cooling crashes surface temps |
| RTMA-RU analysis | NWP | Lowest-latency (~29 min) gridded surface analysis |
| NBM probabilistic quantiles (p10/p90) | NBM COG | Currently null in backfill; would add ensemble spread |
| Historical bias matrix | Static lookup | Station-specific NWP bias by month and wind direction |
| Neighbor station gradient | Wethr push for 2nd station | Detect approaching fronts (e.g. KORD–KMDW for Chicago) |

### Kalshi Fee Structure

Backtesting P&L accounts for Kalshi's formula-based fees (as of early 2026). We simulate **taker** fills (executing at the ask):

- **Taker fee**: $\text{round up}(0.07 \times C \times P \times (1 - P))$ where $C$ = contracts, $P$ = price in dollars
- Fee peaks at 50¢ (max uncertainty) and drops toward 0 at 0¢ or 100¢
- EV calculations and P&L settlement both subtract this fee

---

*Last updated: 2026-03-02. This document covers the full inference pipeline (Weather Brain stages 1–5 + Execution Brain stage 6).*