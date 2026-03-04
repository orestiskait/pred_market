"""Stage 1 — Feature Engine.

Transforms raw observations + NWP data into the ~36-dimensional feature
vector described in MODELING_IDEA.MD §5.

Feature groups (DRY — every feature defined exactly once here):
  §5.1  State of the Day         (6 features)
  §5.2  Micro-Momentum           (6 features)
  §5.3  Meteorological Context   (7 features)
  §5.4  NBM Forecast             (6 features)
  §5.5  RRFS Forecast            (5 features)
  §5.6  Advection Gap            (2 unique + 2 aliases from §5.4/5.5)
  §5.7  Solar Geometry           (4 features)
  §5.8  Calendar / Seasonal      (2 features)

Total: ~36 unique features (some conceptually shared between groups).

Latency model (§6):
  We implement the DECAY-WEIGHT approach, NOT strict-drop.
  Missing NWP cycles → NaN features, row is KEPT.
  cycle_age_minutes > CYCLE_AGE_CAP_MINUTES → treat as fully missing (NaN).
"""

from __future__ import annotations

import logging
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional

import numpy as np
import pandas as pd

from services.model.constants import (
    NBM_LATENCY_SECONDS,
    RRFS_LATENCY_SECONDS,
    CYCLE_AGE_CAP_MINUTES,
)
from services.model.time_utils import (
    climate_day_end_utc,
    hours_since_midnight_lst,
    series_to_lst_climate_date,
)

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Solar geometry helpers (no external API — pure astronomy)
# ──────────────────────────────────────────────────────────────────────

def _solar_position(dt_utc: datetime, lat: float, lon: float) -> dict[str, float]:
    """Compute solar elevation and azimuth using a simplified but accurate
    astronomical algorithm (Spencer / Iqbal equations).

    Returns elevation (degrees, negative when below horizon) and azimuth
    (degrees, 0=N 90=E 180=S 270=W), plus solar noon time (decimal UTC hour).

    This is a self-contained implementation so pvlib is not a hard dependency.
    Accuracy is within ~0.5° for latitudes < 65°, sufficient for our use case.
    """
    # Day-of-year (1-based)
    doy = dt_utc.timetuple().tm_yday
    B = math.radians((360 / 365) * (doy - 81))

    # Equation of time (minutes)
    eot = (9.87 * math.sin(2 * B)) - (7.53 * math.cos(B)) - (1.5 * math.sin(B))

    # Solar declination (degrees)
    declination_rad = math.radians(
        23.45 * math.sin(math.radians((360 / 365) * (doy - 81)))
    )

    # Local Solar Time (decimal hours)
    utc_decimal = dt_utc.hour + dt_utc.minute / 60.0 + dt_utc.second / 3600.0
    lstm = 15 * round(lon / 15)  # local standard time meridian (degrees)
    time_correction = eot + 4 * (lon - lstm)  # minutes
    lst_decimal = utc_decimal + lon / 15 + time_correction / 60.0

    # Hour angle (degrees): negative in morning, positive in afternoon
    hour_angle_deg = 15 * (lst_decimal - 12.0)
    hour_angle_rad = math.radians(hour_angle_deg)
    lat_rad = math.radians(lat)

    # Elevation angle
    sin_elev = (
        math.sin(lat_rad) * math.sin(declination_rad)
        + math.cos(lat_rad) * math.cos(declination_rad) * math.cos(hour_angle_rad)
    )
    elevation_rad = math.asin(max(-1.0, min(1.0, sin_elev)))
    elevation_deg = math.degrees(elevation_rad)

    # Azimuth (N=0, E=90, S=180, W=270)
    cos_az = (
        math.sin(declination_rad) * math.cos(lat_rad)
        - math.cos(declination_rad) * math.sin(lat_rad) * math.cos(hour_angle_rad)
    ) / max(1e-9, math.cos(elevation_rad))
    cos_az = max(-1.0, min(1.0, cos_az))
    azimuth_deg = math.degrees(math.acos(cos_az))
    if hour_angle_deg > 0:  # past noon → sun in west half
        azimuth_deg = 360 - azimuth_deg

    # Solar noon (decimal UTC hour when hour_angle = 0)
    solar_noon_utc_hour = 12.0 - lon / 15.0 - time_correction / 60.0
    hours_until_noon = solar_noon_utc_hour - utc_decimal

    return {
        "solar_elevation_deg": elevation_deg,
        "solar_azimuth_deg": azimuth_deg,
        "hours_until_solar_noon": hours_until_noon,
    }





# ──────────────────────────────────────────────────────────────────────
# Wind direction helpers
# ──────────────────────────────────────────────────────────────────────

def _wind_dir_sincos(wind_direction: object) -> tuple[float, float]:
    """Return (sin, cos) of wind direction in degrees.

    Returns (0, 0) for VRB (variable) or non-numeric values.
    """
    if wind_direction is None or (isinstance(wind_direction, float) and math.isnan(wind_direction)):
        return (float("nan"), float("nan"))
    s = str(wind_direction).strip().upper()
    if s in ("", "VRB", "VAR", "CALM"):
        return (0.0, 0.0)
    try:
        deg = float(s)
        rad = math.radians(deg)
        return (math.sin(rad), math.cos(rad))
    except ValueError:
        return (float("nan"), float("nan"))


# ──────────────────────────────────────────────────────────────────────
# NWP cycle selection helpers
# ──────────────────────────────────────────────────────────────────────

def _find_latest_safe_cycle(
    nwp_df: pd.DataFrame,
    observation_time: datetime,
    fallback_latency_seconds: int,
) -> Optional[pd.Timestamp]:
    """Return the most recent model_run_time_utc that satisfies the latency constraint.

    If 'notification_ts_utc' exists in the df, use it directly (ground truth
    of when the data arrived). If missing/NaN, fall back to adding back the
    P95 latency to the model run time.
    """
    if nwp_df.empty:
        return None

    obs_ts = pd.Timestamp(observation_time)
    
    if "notification_ts_utc" in nwp_df.columns:
        # Use exact Parquet timestamps when available
        valid_rows = nwp_df[nwp_df["notification_ts_utc"] <= obs_ts]
        # For legacy rows backfilled without notification_ts_utc, use the fallback
        missing_ts = nwp_df["notification_ts_utc"].isna()
        fallback_rows = nwp_df[
            missing_ts & ((nwp_df["model_run_time_utc"] + pd.Timedelta(seconds=fallback_latency_seconds)) <= obs_ts)
        ]
        qualifying = pd.concat([valid_rows, fallback_rows])["model_run_time_utc"]
    else:
        # Pure fallback
        safe_horizon = obs_ts - pd.Timedelta(seconds=fallback_latency_seconds)
        cycles = nwp_df["model_run_time_utc"]
        qualifying = cycles[cycles <= safe_horizon]

    if qualifying.empty:
        return None

    return qualifying.max()


def _cycle_age_minutes(
    observation_time: datetime,
    cycle_time: Optional[pd.Timestamp],
    cap: int = CYCLE_AGE_CAP_MINUTES,
) -> Optional[float]:
    """Compute cycle_age_minutes, capped at CYCLE_AGE_CAP_MINUTES.

    Returns None (→ NaN features) if cycle_time is None OR if the raw age
    exceeds the cap (indicating a data gap too wide to be informative).
    """
    if cycle_time is None:
        return None
    obs_ts = pd.Timestamp(observation_time)
    age_minutes = (obs_ts - cycle_time).total_seconds() / 60.0
    if age_minutes > cap:
        return None  # Treat as fully missing — past informative range
    return age_minutes


# ──────────────────────────────────────────────────────────────────────
# NWP feature extraction
# ──────────────────────────────────────────────────────────────────────

def _extract_nwp_features(
    nwp_df: pd.DataFrame,
    prefix: str,
    observation_time: datetime,
    custom_intraday_max_f: float,
    current_temp_f: float,
    day_end_utc: datetime,
    latency_seconds: int,
    extra_features: bool = False,
) -> dict[str, float]:
    """Extract NWP forecast features for a single observation.

    Implements §5.4 (NBM) and §5.5 (RRFS) logic.
    Returns a dict of feature_name → value (NaN when data unavailable).

    Parameters
    ----------
    nwp_df:               Full NWP DataFrame for the relevant period.
    prefix:               ``"nbm"`` or ``"rrfs"``.
    observation_time:     UTC datetime of the ASOS-HR observation.
    custom_intraday_max_f: Running max temp at observation time.
    current_temp_f:       Current observation temperature.
    day_end_utc:          UTC end of the LST climate day.
    latency_seconds:      NWP-model-specific P95 latency.
    extra_features:       If True, also compute NBM-specific features
                          (nbm_heating_trend_f, nbm_hours_of_forecast_remaining).
    """
    nan = float("nan")
    result: dict[str, float] = {}

    # Identify best available cycle (preferring exact notification_ts_utc)
    cycle_time = _find_latest_safe_cycle(nwp_df, observation_time, latency_seconds)
    age = _cycle_age_minutes(observation_time, cycle_time)

    if age is None:
        # Fully missing — set all NWP features to NaN
        result[f"{prefix}_max_remaining_f"]  = nan
        result[f"{prefix}_expected_delta_f"] = nan
        result[f"{prefix}_current_error_f"]  = nan
        result[f"{prefix}_cycle_age_minutes"] = nan
        if extra_features:
            result[f"{prefix}_heating_trend_f"]           = nan
            result[f"{prefix}_hours_of_forecast_remaining"] = nan
        return result

    result[f"{prefix}_cycle_age_minutes"] = age

    # Filter to selected cycle's rows
    cycle_rows = nwp_df[nwp_df["model_run_time_utc"] == cycle_time].copy()

    # Remaining: forecast valid AFTER observation_time AND before climate day end
    obs_ts = pd.Timestamp(observation_time)
    end_ts = pd.Timestamp(day_end_utc)

    remaining = cycle_rows[
        (cycle_rows["forecast_target_time_utc"] > obs_ts)
        & (cycle_rows["forecast_target_time_utc"] <= end_ts)
        & cycle_rows["tmp_2m_f"].notna()
    ]

    if remaining.empty:
        result[f"{prefix}_max_remaining_f"]  = nan
        result[f"{prefix}_expected_delta_f"] = nan
        result[f"{prefix}_current_error_f"]  = nan
        if extra_features:
            result[f"{prefix}_heating_trend_f"]           = nan
            result[f"{prefix}_hours_of_forecast_remaining"] = nan
        return result

    max_remaining = float(remaining["tmp_2m_f"].max())
    result[f"{prefix}_max_remaining_f"]  = max_remaining
    result[f"{prefix}_expected_delta_f"] = max_remaining - custom_intraday_max_f

    # Forecast valid closest to observation_time (for current-error / advection)
    all_cycle = cycle_rows[cycle_rows["tmp_2m_f"].notna()].copy()
    all_cycle["_time_diff"] = (
        all_cycle["forecast_target_time_utc"] - obs_ts
    ).abs()
    nearest = all_cycle.sort_values("_time_diff").iloc[0]
    nwp_at_t = float(nearest["tmp_2m_f"])
    result[f"{prefix}_current_error_f"] = current_temp_f - nwp_at_t

    if extra_features:
        result[f"{prefix}_hours_of_forecast_remaining"] = float(len(remaining))
        result[f"{prefix}_heating_trend_f"] = max_remaining - nwp_at_t

    return result


# ──────────────────────────────────────────────────────────────────────
# 5-minute resampled timeline helpers (§5.2 micro-momentum)
# ──────────────────────────────────────────────────────────────────────

# How much prior-day tail to include in the 5-min grid so that momentum
# features (max look-back = 60 min) are never starved at day boundaries.
_MOMENTUM_LOOKBACK_MINUTES: int = 90


def _build_5min_grid(
    obs_df: pd.DataFrame,
    climate_date: date,
    prev_tail_df: Optional[pd.DataFrame] = None,
) -> pd.DataFrame:
    """Build a 5-minute resampled temperature timeline for one climate day.

    Input: observations DataFrame (both ASOS-HFM and ASOS-HR), pre-filtered
           to the current climate_date.
    prev_tail_df: optional slice of the *previous* climate day's observations
                  to prepend as a warm-up tail (enables momentum features for
                  the first hour of the new day).

    Returns a DataFrame indexed by 5-minute UTC timestamps with the column
    ``temperature_fahrenheit`` forward-filled (last observed value carries).
    """
    day_obs = obs_df[obs_df["_climate_date"] == climate_date].copy()
    if prev_tail_df is not None and not prev_tail_df.empty:
        day_obs = pd.concat([prev_tail_df, day_obs], ignore_index=True)

    if day_obs.empty:
        return pd.DataFrame(columns=["temperature_fahrenheit"])

    day_obs = day_obs.sort_values("observation_time_utc").set_index("observation_time_utc")
    # Resample at 5-minute frequency — take mean within each bucket, ffill gaps
    grid = (
        day_obs["temperature_fahrenheit"]
        .resample("5min")
        .mean()
        .ffill()
        .rename("temperature_fahrenheit")
    )
    return grid.to_frame()


def _momentum_features(
    grid_5min: pd.DataFrame,
    obs_time: pd.Timestamp,
) -> dict[str, float]:
    """Extract velocity and volatility features from the 5-minute grid.

    Implements §5.2.  All features are relative to the observation timestamp.
    Returns NaN for any window that cannot be filled.
    """
    nan = float("nan")

    def temp_at(offset_minutes: int) -> float:
        t = obs_time - pd.Timedelta(minutes=offset_minutes)
        try:
            # Use .asof() to get the last known value at or before t
            val = grid_5min["temperature_fahrenheit"].asof(t)
            return float(val) if pd.notna(val) else nan
        except Exception:
            return nan

    t0   = temp_at(0)
    t15  = temp_at(15)
    t30  = temp_at(30)
    t60  = temp_at(60)
    t15b = temp_at(30)  # 15 min before t15 → for acceleration

    vel_15 = t0 - t15  if not any(math.isnan(x) for x in [t0, t15])  else nan
    vel_30 = t0 - t30  if not any(math.isnan(x) for x in [t0, t30])  else nan
    vel_60 = t0 - t60  if not any(math.isnan(x) for x in [t0, t60])  else nan

    # Acceleration: change in 15-min velocity
    vel_15_prev = t15 - t15b if not any(math.isnan(x) for x in [t15, t15b]) else nan
    accel_15    = vel_15 - vel_15_prev if not any(math.isnan(x) for x in [vel_15, vel_15_prev]) else nan

    # Rolling std over last 30 and 60 min
    end_slice   = obs_time
    start_30    = obs_time - pd.Timedelta(minutes=30)
    start_60    = obs_time - pd.Timedelta(minutes=60)
    slice_30    = grid_5min.loc[start_30:end_slice, "temperature_fahrenheit"].dropna()
    slice_60    = grid_5min.loc[start_60:end_slice, "temperature_fahrenheit"].dropna()

    vol_30 = float(slice_30.std()) if len(slice_30) >= 2 else nan
    vol_60 = float(slice_60.std()) if len(slice_60) >= 2 else nan

    return {
        "velocity_15m_f":       vel_15,
        "velocity_30m_f":       vel_30,
        "velocity_60m_f":       vel_60,
        "acceleration_15m_f":   accel_15,
        "temp_volatility_30m_f": vol_30,
        "temp_volatility_60m_f": vol_60,
    }


# ──────────────────────────────────────────────────────────────────────
# Feature row builder (single ASOS-HR observation)
# ──────────────────────────────────────────────────────────────────────


def _build_feature_row_for_hr_obs(
    row: Any,
    climate_date: date,
    day_end: pd.Timestamp,
    grid_5min: pd.DataFrame,
    nbm_df: pd.DataFrame,
    rrfs_df: pd.DataFrame,
    custom_intraday_max_f: float,
    running_max_time: pd.Timestamp,
    advection_history: list[tuple[pd.Timestamp, float]],
    lat: float,
    lon: float,
    tz: str,
    max_source_is_hfm: int = 0,
    hfm_hr_divergence_f: float = float("nan"),
    dsm_update_received: int = 0,
) -> tuple[dict[str, Any], list[tuple[pd.Timestamp, float]]]:
    """Build the feature dict for one ASOS-HR observation.

    Returns (feature_row_dict, updated_advection_history).
    """
    obs_time = row.observation_time_utc
    obs_dt = obs_time.to_pydatetime()
    temp_f = float(row.temperature_fahrenheit)

    # §5.1 State of the Day
    hours_since_midnight = hours_since_midnight_lst(obs_dt, tz)
    t_max_lag = (obs_time - running_max_time).total_seconds() / 60.0

    distance_from_max = custom_intraday_max_f - temp_f
    state = {
        "custom_intraday_max_f": custom_intraday_max_f,
        "distance_from_max_f": distance_from_max,
        "distance_from_max_ratio": distance_from_max / max(custom_intraday_max_f, 1.0),
        "hours_since_midnight_lst": hours_since_midnight,
        "t_max_lag_minutes": t_max_lag,
        "max_source_is_hfm": max_source_is_hfm,
        "hfm_hr_divergence_f": hfm_hr_divergence_f,
        "dsm_update_received": dsm_update_received,
    }

    # §5.2 Micro-Momentum
    momentum = _momentum_features(grid_5min, obs_time)

    # §5.3 Meteorological Context
    dew_f = getattr(row, "dew_point_fahrenheit", float("nan"))
    wind_s = float(getattr(row, "wind_speed_mph", 0) or 0)
    wind_g = float(getattr(row, "wind_gust_mph", 0) or 0)
    altimeter = getattr(row, "altimeter_inhg", float("nan"))
    wind_sin, wind_cos = _wind_dir_sincos(getattr(row, "wind_direction", None))

    # visibility_miles: parse from string (e.g. "10" or "1.5"); null/missing → NaN
    vis_raw = getattr(row, "visibility_miles", None)
    if vis_raw is None or (isinstance(vis_raw, float) and math.isnan(vis_raw)):
        visibility_mi = float("nan")
    else:
        try:
            visibility_mi = float(str(vis_raw).strip())
        except (ValueError, TypeError):
            visibility_mi = float("nan")

    met = {
        "dew_point_f": float(dew_f) if pd.notna(dew_f) else float("nan"),
        "dew_point_depression_f": (temp_f - float(dew_f)) if pd.notna(dew_f) else float("nan"),
        "wind_speed_mph": wind_s,
        "wind_gust_mph": wind_g,
        "wind_dir_sin": wind_sin,
        "wind_dir_cos": wind_cos,
        "altimeter_inhg": float(altimeter) if pd.notna(altimeter) else float("nan"),
        "visibility_miles": visibility_mi,
    }

    # §5.4 NBM features
    nbm_feats = _extract_nwp_features(
        nwp_df=nbm_df,
        prefix="nbm",
        observation_time=obs_dt,
        custom_intraday_max_f=custom_intraday_max_f,
        current_temp_f=temp_f,
        day_end_utc=day_end,
        latency_seconds=NBM_LATENCY_SECONDS,
        extra_features=True,
    )

    # §5.5 RRFS features
    rrfs_feats = _extract_nwp_features(
        nwp_df=rrfs_df,
        prefix="rrfs",
        observation_time=obs_dt,
        custom_intraday_max_f=custom_intraday_max_f,
        current_temp_f=temp_f,
        day_end_utc=day_end,
        latency_seconds=RRFS_LATENCY_SECONDS,
        extra_features=False,
    )

    rrfs_feats["rrfs_nbm_delta_divergence_f"] = (
        rrfs_feats.get("rrfs_expected_delta_f", float("nan"))
        - nbm_feats.get("nbm_expected_delta_f", float("nan"))
    )
    nbm_max = nbm_feats.get("nbm_max_remaining_f", float("nan"))
    rrfs_max = rrfs_feats.get("rrfs_max_remaining_f", float("nan"))
    rrfs_feats["nbm_rrfs_max_spread_f"] = (
        abs(nbm_max - rrfs_max)
        if pd.notna(nbm_max) and pd.notna(rrfs_max)
        else float("nan")
    )

    # §5.6 Advection Gap
    nbm_adv = nbm_feats.get("nbm_current_error_f", float("nan"))
    rrfs_adv = rrfs_feats.get("rrfs_current_error_f", float("nan"))

    cutoff_60 = obs_time - pd.Timedelta(minutes=60)
    recent_adv = [
        v for ts, v in advection_history
        if ts >= cutoff_60 and not math.isnan(v)
    ]
    adv_trend_60m = (
        nbm_adv - recent_adv[0]
        if recent_adv and not math.isnan(nbm_adv)
        else float("nan")
    )
    adv_values = [v for v in [nbm_adv, rrfs_adv] if not math.isnan(v)]
    consensus_adv = float(np.mean(adv_values)) if adv_values else float("nan")

    adv_feats = {
        "advection_gap_trend_60m_f": adv_trend_60m,
        "nwp_consensus_advection_f": consensus_adv,
    }

    # Update advection history for next iteration
    new_history = list(advection_history)
    if not math.isnan(nbm_adv):
        new_history.append((obs_time, nbm_adv))
    new_history = [
        (ts, v) for ts, v in new_history
        if ts >= obs_time - pd.Timedelta(hours=2)
    ]

    # §5.7 Solar Geometry
    solar = _solar_position(obs_dt, lat, lon)

    # §5.8 Calendar
    calendar = {
        "day_of_year": obs_dt.timetuple().tm_yday,
        "month": obs_dt.month,
    }

    feature_row = {
        "observation_time_utc": obs_time,
        "climate_date_lst": str(climate_date),
        **state,
        **momentum,
        **met,
        **nbm_feats,
        **rrfs_feats,
        **adv_feats,
        **solar,
        **calendar,
    }
    return (feature_row, new_history)


def _parse_altimeter(raw: object) -> float:
    """Parse altimeter_inhg to float; return NaN if invalid."""
    if raw is None or (isinstance(raw, float) and math.isnan(raw)):
        return float("nan")
    try:
        return float(str(raw).strip())
    except (ValueError, TypeError):
        return float("nan")


# ──────────────────────────────────────────────────────────────────────
# Tri-Factor Max (Smart Max) — guards against ASOS-HFM rounding bias
# ──────────────────────────────────────────────────────────────────────

def _compute_smart_max(
    hr_anchor_max_f: Optional[float],
    hfm_spike_max_c: Optional[float],
    dsm_floor_max_f: float,
) -> tuple[float, str, int, float, int]:
    """Synthesize custom_intraday_max_f from three sources.

    Protects against ASOS-HFM upward rounding bias (integer °C, 0.5 rounds up).
    Returns (custom_intraday_max_f, peak_source, max_source_is_hfm, hfm_hr_divergence_f, dsm_update_received).
    """
    nan = float("nan")
    hr_f = hr_anchor_max_f if hr_anchor_max_f is not None else nan
    dsm_valid = pd.notna(dsm_floor_max_f)

    # HFM lower bound: T°C implies true temp in [T-0.5, T+0.5); conservative bound
    if hfm_spike_max_c is not None and not math.isnan(hfm_spike_max_c):
        hfm_lower_bound_f = (float(hfm_spike_max_c) - 0.5) * 1.8 + 32.0
        hfm_raw_f = float(hfm_spike_max_c) * 1.8 + 32.0
    else:
        hfm_lower_bound_f = nan
        hfm_raw_f = nan

    hfm_hr_divergence_f = (hfm_raw_f - hr_f) if pd.notna(hfm_raw_f) and pd.notna(hr_f) else nan
    dsm_update_received = 1 if dsm_valid else 0

    # Priority 1: DSM floor (official NWS intraday update)
    if dsm_valid and dsm_floor_max_f > max(
        hr_f if pd.notna(hr_f) else -float("inf"),
        hfm_lower_bound_f if pd.notna(hfm_lower_bound_f) else -float("inf"),
    ):
        return (dsm_floor_max_f, "DSM", 0, hfm_hr_divergence_f, dsm_update_received)

    # Priority 2: HFM spike (inter-hour spike that HR missed)
    if pd.notna(hfm_lower_bound_f) and (pd.isna(hr_f) or hfm_lower_bound_f > hr_f):
        return (hfm_raw_f, "HFM_SPIKE", 1, hfm_hr_divergence_f, dsm_update_received)

    # Priority 3: HR anchor (precise 0.1°C reading)
    if pd.notna(hr_f):
        return (hr_f, "HR_ANCHOR", 0, hfm_hr_divergence_f, dsm_update_received)

    # Fallback: HFM raw if no HR yet (e.g. very early in day)
    if pd.notna(hfm_raw_f):
        return (hfm_raw_f, "HFM_SPIKE", 1, hfm_hr_divergence_f, dsm_update_received)

    # No valid source
    return (nan, "NONE", 0, nan, dsm_update_received)


def _dsm_floor_max_for_time(
    dsm_day: pd.DataFrame,
    obs_time: pd.Timestamp,
    climate_date_str: str,
) -> float:
    """Max high_f from DSM rows for this climate day with received_ts_utc <= obs_time."""
    if dsm_day.empty:
        return float("nan")
    if "received_ts_utc" not in dsm_day.columns or "high_f" not in dsm_day.columns:
        return float("nan")
    if "for_date_lst" not in dsm_day.columns:
        return float("nan")

    mask = (
        (dsm_day["for_date_lst"].astype(str) == climate_date_str)
        & (pd.to_datetime(dsm_day["received_ts_utc"], utc=True) <= obs_time)
    )
    eligible = dsm_day.loc[mask, "high_f"].dropna()
    if eligible.empty:
        return float("nan")
    return float(eligible.max())


def _process_climate_day(
    day_obs: pd.DataFrame,
    climate_date: date,
    obs_df: pd.DataFrame,
    nbm_df: pd.DataFrame,
    rrfs_df: pd.DataFrame,
    dsm_df: pd.DataFrame,
    prev_advection_history: list[tuple[pd.Timestamp, float]],
    prev_pressure_history: list[tuple[pd.Timestamp, float]],
    prev_day_tail_df: Optional[pd.DataFrame],
    lat: float,
    lon: float,
    tz: str,
) -> tuple[list[dict[str, Any]], list[tuple[pd.Timestamp, float]], list[tuple[pd.Timestamp, float]], Optional[pd.DataFrame]]:
    """Process one climate day's observations into feature rows.

    Uses Tri-Factor Max (Smart Max) to guard against ASOS-HFM rounding bias.
    Returns (rows, updated_advection_history, updated_pressure_history, prev_day_tail_df for next day).
    """
    day_end = climate_day_end_utc(climate_date, tz)
    grid_5min = _build_5min_grid(obs_df, climate_date, prev_tail_df=prev_day_tail_df)
    climate_date_str = str(climate_date)

    # Tri-Factor Max: track hr_anchor (ASOS-HR) and hfm_spike (ASOS-HFM) separately
    hr_anchor_max_f: Optional[float] = None
    hfm_spike_max_c: Optional[float] = None
    custom_intraday_max_f: Optional[float] = None
    running_max_time: Optional[pd.Timestamp] = None
    advection_history: list[tuple[pd.Timestamp, float]] = list(prev_advection_history)
    pressure_history: list[tuple[pd.Timestamp, float]] = list(prev_pressure_history)
    rows: list[dict[str, Any]] = []

    for row in day_obs.itertuples(index=False):
        obs_time = row.observation_time_utc
        temp_f = getattr(row, "temperature_fahrenheit", None)
        temp_c = getattr(row, "temperature_celsius", None)
        product_str = str(getattr(row, "product", "") or "").upper()

        # Update pressure history from all observations (ASOS-HFM + ASOS-HR)
        altimeter_raw = getattr(row, "altimeter_inhg", None)
        altimeter_val = _parse_altimeter(altimeter_raw)
        if not math.isnan(altimeter_val):
            pressure_history.append((obs_time, altimeter_val))
        pressure_history = [
            (ts, p) for ts, p in pressure_history
            if ts >= obs_time - pd.Timedelta(hours=2)
        ]

        # Pressure tendency: change over last ~60 min (falling = often warming)
        cutoff = obs_time - pd.Timedelta(minutes=55)
        candidates = [(ts, p) for ts, p in pressure_history if ts <= cutoff and not math.isnan(p)]
        pressure_60m_ago = max(candidates, key=lambda x: x[0])[1] if candidates else float("nan")
        pressure_tendency = (
            altimeter_val - pressure_60m_ago
            if not math.isnan(altimeter_val) and not math.isnan(pressure_60m_ago)
            else float("nan")
        )

        if pd.isna(temp_f):
            continue

        temp_f = float(temp_f)

        # Update Tri-Factor anchors by product type
        if product_str == "ASOS-HR":
            if hr_anchor_max_f is None or temp_f > hr_anchor_max_f:
                hr_anchor_max_f = temp_f
        elif product_str == "ASOS-HFM":
            # Use temperature_celsius when available; else derive from Fahrenheit
            if temp_c is not None and not (isinstance(temp_c, float) and math.isnan(temp_c)):
                tc = float(temp_c)
            else:
                tc = (temp_f - 32.0) / 1.8
            if hfm_spike_max_c is None or tc > hfm_spike_max_c:
                hfm_spike_max_c = tc

        # DSM floor: max(high_f) from DSMs received before obs_time
        dsm_floor_max_f = _dsm_floor_max_for_time(dsm_df, obs_time, climate_date_str)

        # Smart Max synthesis
        smart_max, _peak_source, max_source_is_hfm, hfm_hr_divergence_f, dsm_update_received = _compute_smart_max(
            hr_anchor_max_f, hfm_spike_max_c, dsm_floor_max_f
        )

        if pd.notna(smart_max):
            prev_custom = custom_intraday_max_f
            custom_intraday_max_f = smart_max
            if prev_custom is None or smart_max > prev_custom:
                running_max_time = obs_time

        # Only generate feature rows from ASOS-HR observations
        if product_str != "ASOS-HR":
            continue

        if custom_intraday_max_f is None or math.isnan(custom_intraday_max_f):
            continue

        feature_row, advection_history = _build_feature_row_for_hr_obs(
            row=row,
            climate_date=climate_date,
            day_end=day_end,
            grid_5min=grid_5min,
            nbm_df=nbm_df,
            rrfs_df=rrfs_df,
            custom_intraday_max_f=custom_intraday_max_f,
            running_max_time=running_max_time,
            advection_history=advection_history,
            lat=lat,
            lon=lon,
            tz=tz,
            max_source_is_hfm=max_source_is_hfm,
            hfm_hr_divergence_f=hfm_hr_divergence_f,
            dsm_update_received=dsm_update_received,
        )
        feature_row["pressure_tendency_1h_inhg"] = pressure_tendency
        rows.append(feature_row)

    # Build tail for next day
    if not day_obs.empty:
        day_obs_sorted = day_obs.sort_values("observation_time_utc")
        last_obs_time = day_obs_sorted["observation_time_utc"].iloc[-1]
        tail_cutoff = last_obs_time - pd.Timedelta(minutes=_MOMENTUM_LOOKBACK_MINUTES)
        next_tail_df = day_obs_sorted[
            day_obs_sorted["observation_time_utc"] >= tail_cutoff
        ].copy()
    else:
        next_tail_df = None

    return (rows, advection_history, pressure_history, next_tail_df)


# ──────────────────────────────────────────────────────────────────────
# FeatureEngine — main class
# ──────────────────────────────────────────────────────────────────────

class FeatureEngine:
    """Stage 1: Convert raw data → ~36-dimensional feature vector.

    One FeatureEngine instance is tied to one station.  The ``build()``
    method accepts pre-loaded DataFrames (output of ModelDataLoader) for
    a date range and returns a fully featurised DataFrame — one row per
    ASOS-HR observation within the training window.

    The caller (TrainingSetBuilder) is responsible for validity filtering.
    """

    #: Ordered list of all feature column names (used externally for
    #: column ordering, schema checks, etc.)
    FEATURE_COLUMNS: tuple[str, ...] = (
        # §5.1 State of the Day
        "custom_intraday_max_f",
        "distance_from_max_f",
        "distance_from_max_ratio",
        "hours_since_midnight_lst",
        "t_max_lag_minutes",
        "max_source_is_hfm",
        "hfm_hr_divergence_f",
        "dsm_update_received",
        # §5.2 Micro-Momentum
        "velocity_15m_f",
        "velocity_30m_f",
        "velocity_60m_f",
        "acceleration_15m_f",
        "temp_volatility_30m_f",
        "temp_volatility_60m_f",
        # §5.3 Meteorological Context
        "dew_point_f",
        "dew_point_depression_f",
        "wind_speed_mph",
        "wind_gust_mph",
        "wind_dir_sin",
        "wind_dir_cos",
        "altimeter_inhg",
        "visibility_miles",
        "pressure_tendency_1h_inhg",
        # §5.4 NBM Forecast
        "nbm_max_remaining_f",
        "nbm_expected_delta_f",
        "nbm_current_error_f",
        "nbm_hours_of_forecast_remaining",
        "nbm_cycle_age_minutes",
        "nbm_heating_trend_f",
        # §5.5 RRFS Forecast
        "rrfs_max_remaining_f",
        "rrfs_expected_delta_f",
        "rrfs_current_error_f",
        "rrfs_cycle_age_minutes",
        "rrfs_nbm_delta_divergence_f",
        "nbm_rrfs_max_spread_f",
        # §5.6 Advection Gap (unique; aliases share values with §5.4/5.5)
        "advection_gap_trend_60m_f",
        "nwp_consensus_advection_f",
        # §5.7 Solar Geometry
        "solar_elevation_deg",
        "solar_azimuth_deg",
        "hours_until_solar_noon",
        # §5.8 Calendar
        "day_of_year",
        "month",
    )

    def __init__(self, icao: str, lat: float, lon: float, tz: str):
        """
        Parameters
        ----------
        icao : Station ICAO code (e.g. ``"KMDW"``).
        lat, lon : Station coordinates for solar geometry computation.
        tz : IANA timezone string (used for LST conversion).
        """
        self.icao = icao
        self.lat = lat
        self.lon = lon
        self.tz = tz

    def build(
        self,
        obs_df: pd.DataFrame,
        nbm_df: pd.DataFrame,
        rrfs_df: pd.DataFrame,
        dsm_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """Build the feature DataFrame from raw DataFrames.

        Parameters
        ----------
        obs_df :  Combined observations (ASOS-HFM + ASOS-HR).
        nbm_df :  NBM forecast data.
        rrfs_df : RRFS forecast data.
        dsm_df :  DSM (Daily Summary Message) data for Tri-Factor Max floor.
                  Optional; if empty/None, dsm_floor_max_f is always NaN.

        Returns
        -------
        pd.DataFrame with one row per ASOS-HR observation and columns
        matching FEATURE_COLUMNS plus metadata columns:
          ``observation_time_utc``, ``climate_date_lst``, ``_is_asos_hr``.
        """
        if obs_df.empty:
            logger.warning("[%s] No observations — returning empty feature DataFrame.", self.icao)
            return pd.DataFrame()

        obs_df = obs_df.copy()
        dsm_df = dsm_df if dsm_df is not None and not dsm_df.empty else pd.DataFrame()

        # Tag each observation with its LST climate date (computed from UTC, not storage)
        obs_df["_climate_date"] = pd.to_datetime(
            series_to_lst_climate_date(obs_df["observation_time_utc"], self.tz)
        ).dt.date

        # Sort once by date then time; groupby with sort=True yields chronological days
        obs_sorted = obs_df.sort_values(["_climate_date", "observation_time_utc"])

        rows: list[dict[str, Any]] = []
        prev_advection_history: list[tuple[pd.Timestamp, float]] = []
        prev_pressure_history: list[tuple[pd.Timestamp, float]] = []
        prev_day_tail_df: Optional[pd.DataFrame] = None

        for climate_date, day_obs in obs_sorted.groupby("_climate_date", sort=True):
            day_obs = day_obs.copy()
            day_rows, prev_advection_history, prev_pressure_history, prev_day_tail_df = _process_climate_day(
                day_obs=day_obs,
                climate_date=climate_date,
                obs_df=obs_df,
                nbm_df=nbm_df,
                rrfs_df=rrfs_df,
                dsm_df=dsm_df,
                prev_advection_history=prev_advection_history,
                prev_pressure_history=prev_pressure_history,
                prev_day_tail_df=prev_day_tail_df,
                lat=self.lat,
                lon=self.lon,
                tz=self.tz,
            )
            rows.extend(day_rows)

        if not rows:
            logger.warning("[%s] No ASOS-HR rows found after feature engineering.", self.icao)
            return pd.DataFrame()

        result = pd.DataFrame(rows)
        # Enforce column ordering (metadata cols first, then feature cols)
        meta_cols = ["observation_time_utc", "climate_date_lst"]
        feat_cols = [c for c in self.FEATURE_COLUMNS if c in result.columns]
        extra_cols = [c for c in result.columns if c not in meta_cols and c not in feat_cols]
        result = result[meta_cols + feat_cols + extra_cols]

        logger.info(
            "[%s] FeatureEngine produced %d rows, %d feature columns.",
            self.icao, len(result), len(feat_cols),
        )
        return result
