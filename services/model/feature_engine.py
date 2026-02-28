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
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
import pandas as pd

from services.model.constants import (
    NBM_LATENCY_SECONDS,
    RRFS_LATENCY_SECONDS,
    CYCLE_AGE_CAP_MINUTES,
)
from services.model.time_utils import (
    utc_to_lst,
    lst_climate_date,
    lst_midnight_utc,
    climate_day_end_utc,
    hours_since_midnight_lst,
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
        "is_post_solar_noon": 1.0 if hours_until_noon < 0 else 0.0,
    }


# ──────────────────────────────────────────────────────────────────────
# Product-type helpers
# ──────────────────────────────────────────────────────────────────────

# ASOS-HR observations are typically published at :53 or :55 past the hour
# for routine METARs.  SPECI reports can fall at any minute but are rare.
# ASOS-HFM are every 5 minutes on the :00/:05/:10/... cycle.
_ASOS_HR_MINUTES: frozenset[int] = frozenset([47, 50, 52, 53, 54, 55, 56, 57, 58, 59, 0, 1, 2])

def _infer_is_asos_hr(
    product: str,
    obs_minute: int,
    prev_obs_time: Optional[pd.Timestamp],
    obs_time: pd.Timestamp,
) -> bool:
    """Determine if an observation is ASOS-HR (hourly METAR with T-group).

    Primary: use ``product`` column when available (explicit label wins).
    Fallback (for older backfill files where product == ''):
      • Minute of hour is in the typical METAR window (:47–:02 next hour)
      • OR inter-observation gap > 30 minutes (ASOS-HFM never gaps > 10 min)

    This never misclassifies ASOS-HFM as HR when explicit labels exist.
    """
    if product:
        return product.upper() == "ASOS-HR"

    # Fallback: infer from timing patterns
    if obs_minute in _ASOS_HR_MINUTES:
        return True
    if prev_obs_time is not None:
        gap_minutes = (obs_time - prev_obs_time).total_seconds() / 60.0
        if gap_minutes > 30:
            return True
    return False


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
    latency_seconds: int,
) -> Optional[pd.Timestamp]:
    """Return the most recent model_run_time_utc that satisfies the latency constraint.

    Implements the decay-weight approach from §6.2:
        latest_cycle = max { c ∈ data | c + latency_seconds ≤ observation_time }

    Returns None if no qualifying cycle exists at all.
    """
    if nwp_df.empty:
        return None

    safe_horizon = pd.Timestamp(observation_time) - pd.Timedelta(seconds=latency_seconds)
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

    # Identify best available cycle
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

def _build_5min_grid(obs_df: pd.DataFrame, climate_date_str: str) -> pd.DataFrame:
    """Build a 5-minute resampled temperature timeline for one climate day.

    Input: observations DataFrame (both ASOS-HFM and ASOS-HR), pre-filtered
           to the current climate_date_str.

    Returns a DataFrame indexed by 5-minute UTC timestamps with the column
    ``temperature_fahrenheit`` forward-filled (last observed value carries).
    """
    day_obs = obs_df[obs_df["_climate_date"] == climate_date_str].copy()
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
        "current_temp_f",
        "custom_intraday_max_f",
        "distance_from_max_f",
        "hours_since_midnight_lst",
        "hours_remaining_in_climate_day",
        "t_max_lag_minutes",
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
        # §5.6 Advection Gap (unique; aliases share values with §5.4/5.5)
        "advection_gap_trend_60m_f",
        "nwp_consensus_advection_f",
        # §5.7 Solar Geometry
        "solar_elevation_deg",
        "solar_azimuth_deg",
        "hours_until_solar_noon",
        "is_post_solar_noon",
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
    ) -> pd.DataFrame:
        """Build the feature DataFrame from raw DataFrames.

        Parameters
        ----------
        obs_df :  Combined observations (ASOS-HFM + ASOS-HR).
        nbm_df :  NBM forecast data.
        rrfs_df : RRFS forecast data.

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
        obs_df["observation_time_utc"] = pd.to_datetime(obs_df["observation_time_utc"], utc=True)

        # Tag each observation with its LST climate date
        obs_df["_climate_date"] = obs_df["observation_time_utc"].apply(
            lambda t: lst_climate_date(t.to_pydatetime(), self.tz).isoformat()
        )

        rows: list[dict] = []

        # Iterate climate day by climate day to build per-day state correctly
        for climate_date_str, day_obs in obs_df.groupby("_climate_date"):
            day_obs = day_obs.sort_values("observation_time_utc")
            climate_date = datetime.strptime(climate_date_str, "%Y-%m-%d").date()
            day_end = climate_day_end_utc(climate_date, self.tz)

            # Build 5-minute rolling temperature grid from ALL obs on this day
            grid_5min = _build_5min_grid(obs_df, climate_date_str)

            # Running state (reset per climate day)
            running_max_f: Optional[float] = None
            running_max_time: Optional[pd.Timestamp] = None
            prev_obs_time: Optional[pd.Timestamp] = None
            # For advection gap trend: store previous nbm_advection values
            advection_history: list[tuple[pd.Timestamp, float]] = []

            # Process observations in chronological order
            for _, row in day_obs.iterrows():
                obs_time = row["observation_time_utc"]
                obs_dt = obs_time.to_pydatetime()
                temp_f = row.get("temperature_fahrenheit")

                if pd.isna(temp_f):
                    continue

                # Update running max
                if running_max_f is None or temp_f > running_max_f:
                    running_max_f = float(temp_f)
                    running_max_time = obs_time

                # Only generate training rows from ASOS-HR observations
                product_str = str(row.get("product", "") or "")
                is_hr = _infer_is_asos_hr(
                    product=product_str,
                    obs_minute=obs_time.minute,
                    prev_obs_time=prev_obs_time,
                    obs_time=obs_time,
                )
                prev_obs_time = obs_time  # update before continue

                if not is_hr:
                    # Still contribute to running state but skip feature row
                    continue

                if running_max_f is None or running_max_time is None:
                    # No prior observations — skip (validity check 3 in builder)
                    continue

                # ── §5.1 State of the Day ──
                hours_since_midnight = hours_since_midnight_lst(obs_dt, self.tz)
                t_max_lag = (obs_time - running_max_time).total_seconds() / 60.0

                state = {
                    "current_temp_f":              float(temp_f),
                    "custom_intraday_max_f":        running_max_f,
                    "distance_from_max_f":          running_max_f - float(temp_f),
                    "hours_since_midnight_lst":     hours_since_midnight,
                    "hours_remaining_in_climate_day": 24.0 - hours_since_midnight,
                    "t_max_lag_minutes":            t_max_lag,
                }

                # ── §5.2 Micro-Momentum ──
                momentum = _momentum_features(grid_5min, obs_time)

                # ── §5.3 Meteorological Context ──
                dew_f = row.get("dew_point_fahrenheit", float("nan"))
                wind_s = float(row.get("wind_speed_mph", 0) or 0)
                wind_g = float(row.get("wind_gust_mph", 0) or 0)
                altimeter = row.get("altimeter_inhg", float("nan"))
                wind_sin, wind_cos = _wind_dir_sincos(row.get("wind_direction"))

                met = {
                    "dew_point_f":           float(dew_f) if pd.notna(dew_f) else float("nan"),
                    "dew_point_depression_f": (float(temp_f) - float(dew_f))
                                              if pd.notna(dew_f) else float("nan"),
                    "wind_speed_mph":        wind_s,
                    "wind_gust_mph":         wind_g,
                    "wind_dir_sin":          wind_sin,
                    "wind_dir_cos":          wind_cos,
                    "altimeter_inhg":        float(altimeter) if pd.notna(altimeter) else float("nan"),
                }

                # ── §5.4 NBM features ──
                nbm_feats = _extract_nwp_features(
                    nwp_df=nbm_df,
                    prefix="nbm",
                    observation_time=obs_dt,
                    custom_intraday_max_f=running_max_f,
                    current_temp_f=float(temp_f),
                    day_end_utc=day_end,
                    latency_seconds=NBM_LATENCY_SECONDS,
                    extra_features=True,
                )

                # ── §5.5 RRFS features ──
                rrfs_feats = _extract_nwp_features(
                    nwp_df=rrfs_df,
                    prefix="rrfs",
                    observation_time=obs_dt,
                    custom_intraday_max_f=running_max_f,
                    current_temp_f=float(temp_f),
                    day_end_utc=day_end,
                    latency_seconds=RRFS_LATENCY_SECONDS,
                    extra_features=False,
                )

                # RRFS–NBM divergence
                rrfs_feats["rrfs_nbm_delta_divergence_f"] = (
                    rrfs_feats.get("rrfs_expected_delta_f", float("nan"))
                    - nbm_feats.get("nbm_expected_delta_f", float("nan"))
                )

                # ── §5.6 Advection Gap ──
                nbm_adv = nbm_feats.get("nbm_current_error_f", float("nan"))
                rrfs_adv = rrfs_feats.get("rrfs_current_error_f", float("nan"))

                # Trend: current advection gap minus gap 60 min ago
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

                # Remember advection for next iteration's trend
                if not math.isnan(nbm_adv):
                    advection_history.append((obs_time, nbm_adv))
                # Trim old history (keep only last 2 hours)
                advection_history = [
                    (ts, v) for ts, v in advection_history
                    if ts >= obs_time - pd.Timedelta(hours=2)
                ]

                # ── §5.7 Solar Geometry ──
                solar = _solar_position(obs_dt, self.lat, self.lon)

                # ── §5.8 Calendar ──
                calendar = {
                    "day_of_year": obs_dt.timetuple().tm_yday,
                    "month":       obs_dt.month,
                }

                # ── Assemble ──
                feature_row = {
                    "observation_time_utc": obs_time,
                    "climate_date_lst":     climate_date_str,
                    **state,
                    **momentum,
                    **met,
                    **nbm_feats,
                    **rrfs_feats,
                    **adv_feats,
                    **solar,
                    **calendar,
                }
                rows.append(feature_row)

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
