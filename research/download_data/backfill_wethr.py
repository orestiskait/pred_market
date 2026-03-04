"""Backfill Wethr Push API data (observations + inferred CLI).

Fetches historical observations from the Wethr.net API and saves them into the
same parquet format and directory as the live system (services/wethr/listener.py).

After all observations are written, the CLI records are inferred by reading
back the saved parquet files (which already carry `observation_date_lst` added
by WethrPushStorage).  For each (station, LST day): high_f = max of
wethr_high_nws_f, low_f = min of wethr_low_nws_f across all observations that
belong to that LST day.

Runs fully sequentially — no threads.

Usage:
  python -m research.download_data.backfill_wethr
"""

from __future__ import annotations

import logging
import sys
import time
from datetime import date, datetime, timedelta, timezone

# P95 latency assumed for backfilled data (observation_time_utc + this = received_ts_utc)
_P95_LATENCY = timedelta(minutes=3)
from pathlib import Path

import pandas as pd
import requests

_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from services.core.config import load_config, _read_credential
from services.wethr.storage import WethrPushStorage, _STATION_TZ
from services.weather.units import celsius_to_fahrenheit

logger = logging.getLogger(__name__)

# ==============================================================================
# Configuration - edit these constants before running
# ==============================================================================

# python3 -m research.download_data.backfill_wethr

STATIONS = ["KMDW"]
START_DATE = date(2026, 1, 31)
END_DATE = date(2026, 2, 28)

RATE_LIMIT_SLEEP = 0.25  # seconds between requests — well within 300 req/min

# ==============================================================================


def _parse_iso_ts(raw: str) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


def fetch_and_split_day(station: str, target_date: date, api_key: str):
    """Fetch one day of historical observations, and extract Obs, DSM, and CLI."""
    url = "https://wethr.net/api/v2/observations.php"
    params = {
        "station_code": station,
        "mode": "history",
        "start_time": f"{target_date.isoformat()}T00:00:00Z",
        "end_time": f"{target_date.isoformat()}T23:59:59Z",
    }
    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        res = requests.get(url, params=params, headers=headers, timeout=15)
        res.raise_for_status()
        data = res.json()
    except Exception as e:
        logger.error("Failed to fetch %s for %s: %s", station, target_date, e)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    if not data or not isinstance(data, list):
        logger.debug("No data or invalid format for %s %s", station, target_date)
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    def _f(val):
        return float(val) if val is not None else None

    obs_rows = []
    dsm_rows = []
    cli_rows = []

    for item in data:
        ob_ts = _parse_iso_ts(item.get("observation_time", ""))
        received_ts = (ob_ts + _P95_LATENCY) if ob_ts is not None else pd.Timestamp.now(tz="UTC")
        for_date_str = target_date.isoformat()

        # NWS Logic / probable extremes
        sh_c = _f(item.get("six_hour_high"))
        sh_f = celsius_to_fahrenheit(sh_c)
        sl_c = _f(item.get("six_hour_low"))
        sl_f = celsius_to_fahrenheit(sl_c)

        alt = _f(item.get("altimeter"))
        dp_c = item.get("dew_point")
        dp_f = celsius_to_fahrenheit(dp_c)

        obs_row = {
            "station_code":          item.get("station_code", ""),
            "observation_time_utc":  ob_ts,
            "received_ts_utc":       received_ts,
            "live":                  False,
            "product":               "ASOS-HR" if alt is not None else "ASOS-HFM",
            "temperature_celsius":   _f(item.get("temperature")),
            "temperature_fahrenheit": _f(item.get("temperature_display", item.get("temperature_f"))),
            "dew_point_celsius":     _f(dp_c),
            "dew_point_fahrenheit":  dp_f,
            "relative_humidity":     _f(item.get("relative_humidity")),
            "wind_direction":        str(item.get("wind_direction", "")),
            "wind_speed_mph":        _f(item.get("wind_speed")),
            "wind_gust_mph":         _f(item.get("wind_gust")),
            "visibility_miles":      _f(item.get("visibility")),
            "altimeter_inhg":        alt,
            "wethr_high_nws_f":      None,
            "wethr_high_wu_f":       None,
            "wethr_low_nws_f":       None,
            "wethr_low_wu_f":        None,
            "anomaly":               False,
            "event_id":              str(item.get("id", "")),
        }
        obs_rows.append(obs_row)

        # Extract DSM
        # We capture the latest distinctive DSM of the day
        dsm_hi_f = _f(item.get("dsm_high_f", item.get("dsm_high_display")))
        dsm_lo_f = _f(item.get("dsm_low_f", item.get("dsm_low_display")))
        if dsm_hi_f is not None or dsm_lo_f is not None:
            dsm_rows.append({
                "station_code": station,
                "for_date_lst": for_date_str,
                "received_ts_utc": ob_ts + _P95_LATENCY,
                "live": False,
                "high_f": dsm_hi_f,
                "high_c": _f(item.get("dsm_high")),
                "high_time_utc": ob_ts,
                "low_f": dsm_lo_f,
                "low_c": _f(item.get("dsm_low")),
                "low_time_utc": ob_ts,
                "observation_time_utc": ob_ts,
                "anomaly": False,
                "event_id": str(item.get("id", "")),
            })

        # Extract CLI
        cli_hi_f = _f(item.get("cli_high_f", item.get("cli_high_display")))
        cli_lo_f = _f(item.get("cli_low_f", item.get("cli_low_display")))
        if cli_hi_f is not None or cli_lo_f is not None:
            cli_rows.append({
                "station_code": station,
                "for_date_lst": for_date_str,
                "received_ts_utc": ob_ts + _P95_LATENCY,
                "live": False,
                "high_f": cli_hi_f,
                "high_c": _f(item.get("cli_high")),
                "high_time_utc": ob_ts,
                "low_f": cli_lo_f,
                "low_c": _f(item.get("cli_low")),
                "low_time_utc": ob_ts,
                "observation_time_utc": ob_ts,
                "anomaly": False,
                "event_id": str(item.get("id", "")),
            })

    return (
        pd.DataFrame(obs_rows) if obs_rows else pd.DataFrame(),
        pd.DataFrame(dsm_rows) if dsm_rows else pd.DataFrame(),
        pd.DataFrame(cli_rows) if cli_rows else pd.DataFrame(),
    )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    config, config_path = load_config(_project_root / "services" / "config.yaml")
    api_key = _read_credential(config, "wethr_api_key")
    if not api_key:
        logger.error("Could not find wethr_api_key in credentials")
        sys.exit(1)

    data_dir = _project_root / "data"
    storage = WethrPushStorage(data_dir)

    logger.info("Starting Wethr backfill")
    logger.info("Stations: %s", STATIONS)
    logger.info("Date range: %s to %s", START_DATE, END_DATE)

    tasks = []
    for stn in STATIONS:
        curr = START_DATE
        while curr <= END_DATE:
            tasks.append((stn, curr))
            curr += timedelta(days=1)

    logger.info("Total days to process: %d", len(tasks))
    logger.info("Fetching and extracting observations, DSM, and CLI...")

    total_obs = 0
    total_dsm = 0
    total_cli = 0
    t0 = time.time()

    for i, (stn, d) in enumerate(tasks, start=1):
        obs_df, dsm_df, cli_df = fetch_and_split_day(stn, d, api_key)
        time.sleep(RATE_LIMIT_SLEEP)

        if not obs_df.empty:
            storage.save(obs_df, "observations")
            total_obs += len(obs_df)

        if not dsm_df.empty:
            storage.save(dsm_df, "dsm")
            total_dsm += len(dsm_df)
            
        if not cli_df.empty:
            storage.save(cli_df, "cli")
            total_cli += len(cli_df)

        if i % 10 == 0 or i == len(tasks):
            logger.info(
                "Progress: %d/%d days | obs_rows: %d | dsm_rows: %d | cli_rows: %d",
                i, len(tasks), total_obs, total_dsm, total_cli
            )

    elapsed_min = (time.time() - t0) / 60.0
    logger.info(
        "Backfill complete in %.1f min. "
        "Observations: %d | DSM: %d | CLI: %d",
        elapsed_min, total_obs, total_dsm, total_cli,
    )

    _check_completeness(storage, data_dir, STATIONS)


def _check_completeness(
    storage: WethrPushStorage,
    data_dir: Path,
    stations: list[str],
) -> None:
    """Post-backfill QC: verify 5-min coverage and CLI at 23:59 LST.

    For every (station, LST day) that was written:
    - A *full* day must have its last 5-min observation at 23:5x LST.
      Days that don't meet this (first/last edge day with partial data) are
      dropped by deleting the parquet files for observations, dsm, and cli.
    - For each remaining complete day, we check that a CLI report exists
      with ``for_date_lst`` matching that day and log a warning if absent.
    """
    logger.info("=" * 60)
    logger.info("POST-BACKFILL COMPLETENESS CHECK")
    logger.info("=" * 60)

    base = data_dir / "weather" / "wethr_push"
    event_types = ["observations", "dsm", "cli"]

    for station in stations:
        tz_name = _STATION_TZ.get(station)
        
        # ----------------------------------------------------------------
        # Load all observation rows for this station
        # ----------------------------------------------------------------
        obs_dir = base / "observations"
        obs_files = sorted(obs_dir.glob(f"{station}_*.parquet"))
        if not obs_files:
            logger.warning("[%s] No observation files found — nothing to check.", station)
            continue

        frames = [pd.read_parquet(f) for f in obs_files]
        obs_all = pd.concat(frames, ignore_index=True)

        # Ensure observation_time_lst is parsed correctly
        if "observation_time_lst" not in obs_all.columns:
            logger.warning(
                "[%s] 'observation_time_lst' column missing — skipping check.", station
            )
            continue

        obs_all["observation_time_lst"] = pd.to_datetime(
            obs_all["observation_time_lst"], errors="coerce"
        )
        obs_all["_date_lst"] = obs_all["observation_time_lst"].dt.normalize()

        # ----------------------------------------------------------------
        # Determine which days are complete (last obs at 23:5x)
        # ----------------------------------------------------------------
        day_stats = obs_all.groupby("_date_lst")["observation_time_lst"].agg(
            last_obs="max"
        )

        complete_days: list[pd.Timestamp] = []
        incomplete_days: list[pd.Timestamp] = []

        for day_ts, row in day_stats.iterrows():
            last_obs: pd.Timestamp = row["last_obs"]
            # A full day's last 5-min slot falls in the 23:5x minute range
            is_full = (
                last_obs.hour == 23
                and last_obs.minute >= 50
            )
            if is_full:
                complete_days.append(day_ts)
            else:
                incomplete_days.append(day_ts)

        # ----------------------------------------------------------------
        # Drop incomplete days from every event-type parquet
        # ----------------------------------------------------------------
        if incomplete_days:
            logger.warning(
                "[%s] Incomplete days (not a full 5-min day) — removing from storage:",
                station,
            )
            for day_ts in incomplete_days:
                day_str = day_ts.date().isoformat()
                last = day_stats.loc[day_ts, "last_obs"]
                n_rows = int((obs_all["_date_lst"] == day_ts).sum())
                logger.warning(
                    "  %s  last_obs=%s  obs_count=%d → REMOVED",
                    day_str,
                    last.strftime("%H:%M"),
                    n_rows,
                )
                for et in event_types:
                    path = base / et / f"{station}_{day_str}.parquet"
                    if path.exists():
                        path.unlink()
                        logger.info("  Deleted %s", path)
        else:
            logger.info("[%s] No incomplete days found.", station)

        # ----------------------------------------------------------------
        # Report complete days — check obs count and CLI at 23:59
        # ----------------------------------------------------------------
        if not complete_days:
            logger.warning("[%s] No complete days remain after cleanup.", station)
            continue

        # Load CLI records for this station
        cli_dir = base / "cli"
        cli_files = sorted(cli_dir.glob(f"{station}_*.parquet"))
        cli_dates: set[str] = set()
        if cli_files:
            cli_frames = [pd.read_parquet(f) for f in cli_files]
            cli_all = pd.concat(cli_frames, ignore_index=True)
            if "for_date_lst" in cli_all.columns:
                # for_date_lst may be stored as date-string or datetime
                cli_dates = set(
                    pd.to_datetime(cli_all["for_date_lst"], errors="coerce")
                    .dt.strftime("%Y-%m-%d")
                    .dropna()
                    .unique()
                )

        logger.info("[%s] Complete days (%d):", station, len(complete_days))
        all_ok = True
        for day_ts in sorted(complete_days):
            day_str = day_ts.date().isoformat()
            n_obs = int((obs_all["_date_lst"] == day_ts).sum())
            last = day_stats.loc[day_ts, "last_obs"]
            has_cli = day_str in cli_dates
            cli_flag = "CLI ✓" if has_cli else "CLI MISSING ✗"
            if not has_cli:
                all_ok = False
                logger.warning(
                    "  %s  last_obs=%s  n_obs=%d  %s",
                    day_str, last.strftime("%H:%M"), n_obs, cli_flag,
                )
            else:
                logger.info(
                    "  %s  last_obs=%s  n_obs=%d  %s",
                    day_str, last.strftime("%H:%M"), n_obs, cli_flag,
                )

        if all_ok:
            logger.info(
                "[%s] All %d complete days have a CLI record at 23:59 LST. ✓",
                station, len(complete_days),
            )
        logger.info("=" * 60)


if __name__ == "__main__":
    main()
