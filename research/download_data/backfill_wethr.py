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
from services.wethr.storage import WethrPushStorage

logger = logging.getLogger(__name__)

# ==============================================================================
# Configuration - edit these constants before running
# ==============================================================================

# python3 -m research.download_data.backfill_wethr

STATIONS = ["KMDW"]
START_DATE = date(2026, 2, 25)
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


def fetch_observations_day(station: str, target_date: date, api_key: str) -> pd.DataFrame:
    """Fetch one day of historical observations and return a DataFrame matching listener schema.

    Only collects raw observations — no CLI/DSM/high/low extraction.
    """
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
        return pd.DataFrame()

    if not data or not isinstance(data, list):
        logger.debug("No data or invalid format for %s %s", station, target_date)
        return pd.DataFrame()

    def _f(val):
        return float(val) if val is not None else None

    rows = []
    for item in data:
        dp_c = item.get("dew_point")
        dp_f = (dp_c * 9 / 5 + 32) if dp_c is not None else None
        alt = _f(item.get("altimeter"))

        rows.append({
            "station_code":          item.get("station_code", ""),
            "observation_time_utc":  (ob_ts := _parse_iso_ts(item.get("observation_time", ""))),
            "received_ts_utc":       (ob_ts + _P95_LATENCY) if ob_ts is not None else pd.Timestamp.now(tz="UTC"),
            "live":                  False,
            "product":               "ASOS-HR" if alt is not None else "ASOS-HFM",
            "temperature_celsius":   _f(item.get("temperature")),
            "temperature_fahrenheit": _f(item.get("temperature_display")),
            "dew_point_celsius":     _f(dp_c),
            "dew_point_fahrenheit":  dp_f,
            "relative_humidity":     _f(item.get("relative_humidity")),
            "wind_direction":        str(item.get("wind_direction", "")),
            "wind_speed_mph":        _f(item.get("wind_speed")),
            "wind_gust_mph":         _f(item.get("wind_gust")),
            "visibility_miles":      _f(item.get("visibility")),
            "altimeter_inhg":        alt,
            "wethr_high_nws_f":      _f(item.get("wethr_high")),
            "wethr_high_wu_f":       None,
            "wethr_low_nws_f":       _f(item.get("wethr_low")),
            "wethr_low_wu_f":        None,
            "anomaly":               False,
            "event_id":              str(item.get("id", "")),
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def infer_cli_from_saved_observations(
    storage: WethrPushStorage,
    station: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Read back saved observations (with storage-added observation_date_lst) and
    infer CLI records per (station, LST day).

    high_f = max(wethr_high_nws_f)  — NWS probable high for the day
    low_f  = min(wethr_low_nws_f)   — NWS probable low for the day

    Schema matches listener.py on_wethr_cli exactly:
      station_code, for_date, received_ts_utc, live,
      high_f, high_c, low_f, low_c, anomaly, event_id
    """
    obs_df = storage.read("observations", station=station, start_date=start_date, end_date=end_date)

    if obs_df.empty:
        logger.warning("No saved observations found for %s; skipping CLI inference.", station)
        return pd.DataFrame()

    if "observation_date_lst" not in obs_df.columns:
        logger.error(
            "observation_date_lst column missing from saved observations for %s. "
            "Storage may not have a timezone entry for this station.",
            station,
        )
        return pd.DataFrame()

    cli_rows = []

    for lst_date, day_df in obs_df.groupby("observation_date_lst"):
        highs = day_df["wethr_high_nws_f"].dropna()
        lows  = day_df["wethr_low_nws_f"].dropna()

        if highs.empty and lows.empty:
            logger.debug("No NWS high/low values for %s on LST %s; skipping.", station, lst_date)
            continue

        high_f = float(highs.max()) if not highs.empty else None
        low_f  = float(lows.min())  if not lows.empty  else None

        # Derive Celsius from the paired temperature_celsius column of the extreme row
        high_c = None
        low_c  = None
        if high_f is not None:
            hi_row = day_df.loc[day_df["wethr_high_nws_f"].idxmax()]
            tc = hi_row.get("temperature_celsius")
            high_c = round(float(tc), 4) if tc is not None and pd.notna(tc) else round((high_f - 32) * 5 / 9, 4)
        if low_f is not None:
            lo_row = day_df.loc[day_df["wethr_low_nws_f"].idxmin()]
            tc = lo_row.get("temperature_celsius")
            low_c = round(float(tc), 4) if tc is not None and pd.notna(tc) else round((low_f - 32) * 5 / 9, 4)

        # for_date is the LST date string, matching listener.py convention
        for_date_str = lst_date.strftime("%Y-%m-%d") if hasattr(lst_date, "strftime") else str(lst_date)

        # received_ts_utc = latest obs time in the LST day + P95 latency
        latest_obs_ts = pd.to_datetime(day_df["observation_time_utc"], utc=True).max()
        cli_received_ts = latest_obs_ts + pd.Timedelta(_P95_LATENCY)

        cli_rows.append({
            "station_code":   station,
            "for_date":       for_date_str,
            "received_ts_utc": cli_received_ts,
            "live":           False,
            "high_f":         high_f,
            "high_c":         high_c,
            "low_f":          low_f,
            "low_c":          low_c,
            "anomaly":        False,
            "event_id":       "",  # inferred — no real SSE event ID
        })

    return pd.DataFrame(cli_rows) if cli_rows else pd.DataFrame()


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

    # Build sequential list of (station, date) tasks
    tasks = []
    for stn in STATIONS:
        curr = START_DATE
        while curr <= END_DATE:
            tasks.append((stn, curr))
            curr += timedelta(days=1)

    logger.info("Total days to process: %d", len(tasks))

    # ── Phase 1: Fetch and save observations (sequential) ───────────────────
    logger.info("Phase 1: Fetching and saving observations...")

    total_obs_rows = 0
    t0 = time.time()

    for i, (stn, d) in enumerate(tasks, start=1):
        obs_df = fetch_observations_day(stn, d, api_key)
        time.sleep(RATE_LIMIT_SLEEP)

        if not obs_df.empty:
            storage.save(obs_df, "observations")
            total_obs_rows += len(obs_df)

        if i % 10 == 0 or i == len(tasks):
            logger.info(
                "Progress: %d/%d days  |  observation rows saved: %d",
                i, len(tasks), total_obs_rows,
            )

    logger.info("Phase 1 complete. %d observation rows saved.", total_obs_rows)

    # ── Phase 2: Infer CLI from saved observations ───────────────────────────
    logger.info("Phase 2: Inferring CLI from saved observations...")

    total_cli_rows = 0

    for stn in STATIONS:
        cli_df = infer_cli_from_saved_observations(storage, stn, START_DATE, END_DATE)

        if not cli_df.empty:
            storage.save(cli_df, "cli")
            total_cli_rows += len(cli_df)
            logger.info(
                "CLI [%s]: saved %d records (%s → %s LST).",
                stn, len(cli_df),
                cli_df["for_date"].min(), cli_df["for_date"].max(),
            )
        else:
            logger.info("CLI [%s]: no records inferred.", stn)

    elapsed_min = (time.time() - t0) / 60.0
    logger.info(
        "Backfill complete in %.1f min. "
        "Observations: %d rows | CLI: %d records.",
        elapsed_min, total_obs_rows, total_cli_rows,
    )


if __name__ == "__main__":
    main()
