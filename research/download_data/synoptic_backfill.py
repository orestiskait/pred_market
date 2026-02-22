"""Backfill Synoptic ASOS 1-min data into synoptic_weather_observations.

Uses Synoptic Time Series REST API. Writes to the same storage as the live
WebSocket collector (synoptic_weather_observations/). Merges with existing data;
live data takes priority when deduplicating by (ob_timestamp, stid).

Data source: https://api.synopticdata.com/v2/stations/timeseries
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests

from services.core.config import load_config, get_synoptic_token
from services.core.storage import ParquetStorage
from services.markets.kalshi_registry import synoptic_station_for_icao

logger = logging.getLogger(__name__)

SYNOPTIC_TIMESERIES_URL = "https://api.synopticdata.com/v2/stations/timeseries"
EXPECTED_OBS_PER_DAY = 1440  # 24 * 60 (1-min resolution)
MIN_COMPLETENESS = 0.95  # Only save days with >= 95% of expected obs


def _fetch_day(icao: str, target_date: date, token: str, timeout: int = 120) -> list[dict]:
    """Fetch one day from Synoptic API. Returns rows for write_synoptic_ws schema."""
    stid = synoptic_station_for_icao(icao)
    if not stid:
        raise ValueError(f"No Synoptic station ID for {icao}")

    start = datetime(target_date.year, target_date.month, target_date.day, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    params = {
        "token": token,
        "stid": stid,
        "start": start.strftime("%Y%m%d%H%M"),
        "end": end.strftime("%Y%m%d%H%M"),
        "vars": "air_temp",
        "units": "english",
        "obtimezone": "UTC",
    }

    logger.info("Fetching Synoptic backfill for %s (%s) on %s", icao, stid, target_date)
    resp = requests.get(SYNOPTIC_TIMESERIES_URL, params=params, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    if "STATION" not in data or not data["STATION"]:
        return []

    rows = []
    for stn in data["STATION"]:
        obs = stn.get("OBSERVATIONS", {})
        times = obs.get("date_time", [])
        temp_key = next((k for k in obs if k.startswith("air_temp_set_")), None)
        if not temp_key or not times:
            continue

        temps = obs[temp_key]
        for t, val in zip(times, temps):
            if val is None:
                continue
            try:
                ob_ts = datetime.fromisoformat(t.replace("Z", "+00:00"))
                rows.append({
                    "received_ts": ob_ts,  # Backfill: no real receive time; use ob_timestamp
                    "ob_timestamp": ob_ts,
                    "stid": stid,
                    "sensor": temp_key,
                    "value": float(val),
                    "source": "backfill",
                })
            except (ValueError, TypeError):
                continue

    return rows


def backfill_range(
    icao: str,
    start_date: date,
    end_date: date,
    token: str,
    data_dir: Path | str,
    skip_dates_with_live: bool = False,
) -> int:
    """Backfill Synoptic data for date range. Merges with existing; live wins on dedup.

    Returns count of days merged.
    """
    storage = ParquetStorage(str(data_dir))
    merged = 0

    current = start_date
    while current <= end_date:
        path = storage.dirs["synoptic_ws"] / f"{current.isoformat()}.parquet"
        if skip_dates_with_live and path.exists():
            df = pd.read_parquet(path)
            if "source" in df.columns and (df["source"] == "live").any():
                logger.debug("Skipping %s (has live data)", current)
                current += timedelta(days=1)
                continue

        try:
            rows = _fetch_day(icao, current, token)
            min_obs = int(EXPECTED_OBS_PER_DAY * MIN_COMPLETENESS)
            if len(rows) < min_obs:
                logger.info(
                    "Skipping %s: %d obs (need >= %d for %.0f%% completeness)",
                    current, len(rows), min_obs, MIN_COMPLETENESS * 100,
                )
            elif rows:
                storage.merge_synoptic_backfill(rows, current)
                merged += 1
        except Exception:
            logger.exception("Failed Synoptic backfill for %s on %s", icao, current)
        current += timedelta(days=1)

    return merged
