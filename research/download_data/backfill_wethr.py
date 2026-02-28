"""Backfill Wethr Push API data (observations).

Fetches historical observations from the Wethr.net API and saves them into the
same parquet format and directory as the live system (services/wethr/listener.py).

Usage:
  python -m research.download_data.backfill_wethr
"""

from __future__ import annotations

import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import json

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
START_DATE = date(2025, 5, 28)
END_DATE = date(2025, 5, 30)

MAX_WORKERS = 4
RATE_LIMIT_SLEEP = 0.25  # seconds per worker loop. 4 workers / 0.25 delay = up to 16 req/sec absolute max. 
                         # Actually thread overhead puts it lower. Let's use 0.5 sec to be well within 300 req/min (5 req/sec).

# ==============================================================================

def _parse_iso_ts(raw: str) -> datetime | None:
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None

def fetch_history_day(station: str, target_date: date, api_key: str) -> dict[str, pd.DataFrame]:
    """Fetch one day of historical observations and convert to listener schema, extracting dsm/cli/new_high/new_low."""
    url = "https://wethr.net/api/v2/observations.php"
    start_time = f"{target_date.isoformat()}T00:00:00Z"
    end_time = f"{target_date.isoformat()}T23:59:59Z"
    
    empty_result = {
        "observations": pd.DataFrame(),
        "dsm": pd.DataFrame(),
        "cli": pd.DataFrame(),
        "new_high": pd.DataFrame(),
        "new_low": pd.DataFrame()
    }
    
    params = {
        "station_code": station,
        "mode": "history",  # Explicitly use history mode
        "start_time": start_time,
        "end_time": end_time
    }
    headers = {"Authorization": f"Bearer {api_key}"}

    try:
        res = requests.get(url, params=params, headers=headers, timeout=15)
        res.raise_for_status()
        data = res.json()
    except Exception as e:
        logger.error("Failed to fetch %s for %s: %s", station, target_date, e)
        return empty_result

    if not data or not isinstance(data, list):
        logger.debug("No data or invalid format for %s %s", station, target_date)
        return empty_result

    rows = []
    dsm_rows = []
    cli_rows = []
    high_rows = []
    low_rows = []

    received_ts = pd.Timestamp.now(tz="UTC")
    
    last_dsm = None
    last_cli = None
    max_high = None
    min_low = None
    
    def _to_float(val):
        return float(val) if val is not None else None

    for item in data:
        # Reconstruct dew point F if absent
        dp_c = item.get("dew_point")
        dp_f = (dp_c * 9/5 + 32) if dp_c is not None else None
        
        row = {
            "station_code": item.get("station_code", ""),
            "observation_time_utc": _parse_iso_ts(item.get("observation_time", "")),
            "received_ts_utc": received_ts,
            "live": False,  # Explicitly mark as not live
            "product": item.get("data_source", "") or "",
            "temperature_celsius": _to_float(item.get("temperature")),
            "temperature_fahrenheit": _to_float(item.get("temperature_display")),
            "dew_point_celsius": _to_float(dp_c),
            "dew_point_fahrenheit": dp_f,
            "relative_humidity": _to_float(item.get("relative_humidity")),
            "wind_direction": str(item.get("wind_direction", "")),
            "wind_speed_mph": _to_float(item.get("wind_speed")),
            "wind_gust_mph": _to_float(item.get("wind_gust")),
            "visibility_miles": _to_float(item.get("visibility")),
            "altimeter_inhg": _to_float(item.get("altimeter")),
            "wethr_high_nws_f": _to_float(item.get("highest_probable_f")), # Best effort proxy
            "wethr_high_wu_f": None,
            "wethr_low_nws_f": _to_float(item.get("lowest_probable_f")),   # Best effort proxy
            "wethr_low_wu_f": None,
            "anomaly": False,
            "event_id": str(item.get("id", "")),
        }
        rows.append(row)

        if item.get("dsm_high") is not None or item.get("dsm_low") is not None:
            last_dsm = item
        if item.get("cli_high") is not None or item.get("cli_low") is not None:
            last_cli = item
            
        if item.get("highest_probable_f") is not None:
            if max_high is None or _to_float(item.get("highest_probable_f")) >= _to_float(max_high.get("highest_probable_f")):
                max_high = item
        if item.get("lowest_probable_f") is not None:
            if min_low is None or _to_float(item.get("lowest_probable_f")) <= _to_float(min_low.get("lowest_probable_f")):
                min_low = item

    if last_dsm:
        dsm_rows.append({
            "station_code": last_dsm.get("station_code", ""),
            "for_date": target_date.strftime("%Y-%m-%d"),
            "received_ts_utc": received_ts,
            "live": False,
            "high_f": _to_float(last_dsm.get("dsm_high_f")),
            "high_c": _to_float(last_dsm.get("dsm_high")),
            "high_time_utc": None,
            "low_f": _to_float(last_dsm.get("dsm_low_f")),
            "low_c": _to_float(last_dsm.get("dsm_low")),
            "low_time_utc": None,
            "anomaly": False,
            "event_id": str(last_dsm.get("id", "")),
        })

    if last_cli:
        cli_rows.append({
            "station_code": last_cli.get("station_code", ""),
            "for_date": target_date.strftime("%Y-%m-%d"),
            "received_ts_utc": received_ts,
            "live": False,
            "high_f": _to_float(last_cli.get("cli_high_f")),
            "high_c": _to_float(last_cli.get("cli_high")),
            "low_f": _to_float(last_cli.get("cli_low_f")),
            "low_c": _to_float(last_cli.get("cli_low")),
            "anomaly": False,
            "event_id": str(last_cli.get("id", "")),
        })

    if max_high:
        high_rows.append({
            "station_code": max_high.get("station_code", ""),
            "observation_time_utc": _parse_iso_ts(max_high.get("observation_time", "")),
            "received_ts_utc": received_ts,
            "live": False,
            "logic": "nws",
            "value_f": _to_float(max_high.get("highest_probable_f")),
            "value_c": _to_float(max_high.get("highest_probable")),
            "prev_value_f": None,
            "prev_value_c": None,
            "event_id": str(max_high.get("id", "")),
        })

    if min_low:
        low_rows.append({
            "station_code": min_low.get("station_code", ""),
            "observation_time_utc": _parse_iso_ts(min_low.get("observation_time", "")),
            "received_ts_utc": received_ts,
            "live": False,
            "logic": "nws",
            "value_f": _to_float(min_low.get("lowest_probable_f")),
            "value_c": _to_float(min_low.get("lowest_probable")),
            "prev_value_f": None,
            "prev_value_c": None,
            "event_id": str(min_low.get("id", "")),
        })

    return {
        "observations": pd.DataFrame(rows) if rows else pd.DataFrame(),
        "dsm": pd.DataFrame(dsm_rows) if dsm_rows else pd.DataFrame(),
        "cli": pd.DataFrame(cli_rows) if cli_rows else pd.DataFrame(),
        "new_high": pd.DataFrame(high_rows) if high_rows else pd.DataFrame(),
        "new_low": pd.DataFrame(low_rows) if low_rows else pd.DataFrame()
    }

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

    # Build queue of (station, date)
    tasks = []
    for stn in STATIONS:
        curr = START_DATE
        while curr <= END_DATE:
            tasks.append((stn, curr))
            curr += timedelta(days=1)
            
    logger.info("Total days to process: %d", len(tasks))

    completed = 0
    total_rows = 0

    def process_task(task):
        stn, d = task
        dfs = fetch_history_day(stn, d, api_key)
        
        # Enforce rate limiting manually inside the worker
        time.sleep(RATE_LIMIT_SLEEP)
        
        return stn, d, dfs

    t0 = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(process_task, t): t for t in tasks}
        
        for fut in as_completed(futures):
            try:
                stn, d, dfs = fut.result()
                completed += 1
                
                rows_this_day = 0
                for event_type, df in dfs.items():
                    if not df.empty:
                        storage.save(df, event_type)  # Saving handles deduplication logic
                        rows_this_day += len(df)
                        
                total_rows += rows_this_day
                    
                if completed % 10 == 0 or completed == len(tasks):
                    logger.info("Progress: %d/%d days completed... Rows saved: %d", completed, len(tasks), total_rows)
                    
            except Exception as e:
                logger.error("Task failed: %s", e, exc_info=True)

    elapsed_min = (time.time() - t0) / 60.0
    logger.info("Backfill complete in %.1f min. Saved %d total rows.", elapsed_min, total_rows)

if __name__ == "__main__":
    main()
