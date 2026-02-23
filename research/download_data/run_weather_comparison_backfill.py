#!/usr/bin/env python3
"""Backfill HRRR, NBM, RRFS, RTMA-RU for weather comparison (last 14 days, KMDW).

Usage:
    python -m research.download_data.run_weather_comparison_backfill

Fetches:
  - HRRR: 15-min sub-hourly (subh), rolling 2h-out forecast (f02, 120 min step)
  - NBM: f02 only (2h-prior forecast), start-1d to end for coverage
  - RRFS: f02 only (2h-prior forecast), start-1d to end for coverage
  - RTMA-RU: 15-min analysis (optional, run separately if needed)
"""

from __future__ import annotations

import logging
import sys
from datetime import date, timedelta
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from services.weather.nwp.hrrr import HRRRFetcher
from services.weather.nwp.nbm import NBMCOGFetcher
from services.weather.nwp.rrfs import RRFSFetcher
from services.weather.station_registry import nwp_station_for_icao

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    datefmt="%H:%M:%S",
)

CONFIG_PATH = _project_root / "services" / "config.yaml"
STATION = "KMDW"
DAYS = 14


def main() -> None:
    end_date = date.today()
    start_date = end_date - timedelta(days=DAYS)
    stations = [nwp_station_for_icao(STATION)]

    print(f"\n=== Weather comparison backfill: {STATION}, last {DAYS} days ===\n")

    # HRRR: 15-min sub-hourly, rolling 2h-out forecast (f02, 120 min step)
    print("--- HRRR (15-min subh, rolling 2h-out) ---")
    hrrr = HRRRFetcher.from_config(CONFIG_PATH)
    df_hrrr = hrrr.fetch_date_range(
        start_date, end_date, stations,
        rolling_lead_minutes=120, save=True,
    )
    print(f"HRRR: {len(df_hrrr)} rows\n")

    # NBM: f02 only, fetch from start-1d
    print("--- NBM (f02, 2h prior) ---")
    nbm = NBMCOGFetcher.from_config(CONFIG_PATH)
    nbm_start = start_date - timedelta(days=1)
    df_nbm = nbm.fetch_date_range(
        nbm_start, end_date, stations,
        fxx_range=range(2, 3), save=True,
    )
    print(f"NBM: {len(df_nbm)} rows\n")

    # RRFS: f02 only, fetch from start-1d
    print("--- RRFS (f02, 2h prior) ---")
    rrfs = RRFSFetcher.from_config(CONFIG_PATH)
    rrfs_start = start_date - timedelta(days=1)
    df_rrfs = rrfs.fetch_date_range(
        rrfs_start, end_date, stations,
        fxx_range=range(2, 3), save=True,
    )
    print(f"RRFS: {len(df_rrfs)} rows\n")

    print("=== Backfill complete ===")


if __name__ == "__main__":
    main()
    sys.exit(0)
