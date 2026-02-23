"""Fetch HRRR 15-minute sub-hourly model data for configured research stations.

Convenience wrapper around the unified NWP runner. For multi-model usage
(HRRR / RTMA-RU / RRFS / NBM) use run_nwp_collection.py.

Usage:
  1. Set MODE, START_DATE, END_DATE, etc. below.
  2. Run: python -m research.download_data.run_hrrr_collection

Modes:
  - latest: fetch most recent available cycle
  - backfill: historical fetch for date range
  - backfill_rolling_2h: last 14 days, 2h-out forecast only (rolling strategy)
"""

from __future__ import annotations

import logging
import sys
from datetime import date, timedelta
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from services.core.config import get_event_series
from services.tz import utc_today
from services.weather.nwp.hrrr import HRRRFetcher
from services.weather.station_registry import (
    nwp_station_for_icao,
    nwp_stations_for_series,
    NWPStation,
)

logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Config — edit these when running as script
# ------------------------------------------------------------------------------
MODE = "latest"  # "latest" | "backfill" | "backfill_rolling_2h"
START_DATE = None  # for backfill; None = end - 14 days when backfill_rolling_2h
END_DATE = None  # for backfill; None means yesterday
STATIONS = []  # e.g. ["KMDW", "KNYC"] — empty = use config.yaml
CYCLES = None  # e.g. "0,12" — None = all 24
MAX_FXX = None  # max forecast hour; None = from config or 18
ROLLING_2H_MINUTES = 120  # for backfill_rolling_2h: 2h-out forecast
CONFIG_PATH = _project_root / "services" / "config.yaml"
# ------------------------------------------------------------------------------


def _resolve_stations(station_overrides: list[str], config_path: Path) -> list[NWPStation]:
    if station_overrides:
        return [nwp_station_for_icao(icao) for icao in station_overrides]

    import yaml
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    series = get_event_series(cfg, "research")
    stations = nwp_stations_for_series(series)

    if not stations:
        logger.error("No research stations configured in config.yaml")
        sys.exit(1)
    return stations


def run_latest(
    fetcher: HRRRFetcher,
    stations: list[NWPStation],
    max_fxx: int | None = None,
) -> None:
    fxx_range = range(0, max_fxx + 1) if max_fxx is not None else None
    df = fetcher.fetch_latest(stations, fxx_range=fxx_range, save=True)

    if df.empty:
        print("No HRRR data available.")
        return

    cycle = df["cycle_utc"].iloc[0]
    fmin = df["forecast_minutes"].min()
    fmax = df["forecast_minutes"].max()
    print(f"\nFetched HRRR cycle {cycle}")
    print(f"  Stations       : {', '.join(df['station'].unique())}")
    print(f"  Forecast range : {fmin}–{fmax} min")
    print(f"  Rows           : {len(df)}")


def run_backfill(
    fetcher: HRRRFetcher,
    stations: list[NWPStation],
    start: date,
    end: date,
    cycles: list[int] | None = None,
    max_fxx: int | None = None,
    rolling_lead_minutes: int | None = None,
) -> None:
    cycles_list = cycles
    fxx_range = range(0, max_fxx + 1) if max_fxx is not None else None
    if rolling_lead_minutes is not None:
        fxx_range = None  # fetch_date_range will set it from rolling_lead_minutes

    stn_names = ", ".join(s.city for s in stations)
    print(f"\nHRRR backfill: {start} → {end}")
    print(f"  Stations: {stn_names} ({len(stations)} total)")
    print(f"  Cycles  : {cycles_list or 'default (all 24)'}")
    if rolling_lead_minutes is not None:
        print(f"  Rolling : {rolling_lead_minutes} min (2h-out forecast)")
    else:
        print(f"  Max fxx : {max_fxx or fetcher.max_forecast_hour}")
    print()

    df = fetcher.fetch_date_range(
        start, end, stations,
        cycles=cycles_list, fxx_range=fxx_range,
        rolling_lead_minutes=rolling_lead_minutes, save=True,
    )

    if df.empty:
        print("No data fetched.")
        return

    print(f"\nBackfill complete:")
    print(f"  Total rows: {len(df)}")
    for icao in sorted(df["station"].unique()):
        n = len(df[df["station"] == icao])
        print(f"    {icao}: {n} rows")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    fetcher = HRRRFetcher.from_config(CONFIG_PATH)
    stations = _resolve_stations(STATIONS, CONFIG_PATH)

    logger.info("HRRR stations: %s", [s.icao for s in stations])

    if MODE == "latest":
        run_latest(fetcher, stations, MAX_FXX)
    elif MODE == "backfill":
        end = END_DATE if END_DATE else utc_today() - timedelta(days=1)
        cycles = [int(c) for c in CYCLES.split(",")] if CYCLES else None
        run_backfill(fetcher, stations, START_DATE, end, cycles, MAX_FXX)
    elif MODE == "backfill_rolling_2h":
        end = END_DATE if END_DATE else utc_today() - timedelta(days=1)
        start = START_DATE if START_DATE else end - timedelta(days=14)
        cycles = [int(c) for c in CYCLES.split(",")] if CYCLES else None
        run_backfill(
            fetcher, stations, start, end, cycles,
            rolling_lead_minutes=ROLLING_2H_MINUTES,
        )
    else:
        logger.error("MODE must be 'latest', 'backfill', or 'backfill_rolling_2h'")
        sys.exit(1)


if __name__ == "__main__":
    main()
