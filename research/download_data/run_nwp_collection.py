"""Fetch NWP model data (HRRR / RTMA-RU / RRFS / NBM) for research stations.

.. deprecated::
   Use ``research.download_data.backfill_nwp`` instead.
   It saves to data/weather/nwp_realtime/{model}/ (same as the live system),
   applies LST-day clamping, downloads fxx files in parallel, and writes
   proper backfill metadata (notification_ts_utc, saved_ts_utc, is_live=False).

Usage:
  1. Set MODEL, MODE, START_DATE, END_DATE, etc. below.
  2. Run: python -m research.download_data.run_nwp_collection
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
from services.weather.nwp.base import NWPPointFetcher
from services.weather.nwp import _load_models, MODEL_REGISTRY
from services.weather.station_registry import (
    nwp_station_for_icao,
    nwp_stations_for_series,
    NWPStation,
)

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# Config — edit these when running as script
# ------------------------------------------------------------------------------
MODEL = "hrrr"  # hrrr | rtma_ru | rrfs | nbm
MODE = "latest"  # "latest" or "backfill"
START_DATE = date(2026, 2, 1)  # for backfill
END_DATE = None  # for backfill; None means yesterday
STATIONS = []  # e.g. ["KMDW", "KNYC"] — empty = use config.yaml
CYCLES = None  # e.g. "0,12,19" for NBM — None = model default
MAX_FXX = None  # max forecast hour; None = model default
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
    fetcher: NWPPointFetcher,
    stations: list[NWPStation],
    max_fxx: int | None = None,
) -> None:
    fxx_range = range(0, max_fxx + 1) if max_fxx is not None else None
    df = fetcher.fetch_latest(stations, fxx_range=fxx_range, save=True)

    if df.empty:
        print(f"No {fetcher.SOURCE_NAME.upper()} data available.")
        return

    cycle = df["cycle_utc"].iloc[0]
    fmin = df["forecast_minutes"].min()
    fmax = df["forecast_minutes"].max()
    print(f"\nFetched {fetcher.SOURCE_NAME.upper()} cycle {cycle}")
    print(f"  Stations       : {', '.join(df['station'].unique())}")
    print(f"  Forecast range : {fmin}–{fmax} min")
    print(f"  Rows           : {len(df)}")


def run_backfill(
    fetcher: NWPPointFetcher,
    stations: list[NWPStation],
    start: date,
    end: date,
    cycles: list[int] | None = None,
    max_fxx: int | None = None,
) -> None:
    cycles_list = cycles
    fxx_range = range(0, max_fxx + 1) if max_fxx is not None else None

    stn_names = ", ".join(s.city for s in stations)
    default_cycles = cycles_list or fetcher.DEFAULT_CYCLES
    print(f"\n{fetcher.SOURCE_NAME.upper()} backfill: {start} → {end}")
    print(f"  Stations: {stn_names} ({len(stations)} total)")
    print(f"  Cycles  : {default_cycles}")
    print(f"  Max fxx : {max_fxx or fetcher.max_forecast_hour}\n")

    df = fetcher.fetch_date_range(
        start, end, stations,
        cycles=cycles_list, fxx_range=fxx_range, save=True,
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
    _load_models()

    if MODEL not in MODEL_REGISTRY:
        logger.error("MODEL must be one of: %s", list(MODEL_REGISTRY.keys()))
        sys.exit(1)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    fetcher_cls = MODEL_REGISTRY[MODEL]
    fetcher = fetcher_cls.from_config(CONFIG_PATH)
    stations = _resolve_stations(STATIONS, CONFIG_PATH)

    logger.info("%s stations: %s", fetcher.SOURCE_NAME.upper(), [s.icao for s in stations])

    if MODE == "latest":
        run_latest(fetcher, stations, MAX_FXX)
    elif MODE == "backfill":
        end = END_DATE if END_DATE else utc_today() - timedelta(days=1)
        cycles = [int(c) for c in CYCLES.split(",")] if CYCLES else None
        run_backfill(fetcher, stations, START_DATE, end, cycles, MAX_FXX)
    else:
        logger.error("MODE must be 'latest' or 'backfill'")
        sys.exit(1)


if __name__ == "__main__":
    main()
