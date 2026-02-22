"""Fetch HRRR 15-minute sub-hourly model data for configured research stations.

Convenience wrapper around the unified NWP runner.  For multi-model usage
(HRRR / RTMA-RU / RRFS) prefer ``run_nwp_collection.py``.

Usage:
    # Latest cycle (suitable for cron / file-watcher trigger)
    python -m research.download_data.run_hrrr_collection latest

    # Historical backfill
    python -m research.download_data.run_hrrr_collection backfill --start 2026-02-01 --end 2026-02-20

    # Latest, specific station
    python -m research.download_data.run_hrrr_collection latest --station KMDW

    # Backfill, specific cycles and forecast hours
    python -m research.download_data.run_hrrr_collection backfill \\
        --start 2026-02-01 --end 2026-02-05 \\
        --cycles 0,12 --max-fxx 6
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from services.core.config import get_event_series
from services.tz import utc_today
from research.download_data.hrrr import HRRRFetcher
from research.weather.hrrr_station_registry import (
    hrrr_station_for_icao,
    hrrr_stations_for_series,
    HRRRStation,
)

logger = logging.getLogger(__name__)


def _resolve_stations(args, config_path: Path) -> list[HRRRStation]:
    if args.station:
        return [hrrr_station_for_icao(icao) for icao in args.station]

    import yaml
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    series = get_event_series(cfg, "research")
    stations = hrrr_stations_for_series(series)

    if not stations:
        logger.error("No research stations configured in config.yaml")
        sys.exit(1)
    return stations


def cmd_latest(args, fetcher: HRRRFetcher, stations: list[HRRRStation]) -> None:
    fxx_range = range(0, args.max_fxx + 1) if args.max_fxx is not None else None
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


def cmd_backfill(args, fetcher: HRRRFetcher, stations: list[HRRRStation]) -> None:
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else utc_today() - timedelta(days=1)

    cycles = [int(c) for c in args.cycles.split(",")] if args.cycles else None
    fxx_range = range(0, args.max_fxx + 1) if args.max_fxx is not None else None

    stn_names = ", ".join(s.city for s in stations)
    print(f"\nHRRR backfill: {start} → {end}")
    print(f"  Stations: {stn_names} ({len(stations)} total)")
    print(f"  Cycles  : {cycles or 'default (all 24)'}")
    print(f"  Max fxx : {args.max_fxx or fetcher.max_forecast_hour}\n")

    df = fetcher.fetch_date_range(
        start, end, stations,
        cycles=cycles, fxx_range=fxx_range, save=True,
    )

    if df.empty:
        print("No data fetched.")
        return

    print(f"\nBackfill complete:")
    print(f"  Total rows: {len(df)}")
    for icao in sorted(df["station"].unique()):
        n = len(df[df["station"] == icao])
        print(f"    {icao}: {n} rows")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch HRRR 15-min sub-hourly data for research stations",
    )
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument(
        "--config",
        default=str(_project_root / "services" / "config.yaml"),
        help="Path to config.yaml",
    )
    parser.add_argument(
        "--station", nargs="*", metavar="ICAO",
        help="Override stations by ICAO code (e.g. KMDW KNYC)",
    )
    parser.add_argument(
        "--max-fxx", type=int, default=None,
        help="Max forecast hour to fetch (default: from config or 18)",
    )

    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser("latest", help="Fetch the most recent available HRRR cycle")

    bp = sub.add_parser("backfill", help="Fetch HRRR data for a date range")
    bp.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    bp.add_argument("--end", default=None, help="End date (YYYY-MM-DD, default: yesterday)")
    bp.add_argument(
        "--cycles", default=None,
        help="Comma-separated cycle hours (default: all 24)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    config_path = Path(args.config)
    fetcher = HRRRFetcher.from_config(config_path)
    stations = _resolve_stations(args, config_path)

    logger.info("HRRR stations: %s", [s.icao for s in stations])

    if args.mode == "latest":
        cmd_latest(args, fetcher, stations)
    elif args.mode == "backfill":
        cmd_backfill(args, fetcher, stations)


if __name__ == "__main__":
    main()
