"""Fetch NWP model data (HRRR / RTMA-RU / RRFS / NBM) for research stations.

Unified CLI for all NWP point-extraction fetchers.

Usage:
    # HRRR 15-minute sub-hourly — latest cycle
    python -m research.download_data.run_nwp_collection hrrr latest

    # HRRR backfill
    python -m research.download_data.run_nwp_collection hrrr backfill \\
        --start 2026-02-01 --end 2026-02-20

    # RTMA-RU latest analysis
    python -m research.download_data.run_nwp_collection rtma_ru latest

    # RRFS backfill (hourly, prototype data up to Nov 2024)
    python -m research.download_data.run_nwp_collection rrfs backfill \\
        --start 2024-08-01 --end 2024-08-15

    # NBM latest forecast
    python -m research.download_data.run_nwp_collection nbm latest

    # NBM backfill
    python -m research.download_data.run_nwp_collection nbm backfill \\
        --start 2026-02-01 --end 2026-02-20 --cycles 1,7,13,19

    # Override stations or forecast depth
    python -m research.download_data.run_nwp_collection hrrr latest --station KMDW KNYC --max-fxx 6
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
from research.download_data.nwp_base import NWPPointFetcher
from research.weather.hrrr_station_registry import (
    hrrr_station_for_icao,
    hrrr_stations_for_series,
    HRRRStation,
)

logger = logging.getLogger(__name__)

MODEL_REGISTRY: dict[str, type[NWPPointFetcher]] = {}


def _load_models() -> None:
    """Lazy-import fetcher classes to populate MODEL_REGISTRY."""
    if MODEL_REGISTRY:
        return
    from research.download_data.hrrr import HRRRFetcher
    from research.download_data.rtma_ru import RTMARUFetcher
    from research.download_data.rrfs import RRFSFetcher
    from research.download_data.nbm import NBMFetcher

    MODEL_REGISTRY["hrrr"] = HRRRFetcher
    MODEL_REGISTRY["rtma_ru"] = RTMARUFetcher
    MODEL_REGISTRY["rrfs"] = RRFSFetcher
    MODEL_REGISTRY["nbm"] = NBMFetcher


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


def cmd_latest(args, fetcher: NWPPointFetcher, stations: list[HRRRStation]) -> None:
    fxx_range = range(0, args.max_fxx + 1) if args.max_fxx is not None else None
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


def cmd_backfill(args, fetcher: NWPPointFetcher, stations: list[HRRRStation]) -> None:
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end) if args.end else utc_today() - timedelta(days=1)

    cycles = [int(c) for c in args.cycles.split(",")] if args.cycles else None
    fxx_range = range(0, args.max_fxx + 1) if args.max_fxx is not None else None

    stn_names = ", ".join(s.city for s in stations)
    default_cycles = cycles or fetcher.DEFAULT_CYCLES
    print(f"\n{fetcher.SOURCE_NAME.upper()} backfill: {start} → {end}")
    print(f"  Stations: {stn_names} ({len(stations)} total)")
    print(f"  Cycles  : {default_cycles}")
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
    _load_models()

    parser = argparse.ArgumentParser(
        description="Fetch NWP model data (HRRR / RTMA-RU / RRFS / NBM) for research stations",
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
        help="Max forecast hour to fetch (default: model-specific)",
    )

    # First positional: model name
    parser.add_argument(
        "model", choices=list(MODEL_REGISTRY.keys()),
        help="NWP model to fetch",
    )

    sub = parser.add_subparsers(dest="mode", required=True)
    sub.add_parser("latest", help="Fetch the most recent available cycle")

    bp = sub.add_parser("backfill", help="Fetch data for a date range")
    bp.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    bp.add_argument("--end", default=None, help="End date (YYYY-MM-DD, default: yesterday)")
    bp.add_argument(
        "--cycles", default=None,
        help="Comma-separated cycle hours (default: model-specific)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    config_path = Path(args.config)
    fetcher_cls = MODEL_REGISTRY[args.model]
    fetcher = fetcher_cls.from_config(config_path)
    stations = _resolve_stations(args, config_path)

    logger.info("%s stations: %s", fetcher.SOURCE_NAME.upper(), [s.icao for s in stations])

    if args.mode == "latest":
        cmd_latest(args, fetcher, stations)
    elif args.mode == "backfill":
        cmd_backfill(args, fetcher, stations)


if __name__ == "__main__":
    main()
