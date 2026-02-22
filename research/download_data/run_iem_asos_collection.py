"""Run IEM ASOS 1-min bulk backfill.

Uses a single bulk API request per range (more reliable than per-day).
IEM has ~24h delay; data through yesterday is typically available.

Usage:
    python -m research.download_data.run_iem_asos_collection --station KMDW --start 2026-01-14 --end 2026-02-21
    python -m research.download_data.run_iem_asos_collection --station KMDW --days 39
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

from services.core.config import load_config
from research.download_data.iem_asos_1min import IEMASOS1MinFetcher
from research.weather.iem_awc_station_registry import station_for_icao


def main():
    parser = argparse.ArgumentParser(
        description="Bulk backfill IEM ASOS 1-min into iem_asos_1min/",
    )
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument(
        "--station",
        default="KMDW",
        help="ICAO station ID (e.g. KMDW). Default: KMDW",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Number of days to fetch (ending yesterday). Overrides --start/--end.",
    )
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-fetch and overwrite existing days",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(name)-25s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )

    config, config_path = load_config(args.config)
    data_dir = (config_path.parent / config.get("storage", {}).get("data_dir", "../data")).resolve()

    if args.days:
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=args.days - 1)
    else:
        start_date = date.fromisoformat(args.start) if args.start else date.today() - timedelta(days=7)
        end_date = date.fromisoformat(args.end) if args.end else date.today() - timedelta(days=1)

    stn = station_for_icao(args.station)
    fetcher = IEMASOS1MinFetcher(data_dir=data_dir)

    print(f"IEM ASOS 1-min bulk backfill: {args.station} ({stn.iata})")
    print(f"  Range: {start_date} → {end_date}")
    print(f"  Output: {fetcher.data_dir}/")

    saved = fetcher.fetch_range_bulk_and_save(
        stn,
        start_date,
        end_date,
        skip_existing=not args.no_skip_existing,
    )
    print(f"  Saved: {saved} days")


if __name__ == "__main__":
    main()
