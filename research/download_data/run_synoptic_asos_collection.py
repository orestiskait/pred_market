"""Run Synoptic ASOS 1-min backfill into synoptic_weather_observations.

Fetches from Synoptic Time Series REST API and merges into the same storage
used by the live WebSocket collector. Live data takes priority when both exist.

Usage:
    python -m research.download_data.run_synoptic_asos_collection --station KMDW --days 365
    python -m research.download_data.run_synoptic_asos_collection --station KMDW --start 2025-01-01 --end 2025-02-22
    python -m research.download_data.run_synoptic_asos_collection --station KMDW --days 365 --skip-live
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

from services.core.config import load_config, get_synoptic_token
from research.download_data.synoptic_backfill import backfill_range


def main():
    parser = argparse.ArgumentParser(
        description="Backfill Synoptic ASOS 1-min into synoptic_weather_observations",
    )
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument(
        "--station",
        default="KMDW",
        help="ICAO station ID (e.g. KMDW, KNYC). Default: KMDW",
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
        "--skip-live",
        action="store_true",
        help="Skip dates that already have live data (avoid overwriting)",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    config, config_path = load_config(args.config)
    token = get_synoptic_token(config)
    data_dir = (config_path.parent / config.get("storage", {}).get("data_dir", "../data")).resolve()

    if args.days is not None:
        end_date = date.today() - timedelta(days=1)
        start_date = end_date - timedelta(days=args.days - 1)
    else:
        if not args.start or not args.end:
            parser.error("Use --days or both --start and --end")
        start_date = date.fromisoformat(args.start)
        end_date = date.fromisoformat(args.end)

    print(f"Backfilling Synoptic ASOS 1-min for {args.station}: {start_date} to {end_date}")
    print(f"Target: {data_dir}/synoptic_weather_observations/")

    merged = backfill_range(
        icao=args.station,
        start_date=start_date,
        end_date=end_date,
        token=token,
        data_dir=str(data_dir),
        skip_dates_with_live=args.skip_live,
    )

    print(f"Merged {merged} day(s) of data")


if __name__ == "__main__":
    main()
