"""Pull METAR (hourly + SPECI) for all days that have both IEM and Synoptic ASOS data.

Uses the same overlap dates as the ASOS vs CLI plateau comparison. Fetches from
AWC API and saves to awc_metar/ (KMDW_YYYY-MM-DD.parquet).

Usage:
    python -m research.download_data.run_metar_collection --station KMDW
    python -m research.download_data.run_metar_collection --station KMDW --start 2026-02-01 --end 2026-02-20
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from services.core.config import load_config
from services.backtest.asos_cli_plateau_analyzer import AsosCliPlateauAnalyzer
from research.download_data.awc_metar import AWCMETARFetcher
from research.weather.iem_awc_station_registry import station_for_icao


def main():
    parser = argparse.ArgumentParser(
        description="Pull METAR/SPECI for overlap days (IEM + Synoptic + CLI)",
    )
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument(
        "--station",
        default="KMDW",
        help="ICAO station ID (e.g. KMDW, KNYC). Default: KMDW",
    )
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD (filter overlap)")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD (filter overlap)")
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-fetch and overwrite existing METAR files",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    config, config_path = load_config(args.config)
    data_dir = (config_path.parent / config.get("storage", {}).get("data_dir", "../data")).resolve()

    overlap, n_iem, n_synoptic = AsosCliPlateauAnalyzer.overlap_dates(data_dir, args.station)
    if not overlap:
        print("No overlap: no days with both IEM and Synoptic ASOS data (and CLI).")
        return

    if args.start:
        start_date = date.fromisoformat(args.start)
        overlap = [d for d in overlap if d >= start_date]
    if args.end:
        end_date = date.fromisoformat(args.end)
        overlap = [d for d in overlap if d <= end_date]

    if not overlap:
        print("No overlap dates in the specified range.")
        return

    stn = station_for_icao(args.station)
    fetcher = AWCMETARFetcher(data_dir=data_dir)

    print(f"METAR/SPECI collection for {args.station} ({stn.iata})")
    print(f"  Overlap days (IEM ∩ Synoptic ∩ CLI): {len(overlap)}")
    print(f"  Range: {overlap[0]} → {overlap[-1]}")
    print(f"  Output: {fetcher.data_dir}/")

    saved = 0
    for d in overlap:
        if not args.no_skip_existing and fetcher.check_exists(stn, d):
            continue
        try:
            df = fetcher.fetch(stn, d)
            if not df.empty:
                path = fetcher.data_dir / f"{stn.icao}_{d.isoformat()}.parquet"
                df.to_parquet(path, index=False)
                saved += 1
        except Exception:
            logging.exception("Failed to fetch METAR for %s on %s", args.station, d)

    print(f"  Saved: {saved} days")


if __name__ == "__main__":
    main()
