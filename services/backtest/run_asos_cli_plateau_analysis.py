"""CLI for ASOS vs CLI high plateau analysis.

Compares Synoptic ASOS 1-minute temperatures with NWS CLI daily high. Analyzes
how often the "stability plateau" max (peak of N consecutive same-integer-round
ASOS observations) matches the official NWS CLI high, compared to the raw ASOS max.

Usage:
    python -m services.backtest.run_asos_cli_plateau_analysis --station KMDW
    python -m services.backtest.run_asos_cli_plateau_analysis --station KMDW --start 2026-02-08 --end 2026-02-20
    python -m services.backtest.run_asos_cli_plateau_analysis --station KMDW --min-consecutive 3
    python -m services.backtest.run_asos_cli_plateau_analysis --station KMDW --export asos_cli_report.csv
"""

from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path

from services.core.config import load_config, configure_logging
from services.backtest.asos_cli_plateau_analyzer import AsosCliPlateauAnalyzer
from services.markets.kalshi_registry import KALSHI_MARKET_REGISTRY


def main():
    parser = argparse.ArgumentParser(
        description="ASOS vs CLI high: compare Synoptic ASOS 1-min plateau temps with NWS CLI daily high.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument(
        "--station", default="KMDW",
        help="ICAO station ID (e.g. KMDW, KNYC). Default: KMDW",
    )
    parser.add_argument(
        "--tz", default=None,
        help="IANA timezone (auto-detected from station registry if omitted)",
    )
    parser.add_argument(
        "--lat", type=float, default=None,
        help="Station latitude (auto from registry). Use for LST hemisphere: lat<0 → Jul 15, else Jan 15.",
    )
    parser.add_argument(
        "--asos-source", choices=["synoptic", "iem"], default="synoptic",
        help="ASOS 1-min source: synoptic or iem (default: synoptic)",
    )
    parser.add_argument("--start", default=None, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", default=None, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--min-consecutive", type=int, default=2,
        help="Minimum consecutive same-round observations for a plateau (default: 2)",
    )
    parser.add_argument("--export", default=None, help="Export results to CSV")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    configure_logging(args.log_level)

    config, config_path = load_config(args.config)
    data_dir = (config_path.parent / config.get("storage", {}).get("data_dir", "../data")).resolve()

    # Auto-detect timezone and latitude from station registry
    tz_name = args.tz
    lat = args.lat
    if lat is None:
        for mc in KALSHI_MARKET_REGISTRY.values():
            if mc.icao == args.station:
                if tz_name is None:
                    tz_name = mc.tz
                lat = mc.lat
                break
    if tz_name is None:
        tz_name = "America/Chicago"
        logging.warning(
            "Station %s not found in registry, using default tz %s",
            args.station, tz_name,
        )

    analyzer = AsosCliPlateauAnalyzer(
        data_dir=str(data_dir),
        station=args.station,
        tz_name=tz_name,
        min_consecutive=args.min_consecutive,
        lat=lat,
        asos_source=args.asos_source,
    )

    start_date = date.fromisoformat(args.start) if args.start else None
    end_date = date.fromisoformat(args.end) if args.end else None

    report = analyzer.run(start_date=start_date, end_date=end_date)
    report.log_summary()
    report.print_table()

    if args.export:
        df = report.to_dataframe()
        df.to_csv(args.export, index=False)
        print(f"\nExported to {args.export}")


if __name__ == "__main__":
    main()
