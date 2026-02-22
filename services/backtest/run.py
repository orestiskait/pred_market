"""CLI entry point for the backtesting framework.

Usage:
    python -m services.backtest.run --start 2026-02-20 --end 2026-02-21
    python -m services.backtest.run --start 2026-02-21 --end 2026-02-21 --series KXHIGHCHI
    python -m services.backtest.run --start 2026-02-20 --end 2026-02-22 --latency fixed_180
    python -m services.backtest.run --start 2026-02-20 --end 2026-02-21 --export results.csv

Options:
    --config       Path to config.yaml (auto-detected if omitted)
    --start        Start date (YYYY-MM-DD) — inclusive
    --end          End date (YYYY-MM-DD) — inclusive
    --series       Limit to specific series (e.g. KXHIGHCHI KXHIGHNY)
    --latency      Latency model: "actual" (default) or "fixed_N" (e.g. "fixed_180")
    --export       Export fills to CSV file
    --log-level    Logging level (default: INFO)
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from services.core.config import load_config, configure_logging
from services.backtest.engine import BacktestEngine


def main():
    parser = argparse.ArgumentParser(
        description="Backtest Kalshi weather trading strategies against historical data.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--series", nargs="+", default=None, help="Limit to specific series")
    parser.add_argument("--latency", default="actual", help="Latency model: actual | fixed_N")
    parser.add_argument("--export", default=None, help="Export fills to CSV")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    args = parser.parse_args()

    configure_logging(args.log_level)

    config, config_path = load_config(args.config)

    # Resolve data directory
    data_dir = (config_path.parent / config.get("storage", {}).get("data_dir", "../data")).resolve()

    start_date = date.fromisoformat(args.start)
    end_date = date.fromisoformat(args.end)

    engine = BacktestEngine(
        config=config,
        data_dir=str(data_dir),
        start_date=start_date,
        end_date=end_date,
        series_filter=args.series,
        latency_model=args.latency,
    )

    result = engine.run()

    if args.export:
        result.to_csv(args.export)

    # Print final summary to stdout
    df = result.to_dataframe()
    if not df.empty:
        print("\n" + "=" * 60)
        print("FILLS TABLE")
        print("=" * 60)
        print(df.to_string(index=False))
        print(f"\nTotal: {result.n_fills} fills, "
              f"{result.total_contracts} contracts, "
              f"${result.total_cost_cents / 100:.2f}")
    else:
        print("\nNo fills during backtest period.")


if __name__ == "__main__":
    main()
