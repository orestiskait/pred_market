"""Script to run weather data collection.

Usage:
  1. Configure the DATE SETTINGS below.
  2. Run: python pred_market_src/collector/run_weather.py
"""

import logging
import sys
from datetime import date, timedelta

from pred_market_src.collector.tz import utc_today
from pathlib import Path

# MEANINGFUL VARIABLES
# ------------------------------------------------------------------------------
# If START_DATE is set, fetches the range [START_DATE, END_DATE].
# If START_DATE is None, fetches only END_DATE.
# If END_DATE is None, defaults to yesterday (relative to run time).

START_DATE = date(2026, 2, 1)  # e.g., date(2026, 2, 1)
END_DATE   = None  # e.g., date(2026, 2, 18) -- None means "Yesterday"
# ------------------------------------------------------------------------------

# Ensure project root is on sys.path
_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from pred_market_src.collector.weather.observations import WeatherObservations


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-30s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )

    # Resolve dates
    target_end = END_DATE if END_DATE else utc_today() - timedelta(days=1)
    target_start = START_DATE if START_DATE else target_end

    config_path = Path(__file__).resolve().parent / "config.yaml"
    
    # Initialize the coordinator
    obs = WeatherObservations.from_config(config_path)
    
    station_codes = [s.city for s in obs.stations]
    
    if target_start < target_end:
        print(f"\nRunning weather collection from {target_start} to {target_end}")
        print(f"Stations: {', '.join(station_codes)} ({len(obs.stations)} total)")
        results = obs.collect_date_range(target_start, target_end)
    else:
        print(f"\nRunning weather collection for {target_end}")
        print(f"Stations: {', '.join(station_codes)} ({len(obs.stations)} total)")
        results = obs.collect_all(target_end)
    
    print("\nSummary:")
    for source, df in results.items():
        if not df.empty:
            print(f"  {source}: {len(df)} rows")
        else:
            print(f"  {source}: No data")


if __name__ == "__main__":
    main()
