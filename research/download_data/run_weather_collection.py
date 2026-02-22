"""Run weather data collection for all configured stations.

Fetches IEM ASOS 1-min, AWC METAR, and IEM Daily Climate for the
date range specified below. Uses services/config.yaml for station list.

Usage:
  1. Set START_DATE and END_DATE below.
  2. Run: python -m research.download_data.run_weather_collection
"""

import logging
import sys
from datetime import date, timedelta
from pathlib import Path

# Ensure project root is on sys.path
_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from services.tz import utc_today
from research.weather.iem_awc_data_collector import IEMAWCDataCollector

# ------------------------------------------------------------------------------
# Date settings
# If START_DATE is set, fetches the range [START_DATE, END_DATE].
# If START_DATE is None, fetches only END_DATE.
# If END_DATE is None, defaults to yesterday (relative to run time).
# ------------------------------------------------------------------------------
START_DATE = date(2026, 2, 1)  # e.g., date(2026, 2, 1)
END_DATE = None  # e.g., date(2026, 2, 18) -- None means "Yesterday"
# ------------------------------------------------------------------------------


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-30s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )

    target_end = END_DATE if END_DATE else utc_today() - timedelta(days=1)
    target_start = START_DATE if START_DATE else target_end

    config_path = _project_root / "services" / "config.yaml"
    collector = IEMAWCDataCollector.from_config(config_path)

    station_codes = [s.city for s in collector.stations]

    if target_start < target_end:
        print(f"\nRunning weather collection from {target_start} to {target_end}")
        print(f"Stations: {', '.join(station_codes)} ({len(collector.stations)} total)")
        results = collector.collect_date_range(target_start, target_end)
    else:
        print(f"\nRunning weather collection for {target_end}")
        print(f"Stations: {', '.join(station_codes)} ({len(collector.stations)} total)")
        results = collector.collect_all(target_end)

    print("\nSummary:")
    for source, df in results.items():
        if not df.empty:
            print(f"  {source}: {len(df)} rows")
        else:
            print(f"  {source}: No data")


if __name__ == "__main__":
    main()
