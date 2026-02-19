"""Script to run weather data collection.

Coordinates fetching ASOS (IEM), METAR (AWC), and Daily Climate (CLI) data.
"""

from datetime import date, timedelta
import logging
from pathlib import Path
import sys

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

    config_path = Path(__file__).resolve().parent / "config.yaml"
    
    # Initialize the coordinator
    obs = WeatherObservations.from_config(config_path)

    # Fetch for yesterday (default)
    target_date = date.today() - timedelta(days=1)
    
    print(f"\nRunning weather collection for {target_date}...")
    
    results = obs.collect_all(target_date)
    
    print("\nSummary:")
    for source, df in results.items():
        if not df.empty:
            print(f"  {source}: {len(df)} rows")
        else:
            print(f"  {source}: No data")

if __name__ == "__main__":
    main()
