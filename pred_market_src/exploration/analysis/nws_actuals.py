"""
Fetch NWS actual high temperatures for NYC (Central Park, KNYC).

Per project_rules/weather_prediction_rules.md, official resolution uses
the NWS Daily Climatological Report from KNYC (Central Park).

Data sources:
- NOAA Climate Data Online (CDO): https://www.ncdc.noaa.gov/cdo-web/
- NWS API: https://api.weather.gov/
- Mesonet / GHCN: https://www.ncei.noaa.gov/products/land-based-station

For historical analysis, compare Kalshi implied probabilities vs actual outcomes.
"""

from datetime import datetime
from pathlib import Path

# Placeholder for NWS/NOAA data integration
# Common approach: use NOAA GHCN or NWS observations API


def get_nws_station_obs(station_id: str, date: datetime) -> dict | None:
    """
    Fetch NWS observations for a station on a given date.

    Station IDs: KNYC (NYC Central Park), etc.
    Returns dict with 'high_f', 'high_c', 'source', etc.
    """
    # TODO: Implement via NWS API or NOAA CDO
    # NWS observations: https://api.weather.gov/stations/{station}/observations
    # GHCN daily: https://www.ncei.noaa.gov/cdo-web/api/v2/data
    return None


def load_actuals_csv(path: str | Path) -> list[dict]:
    """
    Load actual highs from a CSV file for analysis.

    Expected format: date,high_f,station
    Example:
        2026-02-10,42,KNYC
        2026-02-09,38,KNYC
    """
    import csv

    rows = []
    with open(path) as f:
        for r in csv.DictReader(f):
            rows.append(r)
    return rows
