"""NWS Daily Climate Report (CLI) fetcher via IEM JSON API.

Source: https://mesonet.agron.iastate.edu/json/cli.py

The CLI product is the **official** NWS daily climate report that Kalshi
uses to resolve temperature-high contracts.  It contains:
  - Official daily high / low (°F, whole degrees)
  - Time of high / low
  - Record high / low and years
  - Normal high / low
  - Departure from normal
  - Precipitation, snow, etc.

Published once daily, typically early morning after midnight local time
(e.g. ~06:00 UTC for eastern stations).

For betting, the `high` field is the ground truth for contract resolution.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

from pred_market_src.collector.weather.base import WeatherFetcherBase
from pred_market_src.collector.weather.stations import StationInfo

logger = logging.getLogger(__name__)

IEM_CLI_URL = "https://mesonet.agron.iastate.edu/json/cli.py"


class DailyClimateFetcher(WeatherFetcherBase):
    """Fetch NWS Daily Climate Reports (CLI) from the IEM archive.

    Each row represents one day's official climate summary for a station.
    The `high` column is the official daily high temperature used by Kalshi
    for contract resolution.

    Key fields for betting analysis:
      - high / low                : Official temps (°F)
      - high_time / low_time      : When the high/low occurred
      - high_record / high_record_years : Historical record context
      - high_normal / high_depart : Climatological context
    """

    SOURCE_NAME = "daily_climate"
    EXPECTED_DAILY_ROWS = 1

    def __init__(self, data_dir: Path | str | None = None, timeout: int = 15):
        super().__init__(data_dir)
        self.timeout = timeout

    def fetch(
        self,
        station: StationInfo,
        target_date: date,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch the CLI report for a specific station and date.

        If the CLI report has not yet been published (e.g., target_date is
        today and it's before ~06:00 UTC), this will return an empty DF.

        Parameters
        ----------
        station : StationInfo
        target_date : date

        Returns
        -------
        pd.DataFrame with one row per day that matched.
        """
        # The IEM CLI API fetches by year; we filter to target_date.
        params = {
            "station": station.icao,
            "year": target_date.year,
        }

        logger.info("Fetching CLI for %s, year=%d", station.icao, target_date.year)

        resp = requests.get(IEM_CLI_URL, params=params, timeout=self.timeout)
        resp.raise_for_status()

        data = resp.json()
        results = data.get("results", [])
        if not results:
            logger.warning("No CLI data returned for %s year=%d",
                           station.icao, target_date.year)
            return pd.DataFrame()

        # Filter to the requested date
        target_str = target_date.isoformat()
        matching = [r for r in results if r.get("valid") == target_str]

        if not matching:
            logger.warning("No CLI report found for %s on %s (may not be published yet)",
                           station.icao, target_date)
            return pd.DataFrame()

        rows = []
        for r in matching:
            row = {
                "station": station.icao,
                "station_iata": station.iata,
                "city": station.city,
                "timezone": station.tz,
                "valid_date": r["valid"],
                "cli_city_name": r.get("name", ""),
                "wfo": r.get("wfo", ""),
                # --- Official temps (the settlement values) ---
                "high_f": _safe_int(r.get("high")),
                "low_f": _safe_int(r.get("low")),
                "high_time": _format_time(r.get("high_time")),
                "low_time": _format_time(r.get("low_time")),
                "high_time_local": _parse_local_time(r["valid"], r.get("high_time")),
                "low_time_local": _parse_local_time(r["valid"], r.get("low_time")),
                # --- Climatological context ---
                "high_normal": _safe_int(r.get("high_normal")),
                "high_depart": _safe_int(r.get("high_depart")),
                "high_record": _safe_int(r.get("high_record")),
                "high_record_years": str(r.get("high_record_years", [])),
                "low_normal": _safe_int(r.get("low_normal")),
                "low_depart": _safe_int(r.get("low_depart")),
                "low_record": _safe_int(r.get("low_record")),
                "low_record_years": str(r.get("low_record_years", [])),
                # --- Precipitation ---
                "precip_in": _safe_float(r.get("precip")),
                "snow_in": _safe_float(r.get("snow")),
                "snowdepth_in": _safe_float(r.get("snowdepth")),
                # --- Wind ---
                "avg_wind_speed": _safe_float(r.get("average_wind_speed")),
                "highest_wind_speed": _safe_int(r.get("highest_wind_speed")),
                "highest_gust_speed": _safe_int(r.get("highest_gust_speed")),
                # --- Sky ---
                "avg_sky_cover": _safe_float(r.get("average_sky_cover")),
                # --- Link to raw product ---
                "cli_product_id": r.get("product", ""),
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        df["valid_date"] = pd.to_datetime(df["valid_date"]).dt.date
        # valid_utc: synthetic sentinel (midnight UTC on the report date).
        # Required by the base class for deduplication.
        # The actual observation covers the full LOCAL calendar day — see valid_date.
        df["valid_utc"] = pd.to_datetime(df["valid_date"])
        df["valid_utc"] = df["valid_utc"].dt.tz_localize("UTC")

        logger.info("Got CLI report for %s on %s: high=%s°F, low=%s°F",
                     station.icao, target_date,
                     df["high_f"].iloc[0] if not df.empty else "?",
                     df["low_f"].iloc[0] if not df.empty else "?")
        return df

    def fetch_year(
        self,
        station: StationInfo,
        year: int,
    ) -> pd.DataFrame:
        """Fetch all CLI reports for a station for an entire year.

        Useful for backtesting: get all official daily highs for a year
        to compare against market predictions.
        """
        params = {
            "station": station.icao,
            "year": year,
        }

        logger.info("Fetching full-year CLI for %s, year=%d", station.icao, year)

        resp = requests.get(IEM_CLI_URL, params=params, timeout=self.timeout)
        resp.raise_for_status()

        data = resp.json()
        results = data.get("results", [])
        if not results:
            return pd.DataFrame()

        rows = []
        for r in results:
            row = {
                "station": station.icao,
                "station_iata": station.iata,
                "city": station.city,
                "timezone": station.tz,
                "valid_date": r["valid"],
                "high_f": _safe_int(r.get("high")),
                "low_f": _safe_int(r.get("low")),
                "high_time": _format_time(r.get("high_time")),
                "low_time": _format_time(r.get("low_time")),
                "high_time_local": _parse_local_time(r["valid"], r.get("high_time")),
                "low_time_local": _parse_local_time(r["valid"], r.get("low_time")),
                "high_normal": _safe_int(r.get("high_normal")),
                "high_depart": _safe_int(r.get("high_depart")),
                "high_record": _safe_int(r.get("high_record")),
                "high_record_years": str(r.get("high_record_years", [])),
                "low_normal": _safe_int(r.get("low_normal")),
                "low_depart": _safe_int(r.get("low_depart")),
                "precip_in": _safe_float(r.get("precip")),
                "snow_in": _safe_float(r.get("snow")),
                "avg_sky_cover": _safe_float(r.get("average_sky_cover")),
                "cli_product_id": r.get("product", ""),
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        df["valid_date"] = pd.to_datetime(df["valid_date"]).dt.date
        # valid_utc: synthetic sentinel — see note in fetch() above.
        df["valid_utc"] = pd.to_datetime(df["valid_date"])
        df["valid_utc"] = df["valid_utc"].dt.tz_localize("UTC")

        logger.info("Got %d CLI reports for %s in %d", len(df), station.icao, year)
        return df

    def fetch_and_save(
        self,
        station: StationInfo,
        target_date: date,
        **kwargs,
    ) -> Path:
        """Fetch and persist to parquet."""
        df = self.fetch(station, target_date, **kwargs)
        return self.save_parquet(df, station, target_date)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_int(val) -> int | None:
    """Convert to int, handling M (missing) and T (trace) from CLI data."""
    if val is None or val == "M":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> float | None:
    """Convert to float, handling M/T from CLI data."""
    if val is None or val == "M":
        return None
    if val == "T":
        return 0.0  # Trace amount
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _format_time(val) -> str:
    """Format '1213 PM' as '12:13 PM'."""
    if not val or val == "M":
        return ""
    val = str(val).strip()
    if " " in val:
        time_part, ampm = val.rsplit(" ", 1)
        if time_part.isdigit() and len(time_part) in (3, 4):
            time_part = time_part.zfill(4)
            return f"{time_part[:2]}:{time_part[2:]} {ampm}"
    return val


def _parse_local_time(valid_date_str: str, time_str) -> pd.Timestamp | None:
    """Parse '2026-02-19' and '1213 PM' into a naive local datetime."""
    formatted = _format_time(time_str)
    if not formatted:
        return None
    try:
        return pd.to_datetime(f"{valid_date_str} {formatted}", format="%Y-%m-%d %I:%M %p")
    except Exception:
        return None
