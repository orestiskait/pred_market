"""Download NWS Daily Climate Report (CLI) from Iowa Environmental Mesonet (IEM).

Data source: https://mesonet.agron.iastate.edu/json/cli.py

The CLI product is the **official** NWS daily climate report that Kalshi
uses to resolve temperature-high contracts. Contains official daily high/low,
times, records, normals, precipitation, etc. Published once daily (~06:00 UTC).
The `high_f` field is the ground truth for contract resolution.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

from research.download_data.fetcher_base import WeatherFetcherBase
from research.weather.iem_awc_station_registry import StationInfo

logger = logging.getLogger(__name__)

IEM_CLI_URL = "https://mesonet.agron.iastate.edu/json/cli.py"


class IEMDailyClimateFetcher(WeatherFetcherBase):
    """Fetch NWS Daily Climate Reports (CLI) from Iowa Environmental Mesonet (IEM)."""

    SOURCE_NAME = "iem_daily_climate"
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
        params = {
            "station": station.icao,
            "year": target_date.year,
        }

        logger.info("Fetching CLI from IEM for %s, year=%d", station.icao, target_date.year)

        resp = requests.get(IEM_CLI_URL, params=params, timeout=self.timeout)
        resp.raise_for_status()

        data = resp.json()
        results = data.get("results", [])
        if not results:
            logger.warning("No CLI data returned for %s year=%d",
                           station.icao, target_date.year)
            return pd.DataFrame()

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
                "low_record": _safe_int(r.get("low_record")),
                "low_record_years": str(r.get("low_record_years", [])),
                "precip_in": _safe_float(r.get("precip")),
                "snow_in": _safe_float(r.get("snow")),
                "snowdepth_in": _safe_float(r.get("snowdepth")),
                "avg_wind_speed": _safe_float(r.get("average_wind_speed")),
                "highest_wind_speed": _safe_int(r.get("highest_wind_speed")),
                "highest_gust_speed": _safe_int(r.get("highest_gust_speed")),
                "avg_sky_cover": _safe_float(r.get("average_sky_cover")),
                "cli_product_id": r.get("product", ""),
            }
            rows.append(row)

        df = pd.DataFrame(rows)
        df["valid_date"] = pd.to_datetime(df["valid_date"]).dt.date
        df["valid_utc"] = pd.to_datetime(df["valid_date"]).dt.tz_localize("UTC")

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
        params = {"station": station.icao, "year": year}

        logger.info("Fetching full-year CLI from IEM for %s, year=%d", station.icao, year)

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
        df["valid_utc"] = pd.to_datetime(df["valid_date"]).dt.tz_localize("UTC")

        logger.info("Got %d CLI reports for %s in %d", len(df), station.icao, year)
        return df

    def fetch_and_save(
        self,
        station: StationInfo,
        target_date: date,
        **kwargs,
    ) -> Path:
        df = self.fetch(station, target_date, **kwargs)
        return self.save_parquet(df, station, target_date)


def _safe_int(val) -> int | None:
    if val is None or val == "M":
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _safe_float(val) -> float | None:
    if val is None or val == "M":
        return None
    if val == "T":
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _format_time(val) -> str:
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
    formatted = _format_time(time_str)
    if not formatted:
        return None
    try:
        return pd.to_datetime(f"{valid_date_str} {formatted}", format="%Y-%m-%d %I:%M %p")
    except Exception:
        return None
