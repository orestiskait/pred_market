"""METAR observation fetcher via the Aviation Weather Center (AWC) API.

Source: https://aviationweather.gov/api/data/metar

METAR reports are issued roughly hourly (routine) plus specials (SPECI)
when conditions change significantly.  The AWC JSON API provides:
  - temp/dewpoint in °C  (we convert to °F for consistency)
  - wind, visibility, ceiling, weather phenomena
  - maxT / minT (6-hour extremes embedded in certain reports)
  - raw METAR text for manual inspection

No authentication required.  Rate limits are generous for on-demand use.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

from pred_market_src.collector.weather.base import WeatherFetcherBase
from pred_market_src.collector.weather.stations import StationInfo

logger = logging.getLogger(__name__)

AWC_METAR_URL = "https://aviationweather.gov/api/data/metar"

# AWC supports up to ~15 days of lookback
MAX_HOURS_BACK = 360  # 15 days


def _c_to_f(celsius: float | None) -> float | None:
    """Convert Celsius to Fahrenheit, preserving None."""
    if celsius is None:
        return None
    return round(celsius * 9.0 / 5.0 + 32.0, 1)


def _safe_float(val) -> float | None:
    """Convert to float, returning None for non-numeric values."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _safe_str(val) -> str:
    """Convert to string; handles int/float/None gracefully."""
    if val is None:
        return ""
    return str(val)


class METARFetcher(WeatherFetcherBase):
    """Fetch decoded METAR observations from the Aviation Weather Center.

    Returns temperature in both °C (original) and °F (converted).
    Also captures the 6-hour max/min temps when available (embedded
    in synoptic-hour METARs at 00Z, 06Z, 12Z, 18Z).

    For live collection (future): poll this endpoint every 1-5 minutes
    to catch SPECI reports as soon as they drop.
    """

    SOURCE_NAME = "metar"
    EXPECTED_DAILY_ROWS = 24

    def __init__(self, data_dir: Path | str | None = None, timeout: int = 15):
        super().__init__(data_dir)
        self.timeout = timeout

    def fetch(
        self,
        station: StationInfo,
        target_date: date,
        *,
        hours_back: int | None = None,
    ) -> pd.DataFrame:
        """Fetch METAR observations for a station.

        Parameters
        ----------
        station : StationInfo
            Station to fetch.
        target_date : date
            The target date.  If today, fetches the last `hours_back` hours.
            If a past date, fetches 24h worth of obs.
        hours_back : int, optional
            Hours of lookback.  Defaults to 24 for past dates, 12 for today.

        Returns
        -------
        pd.DataFrame with decoded METAR fields.
        """
        now_utc = datetime.now(timezone.utc)
        target_start = datetime(target_date.year, target_date.month, target_date.day,
                                tzinfo=timezone.utc)
        target_end = target_start + timedelta(days=1)

        if hours_back is None:
            if target_date == now_utc.date():
                hours_back = 12
            else:
                # For past dates, compute hours from now back to start of target day
                hours_back = int((now_utc - target_start).total_seconds() / 3600) + 1
                hours_back = min(hours_back, MAX_HOURS_BACK)

        params = {
            "ids": station.icao,
            "format": "json",
            "hours": hours_back,
        }

        logger.info("Fetching METAR for %s, hours_back=%d", station.icao, hours_back)

        resp = requests.get(AWC_METAR_URL, params=params, timeout=self.timeout)
        resp.raise_for_status()

        data = resp.json()
        if not data:
            logger.warning("No METAR data returned for %s", station.icao)
            return pd.DataFrame()

        rows = []
        for obs in data:
            report_time = pd.to_datetime(obs.get("reportTime"), utc=True)

            # Filter to target date window
            if report_time < target_start or report_time >= target_end:
                continue

            row = {
                "station": station.icao,
                "valid_utc": report_time,
                "metar_type": obs.get("metarType", ""),  # METAR or SPECI
                "temp_c": obs.get("temp"),
                "dewp_c": obs.get("dewp"),
                "temp_f": _c_to_f(obs.get("temp")),
                "dewp_f": _c_to_f(obs.get("dewp")),
                "wdir": _safe_str(obs.get("wdir")),
                "wspd_kt": _safe_float(obs.get("wspd")),
                "visibility_sm": _safe_float(obs.get("visib")),
                "altimeter_inhg": _safe_float(obs.get("altim")),
                "slp_mb": _safe_float(obs.get("slp")),
                "wx_string": obs.get("wxString", ""),
                "cover": obs.get("cover", ""),
                "flt_cat": obs.get("fltCat", ""),
                # 6-hour extremes (only present at synoptic hours)
                "max_temp_6hr_c": obs.get("maxT"),
                "min_temp_6hr_c": obs.get("minT"),
                "max_temp_6hr_f": _c_to_f(obs.get("maxT")),
                "min_temp_6hr_f": _c_to_f(obs.get("minT")),
                # 24-hour extremes (less common)
                "max_temp_24hr_c": obs.get("maxT24"),
                "min_temp_24hr_c": obs.get("minT24"),
                "max_temp_24hr_f": _c_to_f(obs.get("maxT24")),
                "min_temp_24hr_f": _c_to_f(obs.get("minT24")),
                # Precip
                "precip_6hr_in": obs.get("pcp6hr"),
                # Raw METAR text for manual inspection
                "raw_ob": obs.get("rawOb", ""),
            }
            rows.append(row)

        if not rows:
            logger.warning("No METAR obs found for %s on %s", station.icao, target_date)
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        # Sort chronologically
        df = df.sort_values("valid_utc").reset_index(drop=True)
        logger.info("Got %d METAR observations for %s on %s",
                     len(df), station.icao, target_date)
        return df

    def fetch_latest(self, station: StationInfo) -> pd.DataFrame:
        """Fetch just the latest METAR for a station (for live monitoring)."""
        params = {
            "ids": station.icao,
            "format": "json",
            "hours": 2,
        }
        resp = requests.get(AWC_METAR_URL, params=params, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return pd.DataFrame()

        obs = data[0]  # most recent
        report_time = pd.to_datetime(obs.get("reportTime"), utc=True)

        row = {
            "station": station.icao,
            "valid_utc": report_time,
            "metar_type": obs.get("metarType", ""),
            "temp_c": obs.get("temp"),
            "temp_f": _c_to_f(obs.get("temp")),
            "dewp_c": obs.get("dewp"),
            "dewp_f": _c_to_f(obs.get("dewp")),
            "max_temp_6hr_c": obs.get("maxT"),
            "max_temp_6hr_f": _c_to_f(obs.get("maxT")),
            "raw_ob": obs.get("rawOb", ""),
        }
        return pd.DataFrame([row])

    def fetch_and_save(
        self,
        station: StationInfo,
        target_date: date,
        **kwargs,
    ) -> Path:
        """Fetch and persist to parquet."""
        df = self.fetch(station, target_date, **kwargs)
        return self.save_parquet(df, station, target_date)
