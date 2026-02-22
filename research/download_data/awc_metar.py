"""Download METAR observations from Aviation Weather Center (AWC).

Data source: https://aviationweather.gov/api/data/metar

Fetches decoded METAR reports (roughly hourly routine + SPECI when conditions
change). Provides temp/dewpoint, wind, visibility, ceiling, and 6-hour/24-hour
extremes when embedded in reports. No authentication required.
"""

from __future__ import annotations

import logging
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

from research.download_data.fetcher_base import WeatherFetcherBase
from research.weather.iem_awc_station_registry import StationInfo

logger = logging.getLogger(__name__)

AWC_METAR_URL = "https://aviationweather.gov/api/data/metar"
MAX_HOURS_BACK = 360


def _c_to_f(celsius: float | None) -> float | None:
    if celsius is None:
        return None
    return round(celsius * 9.0 / 5.0 + 32.0, 1)


def _parse_high_accuracy_temp(raw_ob: str) -> float | None:
    if not raw_ob:
        return None
    m = re.search(r'\bT([01])(\d{3})', raw_ob)
    if m:
        sign = 1 if m.group(1) == '0' else -1
        return sign * int(m.group(2)) / 10.0
    return None


def _parse_6hr_max_temp(raw_ob: str) -> float | None:
    if not raw_ob:
        return None
    m = re.search(r'\b1([01])(\d{3})\b', raw_ob)
    if m:
        sign = 1 if m.group(1) == '0' else -1
        return sign * int(m.group(2)) / 10.0
    return None


class AWCMETARFetcher(WeatherFetcherBase):
    """Fetch decoded METAR observations from Aviation Weather Center (AWC)."""

    SOURCE_NAME = "awc_metar"
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
        now_utc = datetime.now(timezone.utc)
        target_start = datetime(target_date.year, target_date.month, target_date.day,
                                tzinfo=timezone.utc)
        target_end = target_start + timedelta(days=1)

        if hours_back is None:
            if target_date == now_utc.date():
                hours_back = 12
            else:
                hours_back = int((now_utc - target_start).total_seconds() / 3600) + 1
                hours_back = min(hours_back, MAX_HOURS_BACK)

        params = {
            "ids": station.icao,
            "format": "json",
            "hours": hours_back,
        }

        logger.info("Fetching METAR from AWC for %s, hours_back=%d", station.icao, hours_back)

        resp = requests.get(AWC_METAR_URL, params=params, timeout=self.timeout)
        resp.raise_for_status()

        data = resp.json()
        if not data:
            logger.warning("No METAR data returned for %s", station.icao)
            return pd.DataFrame()

        rows = []
        for obs in data:
            report_time = pd.to_datetime(obs.get("reportTime"), utc=True)

            if report_time < target_start or report_time >= target_end:
                continue

            raw_ob = obs.get("rawOb", "")

            parsed_high_acc_c = _parse_high_accuracy_temp(raw_ob)
            if parsed_high_acc_c is not None:
                temp_c = parsed_high_acc_c
            else:
                temp_c = obs.get("temp")

            parsed_max_6hr_c = _parse_6hr_max_temp(raw_ob)
            if parsed_max_6hr_c is not None:
                max_6hr_c = parsed_max_6hr_c
            else:
                max_6hr_c = obs.get("maxT")

            row = {
                "station": station.icao,
                "station_iata": station.iata,
                "city": station.city,
                "timezone": station.tz,
                "valid_utc": report_time,
                "valid_local": report_time.tz_convert(station.tz).tz_localize(None),
                "metar_type": obs.get("metarType", ""),
                "temp_c": temp_c,
                "dewp_c": obs.get("dewp"),
                "temp_f": _c_to_f(temp_c),
                "dewp_f": _c_to_f(obs.get("dewp")),
                "max_temp_6hr_c": max_6hr_c,
                "min_temp_6hr_c": obs.get("minT"),
                "max_temp_6hr_f": _c_to_f(max_6hr_c),
                "min_temp_6hr_f": _c_to_f(obs.get("minT")),
                "max_temp_24hr_c": obs.get("maxT24"),
                "min_temp_24hr_c": obs.get("minT24"),
                "max_temp_24hr_f": _c_to_f(obs.get("maxT24")),
                "min_temp_24hr_f": _c_to_f(obs.get("minT24")),
                "raw_ob": raw_ob,
            }
            rows.append(row)

        if not rows:
            logger.warning("No METAR obs found for %s on %s", station.icao, target_date)
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df = df.sort_values("valid_utc").reset_index(drop=True)
        logger.info("Got %d METAR observations for %s on %s",
                     len(df), station.icao, target_date)
        return df

    def fetch_latest(self, station: StationInfo) -> pd.DataFrame:
        params = {"ids": station.icao, "format": "json", "hours": 2}
        resp = requests.get(AWC_METAR_URL, params=params, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            return pd.DataFrame()

        obs = data[0]
        report_time = pd.to_datetime(obs.get("reportTime"), utc=True)
        raw_ob = obs.get("rawOb", "")

        parsed_high_acc_c = _parse_high_accuracy_temp(raw_ob)
        temp_c = parsed_high_acc_c if parsed_high_acc_c is not None else obs.get("temp")
        parsed_max_6hr_c = _parse_6hr_max_temp(raw_ob)
        max_6hr_c = parsed_max_6hr_c if parsed_max_6hr_c is not None else obs.get("maxT")

        row = {
            "station": station.icao,
            "station_iata": station.iata,
            "city": station.city,
            "timezone": station.tz,
            "valid_utc": report_time,
            "valid_local": report_time.tz_convert(station.tz).tz_localize(None),
            "metar_type": obs.get("metarType", ""),
            "temp_c": temp_c,
            "temp_f": _c_to_f(temp_c),
            "dewp_c": obs.get("dewp"),
            "dewp_f": _c_to_f(obs.get("dewp")),
            "max_temp_6hr_c": max_6hr_c,
            "max_temp_6hr_f": _c_to_f(max_6hr_c),
            "raw_ob": raw_ob,
        }
        return pd.DataFrame([row])

    def fetch_and_save(
        self,
        station: StationInfo,
        target_date: date,
        **kwargs,
    ) -> Path:
        df = self.fetch(station, target_date, **kwargs)
        return self.save_parquet(df, station, target_date)
