"""Download ASOS 1-minute temperature data from Iowa Environmental Mesonet (IEM).

Data source: https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py

Fetches NCEI 1-minute ASOS data with ~24h delay. Best publicly available proxy
for the raw 1-minute peaks the NWS uses to compute the official daily high.
Uses IATA station codes (e.g. MDW) for the IEM API.
"""

from __future__ import annotations

import io
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

from research.download_data.fetcher_base import WeatherFetcherBase
from research.weather.iem_awc_station_registry import StationInfo

logger = logging.getLogger(__name__)

IEM_ASOS_1MIN_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py"

DEFAULT_VARS = ["tmpf", "dwpf"]


class IEMASOS1MinFetcher(WeatherFetcherBase):
    """Fetch 1-minute ASOS observations from Iowa Environmental Mesonet (IEM)."""

    SOURCE_NAME = "iem_asos_1min"
    EXPECTED_DAILY_ROWS = 1440

    def __init__(self, data_dir: Path | str | None = None, timeout: int = 30):
        super().__init__(data_dir)
        self.timeout = timeout

    def fetch(
        self,
        station: StationInfo,
        target_date: date,
        *,
        vars: list[str] | None = None,
    ) -> pd.DataFrame:
        if vars is None:
            vars = DEFAULT_VARS

        start = datetime(target_date.year, target_date.month, target_date.day)
        end = start + timedelta(days=1)

        params = {
            "station": station.iata,
            "vars": ",".join(vars),
            "sts": start.strftime("%Y-%m-%dT%H:%MZ"),
            "ets": end.strftime("%Y-%m-%dT%H:%MZ"),
            "sample": "1min",
            "what": "download",
            "tz": "UTC",
        }

        logger.info("Fetching ASOS 1-min from IEM for %s (%s) on %s",
                     station.icao, station.iata, target_date)

        resp = requests.get(IEM_ASOS_1MIN_URL, params=params, timeout=self.timeout)
        resp.raise_for_status()

        df = pd.read_csv(io.StringIO(resp.text))

        if df.empty:
            logger.warning("No ASOS 1-min data returned for %s on %s",
                           station.icao, target_date)
            return pd.DataFrame()

        df = df.rename(columns={"valid(UTC)": "valid_utc"})
        df["valid_utc"] = pd.to_datetime(df["valid_utc"], utc=True)
        df = df.rename(columns={"station": "station_iata"})
        df["station"] = station.icao
        df["city"] = station.city
        df["timezone"] = station.tz
        df["valid_local"] = df["valid_utc"].dt.tz_convert(station.tz).dt.tz_localize(None)

        for v in vars:
            if v in df.columns:
                df[v] = pd.to_numeric(df[v], errors="coerce")

        logger.info("Got %d 1-minute observations for %s", len(df), station.icao)
        return df

    def fetch_range_bulk(
        self,
        station: StationInfo,
        start_date: date,
        end_date: date,
        *,
        vars: list[str] | None = None,
        pad_days: int = 1,
    ) -> pd.DataFrame:
        if vars is None:
            vars = DEFAULT_VARS

        fetch_start = start_date - timedelta(days=pad_days)
        fetch_end = end_date + timedelta(days=pad_days)

        params = {
            "station": station.iata,
            "vars": ",".join(vars),
            "sts": f"{fetch_start}T00:00Z",
            "ets": f"{fetch_end}T23:59Z",
            "sample": "1min",
            "what": "download",
            "tz": "UTC",
        }

        logger.info("Fetching ASOS 1-min bulk from IEM for %s (%s): %s → %s",
                     station.icao, station.iata, fetch_start, fetch_end)

        resp = requests.get(IEM_ASOS_1MIN_URL, params=params, timeout=120)
        resp.raise_for_status()

        df = pd.read_csv(io.StringIO(resp.text))

        if df.empty:
            logger.warning("No ASOS 1-min data returned for %s (%s → %s)",
                           station.icao, fetch_start, fetch_end)
            return pd.DataFrame()

        df = df.rename(columns={"valid(UTC)": "valid_utc", "station": "station_iata"})
        df["valid_utc"] = pd.to_datetime(df["valid_utc"], utc=True)
        df["station"] = station.icao
        df["city"] = station.city
        df["timezone"] = station.tz
        df["valid_local"] = df["valid_utc"].dt.tz_convert(station.tz).dt.tz_localize(None)

        for v in vars:
            if v in df.columns:
                df[v] = pd.to_numeric(df[v], errors="coerce")

        logger.info("Got %d 1-minute observations for %s (%s → %s)",
                     len(df), station.icao, fetch_start, fetch_end)
        return df

    def fetch_range_bulk_and_save(
        self,
        station: StationInfo,
        start_date: date,
        end_date: date,
        *,
        skip_existing: bool = True,
        min_completeness: float = 0.95,
    ) -> int:
        """Bulk fetch a date range and save per-day parquets.

        Uses a single API request for the full range (more reliable than per-day).
        Saves each day with >= min_completeness of expected rows (default 95%).
        Returns number of days saved.
        """
        df = self.fetch_range_bulk(station, start_date, end_date, pad_days=0)
        if df.empty:
            return 0

        df["_date"] = df["valid_utc"].dt.date
        min_rows = int(self.EXPECTED_DAILY_ROWS * min_completeness)
        saved = 0
        for d, grp in df.groupby("_date"):
            if start_date <= d <= end_date:
                if skip_existing:
                    path = self.data_dir / f"{station.icao}_{d.isoformat()}.parquet"
                    if path.exists():
                        continue
                day_df = grp.drop(columns=["_date"])
                if len(day_df) >= min_rows:
                    self.save_parquet(day_df, station, d)
                    saved += 1
                else:
                    logger.debug("Skipping %s: %d rows < %d",
                                 d, len(day_df), min_rows)
        return saved
