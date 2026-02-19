"""ASOS 1-minute temperature data fetcher via Iowa Environmental Mesonet.

Source: https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py

The IEM archives NCEI 1-minute ASOS data with ~24h delay.  For on-demand
analysis this is the best publicly available proxy for the raw 1-minute
peaks the NWS uses to compute the official daily high.

Note: The IEM API uses the 3-letter FAA/IATA station code (e.g. "MDW"),
not the 4-letter ICAO code (e.g. "KMDW").
"""

from __future__ import annotations

import io
import logging
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

from pred_market_src.collector.weather.base import WeatherFetcherBase
from pred_market_src.collector.weather.stations import StationInfo

logger = logging.getLogger(__name__)

IEM_ASOS_1MIN_URL = "https://mesonet.agron.iastate.edu/cgi-bin/request/asos1min.py"

# Temperature vars available in IEM ASOS 1-min:
#   tmpf  = air temperature (°F, 1-minute average)
#   dwpf  = dew point (°F)
# We request both for potential heat-index / rounding analysis.
DEFAULT_VARS = ["tmpf", "dwpf"]


class ASOS1MinFetcher(WeatherFetcherBase):
    """Fetch 1-minute ASOS observations from the IEM archive.

    Data is returned as whole-degree Fahrenheit (the IEM's representation
    of the NCEI 1-minute averages).  There is typically a ~24h delay.

    Important for betting:
      - These are 1-minute AVERAGES, not the raw 1-minute PEAKS.
      - The NWS official high is based on the peak 1-minute value,
        so the official high can be ≥1°F higher than any value here.
      - Still, this is the highest-resolution public data available.
    """

    SOURCE_NAME = "asos_1min"

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
        """Fetch 1-minute data for a single station and calendar day (UTC).

        Parameters
        ----------
        station : StationInfo
            Station to fetch.
        target_date : date
            The calendar date to fetch (full 24h window in UTC).
        vars : list[str], optional
            IEM variable names to request; defaults to ["tmpf", "dwpf"].

        Returns
        -------
        pd.DataFrame with columns: station, station_name, valid_utc, tmpf, dwpf, ...
        """
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

        logger.info("Fetching ASOS 1-min for %s (%s) on %s",
                     station.icao, station.iata, target_date)

        resp = requests.get(IEM_ASOS_1MIN_URL, params=params, timeout=self.timeout)
        resp.raise_for_status()

        # IEM returns CSV with header row
        df = pd.read_csv(io.StringIO(resp.text))

        if df.empty:
            logger.warning("No ASOS 1-min data returned for %s on %s",
                           station.icao, target_date)
            return pd.DataFrame()

        # Normalize column names
        df = df.rename(columns={"valid(UTC)": "valid_utc"})
        df["valid_utc"] = pd.to_datetime(df["valid_utc"], utc=True)
        df = df.rename(columns={"station": "station_iata"})
        df["station"] = station.icao

        # Coerce temperature columns to numeric (M = missing → NaN)
        for v in vars:
            if v in df.columns:
                df[v] = pd.to_numeric(df[v], errors="coerce")

        logger.info("Got %d 1-minute observations for %s", len(df), station.icao)
        return df

    def fetch_and_save(
        self,
        station: StationInfo,
        target_date: date,
        **kwargs,
    ) -> Path:
        """Fetch and persist to parquet in one step."""
        df = self.fetch(station, target_date, **kwargs)
        return self.save_parquet(df, station, target_date)

    def fetch_range(
        self,
        station: StationInfo,
        start_date: date,
        end_date: date,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch multiple days of 1-min data, concatenated."""
        frames = []
        current = start_date
        while current <= end_date:
            try:
                df = self.fetch(station, current, **kwargs)
                if not df.empty:
                    frames.append(df)
            except Exception:
                logger.exception("Failed ASOS 1-min fetch for %s on %s",
                                 station.icao, current)
            current += timedelta(days=1)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
