"""Reader for LDM-ingested surface observations.

Provides the same interface as the other weather fetchers (WeatherFetcherBase)
but reads from the parquet files written by ldm_ingest.py instead of fetching
from an external API.

Data lives at: data/weather_obs/ldm_surface/<ICAO>_<date>.parquet

This module is used by the orchestrator (observations.py) and analysis tools
to access real-time surface data received via the LDM feed.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from pred_market_src.collector.weather.base import WeatherFetcherBase
from pred_market_src.collector.weather.stations import StationInfo

logger = logging.getLogger(__name__)


class LDMSurfaceReader(WeatherFetcherBase):
    """Read LDM-ingested surface observations from parquet.

    Unlike the other fetchers, this class does not make external API calls.
    It reads data that was written by ldm_ingest.py (invoked by LDM pqact).

    The fetch() method simply reads from the on-disk parquet file, making
    this class suitable for use in the WeatherObservations orchestrator
    alongside ASOS1MinFetcher, METARFetcher, and DailyClimateFetcher.
    """

    SOURCE_NAME = "ldm_surface"

    def __init__(self, data_dir: Path | str | None = None, timeout: int = 5):
        super().__init__(data_dir)

    def fetch(
        self,
        station: StationInfo,
        target_date: date,
        **kwargs,
    ) -> pd.DataFrame:
        """Read LDM surface observations for a station and date.

        Parameters
        ----------
        station : StationInfo
            Station to read.
        target_date : date
            Date to read.

        Returns
        -------
        pd.DataFrame with decoded METAR fields from the LDM feed.
        """
        path = self.data_dir / f"{station.icao}_{target_date.isoformat()}.parquet"
        if not path.exists():
            logger.info(
                "No LDM surface data for %s on %s (file not found: %s)",
                station.icao, target_date, path,
            )
            return pd.DataFrame()

        df = pd.read_parquet(path)
        logger.info(
            "Read %d LDM surface obs for %s on %s",
            len(df), station.icao, target_date,
        )
        return df

    def fetch_latest(self, station: StationInfo) -> pd.DataFrame:
        """Read the most recent LDM observation for a station.

        Reads today's parquet file and returns the last row.
        """
        from datetime import datetime, timezone

        today = datetime.now(timezone.utc).date()
        df = self.fetch(station, today)
        if df.empty:
            return df
        return df.tail(1).reset_index(drop=True)

    def fetch_and_save(
        self,
        station: StationInfo,
        target_date: date,
        **kwargs,
    ) -> Path:
        """No-op: LDM data is already saved by ldm_ingest.py.

        Returns the path where data would be (or is) stored.
        """
        return self.data_dir / f"{station.icao}_{target_date.isoformat()}.parquet"

    def is_receiving(self, station: StationInfo | None = None) -> bool:
        """Check if LDM data is flowing (has recent observations).

        Parameters
        ----------
        station : StationInfo, optional
            Check a specific station. If None, checks any station.

        Returns
        -------
        True if data from the last 2 hours exists.
        """
        from datetime import datetime, timedelta, timezone

        now = datetime.now(timezone.utc)
        today = now.date()
        yesterday = today - timedelta(days=1)

        for check_date in [today, yesterday]:
            if station:
                candidates = [self.data_dir / f"{station.icao}_{check_date.isoformat()}.parquet"]
            else:
                candidates = list(self.data_dir.glob(f"*_{check_date.isoformat()}.parquet"))

            for path in candidates:
                if path.exists():
                    df = pd.read_parquet(path)
                    if not df.empty and "valid_utc" in df.columns:
                        latest = pd.to_datetime(df["valid_utc"]).max()
                        if latest.tzinfo is None:
                            latest = latest.replace(tzinfo=timezone.utc)
                        age = now - latest
                        if age < timedelta(hours=2):
                            return True
        return False
