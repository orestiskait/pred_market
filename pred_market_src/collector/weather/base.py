"""Abstract base for all weather fetcher classes.

Provides:
  - Config loading (reads weather_stations from config.yaml)
  - Parquet storage helpers (append-friendly, one file per station per day)
  - Logging setup
  - Abstract fetch() / fetch_many() interface
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from pred_market_src.collector.weather.stations import StationInfo

logger = logging.getLogger(__name__)


class WeatherFetcherBase(ABC):
    """Base class for weather data fetchers.

    Subclasses must implement:
      - SOURCE_NAME (class attribute)  – e.g. "asos_1min"
      - fetch(station, ...) -> pd.DataFrame
    """

    SOURCE_NAME: str = ""  # override in subclass

    def __init__(self, data_dir: Path | str | None = None):
        if data_dir is None:
            data_dir = Path(__file__).resolve().parent.parent / "data" / "weather_obs"
        self.data_dir = Path(data_dir) / self.SOURCE_NAME
        self.data_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch(self, station: StationInfo, target_date: date, **kwargs) -> pd.DataFrame:
        """Fetch observations for a single station and date.

        Returns a DataFrame with at minimum a 'valid_utc' datetime column
        and a 'station' string column.
        """

    # ------------------------------------------------------------------
    # Concrete helpers
    # ------------------------------------------------------------------

    def fetch_many(
        self,
        stations: list[StationInfo],
        target_date: date,
        skip_existing: bool = False,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch for multiple stations and concatenate results."""
        frames: list[pd.DataFrame] = []
        for stn in stations:
            if skip_existing and self.check_exists(stn, target_date):
                logger.info("Skipping %s for %s on %s (already exists)",
                            self.SOURCE_NAME, stn.icao, target_date)
                continue

            try:
                df = self.fetch(stn, target_date, **kwargs)
                if not df.empty:
                    frames.append(df)
            except Exception:
                logger.exception("Failed to fetch %s data for %s on %s",
                                 self.SOURCE_NAME, stn.icao, target_date)
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def save_parquet(self, df: pd.DataFrame, station: StationInfo, target_date: date) -> Path:
        """Save a DataFrame as a parquet file: <source>/<ICAO>_<date>.parquet.

        If the file already exists, the new rows are appended (deduplicated
        by the 'valid_utc' + 'station' natural key).
        """
        if df.empty:
            logger.warning("Empty DataFrame, nothing to save for %s/%s",
                           station.icao, target_date)
            return self.data_dir  # return dir as sentinel

        path = self.data_dir / f"{station.icao}_{target_date.isoformat()}.parquet"

        if path.exists():
            existing = pd.read_parquet(path)
            df = pd.concat([existing, df], ignore_index=True)
            # Deduplicate: keep last occurrence (newest fetch wins)
            dedup_cols = [c for c in ("valid_utc", "station") if c in df.columns]
            if dedup_cols:
                df = df.drop_duplicates(subset=dedup_cols, keep="last")

        df.to_parquet(path, index=False)
        logger.info("Saved %d rows → %s", len(df), path)
        return path

    def read_parquet(self, station_icao: str, target_date: date) -> pd.DataFrame:
        """Read back a previously saved parquet for one station/date."""
        path = self.data_dir / f"{station_icao}_{target_date.isoformat()}.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    def read_all(self, start_date: date | None = None, end_date: date | None = None) -> pd.DataFrame:
        """Read all saved parquets, optionally filtered by date range."""
        files = sorted(self.data_dir.glob("*.parquet"))
        if not files:
            return pd.DataFrame()

        frames = []
        for f in files:
            # filename: KNYC_2026-02-18.parquet
            parts = f.stem.split("_", 1)
            if len(parts) == 2:
                try:
                    file_date = date.fromisoformat(parts[1])
                except ValueError:
                    file_date = None
                if file_date:
                    if start_date and file_date < start_date:
                        continue
                    if end_date and file_date > end_date:
                        continue
            frames.append(pd.read_parquet(f))

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def check_exists(self, station: StationInfo, target_date: date) -> bool:
        """Check if data already exists for this station/date."""
        path = self.data_dir / f"{station.icao}_{target_date.isoformat()}.parquet"
        return path.exists()
