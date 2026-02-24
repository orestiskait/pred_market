"""Parquet storage for aviation weather METAR data with latency tracking.

Storage layout:
  data/aviationweather_metar/<source>/<ICAO>_<YYYY-MM-DD>.parquet

Sources: awc_metar (Aviation Weather Center), nws_observations (api.weather.gov)
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from services.core.parquet_store import PerStationDayStore

logger = logging.getLogger(__name__)


class MetarStorage(PerStationDayStore):
    """Append-friendly parquet I/O for aviation weather METAR with latency tracking."""

    DEDUP_COLS = ["station", "ob_time_utc", "source"]
    SORT_COLS = ["ob_time_utc"]

    def __init__(self, data_dir: str | Path):
        super().__init__(Path(data_dir) / "aviationweather_metar")

    def save(self, df: pd.DataFrame, source_name: str) -> None:
        if df.empty:
            return

        df = df.copy()
        saved_ts = pd.Timestamp.now(tz="UTC")
        df["saved_ts"] = saved_ts

        if "ob_time_utc" in df.columns:
            ob_time = pd.to_datetime(df["ob_time_utc"], utc=True)
            df["total_latency_s"] = (saved_ts - ob_time).dt.total_seconds().round(1)

        source_dir = self._subdir(source_name)
        for station_icao in df["station"].unique():
            stn_df = df[df["station"] == station_icao]
            for obs_date in stn_df["ob_time_utc"].dt.date.unique():
                day_df = stn_df[stn_df["ob_time_utc"].dt.date == obs_date]
                path = self._append_parquet(
                    source_dir, station_icao, obs_date, day_df,
                    dedup_cols=self.DEDUP_COLS, sort_cols=self.SORT_COLS,
                )
                lat = day_df["total_latency_s"].mean() if "total_latency_s" in day_df.columns else 0
                logger.info("METAR: saved %d rows → %s (latency=%.0fs)", len(day_df), path, lat)

    def read(
        self,
        source_name: str,
        station_icao: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        return self._read_parquets(
            self.base_dir / source_name, station_icao, start_date, end_date,
        )
