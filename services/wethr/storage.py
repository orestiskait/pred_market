"""Parquet storage for Wethr.net Push API data with latency tracking.

Stores each event type in a separate parquet file, organized by date:
  data/weather/wethr_push/observations/<ICAO>_YYYY-MM-DD.parquet
  data/weather/wethr_push/dsm/<ICAO>_YYYY-MM-DD.parquet
  data/weather/wethr_push/cli/<ICAO>_YYYY-MM-DD.parquet
  data/weather/wethr_push/new_high/<ICAO>_YYYY-MM-DD.parquet
  data/weather/wethr_push/new_low/<ICAO>_YYYY-MM-DD.parquet

Every row includes `received_ts` (when our client received the SSE event) for
post-hoc latency analysis: latency = received_ts - observation_time_utc.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

from services.core.parquet_store import PerStationDayStore

logger = logging.getLogger(__name__)


_EVENT_META: dict[str, dict] = {
    "observations": {
        "date_col": "observation_time_utc",
        "dedup": ["station_code", "observation_time_utc"],
        "sort": "observation_time_utc",
    },
    "dsm": {
        "date_col": "for_date",
        "dedup": ["station_code", "for_date"],
        "sort": "for_date",
    },
    "cli": {
        "date_col": "for_date",
        "dedup": ["station_code", "for_date"],
        "sort": "for_date",
    },
    "new_high": {
        "date_col": "observation_time_utc",
        "dedup": ["station_code", "observation_time_utc", "logic"],
        "sort": "observation_time_utc",
    },
    "new_low": {
        "date_col": "observation_time_utc",
        "dedup": ["station_code", "observation_time_utc", "logic"],
        "sort": "observation_time_utc",
    },
}


class WethrPushStorage(PerStationDayStore):
    """Append-friendly parquet I/O for Wethr.net Push API events."""

    EVENT_TYPES = tuple(_EVENT_META.keys())

    def __init__(self, data_dir: str | Path):
        super().__init__(Path(data_dir) / "weather" / "wethr_push")
        for et in self.EVENT_TYPES:
            (self.base_dir / et).mkdir(parents=True, exist_ok=True)

    def save(self, df: pd.DataFrame, event_type: str) -> None:
        if df.empty:
            return

        meta = _EVENT_META.get(event_type)
        if meta is None:
            logger.error("Unknown event type: %s", event_type)
            return

        df = df.copy()
        if "received_ts" not in df.columns:
            df["received_ts"] = pd.Timestamp.now(tz="UTC")

        date_col = meta["date_col"]
        event_dir = self._subdir(event_type)

        for station in df["station_code"].unique():
            stn_df = df[df["station_code"] == station]

            if date_col and date_col in stn_df.columns:
                dates = pd.to_datetime(stn_df[date_col], utc=True).dt.date.unique()
            else:
                dates = stn_df["received_ts"].dt.date.unique()

            for obs_date in dates:
                if date_col and date_col in stn_df.columns:
                    day_df = stn_df[pd.to_datetime(stn_df[date_col], utc=True).dt.date == obs_date]
                else:
                    day_df = stn_df[stn_df["received_ts"].dt.date == obs_date]

                path = self._append_parquet(
                    event_dir, station, obs_date, day_df,
                    dedup_cols=meta["dedup"],
                    sort_cols=[meta["sort"]],
                )
                logger.info("Wethr %s: saved %d rows -> %s", event_type, len(day_df), path)

    def read(
        self,
        event_type: str,
        station: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        return self._read_parquets(
            self.base_dir / event_type, station, start_date, end_date,
        )
