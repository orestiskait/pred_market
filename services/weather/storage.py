"""Parquet storage for NWP model data and MADIS observations.

Storage layouts:
  data/weather/nwp_realtime/<model>/<ICAO>_<YYYY-MM-DD>.parquet
  data/weather/madis_realtime/<source>/<ICAO>_<YYYY-MM-DD>.parquet

Metadata columns (added automatically by save()):
  - notification_ts_utc : when AWS SNS published the S3 event
  - saved_ts_utc        : when we persisted to parquet
  - is_live             : boolean indicating boolean live ingestion
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from services.core.parquet_store import PerStationDayStore

logger = logging.getLogger(__name__)


def _add_metadata_columns(
    df: pd.DataFrame,
    notification_ts: datetime,
) -> pd.DataFrame:
    """Add notification_ts_utc, saved_ts_utc and is_live columns to *df* in-place."""
    saved_ts = pd.Timestamp.now(tz="UTC")
    notif_ts = pd.Timestamp(notification_ts)
    if notif_ts.tzinfo is None:
        notif_ts = notif_ts.tz_localize("UTC")

    df["notification_ts_utc"] = notif_ts
    df["saved_ts_utc"] = saved_ts
    df["is_live"] = True

    return df


def _log_save(label: str, n_rows: int, path: Path) -> None:
    logger.info(
        "%s: saved %d rows → %s",
        label,
        n_rows,
        path,
    )


class NWPRealtimeStorage(PerStationDayStore):
    """Append-friendly parquet I/O for real-time NWP data."""

    DEDUP_COLS = ["station", "model_run_time_utc", "lead_time_minutes", "model"]
    SORT_COLS = ["model_run_time_utc", "lead_time_minutes"]

    def __init__(self, data_dir: str | Path):
        super().__init__(Path(data_dir) / "weather" / "nwp_realtime")

    def save(
        self,
        df: pd.DataFrame,
        model_name: str,
        notification_ts: datetime,
    ) -> None:
        if df.empty:
            return

        df = df.copy()
        _add_metadata_columns(df, notification_ts)

        model_dir = self._subdir(model_name)
        for station_icao in df["station"].unique():
            stn_df = df[df["station"] == station_icao]
            for cycle_date in stn_df["model_run_time_utc"].dt.date.unique():
                day_df = stn_df[stn_df["model_run_time_utc"].dt.date == cycle_date]
                path = self._append_parquet(
                    model_dir, station_icao, cycle_date, day_df,
                    dedup_cols=self.DEDUP_COLS, sort_cols=self.SORT_COLS,
                )
                _log_save(model_name, len(day_df), path)

    def read(
        self,
        model_name: str,
        station_icao: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        return self._read_parquets(
            self.base_dir / model_name, station_icao, start_date, end_date,
        )


class MADISRealtimeStorage(PerStationDayStore):
    """Append-friendly parquet I/O for MADIS observation data with latency tracking.

    Storage layout:
      data/weather/madis_realtime/<source_name>/<ICAO>_<YYYY-MM-DD>.parquet
    """

    DEDUP_COLS = ["station", "obs_time_utc", "source"]
    SORT_COLS = ["obs_time_utc"]

    def __init__(self, data_dir: str | Path):
        super().__init__(Path(data_dir) / "weather" / "madis_realtime")

    def save(
        self,
        df: pd.DataFrame,
        source_name: str,
        notification_ts: datetime,
    ) -> None:
        if df.empty:
            return

        df = df.copy()
        _add_metadata_columns(df, notification_ts)
        
        # Ensure obs_time_utc is set appropriately to be safe, if we still use it (optional)
        if "obs_time_utc" in df.columns:
            df["obs_time_utc"] = pd.to_datetime(df["obs_time_utc"], utc=True)

        source_dir = self._subdir(source_name)
        for station_icao in df["station"].unique():
            stn_df = df[df["station"] == station_icao]
            for obs_date in stn_df["obs_time_utc"].dt.date.unique():
                day_df = stn_df[stn_df["obs_time_utc"].dt.date == obs_date]
                path = self._append_parquet(
                    source_dir, station_icao, obs_date, day_df,
                    dedup_cols=self.DEDUP_COLS, sort_cols=self.SORT_COLS,
                )
                _log_save(source_name, len(day_df), path)

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


class SQSMessagesStorage(PerStationDayStore):
    """Append-friendly parquet I/O for SQS message counts.

    Storage layout:
      data/weather/sqs_counts/<queue_name>_<YYYY-MM-DD>.parquet
    """

    DEDUP_COLS = ["date", "queue_name", "model"]
    SORT_COLS = ["date"]

    def __init__(self, data_dir: str | Path):
        super().__init__(Path(data_dir) / "weather" / "sqs_counts")

    def save(self, df: pd.DataFrame) -> None:
        """Save SQS message counts.

        Expects df with columns: [date, queue_name, model, message_count]
        """
        if df.empty:
            return

        # Use queue_name as the 'station' equivalent for partitioning
        for queue_name in df["queue_name"].unique():
            queue_df = df[df["queue_name"] == queue_name]
            for date_val in queue_df["date"].unique():
                day_df = queue_df[queue_df["date"] == date_val]
                # We save directly into base_dir (no model subdirs like NWP)
                # but use queue_name in the filename.
                self._append_parquet(
                    self.base_dir, queue_name, date_val, day_df,
                    dedup_cols=self.DEDUP_COLS, sort_cols=self.SORT_COLS,
                )

    def read(
        self,
        queue_name: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """Read SQS message counts."""
        return self._read_parquets(
            self.base_dir, queue_name, start_date, end_date,
        )
