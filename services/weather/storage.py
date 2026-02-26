"""Parquet storage for NWP model data and MADIS observations with latency tracking.

Storage layouts:
  data/weather/nwp_realtime/<model>/<ICAO>_<YYYY-MM-DD>.parquet
  data/weather/madis_realtime/<source>/<ICAO>_<YYYY-MM-DD>.parquet

Latency columns (added automatically by save()):
  - notification_ts        : when AWS SNS published the S3 event
  - saved_ts               : when we persisted to parquet
  - notification_latency_s : notification_ts minus data reference time
  - ingest_latency_s       : saved_ts minus notification_ts
  - total_latency_s        : saved_ts minus data reference time
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from services.core.parquet_store import PerStationDayStore

logger = logging.getLogger(__name__)


def _add_latency_columns(
    df: pd.DataFrame,
    notification_ts: datetime,
    reference_col: str,
) -> pd.DataFrame:
    """Add notification_ts, saved_ts, and latency columns to *df* in-place."""
    saved_ts = pd.Timestamp.now(tz="UTC")
    notif_ts = pd.Timestamp(notification_ts)
    if notif_ts.tzinfo is None:
        notif_ts = notif_ts.tz_localize("UTC")

    df["notification_ts"] = notif_ts
    df["saved_ts"] = saved_ts

    if reference_col in df.columns:
        df[reference_col] = pd.to_datetime(df[reference_col], utc=True)
        ref = df[reference_col]
        df["notification_latency_s"] = (notif_ts - ref).dt.total_seconds().round(1)
        df["total_latency_s"] = (saved_ts - ref).dt.total_seconds().round(1)

    df["ingest_latency_s"] = round((saved_ts - notif_ts).total_seconds(), 1)
    return df


def _log_save(label: str, n_rows: int, path: Path, df: pd.DataFrame) -> None:
    logger.info(
        "%s: saved %d rows → %s (notification_latency=%.0fs, ingest_latency=%.1fs)",
        label,
        n_rows,
        path,
        df["notification_latency_s"].mean() if "notification_latency_s" in df.columns else 0,
        df["ingest_latency_s"].mean() if "ingest_latency_s" in df.columns else 0,
    )


class NWPRealtimeStorage(PerStationDayStore):
    """Append-friendly parquet I/O for real-time NWP data with latency tracking."""

    DEDUP_COLS = ["station", "cycle_utc", "forecast_minutes", "model"]
    SORT_COLS = ["cycle_utc", "forecast_minutes"]

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
        _add_latency_columns(df, notification_ts, "cycle_utc")

        model_dir = self._subdir(model_name)
        for station_icao in df["station"].unique():
            stn_df = df[df["station"] == station_icao]
            for cycle_date in stn_df["cycle_utc"].dt.date.unique():
                day_df = stn_df[stn_df["cycle_utc"].dt.date == cycle_date]
                path = self._append_parquet(
                    model_dir, station_icao, cycle_date, day_df,
                    dedup_cols=self.DEDUP_COLS, sort_cols=self.SORT_COLS,
                )
                _log_save(model_name, len(day_df), path, day_df)

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
        _add_latency_columns(df, notification_ts, "obs_time_utc")

        source_dir = self._subdir(source_name)
        for station_icao in df["station"].unique():
            stn_df = df[df["station"] == station_icao]
            for obs_date in stn_df["obs_time_utc"].dt.date.unique():
                day_df = stn_df[stn_df["obs_time_utc"].dt.date == obs_date]
                path = self._append_parquet(
                    source_dir, station_icao, obs_date, day_df,
                    dedup_cols=self.DEDUP_COLS, sort_cols=self.SORT_COLS,
                )
                _log_save(source_name, len(day_df), path, day_df)

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
