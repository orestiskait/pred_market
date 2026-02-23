"""Parquet storage for NWP model data with latency tracking.

Provides append-friendly parquet I/O for NWP data ingested via SNS/SQS
notifications. Every row includes latency columns tracking the time
between data reference (model cycle/valid time) and when we received
and persisted it.

Storage layout:
  data/nwp_realtime/<model>/<ICAO>_<YYYY-MM-DD>.parquet

Latency columns:
  - notification_ts       : when AWS SNS published the S3 event
  - saved_ts              : when we persisted to parquet
  - notification_latency_s: notification_ts - cycle_utc (data availability delay)
  - ingest_latency_s      : saved_ts - notification_ts (our processing time)
  - total_latency_s       : saved_ts - cycle_utc (end-to-end)
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class NWPRealtimeStorage:
    """Append-friendly parquet I/O for real-time NWP data with latency tracking."""

    def __init__(self, data_dir: str | Path):
        self.base_dir = Path(data_dir) / "nwp_realtime"
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _model_dir(self, model_name: str) -> Path:
        d = self.base_dir / model_name
        d.mkdir(parents=True, exist_ok=True)
        return d

    def save(
        self,
        df: pd.DataFrame,
        model_name: str,
        notification_ts: datetime,
    ) -> None:
        """Save NWP data with latency metadata.

        Adds latency tracking columns and persists per-station per-day.

        Parameters
        ----------
        df : pd.DataFrame
            NWP point data from any fetcher (must have cycle_utc, station columns).
        model_name : str
            Model identifier (e.g. "hrrr", "rrfs", "nbm").
        notification_ts : datetime
            When the SNS notification was published (from the SNS message).
        """
        if df.empty:
            return

        df = df.copy()
        saved_ts = pd.Timestamp.now(tz="UTC")
        notif_ts = pd.Timestamp(notification_ts)
        if notif_ts.tzinfo is None:
            notif_ts = notif_ts.tz_localize("UTC")

        df["notification_ts"] = notif_ts
        df["saved_ts"] = saved_ts

        # Compute latencies (in seconds)
        if "cycle_utc" in df.columns:
            cycle = pd.to_datetime(df["cycle_utc"], utc=True)
            df["notification_latency_s"] = (
                (notif_ts - cycle).dt.total_seconds().round(1)
            )
            df["total_latency_s"] = (
                (saved_ts - cycle).dt.total_seconds().round(1)
            )
        df["ingest_latency_s"] = round(
            (saved_ts - notif_ts).total_seconds(), 1
        )

        # Persist per station per day
        model_dir = self._model_dir(model_name)
        for station_icao in df["station"].unique():
            stn_df = df[df["station"] == station_icao]
            for cycle_date in stn_df["cycle_utc"].dt.date.unique():
                day_df = stn_df[stn_df["cycle_utc"].dt.date == cycle_date]
                self._append(model_dir, station_icao, cycle_date, day_df)

    def _append(
        self,
        model_dir: Path,
        station_icao: str,
        cycle_date: date,
        df: pd.DataFrame,
    ) -> None:
        """Append rows to a per-station per-day parquet file."""
        path = model_dir / f"{station_icao}_{cycle_date.isoformat()}.parquet"

        if path.exists():
            existing = pd.read_parquet(path)
            combined = pd.concat([existing, df], ignore_index=True)
            dedup_cols = [
                c for c in ("station", "cycle_utc", "forecast_minutes", "model")
                if c in combined.columns
            ]
            if dedup_cols:
                combined = combined.drop_duplicates(subset=dedup_cols, keep="last")
        else:
            combined = df

        sort_cols = [c for c in ("cycle_utc", "forecast_minutes") if c in combined.columns]
        if sort_cols:
            combined = combined.sort_values(sort_cols, ignore_index=True)

        combined.to_parquet(path, index=False)
        logger.info(
            "Saved %d rows → %s (notification_latency=%.0fs, ingest_latency=%.1fs)",
            len(df), path,
            df["notification_latency_s"].mean() if "notification_latency_s" in df.columns else 0,
            df["ingest_latency_s"].mean() if "ingest_latency_s" in df.columns else 0,
        )

    def read(
        self,
        model_name: str,
        station_icao: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """Read saved NWP realtime data, optionally filtered."""
        model_dir = self.base_dir / model_name
        if not model_dir.exists():
            return pd.DataFrame()

        pattern = f"{station_icao}_*.parquet" if station_icao else "*.parquet"
        files = sorted(model_dir.glob(pattern))
        if not files:
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        for f in files:
            parts = f.stem.split("_", 1)
            if len(parts) == 2:
                try:
                    file_date = date.fromisoformat(parts[1])
                except ValueError:
                    continue
                if start_date and file_date < start_date:
                    continue
                if end_date and file_date > end_date:
                    continue
            frames.append(pd.read_parquet(f))

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)


class MADISRealtimeStorage:
    """Append-friendly parquet I/O for MADIS observation data with latency tracking.

    Storage layout:
      data/madis_realtime/<source_name>/<ICAO>_<YYYY-MM-DD>.parquet

    Where <source_name> is "madis_metar" or "madis_omo".

    Latency columns:
      - notification_ts       : when AWS SNS published the S3 event
      - saved_ts              : when we persisted to parquet
      - notification_latency_s: notification_ts - obs_time_utc (obs availability delay)
      - ingest_latency_s      : saved_ts - notification_ts (our processing time)
      - total_latency_s       : saved_ts - obs_time_utc (end-to-end)
    """

    def __init__(self, data_dir: str | Path):
        self.base_dir = Path(data_dir) / "madis_realtime"
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _source_dir(self, source_name: str) -> Path:
        d = self.base_dir / source_name
        d.mkdir(parents=True, exist_ok=True)
        return d

    def save(
        self,
        df: pd.DataFrame,
        source_name: str,
        notification_ts: datetime,
    ) -> None:
        """Save MADIS obs data with latency metadata.

        Parameters
        ----------
        df : pd.DataFrame
            MADIS observation data (must have obs_time_utc, station columns).
        source_name : str
            Source identifier ("madis_metar" or "madis_omo").
        notification_ts : datetime
            When the SNS notification was published.
        """
        if df.empty:
            return

        df = df.copy()
        saved_ts = pd.Timestamp.now(tz="UTC")
        notif_ts = pd.Timestamp(notification_ts)
        if notif_ts.tzinfo is None:
            notif_ts = notif_ts.tz_localize("UTC")

        df["notification_ts"] = notif_ts
        df["saved_ts"] = saved_ts

        # Compute latencies using obs_time_utc
        if "obs_time_utc" in df.columns:
            obs_time = pd.to_datetime(df["obs_time_utc"], utc=True)
            df["notification_latency_s"] = (
                (notif_ts - obs_time).dt.total_seconds().round(1)
            )
            df["total_latency_s"] = (
                (saved_ts - obs_time).dt.total_seconds().round(1)
            )
        df["ingest_latency_s"] = round(
            (saved_ts - notif_ts).total_seconds(), 1
        )

        # Persist per station per day
        source_dir = self._source_dir(source_name)
        for station_icao in df["station"].unique():
            stn_df = df[df["station"] == station_icao]
            for obs_date in stn_df["obs_time_utc"].dt.date.unique():
                day_df = stn_df[stn_df["obs_time_utc"].dt.date == obs_date]
                self._append(source_dir, station_icao, obs_date, day_df)

    def _append(
        self,
        source_dir: Path,
        station_icao: str,
        obs_date: date,
        df: pd.DataFrame,
    ) -> None:
        """Append rows to a per-station per-day parquet file."""
        path = source_dir / f"{station_icao}_{obs_date.isoformat()}.parquet"

        if path.exists():
            existing = pd.read_parquet(path)
            combined = pd.concat([existing, df], ignore_index=True)
            dedup_cols = [
                c for c in ("station", "obs_time_utc", "source")
                if c in combined.columns
            ]
            if dedup_cols:
                combined = combined.drop_duplicates(subset=dedup_cols, keep="last")
        else:
            combined = df

        sort_cols = [c for c in ("obs_time_utc",) if c in combined.columns]
        if sort_cols:
            combined = combined.sort_values(sort_cols, ignore_index=True)

        combined.to_parquet(path, index=False)
        logger.info(
            "Saved %d rows → %s (notification_latency=%.0fs, ingest_latency=%.1fs)",
            len(df), path,
            df["notification_latency_s"].mean() if "notification_latency_s" in df.columns else 0,
            df["ingest_latency_s"].mean() if "ingest_latency_s" in df.columns else 0,
        )

    def read(
        self,
        source_name: str,
        station_icao: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """Read saved MADIS realtime data, optionally filtered."""
        source_dir = self.base_dir / source_name
        if not source_dir.exists():
            return pd.DataFrame()

        pattern = f"{station_icao}_*.parquet" if station_icao else "*.parquet"
        files = sorted(source_dir.glob(pattern))
        if not files:
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []
        for f in files:
            parts = f.stem.split("_", 1)
            if len(parts) == 2:
                try:
                    file_date = date.fromisoformat(parts[1])
                except ValueError:
                    continue
                if start_date and file_date < start_date:
                    continue
                if end_date and file_date > end_date:
                    continue
            frames.append(pd.read_parquet(f))

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
