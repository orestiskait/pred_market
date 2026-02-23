"""Parquet storage for aviation weather METAR data with latency tracking.

Provides append-friendly parquet I/O for AWC METAR and NWS observations.
Every row includes saved_ts (when we persisted) and ob_time_utc (data reference).
Tracks temperature, raw observation, RMK portion, and 6hr/24hr min/max.

Storage layout:
  data/aviationweather_metar/<source>/<ICAO>_<YYYY-MM-DD>.parquet

Sources: awc_metar (Aviation Weather Center), nws_observations (api.weather.gov)
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class AviationWeatherMetarStorage:
    """Append-friendly parquet I/O for aviation weather METAR with latency tracking."""

    def __init__(self, data_dir: str | Path):
        self.base_dir = Path(data_dir) / "aviationweather_metar"
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _source_dir(self, source_name: str) -> Path:
        d = self.base_dir / source_name
        d.mkdir(parents=True, exist_ok=True)
        return d

    def save(
        self,
        df: pd.DataFrame,
        source_name: str,
    ) -> None:
        """Save aviation weather data with saved_ts and latency metadata.

        Parameters
        ----------
        df : pd.DataFrame
            Must have: ob_time_utc, station, source. Optional: temp_c, temp_f,
            raw_ob, rmk, temp_6hr_min, temp_6hr_max, temp_24hr_min, temp_24hr_max.
        source_name : str
            "awc_metar" or "nws_observations"
        """
        if df.empty:
            return

        df = df.copy()
        saved_ts = pd.Timestamp.now(tz="UTC")
        df["saved_ts"] = saved_ts

        if "ob_time_utc" in df.columns:
            ob_time = pd.to_datetime(df["ob_time_utc"], utc=True)
            df["total_latency_s"] = (
                (saved_ts - ob_time).dt.total_seconds().round(1)
            )

        source_dir = self._source_dir(source_name)
        for station_icao in df["station"].unique():
            stn_df = df[df["station"] == station_icao]
            for obs_date in stn_df["ob_time_utc"].dt.date.unique():
                day_df = stn_df[stn_df["ob_time_utc"].dt.date == obs_date]
                self._append(source_dir, station_icao, obs_date, day_df)

    def _append(
        self,
        source_dir: Path,
        station_icao: str,
        obs_date: date,
        df: pd.DataFrame,
    ) -> None:
        path = source_dir / f"{station_icao}_{obs_date.isoformat()}.parquet"

        if path.exists():
            existing = pd.read_parquet(path)
            combined = pd.concat([existing, df], ignore_index=True)
            dedup_cols = ["station", "ob_time_utc", "source"]
            dedup_cols = [c for c in dedup_cols if c in combined.columns]
            if dedup_cols:
                combined = combined.drop_duplicates(subset=dedup_cols, keep="last")
        else:
            combined = df

        if "ob_time_utc" in combined.columns:
            combined = combined.sort_values("ob_time_utc", ignore_index=True)

        combined.to_parquet(path, index=False)
        lat = df["total_latency_s"].mean() if "total_latency_s" in df.columns else 0
        logger.info(
            "AviationWeather: saved %d rows → %s (latency=%.0fs)",
            len(df), path, lat,
        )

    def read(
        self,
        source_name: str,
        station_icao: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """Read saved aviation weather data, optionally filtered."""
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
