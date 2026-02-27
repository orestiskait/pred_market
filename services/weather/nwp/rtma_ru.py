"""Fetch RTMA Rapid Update analysis data at station coordinates.

RTMA-RU (Real-Time Mesoscale Analysis — Rapid Update) is an analysis product,
not a forecast.  It provides the best estimate of current surface conditions
on a 2.5 km NDFD grid.

RTMA-RU specifics:
  - 2.5 km grid resolution over CONUS
  - Analysis only (fxx=0, no forecast hours)
  - 15-minute temporal resolution (00, 15, 30, 45 past each hour)
  - No .idx file on S3 — Herbie downloads the full GRIB2 file

Data source: NOAA RTMA via AWS S3 (s3://noaa-rtma-pds), public, no auth.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from services.weather.nwp.base import NWPPointFetcher
from services.weather.station_registry import NWPStation

logger = logging.getLogger(__name__)


class RTMARUFetcher(NWPPointFetcher):
    """RTMA Rapid Update analysis point fetcher (15-minute cadence)."""

    SOURCE_NAME = "rtma_ru"
    HERBIE_MODEL = "rtma_ru"
    HERBIE_PRODUCT = "anl"
    DEFAULT_MAX_FXX = 0
    # 15-min cycles: 96 per day (00, 15, 30, 45 past each hour)
    CYCLE_INTERVAL_MINUTES = 15
    MODEL_VERSION = "v2.9"

    def _cycle_datetimes(self, d: date) -> list[datetime]:
        """Yield all 15-min cycle datetimes for a given date (UTC)."""
        base = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
        return [
            base + timedelta(minutes=i * self.CYCLE_INTERVAL_MINUTES)
            for i in range(24 * 60 // self.CYCLE_INTERVAL_MINUTES)
        ]

    def fetch_date_range(
        self,
        start_date: date,
        end_date: date,
        stations: list[NWPStation],
        cycles: list[int] | None = None,
        fxx_range: range | None = None,
        save: bool = True,
    ) -> pd.DataFrame:
        """Fetch 15-minute RTMA-RU analysis for a date range (96 cycles/day)."""
        all_frames = []
        current = start_date
        while current <= end_date:
            for cycle_dt in self._cycle_datetimes(current):
                logger.info(
                    "%s: fetching cycle %s",
                    self.SOURCE_NAME,
                    cycle_dt.strftime("%Y-%m-%d %H:%MZ"),
                )
                try:
                    df = self.fetch_cycle(cycle_dt, stations, fxx_range)
                except Exception:
                    logger.exception(
                        "%s: failed cycle %s",
                        self.SOURCE_NAME,
                        cycle_dt.strftime("%Y-%m-%d %H:%MZ"),
                    )
                    continue

                if not df.empty:
                    if save:
                        self._save_by_station(df, current)
                    all_frames.append(df)

            current += timedelta(days=1)

        if not all_frames:
            return pd.DataFrame()
        return pd.concat(all_frames, ignore_index=True)

    def fetch_latest(
        self,
        stations: list[NWPStation],
        fxx_range: range | None = None,
        lookback_hours: int = 6,
        save: bool = True,
    ) -> pd.DataFrame:
        """Find the most recent 15-min cycle and fetch it."""
        # Round down to nearest 15 min, then search backwards
        now = datetime.now(timezone.utc)
        minute = (now.minute // self.CYCLE_INTERVAL_MINUTES) * self.CYCLE_INTERVAL_MINUTES
        now = now.replace(minute=minute, second=0, microsecond=0)

        n_candidates = lookback_hours * 60 // self.CYCLE_INTERVAL_MINUTES
        for i in range(n_candidates):
            candidate = now - timedelta(minutes=i * self.CYCLE_INTERVAL_MINUTES)
            try:
                self._make_herbie(candidate, fxx=0)
            except Exception:
                continue

            logger.info(
                "%s: latest available cycle: %s",
                self.SOURCE_NAME,
                candidate.strftime("%Y-%m-%d %H:%MZ"),
            )
            df = self.fetch_cycle(candidate, stations, fxx_range)
            if save and not df.empty:
                self._save_by_station(df, candidate.date())
            return df

        logger.warning(
            "%s: no cycle found in the last %d hours",
            self.SOURCE_NAME,
            lookback_hours,
        )
        return pd.DataFrame()
