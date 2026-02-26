"""Per-station, per-day Parquet storage base class.

Provides append-friendly parquet I/O with deduplication and date-range reads.
Subclass and override save()/read() for domain-specific logic; use the
protected helpers for the common append + read pattern.

Storage layout (conventional):
  <base_dir>/<subdirectory>/<STATION>_<YYYY-MM-DD>.parquet
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


class PerStationDayStore:
    """Base class for per-station, per-day parquet storage with append + dedup."""

    def __init__(self, base_dir: Path):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _subdir(self, name: str) -> Path:
        d = self.base_dir / name
        d.mkdir(parents=True, exist_ok=True)
        return d

    # ------------------------------------------------------------------
    # Append
    # ------------------------------------------------------------------

    def _append_parquet(
        self,
        directory: Path,
        station: str,
        day: date,
        df: pd.DataFrame,
        *,
        dedup_cols: list[str],
        sort_cols: list[str],
    ) -> Path:
        """Append *df* to ``<directory>/<station>_<day>.parquet``.

        Reads existing data (if any), concatenates, deduplicates (keeping
        the last occurrence), sorts, and rewrites.  Returns the file path.
        """
        path = directory / f"{station}_{day.isoformat()}.parquet"

        if path.exists():
            existing = pd.read_parquet(path)
            # Ensure consistent timezones for concat and sort
            for col in sort_cols:
                if col in existing.columns and pd.api.types.is_datetime64_any_dtype(existing[col]):
                    existing[col] = pd.to_datetime(existing[col], utc=True)
                if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = pd.to_datetime(df[col], utc=True)

            combined = pd.concat([existing, df], ignore_index=True)
            cols = [c for c in dedup_cols if c in combined.columns]
            if cols:
                combined = combined.drop_duplicates(subset=cols, keep="last")
        else:
            combined = df
            for col in sort_cols:
                if col in combined.columns and pd.api.types.is_datetime64_any_dtype(combined[col]):
                    combined[col] = pd.to_datetime(combined[col], utc=True)

        cols = [c for c in sort_cols if c in combined.columns]
        if cols:
            combined = combined.sort_values(cols, ignore_index=True)

        combined.to_parquet(path, index=False)
        return path

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def _read_parquets(
        self,
        directory: Path,
        station: str | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """Read and concatenate per-station per-day parquet files.

        Filters by station glob and date range extracted from filenames.
        """
        if not directory.exists():
            return pd.DataFrame()

        pattern = f"{station}_*.parquet" if station else "*.parquet"
        files = sorted(directory.glob(pattern))
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
