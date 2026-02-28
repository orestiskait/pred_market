"""Data loading layer for the Weather Brain model pipeline.

Reads parquet files from the standard data directory layout and returns
clean, typed DataFrames ready for feature engineering.  No business logic
or feature computation happens here — this module is purely I/O.

Directory layout expected:
  data/weather/wethr_push/observations/{ICAO}_{date}.parquet
  data/weather/wethr_push/cli/{ICAO}_{date}.parquet
  data/weather/wethr_push/dsm/{ICAO}_{date}.parquet
  data/weather/nwp_realtime/nbm/{ICAO}_{date}.parquet
  data/weather/nwp_realtime/rrfs/{ICAO}_{date}.parquet
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────────────────────────────

def _read_icao_date_files(
    base_dir: Path,
    icao: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Glob and concatenate parquet files matching '{ICAO}_{date}.parquet'.

    Files are filtered by date stem (yyyy-mm-dd) in [start_date, end_date].
    Returns an empty DataFrame if no files exist.
    """
    pattern = f"{icao}_*.parquet"
    files = sorted(base_dir.glob(pattern))
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()
    # Filter by date stem embedded in filename: KMDW_2025-05-28.parquet → 2025-05-28
    files = [
        f for f in files
        if start_str <= f.stem[len(icao) + 1:] <= end_str
    ]
    if not files:
        return pd.DataFrame()

    tables = [pq.read_table(f) for f in files]
    import pyarrow as pa
    combined = pa.concat_tables(tables, promote_options="default").to_pandas()
    logger.debug(
        "Loaded %d rows from %d files in %s for %s [%s→%s]",
        len(combined), len(files), base_dir.name, icao, start_str, end_str,
    )
    return combined


# ──────────────────────────────────────────────────────────────────────
# ModelDataLoader
# ──────────────────────────────────────────────────────────────────────

class ModelDataLoader:
    """Load all data sources for Weather Brain training and inference.

    Parameters
    ----------
    data_dir : str | Path
        Project root ``data/`` directory.
    icao : str
        Station ICAO code (e.g. ``"KMDW"``).
    """

    # Sub-path constants (relative to data_dir)
    _OBS_SUBPATH  = Path("weather", "wethr_push", "observations")
    _CLI_SUBPATH  = Path("weather", "wethr_push", "cli")
    _DSM_SUBPATH  = Path("weather", "wethr_push", "dsm")
    _NBM_SUBPATH  = Path("weather", "nwp_realtime", "nbm")
    _RRFS_SUBPATH = Path("weather", "nwp_realtime", "rrfs")

    def __init__(self, data_dir: str | Path, icao: str):
        self.data_dir = Path(data_dir)
        self.icao = icao

    # ------------------------------------------------------------------
    # Individual source loaders
    # ------------------------------------------------------------------

    def load_observations(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Load wethr_push observations (both ASOS-HFM and ASOS-HR).

        Key columns: observation_time_utc, product, temperature_fahrenheit,
        dew_point_fahrenheit, wind_speed_mph, wind_gust_mph, wind_direction,
        altimeter_inhg.
        """
        df = _read_icao_date_files(
            self.data_dir / self._OBS_SUBPATH, self.icao, start_date, end_date
        )
        if df.empty:
            return df

        # Normalise timestamp column (observed parquet has observation_time_utc)
        if "observation_time_utc" in df.columns:
            df["observation_time_utc"] = pd.to_datetime(
                df["observation_time_utc"], utc=True
            )
        df = df.sort_values("observation_time_utc").reset_index(drop=True)
        return df

    def load_cli(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Load CLI (Climatological Local report) ground-truth labels.

        Key columns: for_date (YYYY-MM-DD LST), high_f.
        """
        df = _read_icao_date_files(
            self.data_dir / self._CLI_SUBPATH, self.icao, start_date, end_date
        )
        if not df.empty and "received_ts_utc" in df.columns:
            df["received_ts_utc"] = pd.to_datetime(df["received_ts_utc"], utc=True)
        return df

    def load_dsm(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Load DSM (Daily Summary Message) fallback labels.

        Key columns: for_date, high_f.  Used only when CLI is unavailable.
        """
        df = _read_icao_date_files(
            self.data_dir / self._DSM_SUBPATH, self.icao, start_date, end_date
        )
        return df

    def load_nbm(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Load NBM hourly forecast data.

        Key columns: model_run_time_utc, forecast_target_time_utc, tmp_2m_f.
        """
        df = _read_icao_date_files(
            self.data_dir / self._NBM_SUBPATH, self.icao, start_date, end_date
        )
        if df.empty:
            return df

        for col in ("model_run_time_utc", "forecast_target_time_utc"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True)
        df = df.sort_values(["model_run_time_utc", "lead_time_minutes"]).reset_index(drop=True)
        return df

    def load_rrfs(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Load RRFS subhourly forecast data.

        Same schema as NBM but with 15-min lead times and 4×-daily cycles.
        Returns empty DataFrame when no RRFS files exist for the period (expected).
        """
        df = _read_icao_date_files(
            self.data_dir / self._RRFS_SUBPATH, self.icao, start_date, end_date
        )
        if df.empty:
            return df

        for col in ("model_run_time_utc", "forecast_target_time_utc"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], utc=True)
        df = df.sort_values(["model_run_time_utc", "lead_time_minutes"]).reset_index(drop=True)
        return df

    # ------------------------------------------------------------------
    # Composite loader
    # ------------------------------------------------------------------

    def load_all(self, start_date: date, end_date: date) -> dict[str, pd.DataFrame]:
        """Load all data sources for a date range.

        Returns a dict with keys: ``obs``, ``cli``, ``dsm``, ``nbm``, ``rrfs``.
        """
        return {
            "obs":  self.load_observations(start_date, end_date),
            "cli":  self.load_cli(start_date, end_date),
            "dsm":  self.load_dsm(start_date, end_date),
            "nbm":  self.load_nbm(start_date, end_date),
            "rrfs": self.load_rrfs(start_date, end_date),
        }

    # ------------------------------------------------------------------
    # Helpers for single-day data access (used during feature engineering)
    # ------------------------------------------------------------------

    def cli_label_map(self, start_date: date, end_date: date) -> dict[str, int]:
        """Return {climate_date_str → high_f} mapping from CLI + DSM.

        CLI takes priority over DSM when both exist for the same date.
        Returns integer °F values (NWS official high).
        """
        label_map: dict[str, int] = {}

        # Load DSM first (lower priority — will be overwritten by CLI)
        dsm = self.load_dsm(start_date, end_date)
        if not dsm.empty and "for_date" in dsm.columns and "high_f" in dsm.columns:
            for _, row in dsm.iterrows():
                label_map[str(row["for_date"])] = int(row["high_f"])

        # Load CLI second (higher priority)
        cli = self.load_cli(start_date, end_date)
        if not cli.empty and "for_date" in cli.columns and "high_f" in cli.columns:
            for _, row in cli.iterrows():
                label_map[str(row["for_date"])] = int(row["high_f"])

        return label_map
