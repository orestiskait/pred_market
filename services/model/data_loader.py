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

from services.model.time_utils import get_latest_record_per_date

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


def _merge_label_map(df: pd.DataFrame, label_map: dict[str, int]) -> None:
    """Write ``{date_str: high_f}`` entries from *df* into *label_map* in-place.

    *df* should already be deduplicated to one row per date.  Existing keys are
    overwritten, which is the intended behaviour when CLI supersedes DSM.
    Only rows where ``high_f`` is non-null are written.
    """
    if df.empty or "for_date_lst" not in df.columns or "high_f" not in df.columns:
        return
    for _, row in df.iterrows():
        if pd.notna(row["high_f"]):
            # Normalise to YYYY-MM-DD (str(Timestamp) may include a time component)
            date_key = str(row["for_date_lst"])[:10]
            label_map[date_key] = int(row["high_f"])


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

        Key columns: for_date_lst (YYYY-MM-DD LST), high_f.
        """
        df = _read_icao_date_files(
            self.data_dir / self._CLI_SUBPATH, self.icao, start_date, end_date
        )
        if not df.empty and "received_ts_utc" in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df["received_ts_utc"]):
                logger.warning("load_cli: 'received_ts_utc' is not datetime in parquet — dtype: %s", df["received_ts_utc"].dtype)
        return df

    def load_dsm(self, start_date: date, end_date: date) -> pd.DataFrame:
        """Load DSM (Daily Summary Message) fallback labels.

        Key columns: for_date_lst, high_f.  Used only when CLI is unavailable.
        """
        df = _read_icao_date_files(
            self.data_dir / self._DSM_SUBPATH, self.icao, start_date, end_date
        )
        if not df.empty and "received_ts_utc" in df.columns:
            if not pd.api.types.is_datetime64_any_dtype(df["received_ts_utc"]):
                logger.warning("load_dsm: 'received_ts_utc' is not datetime in parquet — dtype: %s", df["received_ts_utc"].dtype)
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
        """Return {date_str → high_f} mapping built from DSM (fallback) + CLI (priority).

        When both sources carry a record for the same ``for_date_lst`` the CLI
        value wins.  Within each source, if multiple rows share the same date
        only the one with the latest ``received_ts_utc`` is kept.

        Returns integer °F values (NWS official high).
        """
        label_map: dict[str, int] = {}

        # DSM first — lower priority; CLI will overwrite any shared dates below.
        dsm = get_latest_record_per_date(self.load_dsm(start_date, end_date))
        _merge_label_map(dsm, label_map)

        # CLI second — higher priority; overwrites DSM entries for the same date.
        cli = get_latest_record_per_date(self.load_cli(start_date, end_date))
        _merge_label_map(cli, label_map)

        return label_map
