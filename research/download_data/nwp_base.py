"""Base class for NWP (Numerical Weather Prediction) model point extraction.

Provides Herbie-based GRIB2 download, nearest-grid-point extraction at station
coordinates, multi-step support (sub-hourly models), Parquet I/O, latest-cycle
discovery, and historical backfill.

All NWP fetchers (HRRR, RTMA-RU, RRFS, NBM) extend this base.  Subclasses
override class attributes to configure for their specific model/product.

Dependencies: herbie-data, xarray, cfgrib, eccodes (in services/requirements.txt).

Data sources:  all public S3, no auth required.
  - HRRR:    s3://noaa-hrrr-bdp-pcs
  - RTMA-RU: s3://noaa-rtma-pds
  - RRFS:    s3://noaa-rrfs-pds
  - NBM:     s3://noaa-nbm-grib2-pds
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.weather.hrrr_station_registry import HRRRStation

logger = logging.getLogger(__name__)

# ======================================================================
# Unit conversions
# ======================================================================

_K_TO_F_SCALE = 9.0 / 5.0
_K_TO_F_OFFSET = 459.67


def kelvin_to_fahrenheit(k: float) -> float:
    return k * _K_TO_F_SCALE - _K_TO_F_OFFSET


# ======================================================================
# Default variable search strings
# ======================================================================

DEFAULT_VARIABLES: list[tuple[str, str]] = [
    (":TMP:2 m above ground:", "tmp_2m"),
]


# ======================================================================
# Base class
# ======================================================================

class NWPPointFetcher:
    """Base for NWP model point extraction at station coordinates.

    Subclasses override the class-level ``HERBIE_*`` attributes and
    ``DEFAULT_*`` settings, then inherit all fetch / storage methods.

    Two main usage patterns:

    1. **Latest cycle** — ``fetch_latest()`` finds the newest available
       model run and extracts all forecast steps.  Designed for cron / watcher.

    2. **Historical backfill** — ``fetch_date_range(start, end)`` pulls
       archived cycles for a date range.

    Storage: one parquet per station per cycle date at
    ``<data_dir>/<SOURCE_NAME>/<ICAO>_<YYYY-MM-DD>.parquet``.
    """

    # --- Override in subclass ------------------------------------------------
    SOURCE_NAME: str = ""
    HERBIE_MODEL: str = ""
    HERBIE_PRODUCT: str = ""
    HERBIE_KWARGS: dict[str, Any] = {}
    DEFAULT_VARIABLES: list[tuple[str, str]] = DEFAULT_VARIABLES
    DEFAULT_MAX_FXX: int = 18
    DEFAULT_CYCLES: list[int] = [0, 6, 12, 18]
    # -------------------------------------------------------------------------

    def __init__(
        self,
        data_dir: Path | str | None = None,
        variables: list[tuple[str, str]] | None = None,
        max_forecast_hour: int | None = None,
    ):
        if data_dir is None:
            data_dir = Path(__file__).resolve().parent.parent.parent / "data"
        self.data_dir = Path(data_dir) / self.SOURCE_NAME
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.variables = variables or list(self.DEFAULT_VARIABLES)
        self.max_forecast_hour = (
            max_forecast_hour if max_forecast_hour is not None else self.DEFAULT_MAX_FXX
        )

    # ------------------------------------------------------------------
    # Config integration
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls, config_path: str | Path) -> "NWPPointFetcher":
        """Create a fetcher using data_dir and model settings from config.yaml."""
        import yaml

        config_path = Path(config_path)
        with open(config_path) as f:
            cfg = yaml.safe_load(f)

        storage = cfg.get("storage", {})
        rel_data_dir = storage.get("data_dir", "../data")
        data_dir = (config_path.parent / rel_data_dir).resolve()

        model_cfg = cfg.get(cls.SOURCE_NAME, {})
        max_fxx = model_cfg.get("max_forecast_hour", cls.DEFAULT_MAX_FXX)

        return cls(data_dir=data_dir, max_forecast_hour=max_fxx)

    # ------------------------------------------------------------------
    # Herbie helpers
    # ------------------------------------------------------------------

    def _make_herbie(self, cycle: datetime, fxx: int):
        """Create a Herbie object for this model, product, and fxx."""
        from herbie import Herbie

        return Herbie(
            cycle.strftime("%Y-%m-%d %H:%M"),
            model=self.HERBIE_MODEL,
            product=self.HERBIE_PRODUCT,
            fxx=fxx,
            verbose=False,
            **self.HERBIE_KWARGS,
        )

    # ------------------------------------------------------------------
    # Core fetch: single model run + single fxx
    # ------------------------------------------------------------------

    def fetch_run(
        self,
        cycle: datetime,
        fxx: int,
        stations: list[HRRRStation],
    ) -> pd.DataFrame:
        """Fetch one model run (cycle + fxx) and extract point values.

        Handles both single-step files (hourly models) and multi-step files
        (sub-hourly HRRR) transparently.

        Returns DataFrame with columns: station, city, model, cycle_utc,
        forecast_minutes, valid_utc, valid_local, <var>_k, <var>_f,
        grid_lat, grid_lon.
        """
        H = self._make_herbie(cycle, fxx)

        all_rows: list[dict] = []
        for search_str, col_prefix in self.variables:
            try:
                ds = H.xarray(search_str, remove_grib=True)
            except Exception:
                logger.warning(
                    "%s: could not read %s for cycle=%s fxx=%02d",
                    self.SOURCE_NAME, search_str,
                    cycle.strftime("%Y-%m-%d %HZ"), fxx,
                )
                continue

            rows = self._extract_from_dataset(ds, cycle, stations, col_prefix)
            all_rows.extend(rows)
            ds.close()

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        df = _add_valid_local(df, stations)
        return df

    def _extract_from_dataset(
        self,
        ds,
        cycle: datetime,
        stations: list[HRRRStation],
        col_prefix: str,
    ) -> list[dict]:
        """Extract point values from an xarray Dataset.

        Handles both multi-step (sub-hourly) and single-step datasets.
        """
        var_name = list(ds.data_vars)[0]
        lats = ds.latitude.values
        lons = ds.longitude.values
        lons_norm = np.where(lons > 180, lons - 360, lons)

        # Precompute nearest grid indices (same for all timesteps)
        stn_idx: dict[str, tuple[int, int, float, float]] = {}
        for stn in stations:
            dist_sq = (lats - stn.lat) ** 2 + (lons_norm - stn.lon) ** 2
            iy, ix = np.unravel_index(dist_sq.argmin(), dist_sq.shape)
            stn_idx[stn.icao] = (
                int(iy), int(ix),
                round(float(lats[iy, ix]), 4),
                round(float(lons_norm[iy, ix]), 4),
            )

        # Determine timesteps in the dataset
        steps = _resolve_steps(ds)

        rows: list[dict] = []
        for step_val in steps:
            forecast_minutes = int(step_val / np.timedelta64(1, "m"))
            valid_utc = cycle + timedelta(minutes=forecast_minutes)

            if "step" in ds.dims:
                data = ds[var_name].sel(step=step_val).values
            else:
                data = ds[var_name].values

            for stn in stations:
                iy, ix, glat, glon = stn_idx[stn.icao]
                val_k = float(data[iy, ix])

                rows.append({
                    "station": stn.icao,
                    "city": stn.city,
                    "model": self.SOURCE_NAME,
                    "cycle_utc": pd.Timestamp(cycle, tz="UTC"),
                    "forecast_minutes": forecast_minutes,
                    "valid_utc": pd.Timestamp(valid_utc, tz="UTC"),
                    f"{col_prefix}_k": round(val_k, 2),
                    f"{col_prefix}_f": round(kelvin_to_fahrenheit(val_k), 1),
                    "grid_lat": glat,
                    "grid_lon": glon,
                })

        return rows

    # ------------------------------------------------------------------
    # Compound fetches
    # ------------------------------------------------------------------

    def fetch_cycle(
        self,
        cycle: datetime,
        stations: list[HRRRStation],
        fxx_range: range | None = None,
    ) -> pd.DataFrame:
        """Fetch all fxx values for one model cycle."""
        if fxx_range is None:
            fxx_range = range(0, self.max_forecast_hour + 1)

        frames: list[pd.DataFrame] = []
        for fxx in fxx_range:
            try:
                df = self.fetch_run(cycle, fxx, stations)
                if not df.empty:
                    frames.append(df)
            except Exception:
                logger.warning(
                    "%s: skipping fxx=%02d for cycle %s (not available)",
                    self.SOURCE_NAME, fxx, cycle.strftime("%Y-%m-%d %HZ"),
                )

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def fetch_latest(
        self,
        stations: list[HRRRStation],
        fxx_range: range | None = None,
        lookback_hours: int = 6,
        save: bool = True,
    ) -> pd.DataFrame:
        """Find the most recent available cycle and fetch it.

        Searches backwards from the current UTC hour.  Safe to call
        repeatedly — ``save_parquet`` deduplicates on
        (station, cycle_utc, forecast_minutes).
        """
        now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

        for offset in range(lookback_hours):
            candidate = now - timedelta(hours=offset)
            try:
                self._make_herbie(candidate, fxx=0)
            except Exception:
                continue

            logger.info(
                "%s: latest available cycle: %s",
                self.SOURCE_NAME, candidate.strftime("%Y-%m-%d %HZ"),
            )
            df = self.fetch_cycle(candidate, stations, fxx_range)
            if save and not df.empty:
                self._save_by_station(df, candidate.date())
            return df

        logger.warning(
            "%s: no cycle found in the last %d hours",
            self.SOURCE_NAME, lookback_hours,
        )
        return pd.DataFrame()

    def fetch_date_range(
        self,
        start_date: date,
        end_date: date,
        stations: list[HRRRStation],
        cycles: list[int] | None = None,
        fxx_range: range | None = None,
        save: bool = True,
    ) -> pd.DataFrame:
        """Historical backfill: fetch data for a date range.

        Parameters
        ----------
        cycles : list[int], optional
            Which UTC cycle hours to fetch (default: cls.DEFAULT_CYCLES).
        """
        if cycles is None:
            cycles = list(self.DEFAULT_CYCLES)

        all_frames: list[pd.DataFrame] = []
        current = start_date
        while current <= end_date:
            for cycle_hour in cycles:
                cycle_dt = datetime(current.year, current.month, current.day, cycle_hour)
                logger.info(
                    "%s: fetching cycle %s",
                    self.SOURCE_NAME, cycle_dt.strftime("%Y-%m-%d %HZ"),
                )
                try:
                    df = self.fetch_cycle(cycle_dt, stations, fxx_range)
                except Exception:
                    logger.exception(
                        "%s: failed cycle %s",
                        self.SOURCE_NAME, cycle_dt.strftime("%Y-%m-%d %HZ"),
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

    # ------------------------------------------------------------------
    # Parquet storage
    # ------------------------------------------------------------------

    def save_parquet(
        self, df: pd.DataFrame, station_icao: str, cycle_date: date
    ) -> Path:
        """Save/append: ``<source>/<ICAO>_<date>.parquet``.

        Deduplicates on (station, cycle_utc, forecast_minutes).
        """
        if df.empty:
            return self.data_dir

        path = self.data_dir / f"{station_icao}_{cycle_date.isoformat()}.parquet"

        if path.exists():
            existing = pd.read_parquet(path)
            combined = pd.concat([existing, df], ignore_index=True)
            dedup_cols = [c for c in ("station", "cycle_utc", "forecast_minutes")
                         if c in combined.columns]
            if dedup_cols:
                combined = combined.drop_duplicates(subset=dedup_cols, keep="last")
        else:
            combined = df

        combined = combined.sort_values(
            ["cycle_utc", "forecast_minutes"], ignore_index=True
        )
        combined.to_parquet(path, index=False)
        logger.info("Saved %d rows → %s", len(combined), path)
        return path

    def _save_by_station(self, df: pd.DataFrame, cycle_date: date) -> None:
        for icao in df["station"].unique():
            self.save_parquet(df[df["station"] == icao].copy(), icao, cycle_date)

    def read_parquet(self, station_icao: str, cycle_date: date) -> pd.DataFrame:
        """Read saved data for one station on one cycle date."""
        path = self.data_dir / f"{station_icao}_{cycle_date.isoformat()}.parquet"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_parquet(path)

    def read_all(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """Read all saved parquets, optionally filtered by date range."""
        files = sorted(self.data_dir.glob("*.parquet"))
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


# ======================================================================
# Helpers
# ======================================================================

def _resolve_steps(ds) -> list:
    """Return a list of numpy timedelta64 step values from a dataset.

    Handles both multi-step datasets (sub-hourly: step is a dimension)
    and single-step datasets (hourly: step is a scalar coordinate).
    """
    if "step" in ds.dims:
        return list(ds.step.values)
    if "step" in ds.coords:
        return [ds.step.values]
    return [np.timedelta64(0, "ns")]


def _add_valid_local(
    df: pd.DataFrame, stations: list[HRRRStation]
) -> pd.DataFrame:
    """Add a 'valid_local' column with timezone-localized valid times."""
    tz_map = {stn.icao: stn.tz for stn in stations}
    parts: list[pd.DataFrame] = []
    for icao, group in df.groupby("station"):
        group = group.copy()
        tz = tz_map.get(icao)
        if tz:
            group["valid_local"] = (
                group["valid_utc"].dt.tz_convert(tz).dt.tz_localize(None)
            )
        parts.append(group)
    return pd.concat(parts, ignore_index=True) if parts else df
