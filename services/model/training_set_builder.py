"""Training Set Builder — assembles a fully featurised training DataFrame.

Orchestrates the data loading and feature engineering pipeline, applies
validity rules from MODELING_IDEA.MD §7, and produces:
  1. A training DataFrame (X, y) ready for XGBoost training.
  2. A structured drop/warning log saved as parquet.

Validity rules (§7.1):
  Check 1: product == ASOS-HR                → DROP (NOT_METAR) — silent pre-filter
  Check 2: CLI label missing for climate day  → DROP (NO_CLI_LABEL)
  Check 3: custom_intraday_max is NaN         → DROP (NO_PRIOR_OBS)
  Check 4: core obs features are NaN          → DROP (OBS_FEATURE_NAN:{col})
  Check 5: Y_t < 0                            → WARNING (NEGATIVE_TARGET), clip to 0
  Check 6: No NBM cycle at all for day        → WARNING (NBM_ALL_MISSING), keep with NaN
  Check 7: No RRFS cycle at all for day       → WARNING (RRFS_ALL_MISSING), keep with NaN

NWP missing data is NOT a drop reason (decay-weight approach).
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from services.model.constants import (
    Y_MIN,
    MODEL_SUBDIR,
    TRAINING_LOGS_SUBDIR,
)
from services.model.data_loader import ModelDataLoader
from services.model.feature_engine import FeatureEngine

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────
# Drop / Warning log schema
# ──────────────────────────────────────────────────────────────────────

_DROP_LOG_SCHEMA = pa.schema([
    ("station",             pa.string()),
    ("observation_time_utc", pa.timestamp("us", tz="UTC")),
    ("climate_date_lst",    pa.string()),
    ("log_level",           pa.string()),    # "DROP" | "WARNING"
    ("reason_code",         pa.string()),
    ("detail",              pa.string()),
    ("timestamp",           pa.timestamp("us", tz="UTC")),
])

# Core observation columns required for a valid training row
_CORE_OBS_COLS: tuple[str, ...] = ("current_temp_f", "dew_point_f")


# ──────────────────────────────────────────────────────────────────────
# TrainingSetBuilder
# ──────────────────────────────────────────────────────────────────────

class TrainingSetBuilder:
    """Build a featurised (X, y) training set from raw parquet data.

    Parameters
    ----------
    data_dir : str | Path
        Project data/ root directory.
    icao : str
        Station ICAO code.
    lat, lon, tz :
        Station coordinates and IANA timezone (from KalshiMarketConfig).
    """

    def __init__(
        self,
        data_dir: str | Path,
        icao: str,
        lat: float,
        lon: float,
        tz: str,
    ):
        self.data_dir = Path(data_dir)
        self.icao = icao
        self.tz = tz

        self._loader = ModelDataLoader(data_dir, icao)
        self._feature_engine = FeatureEngine(icao, lat, lon, tz)
        self._log_records: list[dict] = []

    # ------------------------------------------------------------------
    # Logging helpers
    # ------------------------------------------------------------------

    def _log(
        self,
        level: str,
        obs_time: Optional[datetime],
        climate_date: str,
        reason_code: str,
        detail: str = "",
    ) -> None:
        self._log_records.append({
            "station":              self.icao,
            "observation_time_utc": (
                pd.Timestamp(obs_time).tz_convert("UTC")
                if obs_time is not None and getattr(obs_time, 'tzinfo', None) is not None
                else pd.Timestamp(obs_time, tz="UTC") if obs_time is not None
                else pd.NaT
            ),
            "climate_date_lst":     climate_date,
            "log_level":            level,
            "reason_code":          reason_code,
            "detail":               detail,
            "timestamp":            pd.Timestamp.now(tz="UTC"),
        })

    def _save_drop_log(self) -> None:
        """Persist accumulated log records to parquet (append if exists)."""
        if not self._log_records:
            return

        log_dir = self.data_dir / MODEL_SUBDIR / TRAINING_LOGS_SUBDIR
        log_dir.mkdir(parents=True, exist_ok=True)
        path = log_dir / f"{self.icao}_drop_log.parquet"

        new_df = pd.DataFrame(self._log_records)
        new_table = pa.Table.from_pandas(new_df, schema=_DROP_LOG_SCHEMA, preserve_index=False)

        if path.exists():
            existing = pq.read_table(path)
            combined = pa.concat_tables([existing, new_table], promote_options="default")
            pq.write_table(combined, path)
        else:
            pq.write_table(new_table, path)

        logger.info(
            "[%s] Saved %d drop/warning log entries to %s",
            self.icao, len(self._log_records), path,
        )
        self._log_records.clear()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def build(
        self,
        start_date: date,
        end_date: date,
        save_log: bool = True,
    ) -> tuple[pd.DataFrame, pd.Series]:
        """Build (X, y) training set for the given date range.

        Parameters
        ----------
        start_date, end_date : date
            Inclusive range (LST climate dates).
        save_log : bool
            Whether to persist the drop/warning log to parquet.

        Returns
        -------
        X : pd.DataFrame
            Feature matrix with columns matching FeatureEngine.FEATURE_COLUMNS.
        y : pd.Series
            Target values Y_t ≥ 0 (already clipped).
        """
        logger.info(
            "[%s] Building training set for %s → %s",
            self.icao, start_date.isoformat(), end_date.isoformat(),
        )

        # ── Load raw data ──
        data = self._loader.load_all(start_date, end_date)
        obs_df  = data["obs"]
        nbm_df  = data["nbm"]
        rrfs_df = data["rrfs"]

        if obs_df.empty:
            logger.warning("[%s] No observations found — empty training set.", self.icao)
            return pd.DataFrame(), pd.Series(dtype=float)

        # ── Build CLI label map (CLI first, DSM fallback) ──
        label_map = self._loader.cli_label_map(start_date, end_date)
        logger.info("[%s] Found CLI/DSM labels for %d climate days.", self.icao, len(label_map))

        # ── Run feature engineering ──
        feat_df = self._feature_engine.build(obs_df, nbm_df, rrfs_df)
        if feat_df.empty:
            return pd.DataFrame(), pd.Series(dtype=float)

        # ── Detect per-day NWP availability (for WARNING logging) ──
        nbm_days_missing  = self._find_days_missing_nwp(feat_df, nbm_df,  "nbm_cycle_age_minutes")
        rrfs_days_missing = self._find_days_missing_nwp(feat_df, rrfs_df, "rrfs_cycle_age_minutes")

        # Log per-day NWP warnings
        for climate_date in nbm_days_missing:
            self._log("WARNING", None, climate_date, "NBM_ALL_MISSING",
                      "No NBM cycle available for entire climate day — NaN features used.")
        for climate_date in rrfs_days_missing:
            self._log("WARNING", None, climate_date, "RRFS_ALL_MISSING",
                      "No RRFS cycle available for entire climate day — NaN features used.")

        # ── Apply validity rules and build (X, y) ──
        keep_rows: list[dict] = []
        y_values:  list[float] = []

        for _, row in feat_df.iterrows():
            obs_time     = row["observation_time_utc"]
            climate_date = str(row["climate_date_lst"])
            obs_dt       = obs_time.to_pydatetime() if hasattr(obs_time, "to_pydatetime") else obs_time

            # Check 2: CLI label must exist
            if climate_date not in label_map:
                self._log("DROP", obs_dt, climate_date, "NO_CLI_LABEL",
                          f"No CLI or DSM label found for {climate_date}")
                continue

            cli_high_f = label_map[climate_date]

            # Check 3: custom_intraday_max must be valid
            if pd.isna(row.get("custom_intraday_max_f")):
                self._log("DROP", obs_dt, climate_date, "NO_PRIOR_OBS",
                          "custom_intraday_max_f is NaN — no prior observations in climate day")
                continue

            # Check 4: core observation features must be non-NaN
            skip = False
            for col in _CORE_OBS_COLS:
                if pd.isna(row.get(col)):
                    self._log("DROP", obs_dt, climate_date, f"OBS_FEATURE_NAN:{col}",
                              f"Required feature {col!r} is NaN")
                    skip = True
                    break
            if skip:
                continue

            # Compute target Y_t
            y_raw = float(cli_high_f) - float(row["custom_intraday_max_f"])

            # Check 5: clip negative targets with WARNING
            if y_raw < Y_MIN:
                self._log("WARNING", obs_dt, climate_date, "NEGATIVE_TARGET",
                          f"Y_t={y_raw:.2f} clipped to 0 (CLI={cli_high_f}, "
                          f"intraday_max={row['custom_intraday_max_f']:.1f})")
                y_raw = Y_MIN

            # Row is valid — collect features only
            feat_dict = {
                col: row[col]
                for col in FeatureEngine.FEATURE_COLUMNS
                if col in row
            }
            feat_dict["observation_time_utc"] = obs_time
            feat_dict["climate_date_lst"]     = climate_date
            keep_rows.append(feat_dict)
            y_values.append(y_raw)

        if not keep_rows:
            logger.warning("[%s] All rows dropped — no valid training samples.", self.icao)
            if save_log:
                self._save_drop_log()
            return pd.DataFrame(), pd.Series(dtype=float)

        X = pd.DataFrame(keep_rows)
        y = pd.Series(y_values, name="y_remaining_delta_f", index=X.index)

        # ── Persist drop log ──
        if save_log:
            self._save_drop_log()

        logger.info(
            "[%s] Training set built: %d rows, %d features, y_mean=%.2f, y_max=%.2f",
            self.icao, len(X), len(FeatureEngine.FEATURE_COLUMNS),
            float(y.mean()), float(y.max()),
        )
        return X, y

    # ------------------------------------------------------------------
    # Summary report (§7.3)
    # ------------------------------------------------------------------

    def summary_report(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        start_date: date,
        end_date: date,
    ) -> dict:
        """Generate a data quality summary report (§7.3).

        Returns a dict suitable for logging or serialisation.
        """
        log_path = (
            self.data_dir
            / MODEL_SUBDIR
            / TRAINING_LOGS_SUBDIR
            / f"{self.icao}_drop_log.parquet"
        )
        drop_log = pd.DataFrame()
        if log_path.exists():
            drop_log = pq.read_table(log_path).to_pandas()

        n_days_total = (end_date - start_date).days + 1
        n_days_with_labels = X["climate_date_lst"].nunique() if not X.empty else 0
        n_rows_final = len(X)
        n_drops = len(drop_log[drop_log["log_level"] == "DROP"]) if not drop_log.empty else 0
        n_warnings = len(drop_log[drop_log["log_level"] == "WARNING"]) if not drop_log.empty else 0

        drops_by_code: dict[str, int] = {}
        if not drop_log.empty:
            d_df = drop_log[drop_log["log_level"] == "DROP"]
            drops_by_code = d_df["reason_code"].value_counts().to_dict()

        nbm_age_mean = float(X["nbm_cycle_age_minutes"].mean()) if (not X.empty and "nbm_cycle_age_minutes" in X.columns) else float("nan")
        rrfs_age_mean = float(X["rrfs_cycle_age_minutes"].mean()) if (not X.empty and "rrfs_cycle_age_minutes" in X.columns) else float("nan")

        report = {
            "station":               self.icao,
            "date_range":            f"{start_date} → {end_date}",
            "climate_days_attempted": n_days_total,
            "climate_days_with_labels": n_days_with_labels,
            "total_rows_final":      n_rows_final,
            "rows_dropped":          n_drops,
            "rows_warned":           n_warnings,
            "drops_by_reason_code":  drops_by_code,
            "nbm_cycle_age_mean_min": nbm_age_mean,
            "rrfs_cycle_age_mean_min": rrfs_age_mean,
            "y_mean_f":              float(y.mean()) if len(y) > 0 else float("nan"),
            "y_std_f":               float(y.std())  if len(y) > 0 else float("nan"),
        }
        return report

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_days_missing_nwp(
        feat_df: pd.DataFrame,
        nwp_df: pd.DataFrame,
        age_col: str,
    ) -> list[str]:
        """Return climate dates where ALL NWP features for a model are NaN.

        This happens when no cycle exists at all (not just a stale cycle).
        """
        if feat_df.empty or age_col not in feat_df.columns:
            return []
        return (
            feat_df.groupby("climate_date_lst")[age_col]
            .apply(lambda s: s.isna().all())
            .pipe(lambda s: s[s].index.tolist())
        )
