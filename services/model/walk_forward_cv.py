"""Walk-Forward Cross-Validation harness (§9).

Simulates real-time deployment: the model is always trained on past data
and evaluated on strictly future data.  The calibrator is always trained
on the TEST fold predictions (the PREVIOUS fold's output), not on training
data — ensuring zero leakage between the XGBoost suite and the
isotonic calibrator.

Walk-Forward Procedure:
  • Expanding training window (uses ALL past data, not rolling)
  • Test fold size: TEST_FOLD_DAYS (≈1 week)
  • Step size: STEP_DAYS (non-overlapping folds)
  • Minimum training size: MIN_TRAINING_DAYS climate days

For each fold:
  1. Train QuantileSuite on training rows
  2. Evaluate on test rows → collect raw quantile preds + targets
  3. Fit IsotonicCalibrator on the PREVIOUS fold's test set (leakage-free)
  4. Compute pinball loss, CRPS, calibration coverage, log-loss

Calibrator leakage protection (explicit):
  • Calibrator is fit on fold N-1's test predictions.
  • First fold has no prior test data → calibrator is untrained.
  • LeakageError is raised if any index overlap is detected.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from services.model.constants import (
    QUANTILE_ALPHAS,
    MIN_TRAINING_DAYS,
    TEST_FOLD_DAYS,
    STEP_DAYS,
)
from services.model.feature_engine import FeatureEngine
from services.model.quantile_suite import QuantileSuite
from services.model.monotonic_mapper import MonotonicMapper
from services.model.strike_pricer import StrikePricer
from services.model.calibrator import IsotonicCalibrator

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Metric helpers
# ──────────────────────────────────────────────────────────────────────

def _pinball_loss(y_true: np.ndarray, y_pred: np.ndarray, alpha: float) -> float:
    """Pinball (quantile) loss for a single quantile level."""
    err = y_true - y_pred
    return float(np.mean(np.where(err >= 0, alpha * err, (alpha - 1) * err)))


def _crps(y_true: np.ndarray, q_preds: dict[float, np.ndarray]) -> float:
    """Approximate CRPS using piecewise linear CDF from quantile predictions.

    CRPS = ∫(F(x) − 1{y ≤ x})² dx, approximated by summing across the
    quantile grid using the trapezoidal rule.
    """
    alphas = sorted(q_preds.keys())
    n = len(y_true)
    total = 0.0
    for i in range(len(alphas) - 1):
        a0, a1 = alphas[i], alphas[i + 1]
        q0, q1 = q_preds[a0], q_preds[a1]
        # Midpoint alpha and quantile for this segment
        alpha_mid = 0.5 * (a0 + a1)
        q_mid = 0.5 * (q0 + q1)
        # Pinball at midpoint
        loss = _pinball_loss(y_true, q_mid, alpha_mid)
        total += loss * (a1 - a0)
    return total


def _coverage(y_true: np.ndarray, y_pred: np.ndarray, alpha: float) -> float:
    """Calibration coverage: fraction of y_true ≤ y_pred (should ≈ alpha)."""
    return float(np.mean(y_true <= y_pred))


# ──────────────────────────────────────────────────────────────────────
# FoldResult dataclass
# ──────────────────────────────────────────────────────────────────────

@dataclass
class FoldResult:
    """Results for a single walk-forward fold."""
    fold_idx:          int
    train_start:       date
    train_end:         date
    test_start:        date
    test_end:          date
    n_train_rows:      int
    n_test_rows:       int

    # Per-quantile pinball losses {alpha → loss}
    pinball_losses:    dict[float, float] = field(default_factory=dict)
    # Calibration coverage {alpha → fraction}
    coverage:          dict[float, float] = field(default_factory=dict)
    # Aggregate metrics
    crps:              float = float("nan")
    mae:               float = float("nan")
    # Calibrator log-loss (if calibrator was fitted)
    calib_log_loss:    Optional[float] = None

    def summary_line(self) -> str:
        pinball_05 = self.pinball_losses.get(0.05, float("nan"))
        pinball_50 = self.pinball_losses.get(0.50, float("nan"))
        pinball_95 = self.pinball_losses.get(0.95, float("nan"))
        return (
            f"Fold {self.fold_idx:2d} | "
            f"Train {self.train_start}–{self.train_end} ({self.n_train_rows:4d} rows) | "
            f"Test {self.test_start}–{self.test_end} ({self.n_test_rows:4d} rows) | "
            f"CRPS={self.crps:.3f} MAE={self.mae:.2f}°F "
            f"PB[05%={pinball_05:.3f} 50%={pinball_50:.3f} 95%={pinball_95:.3f}]"
        )


# ──────────────────────────────────────────────────────────────────────
# WalkForwardCV
# ──────────────────────────────────────────────────────────────────────

class WalkForwardCV:
    """Expanding-window walk-forward cross-validation for the Weather Brain.

    Parameters
    ----------
    icao : Station ICAO code.
    alphas : Quantile levels.
    xgb_params : XGBoost hyperparameter overrides.
    min_training_days : Minimum climate days before generating the first test fold.
    test_fold_days : Size of each test fold.
    step_days : Number of days to advance per fold.
    model_dir : Optional directory to save models from each fold.
    """

    def __init__(
        self,
        icao: str,
        alphas: tuple[float, ...] = QUANTILE_ALPHAS,
        xgb_params: Optional[dict] = None,
        min_training_days: int = MIN_TRAINING_DAYS,
        test_fold_days: int = TEST_FOLD_DAYS,
        step_days: int = STEP_DAYS,
        model_dir: Optional[Path] = None,
    ):
        self.icao = icao
        self.alphas = alphas
        self.xgb_params = xgb_params
        self.min_training_days = min_training_days
        self.test_fold_days    = test_fold_days
        self.step_days         = step_days
        self.model_dir         = model_dir

        self._mapper  = MonotonicMapper(alphas)
        self._pricer  = StrikePricer(alphas)

    def run(
        self,
        X_full: pd.DataFrame,
        y_full: pd.Series,
    ) -> list[FoldResult]:
        """Run the full walk-forward CV procedure.

        Parameters
        ----------
        X_full : Feature DataFrame with ``climate_date_lst`` and
                 ``observation_time_utc`` metadata columns (from TrainingSetBuilder).
        y_full : Target series (same index as X_full).

        Returns
        -------
        List of FoldResult, one per fold.
        """
        if X_full.empty:
            logger.warning("[%s] Empty input — no folds to run.", self.icao)
            return []

        # Build fold boundaries from unique climate dates
        all_dates = sorted(X_full["climate_date_lst"].unique())
        if len(all_dates) < self.min_training_days + self.test_fold_days:
            logger.warning(
                "[%s] Not enough climate days (%d) for WF-CV "
                "(need min_train=%d + test=%d).",
                self.icao, len(all_dates),
                self.min_training_days, self.test_fold_days,
            )
            return []

        folds = self._build_folds(all_dates)
        logger.info("[%s] Walk-Forward CV: %d folds.", self.icao, len(folds))

        results: list[FoldResult] = []
        prev_test_preds: Optional[tuple[np.ndarray, np.ndarray]] = None  # (p_raw, y_binary)
        # For calibrator leakage guard
        prev_test_index: Optional[pd.Index] = None

        for fold_idx, (train_dates, test_dates) in enumerate(folds):
            train_mask = X_full["climate_date_lst"].isin(train_dates)
            test_mask  = X_full["climate_date_lst"].isin(test_dates)

            X_train = X_full[train_mask]
            y_train = y_full[train_mask]
            X_test  = X_full[test_mask]
            y_test  = y_full[test_mask]

            if len(X_train) < 10 or len(X_test) < 1:
                logger.warning("Fold %d: insufficient data — skipping.", fold_idx)
                continue

            logger.info(
                "[%s] Fold %d: train=%d rows (%s–%s), test=%d rows (%s–%s)",
                self.icao, fold_idx,
                len(X_train), train_dates[0], train_dates[-1],
                len(X_test),  test_dates[0],  test_dates[-1],
            )

            # ── Stage 2: Train QuantileSuite ──────────────────────────
            suite = QuantileSuite(alphas=self.alphas, xgb_params=self.xgb_params)
            # Use last 20% of training rows as validation for early stopping
            val_split = max(1, int(len(X_train) * 0.8))
            X_tr, y_tr = X_train.iloc[:val_split], y_train.iloc[:val_split]
            X_v,  y_v  = X_train.iloc[val_split:], y_train.iloc[val_split:]
            suite.fit(X_tr, y_tr, X_v, y_v)

            # ── Predict on test set ───────────────────────────────────
            raw_preds = suite.predict(X_test)  # {alpha → np.ndarray}

            # Apply monotonic mapping row-by-row
            mono_preds = self._mapper.transform_batch(raw_preds)

            # ── Compute statistical metrics ───────────────────────────
            y_arr = y_test.values.astype(np.float64)
            pinball: dict[float, float] = {}
            coverage: dict[float, float] = {}
            for alpha in self.alphas:
                q_arr = mono_preds[alpha].astype(np.float64)
                pinball[alpha]  = _pinball_loss(y_arr, q_arr, alpha)
                coverage[alpha] = _coverage(y_arr, q_arr, alpha)

            crps_val = _crps(y_arr, {a: mono_preds[a].astype(np.float64) for a in self.alphas})
            mae_val  = float(np.mean(np.abs(y_arr - mono_preds[0.50].astype(np.float64))))

            # ── Stage 5: Fit calibrator on PREVIOUS fold's test data ──
            calib_log_loss: Optional[float] = None
            if prev_test_preds is not None and prev_test_index is not None:
                p_raw_prev, y_bin_prev = prev_test_preds
                try:
                    cal = IsotonicCalibrator()
                    cal.fit(
                        p_raw_cal=p_raw_prev,
                        y_outcome_cal=y_bin_prev,
                        train_index=X_train.index,
                        cal_index=prev_test_index,
                    )
                    calib_log_loss = cal._calibration_log_loss
                except Exception as e:
                    logger.warning("Fold %d calibration failed: %s", fold_idx, e)

            # Save the test predictions for calibrating the NEXT fold
            # (use median quantile as a proxy raw probability score)
            p_raw_this = mono_preds[0.50].astype(np.float64)  # median as raw score
            # Note: in full production we would generate per-strike binary outcomes.
            # For CV evaluation we use a synthetic binary: y > 0 (did temp still rise?)
            y_binary_this = (y_arr > 0).astype(np.float64)
            prev_test_preds = (p_raw_this, y_binary_this)
            prev_test_index = X_test.index

            # ── Optionally save fold model ────────────────────────────
            if self.model_dir is not None:
                fold_dir = self.model_dir / self.icao
                suite.save(fold_dir, version=f"fold_{fold_idx:02d}")

            result = FoldResult(
                fold_idx=fold_idx,
                train_start=date.fromisoformat(train_dates[0]),
                train_end=date.fromisoformat(train_dates[-1]),
                test_start=date.fromisoformat(test_dates[0]),
                test_end=date.fromisoformat(test_dates[-1]),
                n_train_rows=len(X_train),
                n_test_rows=len(X_test),
                pinball_losses=pinball,
                coverage=coverage,
                crps=crps_val,
                mae=mae_val,
                calib_log_loss=calib_log_loss,
            )
            logger.info(result.summary_line())
            results.append(result)

        return results

    # ------------------------------------------------------------------
    # Final model training (on all data, for production deployment)
    # ------------------------------------------------------------------

    def fit_final(
        self,
        X_full: pd.DataFrame,
        y_full: pd.Series,
        model_dir: Path,
        calibration_holdout_fraction: float = 0.15,
    ) -> tuple[QuantileSuite, IsotonicCalibrator]:
        """Train final production models on all available data.

        Uses the last ``calibration_holdout_fraction`` of data (sorted by
        date) as a held-out calibration set for the IsotonicCalibrator.
        The QuantileSuite is trained on the earlier portion.

        This split is temporal (no shuffling) to avoid future-seeing.

        Parameters
        ----------
        calibration_holdout_fraction : fraction of rows reserved for calibration
                                        (chronologically last). Default: 15%.

        Returns
        -------
        (QuantileSuite, IsotonicCalibrator) — both saved to model_dir.
        """
        if X_full.empty:
            raise ValueError("Cannot fit final model on empty training set.")

        # Temporal split — no shuffling
        all_dates = sorted(X_full["climate_date_lst"].unique())
        n_cal_days = max(1, int(len(all_dates) * calibration_holdout_fraction))
        train_dates = set(all_dates[:-n_cal_days])
        cal_dates   = set(all_dates[-n_cal_days:])

        train_mask = X_full["climate_date_lst"].isin(train_dates)
        cal_mask   = X_full["climate_date_lst"].isin(cal_dates)

        X_train = X_full[train_mask]
        y_train = y_full[train_mask]
        X_cal   = X_full[cal_mask]
        y_cal   = y_full[cal_mask]

        logger.info(
            "[%s] fit_final: %d train rows, %d calibration rows (%.0f%% holdout)",
            self.icao, len(X_train), len(X_cal), 100 * calibration_holdout_fraction,
        )

        # Train QuantileSuite
        suite = QuantileSuite(alphas=self.alphas, xgb_params=self.xgb_params)
        # Keep last 10% of train for early stopping validation
        es_split = max(1, int(len(X_train) * 0.9))
        suite.fit(
            X_train.iloc[:es_split], y_train.iloc[:es_split],
            X_train.iloc[es_split:], y_train.iloc[es_split:],
        )

        # Generate calibration predictions
        raw_preds_cal = suite.predict(X_cal)
        mono_preds_cal = self._mapper.transform_batch(raw_preds_cal)

        # Use median raw prediction as the "probability-like" score.
        # In production we would use per-strike probs, but for the final
        # model calibration we approximate with the median surplus-delta CDF.
        p_raw_cal = mono_preds_cal[0.50].astype(np.float64)
        y_binary_cal = (y_cal.values > 0).astype(np.float64)

        # Fit calibrator (leakage guard will pass: train vs cal indices differ)
        calibrator = IsotonicCalibrator()
        calibrator.fit(
            p_raw_cal=p_raw_cal,
            y_outcome_cal=y_binary_cal,
            train_index=X_train.index,
            cal_index=X_cal.index,
        )

        # Save both
        out_dir = model_dir / self.icao
        suite.save(out_dir, version="final")
        cal_path = out_dir / "final" / "calibrator.pkl"
        calibrator.save(cal_path)

        logger.info("[%s] Final models saved to %s", self.icao, out_dir)
        return suite, calibrator

    # ------------------------------------------------------------------
    # Internal — fold boundary construction
    # ------------------------------------------------------------------

    def _build_folds(
        self, all_dates: list[str]
    ) -> list[tuple[list[str], list[str]]]:
        """Build (train_dates, test_dates) pairs for all folds.

        Uses an expanding training window with non-overlapping test folds.
        """
        folds: list[tuple[list[str], list[str]]] = []
        n = len(all_dates)
        test_start_idx = self.min_training_days

        while test_start_idx + self.test_fold_days <= n:
            train_dates = all_dates[:test_start_idx]
            test_dates  = all_dates[test_start_idx: test_start_idx + self.test_fold_days]
            folds.append((train_dates, test_dates))
            test_start_idx += self.step_days

        return folds
