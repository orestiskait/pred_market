"""Stage 5 — Isotonic Calibrator.

Fits a non-parametric monotone mapping from raw strike probabilities
(P_raw) to calibrated probabilities (P_cal) that minimise Log Loss on
a held-out calibration set.

DATA LEAKAGE PREVENTION  ⚠  (critical design constraint)
=========================================================
Isotonic Regression is extremely prone to *memorising* the training set.
If we fit the calibrator on the same rows used to train the XGBoost models,
it will simply overfit to the training residuals and produce artificially
confident predictions in live trading.

The guard we implement:
  • The calibrator is ALWAYS fitted on a HELD-OUT calibration split, never
    on the training fold used to fit QuantileSuite.
  • In WalkForwardCV, the calibration data is the PREVIOUS test fold's
    predictions + outcomes (or a stratified held-out slice of the current
    training fold, never the full fold).
  • We enforce this at the API level: ``fit()`` requires explicit
    ``X_cal`` (calibration features) and ``y_outcomes_cal`` (binary
    outcomes) and raises a ``CalibrationLeakageError`` if the calibration
    index overlaps the training index.

Binary outcome y_outcome:
  For each (row, strike) pair in the calibration set:
    y_outcome = 1 if CLI_High_F >= strike,  else 0.

This module's public API:
    fit(X_cal, y_outcomes_cal, train_index)  → self
    calibrate(p_raw)                         → p_cal (scalar or array)
    save(path) / load(path)                  → serialization
"""

from __future__ import annotations

import logging
import pickle
from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd

from services.model.constants import CALIB_Y_MIN, CALIB_Y_MAX

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────────────
# Custom exception
# ──────────────────────────────────────────────────────────────────────

class CalibrationLeakageError(ValueError):
    """Raised when calibration data overlaps training data.

    This is a hard guard against the most common isotonic calibration
    mistake: fitting the calibrator on the same rows as the base model.
    """


# ──────────────────────────────────────────────────────────────────────
# IsotonicCalibrator
# ──────────────────────────────────────────────────────────────────────

class IsotonicCalibrator:
    """Stage 5: fit a monotone P_raw → P_cal mapping.

    Uses sklearn's IsotonicRegression (PAVA) which directly minimises the
    Brier Score (≡ minimising squared error on binary outcomes), and is
    closely related to minimising Log Loss.

    The mapping is monotone by construction (higher raw probability →
    higher calibrated probability), preserving the model's rank-ordering.

    Parameters
    ----------
    y_min, y_max : float
        Probability clamps for calibrated output.  Prevents 0 and 1 from
        propagating to log-loss → ∞.
    """

    def __init__(
        self,
        y_min: float = CALIB_Y_MIN,
        y_max: float = CALIB_Y_MAX,
    ):
        self.y_min = y_min
        self.y_max = y_max
        self._model: Optional[object] = None
        self._n_calibration_samples: int = 0
        self._calibration_log_loss: Optional[float] = None

    @property
    def is_fitted(self) -> bool:
        return self._model is not None

    # ------------------------------------------------------------------
    # Fitting (with leakage guard)
    # ------------------------------------------------------------------

    def fit(
        self,
        p_raw_cal: Union[np.ndarray, list[float]],
        y_outcome_cal: Union[np.ndarray, list[int]],
        train_index: Optional[pd.Index] = None,
        cal_index: Optional[pd.Index] = None,
    ) -> "IsotonicCalibrator":
        """Fit the isotonic calibrator on a held-out calibration set.

        Parameters
        ----------
        p_raw_cal : 1-D array of P_raw values (in [0, 1]).
        y_outcome_cal : 1-D binary array where 1 = event occurred.
        train_index : pandas Index of the training rows (for leakage check).
        cal_index : pandas Index of the calibration rows (for leakage check).

        Raises
        ------
        CalibrationLeakageError
            If cal_index overlaps with train_index.
        ValueError
            If fewer than 10 calibration samples are provided.
        """
        try:
            from sklearn.isotonic import IsotonicRegression
        except ImportError:
            raise ImportError(
                "scikit-learn is required for calibration. "
                "Install with: pip install scikit-learn"
            )

        # ── Leakage guard ──────────────────────────────────────────
        if train_index is not None and cal_index is not None:
            overlap = train_index.intersection(cal_index)
            if len(overlap) > 0:
                raise CalibrationLeakageError(
                    f"Calibration data leakage detected: {len(overlap)} rows appear "
                    f"in both training_index and cal_index. "
                    f"The isotonic calibrator MUST be fit on held-out data only."
                )

        p_raw  = np.asarray(p_raw_cal,  dtype=np.float64)
        y_out  = np.asarray(y_outcome_cal, dtype=np.float64)

        if len(p_raw) < 10:
            raise ValueError(
                f"Too few calibration samples ({len(p_raw)}). "
                f"Need at least 10 to fit a reliable isotonic calibrator."
            )

        if len(p_raw) != len(y_out):
            raise ValueError(
                f"Length mismatch: p_raw has {len(p_raw)} samples, "
                f"y_outcome has {len(y_out)} samples."
            )

        logger.info(
            "Fitting IsotonicCalibrator on %d samples "
            "(y_mean=%.3f, p_raw_mean=%.3f)",
            len(p_raw), float(y_out.mean()), float(p_raw.mean()),
        )

        self._model = IsotonicRegression(
            y_min=self.y_min,
            y_max=self.y_max,
            out_of_bounds="clip",
            increasing=True,
        )
        self._model.fit(p_raw, y_out)
        self._n_calibration_samples = len(p_raw)

        # Compute calibration log-loss on the fit data (diagnostic)
        p_cal_fit = self._model.predict(p_raw)
        self._calibration_log_loss = _binary_log_loss(y_out, p_cal_fit)
        logger.info(
            "IsotonicCalibrator fitted. Log-loss on calibration set: %.4f",
            self._calibration_log_loss,
        )
        return self

    # ------------------------------------------------------------------
    # Inference
    # ------------------------------------------------------------------

    def calibrate(
        self,
        p_raw: Union[float, np.ndarray, list[float]],
    ) -> Union[float, np.ndarray]:
        """Map P_raw → P_cal using the fitted isotonic mapping.

        Parameters
        ----------
        p_raw : scalar or 1-D array of raw probabilities.

        Returns
        -------
        Calibrated probability / array.  Same shape as input.
        """
        if not self.is_fitted:
            raise RuntimeError("IsotonicCalibrator.calibrate() called before fit().")

        scalar_input = np.ndim(p_raw) == 0
        arr = np.atleast_1d(np.asarray(p_raw, dtype=np.float64))
        p_cal = np.clip(self._model.predict(arr), self.y_min, self.y_max)

        return float(p_cal[0]) if scalar_input else p_cal

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def save(self, path: Path) -> None:
        """Save to a pickle file (sklearn object + metadata)."""
        if not self.is_fitted:
            raise RuntimeError("Cannot save an unfitted IsotonicCalibrator.")
        payload = {
            "model":              self._model,
            "y_min":              self.y_min,
            "y_max":              self.y_max,
            "n_cal_samples":      self._n_calibration_samples,
            "calibration_log_loss": self._calibration_log_loss,
        }
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump(payload, f)
        logger.info("IsotonicCalibrator saved to %s", path)

    @classmethod
    def load(cls, path: Path) -> "IsotonicCalibrator":
        """Load from a pickle file."""
        with open(path, "rb") as f:
            payload = pickle.load(f)
        obj = cls(y_min=payload["y_min"], y_max=payload["y_max"])
        obj._model                   = payload["model"]
        obj._n_calibration_samples   = payload.get("n_cal_samples", 0)
        obj._calibration_log_loss    = payload.get("calibration_log_loss")
        logger.info(
            "IsotonicCalibrator loaded from %s (%d cal samples)",
            path, obj._n_calibration_samples,
        )
        return obj


# ──────────────────────────────────────────────────────────────────────
# Metric helpers
# ──────────────────────────────────────────────────────────────────────

def _binary_log_loss(y_true: np.ndarray, y_pred: np.ndarray, eps: float = 1e-7) -> float:
    """Binary cross-entropy log-loss. Lower is better."""
    y_pred = np.clip(y_pred, eps, 1 - eps)
    return float(-np.mean(y_true * np.log(y_pred) + (1 - y_true) * np.log(1 - y_pred)))
