"""Weather Brain — XGBoost Quantile Regression Pipeline.

6-stage inference pipeline (Stages 1–5 = Weather Brain, Stage 6 = Execution Brain):

  Stage 1  FeatureEngine          — raw data → ~36-dimensional feature vector
  Stage 2  QuantileSuite          — feature vector → 7 raw quantile predictions
  Stage 3  MonotonicMapper        — enforce q₀₅ ≤ q₁₀ ≤ … ≤ q₉₅
  Stage 4  StrikePricer           — CDF interpolation → P(CLI_High ≥ strike)
  Stage 5  IsotonicCalibrator     — raw probabilities → calibrated probabilities
  Stage 6  Execution Brain        — (separate module, not part of this package)

Training:
  TrainingSetBuilder     — assembles featurised rows with validity checks + logging
  WalkForwardCV          — expanding-window walk-forward cross-validation harness
"""

from services.model.constants import (
    NBM_LATENCY_SECONDS,
    RRFS_LATENCY_SECONDS,
    QUANTILE_ALPHAS,
    CYCLE_AGE_CAP_MINUTES,
    MIN_TRAINING_DAYS,
    TEST_FOLD_DAYS,
    STEP_DAYS,
)
from services.model.feature_engine import FeatureEngine
from services.model.training_set_builder import TrainingSetBuilder
from services.model.quantile_suite import QuantileSuite
from services.model.monotonic_mapper import MonotonicMapper
from services.model.strike_pricer import StrikePricer
from services.model.calibrator import IsotonicCalibrator
from services.model.walk_forward_cv import WalkForwardCV

__all__ = [
    "NBM_LATENCY_SECONDS",
    "RRFS_LATENCY_SECONDS",
    "QUANTILE_ALPHAS",
    "CYCLE_AGE_CAP_MINUTES",
    "MIN_TRAINING_DAYS",
    "TEST_FOLD_DAYS",
    "STEP_DAYS",
    "FeatureEngine",
    "TrainingSetBuilder",
    "QuantileSuite",
    "MonotonicMapper",
    "StrikePricer",
    "IsotonicCalibrator",
    "WalkForwardCV",
]
