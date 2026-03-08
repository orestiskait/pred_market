"""Shared constants for the Weather Brain model pipeline.

All tunable constants live here so every module imports from one place
(DRY principle).  Changing a latency constant here propagates to feature
engineering, training set builder, and the inference pipeline automatically.
"""

from __future__ import annotations

# ──────────────────────────────────────────────────────────────────────
# NWP Latency (P95 conservative bounds, in seconds)
# ──────────────────────────────────────────────────────────────────────
# A cycle c is considered "available" at observation time t only when:
#   c + LATENCY_SECONDS ≤ t
# This prevents future-seeing in both backtesting and production.

NBM_LATENCY_SECONDS: int = 4860   # 1h 21m 00s (P95 conservative)
RRFS_LATENCY_SECONDS: int = 7291  # 2h 1m 30.76s (P95 for backtest)

# ──────────────────────────────────────────────────────────────────────
# Cycle-age cap  (MODEL DESIGN DECISION)
# ──────────────────────────────────────────────────────────────────────
# Purpose:
#   cycle_age_minutes is passed as a feature and acts as an implicit
#   "trust decay" weight.  Without a cap, a completely missing RRFS day
#   could produce age values of 1000+ minutes, which are far outside the
#   training distribution and generate wild tree-split extrapolations.
#
#   Instead: if no valid cycle exists within CYCLE_AGE_CAP_MINUTES of t,
#   we treat the model as fully missing (NaN features).  This keeps the
#   cycle_age feature in a bounded, learnable range.
#
# 360 minutes = 6 hours — chosen because:
#   • NBM is hourly, so "stale" at 360 min means 5+ missed cycles — extreme.
#   • RRFS is 4× daily (6-hour gaps), so 360 min = one full RRFS cycle gap.
#     If even that is missing we have no useful RRFS signal.

CYCLE_AGE_CAP_MINUTES: int = 360

# ──────────────────────────────────────────────────────────────────────
# Quantile Regression — target quantile levels
# ──────────────────────────────────────────────────────────────────────
# 7 models: one per quantile.  Symmetric around the median with extra
# tail coverage for accurate CDF interpolation at the tails.

QUANTILE_ALPHAS: tuple[float, ...] = (0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95)

# ──────────────────────────────────────────────────────────────────────
# XGBoost hyperparameter defaults (single source)
# ──────────────────────────────────────────────────────────────────────

XGB_DEFAULTS: dict = {
    "objective": "reg:quantileerror",
    "n_estimators": 500,
    "max_depth": 6,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "min_child_weight": 10,
    "tree_method": "hist",
    "early_stopping_rounds": 50,
    "verbosity": 0,
}

# ──────────────────────────────────────────────────────────────────────
# Walk-Forward Cross-Validation parameters
# ──────────────────────────────────────────────────────────────────────

MIN_TRAINING_DAYS: int = 30   # minimum historical days before first test fold
TEST_FOLD_DAYS: int = 7       # size of each test fold (≈1 week)
STEP_DAYS: int = 7            # how many days to advance per fold

# ──────────────────────────────────────────────────────────────────────
# Target variable clipping
# ──────────────────────────────────────────────────────────────────────

# Y_t = CLI_High_F − custom_intraday_max_t
# Physically cannot be negative (you can't "un-warm" the peak).
# Negative values are clipped to 0 and flagged as NEGATIVE_TARGET warnings.
Y_MIN: float = 0.0

# ──────────────────────────────────────────────────────────────────────
# Strike pricing — probability clamps (avoid 0 / 1 boundary)
# ──────────────────────────────────────────────────────────────────────

STRIKE_P_FLOOR: float = 0.05   # below q05 → P_raw = 0.95 (already exceeded)
STRIKE_P_CEIL: float = 0.95    # above q95 → P_raw = 0.05 (very unlikely)

# ──────────────────────────────────────────────────────────────────────
# Isotonic Calibrator bounds (anti-extrapolation)
# ──────────────────────────────────────────────────────────────────────

CALIB_Y_MIN: float = 0.01
CALIB_Y_MAX: float = 0.99

# ──────────────────────────────────────────────────────────────────────
# File / path conventions
# ──────────────────────────────────────────────────────────────────────

# Relative to the project data/ root:
#   data/model/training_logs/{ICAO}_drop_log.parquet
#   data/model/trained/{ICAO}/{version}/
MODEL_SUBDIR = "model"
TRAINING_LOGS_SUBDIR = "training_logs"
TRAINED_MODELS_SUBDIR = "trained"
