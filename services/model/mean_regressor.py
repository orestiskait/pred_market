"""XGBoost mean regression model for remaining heating delta.

Trains a single XGBoost model with reg:squarederror to predict E[Y_t],
the expected remaining heating delta. Complements the QuantileSuite
(which predicts quantiles) for point-forecast evaluation.
"""

from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

from services.model.constants import XGB_DEFAULTS
from services.model.feature_engine import FeatureEngine

logger = logging.getLogger(__name__)


class MeanRegressor:
    """XGBoost mean regression for E[remaining heating delta].

    Uses the same features as QuantileSuite but predicts the conditional
    mean instead of quantiles. Useful for temperature accuracy metrics.
    """

    def __init__(self, xgb_params: Optional[dict] = None):
        self._params = {**XGB_DEFAULTS, **(xgb_params or {})}
        # Override for mean regression (no quantile)
        self._params["objective"] = "reg:squarederror"
        self._params.pop("quantile_alpha", None)
        self._model: Optional[object] = None

    @property
    def is_fitted(self) -> bool:
        return self._model is not None

    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame] = None,
        y_val: Optional[pd.Series] = None,
    ) -> "MeanRegressor":
        """Train the mean regression model."""
        try:
            from xgboost import XGBRegressor
        except ImportError:
            raise ImportError("xgboost is required. Install with: pip install xgboost")

        feature_cols = self._feature_cols(X_train)
        X_tr = X_train[feature_cols].values.astype(np.float32)
        y_tr = y_train.values.astype(np.float32)

        eval_set = None
        if X_val is not None and y_val is not None and len(X_val) > 0:
            X_v = X_val[feature_cols].values.astype(np.float32)
            y_v = y_val.values.astype(np.float32)
            eval_set = [(X_v, y_v)]

        params = {k: v for k, v in self._params.items() if k != "early_stopping_rounds"}
        if eval_set:
            params["early_stopping_rounds"] = self._params.get("early_stopping_rounds", 50)

        self._model = XGBRegressor(**params)
        fit_kwargs: dict = {"eval_set": eval_set} if eval_set else {}
        if eval_set:
            fit_kwargs["verbose"] = False
        self._model.fit(X_tr, y_tr, **fit_kwargs)

        logger.info(
            "MeanRegressor fitted — %d training rows.",
            len(X_train),
        )
        return self

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Predict mean delta for each row."""
        if not self.is_fitted:
            raise RuntimeError("MeanRegressor.predict() called before fit().")
        feature_cols = self._feature_cols(X)
        X_arr = X[feature_cols].values.astype(np.float32)
        return self._model.predict(X_arr)

    def predict_row(self, x: pd.Series) -> float:
        """Predict mean delta for a single feature row."""
        X_df = pd.DataFrame([x])
        return float(self.predict(X_df)[0])

    @staticmethod
    def _feature_cols(X: pd.DataFrame) -> list[str]:
        """Return ordered feature columns intersected with FeatureEngine.FEATURE_COLUMNS."""
        return [c for c in FeatureEngine.FEATURE_COLUMNS if c in X.columns]
