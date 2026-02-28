"""Stage 2 — Quantile Suite (7 × XGBoost).

Trains one XGBoost model per target quantile (reg:quantileerror).
The suite produces a family of models whose outputs approximate a full
CDF of the remaining heating delta Y_t.

Target quantiles (from §8.2):
    α ∈ {0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95}

DRY principle: all hyperparameter defaults live in constants.XGB_DEFAULTS.
The only per-model parameter is ``quantile_alpha``.

Save/load:
  Serialized as XGBoost native JSON with a YAML sidecar for metadata.
  Path convention: data/model/trained/{icao}/{version}/q{alpha:03d}.json
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from services.model.constants import QUANTILE_ALPHAS, XGB_DEFAULTS
from services.model.feature_engine import FeatureEngine

logger = logging.getLogger(__name__)


class QuantileSuite:
    """Trains and wraps 7 XGBoost quantile regression models.

    Parameters
    ----------
    alphas : tuple[float, ...]
        Quantile levels to model.  Defaults to ``QUANTILE_ALPHAS``.
    xgb_params : dict | None
        Override XGBoost hyperparameters.  Merged on top of ``XGB_DEFAULTS``.
    """

    def __init__(
        self,
        alphas: tuple[float, ...] = QUANTILE_ALPHAS,
        xgb_params: Optional[dict] = None,
    ):
        self.alphas = alphas
        self._params = {**XGB_DEFAULTS, **(xgb_params or {})}
        self._models: dict[float, object] = {}  # alpha → XGBRegressor

    @property
    def is_fitted(self) -> bool:
        return bool(self._models)

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def fit(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        X_val: Optional[pd.DataFrame] = None,
        y_val: Optional[pd.Series] = None,
    ) -> "QuantileSuite":
        """Train one XGBoost model per quantile.

        Parameters
        ----------
        X_train, y_train : training features and targets.
        X_val, y_val     : optional validation set for early stopping.

        Returns self for chaining.
        """
        try:
            from xgboost import XGBRegressor
        except ImportError:
            raise ImportError(
                "xgboost is required. Install with: pip install xgboost"
            )

        feature_cols = self._feature_cols(X_train)
        X_tr = X_train[feature_cols].values.astype(np.float32)
        y_tr = y_train.values.astype(np.float32)

        eval_set = None
        if X_val is not None and y_val is not None and len(X_val) > 0:
            X_v  = X_val[feature_cols].values.astype(np.float32)
            y_v  = y_val.values.astype(np.float32)
            eval_set = [(X_v, y_v)]

        self._models = {}
        for alpha in self.alphas:
            params = {k: v for k, v in self._params.items() if k != "early_stopping_rounds"}
            params["quantile_alpha"] = alpha

            model = XGBRegressor(**params)

            fit_kwargs: dict = {"eval_set": eval_set} if eval_set else {}
            if eval_set:
                fit_kwargs["early_stopping_rounds"] = self._params.get("early_stopping_rounds", 50)
                fit_kwargs["verbose"] = False

            model.fit(X_tr, y_tr, **fit_kwargs)
            self._models[alpha] = model

            best_iter = getattr(model, "best_iteration", self._params.get("n_estimators"))
            logger.info("  q=%.2f trained (%d trees)", alpha, best_iter or 0)

        logger.info("QuantileSuite fitted — %d models, %d training rows.", len(self.alphas), len(X_train))
        return self

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def predict(self, X: pd.DataFrame) -> dict[float, np.ndarray]:
        """Predict raw quantile values for each row in X.

        Returns dict mapping alpha → 1-D array of predictions (one per row).
        """
        if not self.is_fitted:
            raise RuntimeError("QuantileSuite.predict() called before fit().")

        feature_cols = self._feature_cols(X)
        X_arr = X[feature_cols].values.astype(np.float32)

        return {
            alpha: model.predict(X_arr)
            for alpha, model in self._models.items()
        }

    def predict_row(self, x: pd.Series) -> dict[float, float]:
        """Predict raw quantile values for a single feature row.

        Convenience wrapper for inference (one observation at a time).
        Returns dict alpha → scalar prediction.
        """
        X_df = pd.DataFrame([x])
        preds = self.predict(X_df)
        return {alpha: float(arr[0]) for alpha, arr in preds.items()}

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def save(self, model_dir: Path, version: str = "latest") -> Path:
        """Save all models + metadata to ``model_dir/{version}/``.

        Format: each model as XGBoost native JSON, plus a metadata YAML.
        """
        if not self.is_fitted:
            raise RuntimeError("Cannot save an unfitted QuantileSuite.")

        out_dir = model_dir / version
        out_dir.mkdir(parents=True, exist_ok=True)

        for alpha, model in self._models.items():
            stem = f"q{int(alpha * 100):03d}"
            model_path = out_dir / f"{stem}.json"
            model.save_model(str(model_path))

        # Save metadata sidecar
        meta = {
            "alphas": list(self.alphas),
            "xgb_params": self._params,
            "feature_columns": list(FeatureEngine.FEATURE_COLUMNS),
            "n_models": len(self._models),
        }
        import yaml
        meta_path = out_dir / "suite_metadata.yaml"
        with open(meta_path, "w") as f:
            yaml.dump(meta, f, default_flow_style=False)

        logger.info("QuantileSuite saved to %s", out_dir)
        return out_dir

    @classmethod
    def load(cls, model_dir: Path, version: str = "latest") -> "QuantileSuite":
        """Load a serialised QuantileSuite from disk."""
        try:
            from xgboost import XGBRegressor
        except ImportError:
            raise ImportError("xgboost is required. Install with: pip install xgboost")

        import yaml
        in_dir = model_dir / version
        meta_path = in_dir / "suite_metadata.yaml"
        with open(meta_path) as f:
            meta = yaml.safe_load(f)

        alphas = tuple(meta["alphas"])
        suite = cls(alphas=alphas, xgb_params=meta.get("xgb_params"))

        for alpha in alphas:
            stem = f"q{int(alpha * 100):03d}"
            model_path = in_dir / f"{stem}.json"
            model = XGBRegressor()
            model.load_model(str(model_path))
            suite._models[alpha] = model

        logger.info("QuantileSuite loaded from %s (%d models)", in_dir, len(suite._models))
        return suite

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _feature_cols(X: pd.DataFrame) -> list[str]:
        """Return ordered feature columns intersected with FeatureEngine.FEATURE_COLUMNS."""
        return [c for c in FeatureEngine.FEATURE_COLUMNS if c in X.columns]
