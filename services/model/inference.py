"""Inference pipeline — given a live feature vector, run all 5 Weather Brain stages.

This module wraps Stages 2–5 into a single ``WeatherBrainInference`` object
that can be used identically in backtesting and production.

Usage (production / live):
    pipeline = WeatherBrainInference.load(model_dir, icao="KMDW")
    result = pipeline.predict(feature_row, custom_intraday_max_f=73.0, strikes=[70, 72, 74, 76])

Usage (backtesting):
    pipeline = WeatherBrainInference.load(model_dir, icao="KMDW", version="fold_03")
    ...

Output is a ``PredictionResult`` dataclass with:
  - Per-strike calibrated probabilities
  - Raw quantile predictions (for diagnostics / logging)
  - Monotone quantile predictions
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from services.model.constants import QUANTILE_ALPHAS
from services.model.quantile_suite import QuantileSuite
from services.model.monotonic_mapper import MonotonicMapper
from services.model.strike_pricer import StrikePricer
from services.model.calibrator import IsotonicCalibrator

logger = logging.getLogger(__name__)


@dataclass
class PredictionResult:
    """Output of one Weather Brain inference call."""
    # Stage 2: raw quantile predictions {alpha → °F delta}
    raw_quantiles:     dict[float, float] = field(default_factory=dict)
    # Stage 3: monotone quantile predictions {alpha → °F delta}
    monotone_quantiles: dict[float, float] = field(default_factory=dict)
    # Stage 4: raw strike probabilities {strike_f → P_raw}
    p_raw_per_strike:  dict[float, float] = field(default_factory=dict)
    # Stage 5: calibrated strike probabilities {strike_f → P_cal}
    p_cal_per_strike:  dict[float, float] = field(default_factory=dict)
    # Whether the calibrator was available (False → p_cal == p_raw)
    calibrator_applied: bool = False


class WeatherBrainInference:
    """Full Weather Brain inference pipeline (Stages 2–5).

    Stage 1 (FeatureEngine) is handled upstream by the caller, which passes
    a pre-built feature row.

    Parameters
    ----------
    suite :      Fitted QuantileSuite (Stage 2).
    mapper :     MonotonicMapper instance (Stage 3, stateless).
    pricer :     StrikePricer instance (Stage 4, stateless).
    calibrator : Optional fitted IsotonicCalibrator (Stage 5).
    """

    def __init__(
        self,
        suite: QuantileSuite,
        mapper: MonotonicMapper,
        pricer: StrikePricer,
        calibrator: Optional[IsotonicCalibrator] = None,
    ):
        self.suite      = suite
        self.mapper     = mapper
        self.pricer     = pricer
        self.calibrator = calibrator

    # ------------------------------------------------------------------
    # Prediction
    # ------------------------------------------------------------------

    def predict(
        self,
        feature_row: pd.Series,
        custom_intraday_max_f: float,
        strikes: list[float],
    ) -> PredictionResult:
        """Run all Weather Brain stages for one observation.

        Parameters
        ----------
        feature_row : feature vector (output of FeatureEngine), as a pd.Series.
        custom_intraday_max_f : running max temperature at observation time.
        strikes : list of Kalshi strike temperatures to price.

        Returns
        -------
        PredictionResult with raw, monotone, and calibrated probabilities.
        """
        # Stage 2: Quantile Suite
        raw_q = self.suite.predict_row(feature_row)

        # Stage 3: Monotonic Mapper
        mono_q = self.mapper.transform(raw_q)

        # Stage 4: Strike Pricer
        p_raw = self.pricer.price_all_strikes(strikes, custom_intraday_max_f, mono_q)

        # Stage 5: Isotonic Calibrator (optional)
        p_cal: dict[float, float] = {}
        calib_applied = False
        if self.calibrator is not None and self.calibrator.is_fitted:
            for strike, p in p_raw.items():
                p_cal[strike] = float(self.calibrator.calibrate(p))
            calib_applied = True
        else:
            p_cal = dict(p_raw)  # pass-through without calibration

        return PredictionResult(
            raw_quantiles=raw_q,
            monotone_quantiles=mono_q,
            p_raw_per_strike=p_raw,
            p_cal_per_strike=p_cal,
            calibrator_applied=calib_applied,
        )

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    @classmethod
    def load(
        cls,
        model_dir: Path,
        icao: str,
        version: str = "final",
        alphas: tuple[float, ...] = QUANTILE_ALPHAS,
    ) -> "WeatherBrainInference":
        """Load all pipeline components from disk.

        Parameters
        ----------
        model_dir : Parent model directory (data/model/trained/).
        icao :      Station ICAO code.
        version :   Model version subdirectory (e.g. ``"final"`` or ``"fold_03"``).
        alphas :    Quantile levels (must match saved models).
        """
        station_dir = model_dir / icao / version

        # Load quantile suite
        suite = QuantileSuite.load(model_dir / icao, version=version)

        # Load calibrator (optional — may not exist for non-final folds)
        calibrator: Optional[IsotonicCalibrator] = None
        cal_path = station_dir / "calibrator.pkl"
        if cal_path.exists():
            calibrator = IsotonicCalibrator.load(cal_path)
        else:
            logger.warning(
                "[%s] No calibrator found at %s — Stage 5 will be a pass-through.",
                icao, cal_path,
            )

        mapper = MonotonicMapper(alphas)
        pricer = StrikePricer(alphas)

        logger.info(
            "[%s] WeatherBrainInference loaded (version=%s, calibrator=%s)",
            icao, version, "yes" if calibrator else "no",
        )
        return cls(suite=suite, mapper=mapper, pricer=pricer, calibrator=calibrator)
