"""Walk-forward backtesting of temperature (high) prediction accuracy.

Evaluates how well the model predicts the daily high temperature (CLI High)
vs. the actual observed value. Uses the same walk-forward train/test split
as the P&L backtest.

Prediction logic:
  - Model predicts remaining heating delta Y_t = CLI_High - custom_intraday_max_f
  - Predicted high = custom_intraday_max_f + predicted_delta (median quantile)
  - Metrics: MAE, RMSE, bias, etc. vs. actual CLI High

Works with XGBoostEVStrategy (or any strategy exposing suite + _mapper).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from services.model.backtest_pnl import TradingStrategy

logger = logging.getLogger("backtest_temperature")


def run_temperature_backtest(
    strategy: "TradingStrategy",
    X_full: pd.DataFrame,
    test_dates: list[str],
    cli_labels: dict[str, float],
    use_last_obs_per_day: bool = True,
) -> tuple[dict[str, float], pd.DataFrame]:
    """Run temperature prediction accuracy backtest on test days.

    Parameters
    ----------
    strategy : TradingStrategy
        Fitted strategy with suite and _mapper (e.g. XGBoostEVStrategy).
    X_full : pd.DataFrame
        Full feature matrix including climate_date_lst, custom_intraday_max_f.
    test_dates : list[str]
        LST climate dates in the test set.
    cli_labels : dict[str, float]
        Map climate_date -> actual CLI high (°F).
    use_last_obs_per_day : bool
        If True, use only the last observation per day (final prediction).
        If False, use all observations (multiple predictions per day).

    Returns
    -------
    metrics : dict
        MAE, RMSE, bias, etc.
    predictions_df : pd.DataFrame
        Per-row predictions with predicted_high_f, actual_high_f, error_f.
    """
    # Require XGBoostEVStrategy-like interface (suite + _mapper)
    suite = getattr(strategy, "suite", None)
    mapper = getattr(strategy, "_mapper", None)
    if suite is None or mapper is None:
        logger.warning(
            "Strategy %s has no suite/_mapper — skipping temperature backtest.",
            type(strategy).__name__,
        )
        return {}, pd.DataFrame()

    if not suite.is_fitted:
        logger.warning("QuantileSuite not fitted — skipping temperature backtest.")
        return {}, pd.DataFrame()

    test_mask = X_full["climate_date_lst"].isin(test_dates)
    X_test = X_full[test_mask]

    if X_test.empty:
        logger.info("No test rows for temperature backtest.")
        return {}, pd.DataFrame()

    records: list[dict] = []

    for idx, row in X_test.iterrows():
        climate_date = str(row["climate_date_lst"])
        actual_high = cli_labels.get(climate_date)
        if actual_high is None:
            continue

        custom_max = row.get("custom_intraday_max_f")
        if pd.isna(custom_max):
            continue

        try:
            raw_q = suite.predict_row(row)
            mono_q = mapper.transform(raw_q)
        except Exception as e:
            logger.debug("Prediction failed for row %s: %s", idx, e)
            continue

        # Median (0.50) as point forecast for high
        pred_delta = mono_q.get(0.50)
        if pred_delta is None:
            pred_delta = np.median(list(mono_q.values()))
        pred_high = float(custom_max) + float(pred_delta)

        records.append({
            "climate_date": climate_date,
            "observation_time_utc": row.get("observation_time_utc"),
            "custom_intraday_max_f": float(custom_max),
            "predicted_delta_f": float(pred_delta),
            "predicted_high_f": pred_high,
            "actual_high_f": float(actual_high),
            "error_f": pred_high - float(actual_high),
        })

    if not records:
        logger.info("No valid temperature predictions.")
        return {}, pd.DataFrame()

    df = pd.DataFrame(records)

    if use_last_obs_per_day:
        # Keep only last observation per day (by observation_time_utc)
        df = (
            df.sort_values("observation_time_utc")
            .groupby("climate_date", as_index=False)
            .last()
        )

    errors = df["error_f"].values
    metrics = {
        "n_days": len(df),
        "mae_f": float(np.mean(np.abs(errors))),
        "rmse_f": float(np.sqrt(np.mean(errors ** 2))),
        "bias_f": float(np.mean(errors)),
        "median_abs_error_f": float(np.median(np.abs(errors))),
        "max_abs_error_f": float(np.max(np.abs(errors))),
    }

    return metrics, df


def print_temperature_report(metrics: dict, predictions_df: pd.DataFrame) -> None:
    """Print a formatted temperature backtest report."""
    if not metrics:
        return

    print("\n" + "=" * 80)
    print("BACKTEST RESULTS — Temperature Prediction Accuracy")
    print("=" * 80)

    print(f"\n  Days evaluated:     {metrics['n_days']}")
    print(f"  MAE (°F):           {metrics['mae_f']:.2f}")
    print(f"  RMSE (°F):          {metrics['rmse_f']:.2f}")
    print(f"  Bias (°F):          {metrics['bias_f']:+.2f}  (positive = over-predict)")
    print(f"  Median |Error|:    {metrics['median_abs_error_f']:.2f}")
    print(f"  Max |Error|:       {metrics['max_abs_error_f']:.2f}")

    if not predictions_df.empty:
        print("\n  Per-Day Predictions:")
        print("  " + "-" * 76)
        for _, row in predictions_df.iterrows():
            err = row["error_f"]
            emoji = "✅" if abs(err) <= 2 else ("⚠️" if abs(err) <= 4 else "❌")
            print(
                f"  {emoji} {row['climate_date']} | "
                f"Pred: {row['predicted_high_f']:>5.1f}°F | "
                f"Actual: {row['actual_high_f']:>5.1f}°F | "
                f"Error: {err:>+5.1f}°F"
            )

    print("=" * 80)
