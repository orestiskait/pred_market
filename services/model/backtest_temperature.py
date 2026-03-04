"""Walk-forward backtesting of temperature (high) prediction accuracy.

Evaluates how well the model predicts the daily high temperature (CLI High)
vs. the actual observed value. Uses the same walk-forward train/test split
as the P&L backtest.

Prediction logic:
  - Model predicts remaining heating delta Y_t = CLI_High - custom_intraday_max_f
  - Predicted high = custom_intraday_max_f + predicted_delta
  - Two point forecasts: (1) quantile median, (2) XGBoost mean model
  - Metrics: MAE, RMSE, bias, etc. for both

Works with XGBoostEVStrategy (suite + _mapper + mean_model).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas as pd

if TYPE_CHECKING:
    from services.model.backtest_pnl import TradingStrategy

logger = logging.getLogger("backtest_temperature")


def _compute_metrics(df: pd.DataFrame, pred_col: str, error_col: str) -> dict[str, float]:
    """Compute MAE, RMSE, bias, etc. from predictions DataFrame."""
    errors = df[error_col].values
    return {
        "n_days": len(df),
        "mean_predicted_high_f": float(df[pred_col].mean()),
        "mean_actual_high_f": float(df["actual_high_f"].mean()),
        "mae_f": float(np.mean(np.abs(errors))),
        "rmse_f": float(np.sqrt(np.mean(errors ** 2))),
        "bias_f": float(np.mean(errors)),
        "median_abs_error_f": float(np.median(np.abs(errors))),
        "max_abs_error_f": float(np.max(np.abs(errors))),
    }


def run_temperature_backtest(
    strategy: "TradingStrategy",
    X_full: pd.DataFrame,
    test_dates: list[str],
    cli_labels: dict[str, float],
    use_last_obs_per_day: bool = True,
) -> tuple[dict[str, dict[str, float]], pd.DataFrame]:
    """Run temperature prediction accuracy backtest on test days.

    Computes metrics for both (1) quantile median and (2) XGBoost mean model.

    Parameters
    ----------
    strategy : TradingStrategy
        Fitted strategy with suite, _mapper, mean_model (e.g. XGBoostEVStrategy).
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
        {"quantile_median": {...}, "mean": {...}} — metrics for each model.
    predictions_df : pd.DataFrame
        Per-row predictions with pred_high_median_f, pred_high_mean_f, etc.
    """
    suite = getattr(strategy, "suite", None)
    mapper = getattr(strategy, "_mapper", None)
    mean_model = getattr(strategy, "mean_model", None)

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

    records: list[dict[str, Any]] = []

    for idx, row in X_test.iterrows():
        climate_date = str(row["climate_date_lst"])
        actual_high = cli_labels.get(climate_date)
        if actual_high is None:
            continue

        custom_max = row.get("custom_intraday_max_f")
        if pd.isna(custom_max):
            continue

        actual_f = float(actual_high)
        rec: dict[str, Any] = {
            "climate_date": climate_date,
            "observation_time_utc": row.get("observation_time_utc"),
            "custom_intraday_max_f": float(custom_max),
            "actual_high_f": actual_f,
        }

        # Quantile median prediction
        try:
            raw_q = suite.predict_row(row)
            mono_q = mapper.transform(raw_q)
            pred_delta_median = mono_q.get(0.50)
            if pred_delta_median is None:
                pred_delta_median = np.median(list(mono_q.values()))
            pred_high_median = float(custom_max) + float(pred_delta_median)
            rec["pred_high_median_f"] = pred_high_median
            rec["error_median_f"] = pred_high_median - actual_f
        except Exception as e:
            logger.debug("Quantile prediction failed for row %s: %s", idx, e)
            rec["pred_high_median_f"] = np.nan
            rec["error_median_f"] = np.nan

        # Mean model prediction
        if mean_model is not None and mean_model.is_fitted:
            try:
                pred_delta_mean = mean_model.predict_row(row)
                pred_high_mean = float(custom_max) + float(pred_delta_mean)
                rec["pred_high_mean_f"] = pred_high_mean
                rec["error_mean_f"] = pred_high_mean - actual_f
            except Exception as e:
                logger.debug("Mean prediction failed for row %s: %s", idx, e)
                rec["pred_high_mean_f"] = np.nan
                rec["error_mean_f"] = np.nan
        else:
            rec["pred_high_mean_f"] = np.nan
            rec["error_mean_f"] = np.nan

        records.append(rec)

    if not records:
        logger.info("No valid temperature predictions.")
        return {}, pd.DataFrame()

    df = pd.DataFrame(records)

    if use_last_obs_per_day:
        df = (
            df.sort_values("observation_time_utc")
            .groupby("climate_date", as_index=False)
            .last()
        )

    metrics: dict[str, dict[str, float]] = {}

    # Quantile median metrics
    df_median = df.dropna(subset=["error_median_f"])
    if not df_median.empty:
        metrics["quantile_median"] = _compute_metrics(
            df_median, "pred_high_median_f", "error_median_f"
        )

    # Mean model metrics
    df_mean = df.dropna(subset=["error_mean_f"])
    if not df_mean.empty:
        metrics["mean"] = _compute_metrics(df_mean, "pred_high_mean_f", "error_mean_f")

    return metrics, df


def print_temperature_report(
    metrics: dict[str, dict[str, float]],
    predictions_df: pd.DataFrame,
) -> None:
    """Print a formatted temperature backtest report for both models."""
    if not metrics:
        return

    print("\n" + "=" * 80)
    print("BACKTEST RESULTS — Temperature Prediction Accuracy")
    print("=" * 80)

    for model_name, m in metrics.items():
        label = "Quantile median" if model_name == "quantile_median" else "Mean model"
        print(f"\n  --- {label} ---")
        print(f"  Days evaluated:      {m['n_days']}")
        print(f"  Mean predicted (°F): {m['mean_predicted_high_f']:.2f}")
        print(f"  Mean actual (°F):    {m['mean_actual_high_f']:.2f}")
        print(f"  MAE (°F):            {m['mae_f']:.2f}")
        print(f"  RMSE (°F):           {m['rmse_f']:.2f}")
        print(f"  Bias (°F):           {m['bias_f']:+.2f}  (positive = over-predict)")
        print(f"  Median |Error|:      {m['median_abs_error_f']:.2f}")
        print(f"  Max |Error|:         {m['max_abs_error_f']:.2f}")

    if not predictions_df.empty:
        print("\n  Per-Day Predictions (last obs per day):")
        print("  " + "-" * 76)
        has_median = "pred_high_median_f" in predictions_df.columns
        has_mean = "pred_high_mean_f" in predictions_df.columns
        for _, row in predictions_df.iterrows():
            parts = [f"  {row['climate_date']} | Actual: {row['actual_high_f']:>5.1f}°F"]
            if has_median and not pd.isna(row.get("pred_high_median_f")):
                err = row["error_median_f"]
                emoji = "✅" if abs(err) <= 2 else ("⚠️" if abs(err) <= 4 else "❌")
                parts.append(f"Median: {row['pred_high_median_f']:>5.1f}°F (err {err:>+5.1f}) {emoji}")
            if has_mean and not pd.isna(row.get("pred_high_mean_f")):
                err = row["error_mean_f"]
                emoji = "✅" if abs(err) <= 2 else ("⚠️" if abs(err) <= 4 else "❌")
                parts.append(f"Mean: {row['pred_high_mean_f']:>5.1f}°F (err {err:>+5.1f}) {emoji}")
            print(" | ".join(parts))

    print("=" * 80)
