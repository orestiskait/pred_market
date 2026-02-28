"""CLI entry point for Weather Brain training.

Usage:
    python -m services.model.run_training \\
        --icao KMDW \\
        --start-date 2025-05-28 \\
        --end-date 2026-02-27 \\
        [--config services/config.yaml] \\
        [--mode cv | final | both] \\
        [--log-level INFO]

Modes:
    cv     — Run walk-forward cross-validation and print metrics.
    final  — Train final production models on all data + save to disk.
    both   — Run CV first, then train final models (default).

Output:
    Trained models → data/model/trained/{ICAO}/
    Drop log       → data/model/training_logs/{ICAO}_drop_log.parquet
    CV results      → logged to stdout
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

from services.core.config import load_config, configure_logging
from services.markets.kalshi_registry import KALSHI_MARKET_REGISTRY
from services.model.constants import MODEL_SUBDIR, TRAINED_MODELS_SUBDIR
from services.model.data_loader import ModelDataLoader
from services.model.training_set_builder import TrainingSetBuilder
from services.model.walk_forward_cv import WalkForwardCV

logger = logging.getLogger("model.run_training")


def _resolve_data_dir(config: dict, config_path: Path) -> Path:
    """Resolve data directory from config (relative to config file location)."""
    storage_cfg = config.get("storage", {})
    data_dir_raw = storage_cfg.get("data_dir", "../data")
    return (config_path.parent / data_dir_raw).resolve()


def _find_market_config(icao: str):
    """Find KalshiMarketConfig by ICAO code."""
    for mc in KALSHI_MARKET_REGISTRY.values():
        if mc.icao == icao:
            return mc
    raise ValueError(
        f"ICAO {icao!r} not found in KALSHI_MARKET_REGISTRY. "
        f"Available: {[mc.icao for mc in KALSHI_MARKET_REGISTRY.values()]}"
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Train Weather Brain XGBoost quantile regression models."
    )
    parser.add_argument("--icao", required=True, help="Station ICAO code (e.g. KMDW)")
    parser.add_argument("--start-date", required=True, help="Training start date YYYY-MM-DD")
    parser.add_argument("--end-date",   required=True, help="Training end date YYYY-MM-DD")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    parser.add_argument(
        "--mode",
        choices=["cv", "final", "both"],
        default="both",
        help="cv=cross-validation only, final=train final model, both=both (default)",
    )
    parser.add_argument("--log-level", default="INFO")

    args = parser.parse_args()
    configure_logging(args.log_level)

    # Load config and resolve paths
    config, config_path = load_config(args.config)
    data_dir = _resolve_data_dir(config, config_path)
    model_dir = data_dir / MODEL_SUBDIR / TRAINED_MODELS_SUBDIR

    icao       = args.icao.upper()
    start_date = date.fromisoformat(args.start_date)
    end_date   = date.fromisoformat(args.end_date)

    logger.info("=" * 65)
    logger.info(" Weather Brain Training")
    logger.info("  Station    : %s", icao)
    logger.info("  Date range : %s → %s", start_date, end_date)
    logger.info("  Mode       : %s", args.mode)
    logger.info("  Data dir   : %s", data_dir)
    logger.info("  Model dir  : %s", model_dir)
    logger.info("=" * 65)

    # Resolve station metadata
    mc = _find_market_config(icao)

    # ── Build training set ────────────────────────────────────────────
    builder = TrainingSetBuilder(
        data_dir=data_dir,
        icao=icao,
        lat=mc.lat,
        lon=mc.lon,
        tz=mc.tz,
    )
    X, y = builder.build(start_date, end_date, save_log=True)

    if X.empty:
        logger.error("Training set is empty — aborting.")
        return 1

    # Print data quality summary
    report = builder.summary_report(X, y, start_date, end_date)
    logger.info("Data Quality Summary:")
    for k, v in report.items():
        logger.info("  %-35s: %s", k, v)

    # ── Cross-Validation ──────────────────────────────────────────────
    if args.mode in ("cv", "both"):
        logger.info("Starting Walk-Forward Cross-Validation...")
        cv = WalkForwardCV(
            icao=icao,
            model_dir=model_dir if args.mode == "both" else None,
        )
        results = cv.run(X, y)

        if results:
            logger.info("=" * 65)
            logger.info("Cross-Validation Summary")
            logger.info("=" * 65)
            import numpy as np
            crps_values = [r.crps for r in results if not (r.crps != r.crps)]  # filter NaN
            mae_values  = [r.mae  for r in results if not (r.mae  != r.mae)]
            for r in results:
                logger.info(r.summary_line())
            if crps_values:
                logger.info("Avg CRPS: %.4f", float(np.mean(crps_values)))
            if mae_values:
                logger.info("Avg MAE:  %.2f°F", float(np.mean(mae_values)))

    # ── Final Model Training ──────────────────────────────────────────
    if args.mode in ("final", "both"):
        logger.info("Training final production models...")
        cv = WalkForwardCV(icao=icao)
        suite, calibrator = cv.fit_final(X, y, model_dir=model_dir)
        logger.info("✅ Final models saved to %s/%s/final/", model_dir, icao)

    logger.info("Training complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
