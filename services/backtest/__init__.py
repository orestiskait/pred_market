"""Backtesting framework for the Kalshi Weather Trading Bot.

Replays historical data through the exact same strategy + execution pipeline
used in production (EventBus → Strategy → OrderIntent → ExecutionManager),
with strict latency modeling and no data leakage.

Modules
-------
engine : BacktestEngine
    Core replay engine with SyncEventBus, BacktestExecutionManager.
data_loader : DataLoader
    Reads parquet data and builds chronological SimEvent timelines.
asos_cli_plateau_analyzer : AsosCliPlateauAnalyzer
    Compare Synoptic ASOS 1-min plateau temps vs NWS CLI daily high.
run : CLI
    ``python -m services.backtest.run`` — backtest bot strategies.
run_asos_cli_plateau_analysis : CLI
    ``python -m services.backtest.run_asos_cli_plateau_analysis`` — ASOS vs CLI high plateau analysis.
"""
