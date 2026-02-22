"""Backtesting framework for the Kalshi Weather Trading Bot.

Replays historical data through the exact same strategy + execution pipeline
used in production (EventBus → Strategy → OrderIntent → ExecutionManager),
with strict latency modeling and no data leakage.
"""
