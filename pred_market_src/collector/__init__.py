"""Kalshi market data collector — live WebSocket streaming, weather data, and trading bots.

Package layout:
  core/       — Config loading, async service lifecycle, Parquet storage
  kalshi/     — Kalshi REST client, WebSocket mixin, live collector
  synoptic/   — Synoptic push WebSocket mixin and live listener
  bot/        — Trading bots (paper & live)
  markets/    — Market registry, station info, event-ticker resolution
  weather/    — Historical weather data fetchers (ASOS 1-min, METAR, CLI)
"""
