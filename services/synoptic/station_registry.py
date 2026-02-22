"""Synoptic WebSocket station IDs for real-time 1-min ASOS data.

Used only by: synoptic/listener, weather_bot.

Data source: Synoptic push API (wss://push.synopticdata.com/).
Station IDs come from MarketConfig.synoptic_station in services.markets.registry
(e.g. "KMDW1M", "KNYC"). This module provides a clear entry point for
Synoptic-specific station lookup — distinct from IEM/AWC which use
research.weather.iem_awc_station_registry.
"""

from __future__ import annotations

from ..markets.registry import all_synoptic_stations

# Re-export with a name that makes the Synoptic context explicit
synoptic_stations_for_series = all_synoptic_stations

__all__ = ["synoptic_stations_for_series"]
