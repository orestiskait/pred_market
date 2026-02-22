"""Station registry — re-exports from the central market registry.

This module exists for backward compatibility with the weather fetcher
package.  All data is defined in ``pred_market_src.collector.markets.registry``
(single source of truth).  Extend that module when adding new cities.
"""

from __future__ import annotations

from pred_market_src.collector.markets.registry import (
    StationInfo,
    STATION_REGISTRY,
    station_for_icao,
    stations_for_series,
)

__all__ = [
    "StationInfo",
    "STATION_REGISTRY",
    "station_for_icao",
    "stations_for_series",
]
