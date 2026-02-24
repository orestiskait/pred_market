"""Wethr.net station identifiers for the Push API.

Maps Kalshi event-series prefixes to Wethr.net station codes (ICAO).
Wethr.net uses ICAO codes directly (e.g. "KMDW", "KAUS").
"""

from __future__ import annotations

from services.markets.kalshi_registry import KALSHI_MARKET_REGISTRY


def wethr_stations_for_series(series_list: list[str]) -> list[str]:
    """Return Wethr.net station codes for the given Kalshi event-series list.

    Wethr.net uses standard ICAO codes (KMDW, KAUS, etc.).
    """
    seen: set[str] = set()
    result: list[str] = []
    for series in series_list:
        if series not in KALSHI_MARKET_REGISTRY:
            continue
        mc = KALSHI_MARKET_REGISTRY[series]
        if mc.icao and mc.icao not in seen:
            seen.add(mc.icao)
            result.append(mc.icao)
    return result


def icao_to_series() -> dict[str, str]:
    """Return a mapping of ICAO code -> Kalshi series prefix."""
    return {mc.icao: mc.series_prefix for mc in KALSHI_MARKET_REGISTRY.values()}
