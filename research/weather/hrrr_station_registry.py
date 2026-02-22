"""Station registry for HRRR point extraction.

Derives HRRRStation (with lat/lon coordinates) from the central market registry
(services.markets.kalshi_registry). Add new cities there; this module exposes
a coordinate-aware view for HRRR fetchers.

Used only by: research/download_data/hrrr.py, research/download_data/run_hrrr_collection.py
"""

from __future__ import annotations

from dataclasses import dataclass

from services.markets.kalshi_registry import KALSHI_MARKET_REGISTRY, KalshiMarketConfig


@dataclass(frozen=True)
class HRRRStation:
    """Station metadata with coordinates for HRRR grid-point extraction."""

    icao: str
    city: str
    tz: str
    lat: float
    lon: float


def _hrrr_station(mc: KalshiMarketConfig) -> HRRRStation:
    return HRRRStation(icao=mc.icao, city=mc.city, tz=mc.tz, lat=mc.lat, lon=mc.lon)


HRRR_STATION_REGISTRY: dict[str, HRRRStation] = {
    k: _hrrr_station(v) for k, v in KALSHI_MARKET_REGISTRY.items()
}


def hrrr_station_for_icao(icao: str) -> HRRRStation:
    """Look up an HRRRStation by ICAO code (e.g. 'KMDW')."""
    for mc in KALSHI_MARKET_REGISTRY.values():
        if mc.icao == icao:
            return _hrrr_station(mc)
    raise KeyError(f"No station with ICAO code {icao!r} in HRRR registry")


def hrrr_stations_for_series(series_list: list[str]) -> list[HRRRStation]:
    """Return unique HRRRStation objects for a list of event-series prefixes."""
    seen: set[str] = set()
    result: list[HRRRStation] = []
    for series in series_list:
        mc = KALSHI_MARKET_REGISTRY[series]
        if mc.icao not in seen:
            seen.add(mc.icao)
            result.append(_hrrr_station(mc))
    return result


__all__ = [
    "HRRRStation",
    "HRRR_STATION_REGISTRY",
    "hrrr_station_for_icao",
    "hrrr_stations_for_series",
]
