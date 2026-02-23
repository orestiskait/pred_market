"""Station registry for NWP model point extraction.

Derives NWPStation (with lat/lon coordinates) from the central market registry
(services.markets.kalshi_registry). Add new cities there; this module exposes
a coordinate-aware view for all NWP fetchers.

Used by: services/weather/nwp/ (all fetcher classes), services/weather/sns_listener.py
"""

from __future__ import annotations

from dataclasses import dataclass

from services.markets.kalshi_registry import KALSHI_MARKET_REGISTRY, KalshiMarketConfig


@dataclass(frozen=True)
class NWPStation:
    """Station metadata with coordinates for NWP grid-point extraction."""

    icao: str
    city: str
    tz: str
    lat: float
    lon: float


def _nwp_station(mc: KalshiMarketConfig) -> NWPStation:
    return NWPStation(icao=mc.icao, city=mc.city, tz=mc.tz, lat=mc.lat, lon=mc.lon)


NWP_STATION_REGISTRY: dict[str, NWPStation] = {
    k: _nwp_station(v) for k, v in KALSHI_MARKET_REGISTRY.items()
}


def nwp_station_for_icao(icao: str) -> NWPStation:
    """Look up an NWPStation by ICAO code (e.g. 'KMDW')."""
    for mc in KALSHI_MARKET_REGISTRY.values():
        if mc.icao == icao:
            return _nwp_station(mc)
    raise KeyError(f"No station with ICAO code {icao!r} in NWP registry")


def nwp_stations_for_series(series_list: list[str]) -> list[NWPStation]:
    """Return unique NWPStation objects for a list of event-series prefixes."""
    seen: set[str] = set()
    result: list[NWPStation] = []
    for series in series_list:
        mc = KALSHI_MARKET_REGISTRY[series]
        if mc.icao not in seen:
            seen.add(mc.icao)
            result.append(_nwp_station(mc))
    return result


__all__ = [
    "NWPStation",
    "NWP_STATION_REGISTRY",
    "nwp_station_for_icao",
    "nwp_stations_for_series",
]
