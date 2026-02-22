"""Station registry for weather fetchers.

Derives StationInfo and lookup helpers from the central market registry
(services.markets.registry). Add new cities there; this module exposes
a lightweight view for weather fetchers that only need ICAO / IATA / city / tz.
"""

from __future__ import annotations

from dataclasses import dataclass

from services.markets.registry import MARKET_REGISTRY, MarketConfig


@dataclass(frozen=True)
class StationInfo:
    """Immutable metadata for a single weather station (ICAO, IATA, city, tz)."""

    icao: str
    iata: str
    city: str
    tz: str


def _station_info(mc: MarketConfig) -> StationInfo:
    return StationInfo(icao=mc.icao, iata=mc.iata, city=mc.city, tz=mc.tz)


STATION_REGISTRY: dict[str, StationInfo] = {
    k: _station_info(v) for k, v in MARKET_REGISTRY.items()
}


def station_for_icao(icao: str) -> StationInfo:
    """Look up a StationInfo by ICAO code (e.g. 'KMDW')."""
    for mc in MARKET_REGISTRY.values():
        if mc.icao == icao:
            return _station_info(mc)
    raise KeyError(f"No station with ICAO code {icao!r} in registry")


def stations_for_series(series_list: list[str]) -> list[StationInfo]:
    """Return unique StationInfo objects for a list of event-series prefixes."""
    seen: set[str] = set()
    result: list[StationInfo] = []
    for series in series_list:
        mc = MARKET_REGISTRY[series]
        if mc.icao not in seen:
            seen.add(mc.icao)
            result.append(_station_info(mc))
    return result


__all__ = [
    "StationInfo",
    "STATION_REGISTRY",
    "station_for_icao",
    "stations_for_series",
]
