"""Station registry for IEM and AWC weather data fetchers.

Used only by: iem_asos_1min, awc_metar, iem_daily_climate (download_data).

Derives StationInfo and lookup helpers from the central market registry
(services.markets.kalshi_registry). Add new cities there; this module exposes
a lightweight view for IEM/AWC fetchers that only need ICAO / IATA / city / tz.

Note: Synoptic uses services.synoptic.station_registry (synoptic_stations_for_series).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

from services.markets.kalshi_registry import KALSHI_MARKET_REGISTRY, KalshiMarketConfig


@dataclass(frozen=True)
class StationInfo:
    """Immutable metadata for a single weather station (ICAO, IATA, city, tz)."""

    icao: str
    iata: str
    city: str
    tz: str


def _station_info(mc: KalshiMarketConfig) -> StationInfo:
    return StationInfo(icao=mc.icao, iata=mc.iata, city=mc.city, tz=mc.tz)


STATION_REGISTRY: dict[str, StationInfo] = {
    k: _station_info(v) for k, v in KALSHI_MARKET_REGISTRY.items()
}


def station_for_icao(icao: str) -> StationInfo:
    """Look up a StationInfo by ICAO code (e.g. 'KMDW')."""
    for mc in KALSHI_MARKET_REGISTRY.values():
        if mc.icao == icao:
            return _station_info(mc)
    raise KeyError(f"No station with ICAO code {icao!r} in registry")


def stations_for_series(series_list: list[str]) -> list[StationInfo]:
    """Return unique StationInfo objects for a list of event-series prefixes."""
    seen: set[str] = set()
    result: list[StationInfo] = []
    for series in series_list:
        mc = KALSHI_MARKET_REGISTRY[series]
        if mc.icao not in seen:
            seen.add(mc.icao)
            result.append(_station_info(mc))
    return result


def lst_offset_hours(tz: str) -> int:
    """UTC offset in hours for Local Standard Time (NWS climate day).

    Uses a winter date to avoid DST; NWS uses standard time year-round.
    """
    dt = datetime(2025, 1, 15, 12, 0, 0, tzinfo=ZoneInfo(tz))
    return int(dt.utcoffset().total_seconds() / 3600)


__all__ = [
    "StationInfo",
    "STATION_REGISTRY",
    "lst_offset_hours",
    "station_for_icao",
    "stations_for_series",
]
