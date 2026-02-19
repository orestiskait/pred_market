"""Station registry mapping Kalshi event series to weather stations.

Each entry maps a Kalshi event-series prefix (e.g. "KXHIGHCHI") to:
  - icao    : 4-letter ICAO identifier used by METAR / NWS CLI
  - iata    : 3-letter code used by IEM ASOS 1-min API (ICAO minus leading K)
  - city    : Human-readable city name
  - tz      : IANA timezone for the station (used for local-day boundaries)
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StationInfo:
    """Immutable metadata for a single weather station."""

    icao: str
    iata: str  # IEM uses the 3-letter FAA/IATA code (ICAO minus leading K)
    city: str
    tz: str


# Maps Kalshi event-series prefix → StationInfo.
# Extend this dict when Kalshi adds new cities.
STATION_REGISTRY: dict[str, StationInfo] = {
    "KXHIGHCHI": StationInfo(icao="KMDW", iata="MDW", city="Chicago", tz="America/Chicago"),
    "KXHIGHNY":  StationInfo(icao="KNYC", iata="NYC", city="New York", tz="America/New_York"),
    "KXHIGHMIA": StationInfo(icao="KMIA", iata="MIA", city="Miami", tz="America/New_York"),
    "KXHIGHDEN": StationInfo(icao="KDEN", iata="DEN", city="Denver", tz="America/Denver"),
    "KXHIGHAUS": StationInfo(icao="KAUS", iata="AUS", city="Austin", tz="America/Chicago"),
    "KXHIGHHOU": StationInfo(icao="KHOU", iata="HOU", city="Houston", tz="America/Chicago"),
    "KXHIGHPHL": StationInfo(icao="KPHL", iata="PHL", city="Philadelphia", tz="America/New_York"),
}


def stations_for_series(series_list: list[str]) -> list[StationInfo]:
    """Return unique StationInfo objects for the given event-series prefixes.

    Raises KeyError if a series prefix is not in the registry.
    """
    seen: set[str] = set()
    result: list[StationInfo] = []
    for series in series_list:
        info = STATION_REGISTRY[series]
        if info.icao not in seen:
            seen.add(info.icao)
            result.append(info)
    return result


def station_for_icao(icao: str) -> StationInfo:
    """Look up a StationInfo by ICAO code."""
    for info in STATION_REGISTRY.values():
        if info.icao == icao:
            return info
    raise KeyError(f"No station with ICAO code {icao!r} in registry")
