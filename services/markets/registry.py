"""Unified station + market registry.

Single source of truth for the mapping between:
  - Kalshi event-series prefix  (e.g. "KXHIGHCHI")
  - Weather station identifiers (ICAO, IATA, Synoptic push ID)
  - Timezone and city metadata

Who uses what:
  - Synoptic (listener, weather_bot): synoptic_station → services.synoptic.station_registry
  - IEM/AWC (iem_asos_1min, awc_metar, iem_daily_climate): icao, iata, city, tz → research.weather.iem_awc_station_registry

Expanding to new cities / markets:
  1. Add a new entry to MARKET_REGISTRY below.
  2. That's it — every service (listener, bot) picks up the new market
     automatically via config.yaml `event_series`.

* All timezone strings are IANA (e.g. ``America/Chicago``) so they work
  with ``zoneinfo`` / ``pytz`` and with Pandas ``tz_convert``.
"""

from __future__ import annotations

from dataclasses import dataclass


# ======================================================================
# Data classes
# ======================================================================

@dataclass(frozen=True)
class MarketConfig:
    """Full configuration for one Kalshi temperature market.

    Combines station metadata with Kalshi / Synoptic identifiers, making
    it trivially easy to add new cities:  just add one entry to
    ``MARKET_REGISTRY``.
    """

    series_prefix: str          # Kalshi event-series prefix (e.g. "KXHIGHCHI")
    icao: str                   # 4-letter ICAO id
    iata: str                   # 3-letter FAA/IATA code
    city: str                   # Human-readable name
    tz: str                     # IANA timezone

    # Synoptic push station ID for real-time 1-min ASOS data.
    # Typically the ICAO code + "1M" suffix (e.g. "KMDW1M"),
    # but KNYC is an exception (no 1M suffix).
    synoptic_station: str = ""


# ======================================================================
# Registry
# ======================================================================

# Maps Kalshi event-series prefix → MarketConfig.
# --- TO ADD A NEW CITY ---
# 1. Add one entry here.
# 2. Add the series prefix to config.yaml → event_series.
# 3. Everything else is automatic.

MARKET_REGISTRY: dict[str, MarketConfig] = {
    "KXHIGHCHI": MarketConfig(
        series_prefix="KXHIGHCHI",
        icao="KMDW", iata="MDW", city="Chicago",
        tz="America/Chicago", synoptic_station="KMDW1M",
    ),
    "KXHIGHNY": MarketConfig(
        series_prefix="KXHIGHNY",
        icao="KNYC", iata="NYC", city="New York",
        tz="America/New_York", synoptic_station="KNYC",
    ),
    "KXHIGHMIA": MarketConfig(
        series_prefix="KXHIGHMIA",
        icao="KMIA", iata="MIA", city="Miami",
        tz="America/New_York", synoptic_station="KMIA1M",
    ),
    "KXHIGHDEN": MarketConfig(
        series_prefix="KXHIGHDEN",
        icao="KDEN", iata="DEN", city="Denver",
        tz="America/Denver", synoptic_station="KDEN1M",
    ),
    "KXHIGHAUS": MarketConfig(
        series_prefix="KXHIGHAUS",
        icao="KAUS", iata="AUS", city="Austin",
        tz="America/Chicago", synoptic_station="KAUS1M",
    ),
    "KXHIGHHOU": MarketConfig(
        series_prefix="KXHIGHHOU",
        icao="KHOU", iata="HOU", city="Houston",
        tz="America/Chicago", synoptic_station="KHOU1M",
    ),
    "KXHIGHPHL": MarketConfig(
        series_prefix="KXHIGHPHL",
        icao="KPHL", iata="PHL", city="Philadelphia",
        tz="America/New_York", synoptic_station="KPHL1M",
    ),
    "KXHIGHATL": MarketConfig(
        series_prefix="KXHIGHATL",
        icao="KATL", iata="ATL", city="Atlanta",
        tz="America/New_York", synoptic_station="KATL1M",
    ),
    "KXHIGHBOS": MarketConfig(
        series_prefix="KXHIGHBOS",
        icao="KBOS", iata="BOS", city="Boston",
        tz="America/New_York", synoptic_station="KBOS1M",
    ),
    "KXHIGHDCA": MarketConfig(
        series_prefix="KXHIGHDCA",
        icao="KDCA", iata="DCA", city="Washington DC",
        tz="America/New_York", synoptic_station="KDCA1M",
    ),
    "KXHIGHDFW": MarketConfig(
        series_prefix="KXHIGHDFW",
        icao="KDFW", iata="DFW", city="Dallas-Fort Worth",
        tz="America/Chicago", synoptic_station="KDFW1M",
    ),
    "KXHIGHLAS": MarketConfig(
        series_prefix="KXHIGHLAS",
        icao="KLAS", iata="LAS", city="Las Vegas",
        tz="America/Los_Angeles", synoptic_station="KLAS1M",
    ),
    "KXHIGHLAX": MarketConfig(
        series_prefix="KXHIGHLAX",
        icao="KLAX", iata="LAX", city="Los Angeles",
        tz="America/Los_Angeles", synoptic_station="KLAX1M",
    ),
    "KXHIGHMSP": MarketConfig(
        series_prefix="KXHIGHMSP",
        icao="KMSP", iata="MSP", city="Minneapolis",
        tz="America/Chicago", synoptic_station="KMSP1M",
    ),
    "KXHIGHMSY": MarketConfig(
        series_prefix="KXHIGHMSY",
        icao="KMSY", iata="MSY", city="New Orleans",
        tz="America/Chicago", synoptic_station="KMSY1M",
    ),
    "KXHIGHOKC": MarketConfig(
        series_prefix="KXHIGHOKC",
        icao="KOKC", iata="OKC", city="Oklahoma City",
        tz="America/Chicago", synoptic_station="KOKC1M",
    ),
    "KXHIGHPHX": MarketConfig(
        series_prefix="KXHIGHPHX",
        icao="KPHX", iata="PHX", city="Phoenix",
        tz="America/Phoenix", synoptic_station="KPHX1M",
    ),
    "KXHIGHSAT": MarketConfig(
        series_prefix="KXHIGHSAT",
        icao="KSAT", iata="SAT", city="San Antonio",
        tz="America/Chicago", synoptic_station="KSAT1M",
    ),
    "KXHIGHSEA": MarketConfig(
        series_prefix="KXHIGHSEA",
        icao="KSEA", iata="SEA", city="Seattle",
        tz="America/Los_Angeles", synoptic_station="KSEA1M",
    ),
    "KXHIGHSFO": MarketConfig(
        series_prefix="KXHIGHSFO",
        icao="KSFO", iata="SFO", city="San Francisco",
        tz="America/Los_Angeles", synoptic_station="KSFO1M",
    ),
}


# ======================================================================
# Lookup helpers
# ======================================================================

def market_for_series(series: str) -> MarketConfig:
    """Look up a MarketConfig by event-series prefix (e.g. 'KXHIGHCHI')."""
    if series in MARKET_REGISTRY:
        return MARKET_REGISTRY[series]
    raise KeyError(f"No market config for series {series!r} in registry")


def all_synoptic_stations(series_list: list[str]) -> list[str]:
    """Return Synoptic push station IDs for the given event-series list.

    Used to build the Synoptic WebSocket subscription URL.
    Synoptic consumers: import synoptic_stations_for_series from
    services.synoptic.station_registry for a clear, data-source-specific entry point.
    """
    seen: set[str] = set()
    result: list[str] = []
    for series in series_list:
        mc = MARKET_REGISTRY[series]
        if mc.synoptic_station and mc.synoptic_station not in seen:
            seen.add(mc.synoptic_station)
            result.append(mc.synoptic_station)
    return result
