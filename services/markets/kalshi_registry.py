"""Kalshi market registry — station + event-series mapping.

This module is KALSHI-SPECIFIC. Kalshi uses event-series prefixes (e.g. "KXHIGHCHI")
and tickers like KXHIGHCHI-26FEB22. Other platforms (e.g. Polymarket) use different
reference systems and ticker formats; they would need their own registry.

Single source of truth for the mapping between:
  - Kalshi event-series prefix  (e.g. "KXHIGHCHI")
  - Weather station identifiers (ICAO, IATA, Synoptic push ID)
  - Timezone and city metadata

Who uses what:
  - Synoptic (listener, weather_bot): synoptic_station → services.synoptic.station_registry
  - IEM/AWC (iem_asos_1min, awc_metar, iem_daily_climate): icao, iata, city, tz → research.weather.iem_awc_station_registry

Expanding to new cities / markets:
  1. Add a new entry to KALSHI_MARKET_REGISTRY below.
  2. Add the series prefix to config.yaml → event_series.
  3. Everything else is automatic.

* All timezone strings are IANA (e.g. ``America/Chicago``) so they work
  with ``zoneinfo`` / ``pytz`` and with Pandas ``tz_convert``.
"""

from __future__ import annotations

from dataclasses import dataclass


# ======================================================================
# Data classes
# ======================================================================

@dataclass(frozen=True)
class KalshiMarketConfig:
    """Configuration for one Kalshi temperature market.

    Kalshi-specific: uses event-series prefix (e.g. "KXHIGHCHI") and Kalshi
    ticker conventions. Combines station metadata with Kalshi / Synoptic
    identifiers. Add new cities by adding one entry to ``KALSHI_MARKET_REGISTRY``.
    """

    series_prefix: str          # Kalshi event-series prefix (e.g. "KXHIGHCHI")
    icao: str                   # 4-letter ICAO id
    iata: str                   # 3-letter FAA/IATA code
    city: str                   # Human-readable name
    tz: str                     # IANA timezone
    lat: float                  # Station latitude (decimal degrees, north-positive)
    lon: float                  # Station longitude (decimal degrees, east-positive)

    # Synoptic push station ID for real-time 1-min ASOS data.
    # Typically the ICAO code + "1M" suffix (e.g. "KMDW1M"),
    # but KNYC is an exception (no 1M suffix).
    synoptic_station: str = ""


# ======================================================================
# Kalshi registry
# ======================================================================

# Maps Kalshi event-series prefix → KalshiMarketConfig.
# --- TO ADD A NEW CITY ---
# 1. Add one entry here.
# 2. Add the series prefix to config.yaml → event_series.
# 3. Everything else is automatic.

KALSHI_MARKET_REGISTRY: dict[str, KalshiMarketConfig] = {
    "KXHIGHCHI": KalshiMarketConfig(
        series_prefix="KXHIGHCHI",
        icao="KMDW", iata="MDW", city="Chicago",
        tz="America/Chicago", lat=41.78417, lon=-87.75528,
        synoptic_station="KMDW1M",
    ),
    "KXHIGHNY": KalshiMarketConfig(
        series_prefix="KXHIGHNY",
        icao="KNYC", iata="NYC", city="New York",
        tz="America/New_York", lat=40.7789, lon=-73.9692,
        synoptic_station="KNYC",
    ),
    "KXHIGHMIA": KalshiMarketConfig(
        series_prefix="KXHIGHMIA",
        icao="KMIA", iata="MIA", city="Miami",
        tz="America/New_York", lat=25.7932, lon=-80.2906,
        synoptic_station="KMIA1M",
    ),
    "KXHIGHDEN": KalshiMarketConfig(
        series_prefix="KXHIGHDEN",
        icao="KDEN", iata="DEN", city="Denver",
        tz="America/Denver", lat=39.8561, lon=-104.6737,
        synoptic_station="KDEN1M",
    ),
    "KXHIGHAUS": KalshiMarketConfig(
        series_prefix="KXHIGHAUS",
        icao="KAUS", iata="AUS", city="Austin",
        tz="America/Chicago", lat=30.1945, lon=-97.6699,
        synoptic_station="KAUS1M",
    ),
    "KXHIGHHOU": KalshiMarketConfig(
        series_prefix="KXHIGHHOU",
        icao="KHOU", iata="HOU", city="Houston",
        tz="America/Chicago", lat=29.6454, lon=-95.2789,
        synoptic_station="KHOU1M",
    ),
    "KXHIGHPHL": KalshiMarketConfig(
        series_prefix="KXHIGHPHL",
        icao="KPHL", iata="PHL", city="Philadelphia",
        tz="America/New_York", lat=39.8721, lon=-75.2411,
        synoptic_station="KPHL1M",
    ),
    "KXHIGHATL": KalshiMarketConfig(
        series_prefix="KXHIGHATL",
        icao="KATL", iata="ATL", city="Atlanta",
        tz="America/New_York", lat=33.6407, lon=-84.4277,
        synoptic_station="KATL1M",
    ),
    "KXHIGHBOS": KalshiMarketConfig(
        series_prefix="KXHIGHBOS",
        icao="KBOS", iata="BOS", city="Boston",
        tz="America/New_York", lat=42.3606, lon=-71.0106,
        synoptic_station="KBOS1M",
    ),
    "KXHIGHDCA": KalshiMarketConfig(
        series_prefix="KXHIGHDCA",
        icao="KDCA", iata="DCA", city="Washington DC",
        tz="America/New_York", lat=38.8481, lon=-77.0341,
        synoptic_station="KDCA1M",
    ),
    "KXHIGHDFW": KalshiMarketConfig(
        series_prefix="KXHIGHDFW",
        icao="KDFW", iata="DFW", city="Dallas-Fort Worth",
        tz="America/Chicago", lat=32.8968, lon=-97.0380,
        synoptic_station="KDFW1M",
    ),
    "KXHIGHLAS": KalshiMarketConfig(
        series_prefix="KXHIGHLAS",
        icao="KLAS", iata="LAS", city="Las Vegas",
        tz="America/Los_Angeles", lat=36.0800, lon=-115.1522,
        synoptic_station="KLAS1M",
    ),
    "KXHIGHLAX": KalshiMarketConfig(
        series_prefix="KXHIGHLAX",
        icao="KLAX", iata="LAX", city="Los Angeles",
        tz="America/Los_Angeles", lat=33.9425, lon=-118.4081,
        synoptic_station="KLAX1M",
    ),
    "KXHIGHMSP": KalshiMarketConfig(
        series_prefix="KXHIGHMSP",
        icao="KMSP", iata="MSP", city="Minneapolis",
        tz="America/Chicago", lat=44.8810, lon=-93.2218,
        synoptic_station="KMSP1M",
    ),
    "KXHIGHMSY": KalshiMarketConfig(
        series_prefix="KXHIGHMSY",
        icao="KMSY", iata="MSY", city="New Orleans",
        tz="America/Chicago", lat=29.9934, lon=-90.2580,
        synoptic_station="KMSY1M",
    ),
    "KXHIGHOKC": KalshiMarketConfig(
        series_prefix="KXHIGHOKC",
        icao="KOKC", iata="OKC", city="Oklahoma City",
        tz="America/Chicago", lat=35.3931, lon=-97.6007,
        synoptic_station="KOKC1M",
    ),
    "KXHIGHPHX": KalshiMarketConfig(
        series_prefix="KXHIGHPHX",
        icao="KPHX", iata="PHX", city="Phoenix",
        tz="America/Phoenix", lat=33.4343, lon=-112.0116,
        synoptic_station="KPHX1M",
    ),
    "KXHIGHSAT": KalshiMarketConfig(
        series_prefix="KXHIGHSAT",
        icao="KSAT", iata="SAT", city="San Antonio",
        tz="America/Chicago", lat=29.5337, lon=-98.4698,
        synoptic_station="KSAT1M",
    ),
    "KXHIGHSEA": KalshiMarketConfig(
        series_prefix="KXHIGHSEA",
        icao="KSEA", iata="SEA", city="Seattle",
        tz="America/Los_Angeles", lat=47.4490, lon=-122.3093,
        synoptic_station="KSEA1M",
    ),
    "KXHIGHSFO": KalshiMarketConfig(
        series_prefix="KXHIGHSFO",
        icao="KSFO", iata="SFO", city="San Francisco",
        tz="America/Los_Angeles", lat=37.6197, lon=-122.3750,
        synoptic_station="KSFO1M",
    ),
}


# ======================================================================
# Lookup helpers
# ======================================================================

def market_for_series(series: str) -> KalshiMarketConfig:
    """Look up a KalshiMarketConfig by Kalshi event-series prefix (e.g. 'KXHIGHCHI')."""
    if series in KALSHI_MARKET_REGISTRY:
        return KALSHI_MARKET_REGISTRY[series]
    raise KeyError(f"No Kalshi market config for series {series!r} in registry")


def all_synoptic_stations(series_list: list[str]) -> list[str]:
    """Return Synoptic push station IDs for the given Kalshi event-series list.

    Used to build the Synoptic WebSocket subscription URL.
    Synoptic consumers: import synoptic_stations_for_series from
    services.synoptic.station_registry for a clear, data-source-specific entry point.
    """
    seen: set[str] = set()
    result: list[str] = []
    for series in series_list:
        mc = KALSHI_MARKET_REGISTRY[series]
        if mc.synoptic_station and mc.synoptic_station not in seen:
            seen.add(mc.synoptic_station)
            result.append(mc.synoptic_station)
    return result


def synoptic_station_for_icao(icao: str) -> str | None:
    """Return Synoptic station ID for the given ICAO (e.g. KMDW -> KMDW1M).

    Used by research Synoptic historical fetchers. Returns None if no
    synoptic_station is configured for that ICAO.
    """
    for mc in KALSHI_MARKET_REGISTRY.values():
        if mc.icao == icao:
            return mc.synoptic_station or None
    return None
