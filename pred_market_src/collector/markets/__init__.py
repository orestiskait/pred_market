"""Market configuration, station registry, and event-ticker resolution."""

from .registry import (
    MarketConfig,
    StationInfo,
    MARKET_REGISTRY,
    station_for_icao,
    stations_for_series,
    market_for_series,
    all_synoptic_stations,
)
from .ticker import resolve_event_tickers, discover_markets
