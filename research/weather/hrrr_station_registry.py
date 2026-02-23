"""Station registry for NWP point extraction — re-export from services.weather.station_registry.

This module has been moved to services/weather/station_registry.py.
The class has been renamed from HRRRStation to NWPStation.
"""

from services.weather.station_registry import (  # noqa: F401
    NWPStation,
    NWP_STATION_REGISTRY,
    nwp_station_for_icao,
    nwp_stations_for_series,
)

# Backward compat aliases used by research scripts
HRRRStation = NWPStation
HRRR_STATION_REGISTRY = NWP_STATION_REGISTRY
hrrr_station_for_icao = nwp_station_for_icao
hrrr_stations_for_series = nwp_stations_for_series
