"""MADIS (Meteorological Assimilation Data Ingest System) station observation fetchers.

Extracts real-time METAR and One-Minute ASOS (OMO) observations from
NOAA's MADIS archive on AWS S3 (s3://noaa-madis-pds).

Data types:
  - "madis_metar"   — decoded METAR with T-group precision (5-min cadence)
  - "madis_omo"     — One-Minute ASOS sensor dumps (1-min cadence)

Module registry:
  MADIS_FETCHERS["madis_metar"]  → MADISMETARFetcher
  MADIS_FETCHERS["madis_omo"]    → MADISOMOFetcher
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from services.weather.madis.metar import MADISMETARFetcher
    from services.weather.madis.omo import MADISOMOFetcher

MADIS_FETCHERS: dict[str, type] = {}


def _load_madis() -> None:
    """Lazy-import MADIS fetcher classes to populate MADIS_FETCHERS."""
    if MADIS_FETCHERS:
        return
    from services.weather.madis.metar import MADISMETARFetcher
    from services.weather.madis.omo import MADISOMOFetcher

    MADIS_FETCHERS["madis_metar"] = MADISMETARFetcher
    MADIS_FETCHERS["madis_omo"] = MADISOMOFetcher
