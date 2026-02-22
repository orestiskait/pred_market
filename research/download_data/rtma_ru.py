"""Fetch RTMA Rapid Update analysis data at station coordinates.

RTMA-RU (Real-Time Mesoscale Analysis — Rapid Update) is an analysis product,
not a forecast.  It provides the best estimate of current surface conditions
on a 2.5 km NDFD grid.

RTMA-RU specifics:
  - 2.5 km grid resolution over CONUS
  - Analysis only (fxx=0, no forecast hours)
  - Runs every hour
  - No .idx file on S3 — Herbie downloads the full GRIB2 file

Data source: NOAA RTMA via AWS S3 (s3://noaa-rtma-pds), public, no auth.
"""

from __future__ import annotations

from research.download_data.nwp_base import NWPPointFetcher


class RTMARUFetcher(NWPPointFetcher):
    """RTMA Rapid Update analysis point fetcher."""

    SOURCE_NAME = "rtma_ru"
    HERBIE_MODEL = "rtma_ru"
    HERBIE_PRODUCT = "anl"
    DEFAULT_MAX_FXX = 0
    DEFAULT_CYCLES = list(range(24))
