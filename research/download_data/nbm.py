"""Fetch NBM (National Blend of Models) data at station coordinates.

NBM is a statistically post-processed blend of multiple NWP models
(GFS, HRRR, NAM, etc.) providing bias-corrected probabilistic and
deterministic guidance on a 2.5 km CONUS grid.

NBM specifics:
  - 2.5 km grid resolution over CONUS (product="co")
  - Runs every hour (00Z–23Z)
  - Hourly forecast output; range varies by cycle
    (short-range ≤36h for most cycles; extended for 01/07/13/19Z)
  - Includes ensemble-calibrated temperature forecasts

Data source: NOAA NBM via AWS S3 (s3://noaa-nbm-grib2-pds), public, no auth.
"""

from __future__ import annotations

from research.download_data.nwp_base import NWPPointFetcher


class NBMFetcher(NWPPointFetcher):
    """NBM CONUS point fetcher."""

    SOURCE_NAME = "nbm"
    HERBIE_MODEL = "nbm"
    HERBIE_PRODUCT = "co"
    DEFAULT_MAX_FXX = 36
    DEFAULT_CYCLES = list(range(24))
