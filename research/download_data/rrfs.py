"""Fetch RRFS (Rapid Refresh Forecast System) model data at station coordinates.

RRFS is NOAA's next-generation convection-allowing model replacing HRRR.
Currently in prototype on AWS S3; operational launch targeted 2026.

RRFS specifics:
  - 3 km grid resolution over CONUS (Lambert conformal)
  - Hourly output only — no sub-hourly/15-minute product exists
  - Prototype data generation was paused Dec 2024 for retrospective testing;
    historical data is still available on S3
  - Deterministic forecasts every hour out to 18h; extended to 60h for
    00/06/12/18Z cycles

Data source: NOAA RRFS via AWS S3 (s3://noaa-rrfs-pds), public, no auth.
"""

from __future__ import annotations

from research.download_data.nwp_base import NWPPointFetcher


class RRFSFetcher(NWPPointFetcher):
    """RRFS hourly point fetcher (control member, CONUS domain)."""

    SOURCE_NAME = "rrfs"
    HERBIE_MODEL = "rrfs"
    HERBIE_PRODUCT = "prslev"
    HERBIE_KWARGS = {"member": "control", "domain": "conus"}
    DEFAULT_MAX_FXX = 18
    DEFAULT_CYCLES = [0, 6, 12, 18]
