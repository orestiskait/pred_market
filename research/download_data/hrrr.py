"""Fetch HRRR 15-minute sub-hourly model data at station coordinates.

Uses the ``subh`` (sub-hourly) product which provides 2 m temperature at
15-minute intervals.  Each fxx file contains 4 sub-steps:
  - fxx=0 → analysis (0 min)
  - fxx=1 → +15, +30, +45, +60 min
  - fxx=2 → +75, +90, +105, +120 min  etc.

HRRR specifics:
  - 3 km grid resolution over CONUS
  - Runs every hour (00Z–23Z)
  - Forecast hours 0–18 (all cycles); 0–48 (00/06/12/18Z)
  - Files available ~45–90 min after cycle time

Data source: NOAA HRRR via AWS S3 (s3://noaa-hrrr-bdp-pcs), public, no auth.
"""

from __future__ import annotations

from research.download_data.nwp_base import NWPPointFetcher


class HRRRFetcher(NWPPointFetcher):
    """HRRR 15-minute sub-hourly point fetcher."""

    SOURCE_NAME = "hrrr"
    HERBIE_MODEL = "hrrr"
    HERBIE_PRODUCT = "subh"
    DEFAULT_MAX_FXX = 18
    DEFAULT_CYCLES = list(range(24))
