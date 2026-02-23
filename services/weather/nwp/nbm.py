"""Fetch NBM (National Blend of Models) data at station coordinates.

NBM is a statistically post-processed blend of multiple NWP models
(GFS, HRRR, NAM, etc.) providing bias-corrected probabilistic and
deterministic guidance on a 2.5 km CONUS grid.

Uses COG bucket (noaa-nbm-pds) for percentage temperature and standard
temperature with lower latency than GRIB2.

Data source: NOAA NBM COG via AWS S3 (s3://noaa-nbm-pds), us-east-1, public, no auth.
"""

from __future__ import annotations

from services.weather.nwp.nbm_cog import NBMCOGFetcher

# Alias for backward compatibility; NBM now uses COG
NBMFetcher = NBMCOGFetcher
