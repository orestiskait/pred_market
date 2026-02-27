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

from services.weather.nwp.base import NWPPointFetcher


# Monkey-patch Herbie's RRFS template to match current NOAA S3 layout (RRFS-A)
# The default Herbie template is outdated as of early 2026.
def _patch_herbie_rrfs():
    try:
        import herbie.models.rrfs as rrfs_class
        
        def fixed_template(self):
            # Set defaults for expected attributes if they aren't there
            if not hasattr(self, "member"): self.member = "control"
            if not hasattr(self, "domain"): self.domain = "conus"
            if not hasattr(self, "product"): self.product = "prslev"

            self.DESCRIPTION = "Rapid Refresh Forecast System (RRFS) - Fixed Template"
            self.PRODUCTS = {"prslev": "pressure levels", "natlev": "native levels"}
            
            # Format domain
            domain_suffix = "conus"
            if self.domain == "alaska": domain_suffix = "ak"
            elif self.domain == "hawaii": domain_suffix = "hi"
            elif self.domain == "puerto rico": domain_suffix = "pr"
            else: domain_suffix = str(self.domain)
            
            # Use the observed S3 layout: rrfs_a/rrfs.YYYYMMDD/HH/rrfs.tHHz.prslev.3km.fXXX.conus.grib2
            self.SOURCES = {
                "aws": (
                    f"https://noaa-rrfs-pds.s3.amazonaws.com/rrfs_a/rrfs.{self.date:%Y%m%d/%H}/"
                    f"rrfs.t{self.date:%H}z.{self.product}.3km.f{self.fxx:03d}.{domain_suffix}.grib2"
                )
            }
            self.LOCALFILE = self.get_remoteFileName

        rrfs_class.template = fixed_template
    except Exception:
        pass

_patch_herbie_rrfs()


class RRFSFetcher(NWPPointFetcher):
    """RRFS hourly point fetcher (control member, CONUS domain)."""

    SOURCE_NAME = "rrfs"
    HERBIE_MODEL = "rrfs"
    HERBIE_PRODUCT = "prslev"
    HERBIE_KWARGS = {"member": "control", "domain": "conus"}
    DEFAULT_MAX_FXX = 18
    DEFAULT_CYCLES = [0, 6, 12, 18]
    MODEL_VERSION = "v1 poc"
