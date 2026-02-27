"""Fetch RRFS (Rapid Refresh Forecast System) model data at station coordinates.

RRFS is NOAA's next-generation convection-allowing model replacing HRRR.
Currently in prototype on AWS S3; operational launch targeted 2026.

RRFS specifics:
  - 3 km grid resolution over CONUS (Lambert conformal)
  - Deterministic forecasts every hour out to 18h; extended to 60h for
    00/06/12/18Z cycles
  - Sub-hourly (15-minute) output available via the subh product.

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
            if not hasattr(self, "product"): self.product = "prslev.3km"

            self.DESCRIPTION = "Rapid Refresh Forecast System (RRFS) - Fixed Template"
            self.PRODUCTS = {"prslev.3km": "pressure levels", "prslev.3km.subh": "pressure sub-hourly"}
            
            # Format domain
            domain_suffix = "conus"
            if self.domain == "alaska": domain_suffix = "ak"
            elif self.domain == "hawaii": domain_suffix = "hi"
            elif self.domain == "puerto rico": domain_suffix = "pr"
            else: domain_suffix = str(self.domain)
            
            self.SOURCES = {
                "aws": (
                    f"https://noaa-rrfs-pds.s3.amazonaws.com/rrfs_a/rrfs.{self.date:%Y%m%d/%H}/"
                    f"rrfs.t{self.date:%H}z.{self.product}.f{self.fxx:03d}.{domain_suffix}.grib2"
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
    HERBIE_PRODUCT = "prslev.3km.subh"
    HERBIE_KWARGS = {"member": "control", "domain": "conus"}
    DEFAULT_MAX_FXX = 18
    DEFAULT_CYCLES = [0, 6, 12, 18]
    MODEL_VERSION = "v1 poc"
