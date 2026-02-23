"""NWP (Numerical Weather Prediction) model fetchers.

Provides fetcher classes for HRRR, RRFS, NBM (COG), and RTMA-RU.
All share a common base (NWPPointFetcher) for GRIB2/COG extraction
at station coordinates with Parquet I/O.

Model registry:
  MODEL_REGISTRY["hrrr"]    → HRRRFetcher
  MODEL_REGISTRY["rtma_ru"] → RTMARUFetcher
  MODEL_REGISTRY["rrfs"]    → RRFSFetcher
  MODEL_REGISTRY["nbm"]     → NBMCOGFetcher
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from services.weather.nwp.base import NWPPointFetcher

MODEL_REGISTRY: dict[str, type["NWPPointFetcher"]] = {}


def _load_models() -> None:
    """Lazy-import fetcher classes to populate MODEL_REGISTRY."""
    if MODEL_REGISTRY:
        return
    from services.weather.nwp.hrrr import HRRRFetcher
    from services.weather.nwp.rtma_ru import RTMARUFetcher
    from services.weather.nwp.rrfs import RRFSFetcher
    from services.weather.nwp.nbm import NBMFetcher

    MODEL_REGISTRY["hrrr"] = HRRRFetcher
    MODEL_REGISTRY["rtma_ru"] = RTMARUFetcher
    MODEL_REGISTRY["rrfs"] = RRFSFetcher
    MODEL_REGISTRY["nbm"] = NBMFetcher
