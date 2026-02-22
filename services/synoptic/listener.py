"""Synoptic listener: live WebSocket-based weather data ingest.

Streams real-time weather observations from the Synoptic push API
and periodically flushes to Parquet.

Usage:
    python -m services.synoptic.listener
    python -m services.synoptic.listener --config path/to/config.yaml
"""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path

from ..core.config import (
    load_config,
    get_synoptic_token,
    build_synoptic_ws_url,
    standard_argparser,
    configure_logging,
)
from ..core.service import AsyncService
from ..core.storage import ParquetStorage
from ..markets.registry import all_synoptic_stations
from .ws import SynopticWSMixin

logger = logging.getLogger(__name__)


class SynopticLiveCollector(AsyncService, SynopticWSMixin):
    """Streams Synoptic WebSocket data and periodically flushes to parquet."""

    def __init__(self, config: dict, config_dir: Path):
        self.config = config

        # Synoptic credentials & URL
        self._synoptic_token = get_synoptic_token()

        # Build station list from event_series in config (via market registry)
        # or fall back to explicit synoptic.stations in config.
        scfg = config.get("synoptic", {})
        stations = scfg.get("stations", None)
        if stations is None:
            # Auto-derive from event_series via the market registry
            stations = all_synoptic_stations(config.get("event_series", []))
        variables = scfg.get("vars", ["air_temp"])

        self.synoptic_ws_url = build_synoptic_ws_url(
            self._synoptic_token, stations, variables,
        )

        # Storage
        data_dir = (config_dir / config["storage"]["data_dir"]).resolve()
        self.storage = ParquetStorage(str(data_dir))
        self.flush_interval = config["storage"].get("flush_interval_seconds", 300)

        # State
        self._running = False
        self._buf: list[dict] = []

    # ------------------------------------------------------------------ #
    # SynopticWSMixin hook                                                 #
    # ------------------------------------------------------------------ #

    def on_synoptic_observation(self, row: dict):
        """Buffer each parsed observation for periodic flush to parquet."""
        self._buf.append(row)

    # ------------------------------------------------------------------ #
    # Flush logic                                                          #
    # ------------------------------------------------------------------ #

    def _flush(self):
        """Write buffered data to parquet and clear buffers."""
        if self._buf:
            logger.info("Flushing %d Synoptic observations to parquet", len(self._buf))
            self.storage.write_synoptic_ws(self._buf)
            self._buf.clear()

    async def _snapshot_loop(self):
        """Periodic buffer flush."""
        last_flush = time.monotonic()
        while self._running:
            await asyncio.sleep(1)
            if not self._running:
                break
            if time.monotonic() - last_flush >= self.flush_interval:
                self._flush()
                last_flush = time.monotonic()

    # ------------------------------------------------------------------ #
    # AsyncService overrides                                               #
    # ------------------------------------------------------------------ #

    def _get_tasks(self) -> list:
        return [self.synoptic_ws_loop(), self._snapshot_loop()]

    def _on_shutdown(self):
        self._flush()
        logger.info("Synoptic buffers flushed.")


# ------------------------------------------------------------------ #
# CLI                                                                  #
# ------------------------------------------------------------------ #

def main():
    parser = standard_argparser("Synoptic listener (live WebSocket weather ingest)")
    args = parser.parse_args()

    configure_logging(args.log_level)

    config, config_path = load_config(args.config)
    svc = SynopticLiveCollector(config, config_dir=config_path.parent)
    asyncio.run(svc.run())


if __name__ == "__main__":
    main()
