"""Synoptic listener: weather observation ingest (streaming or polling).

Streaming: WebSocket push from Synoptic (requires streaming-enabled token).
Polling: REST API timeseries (default; works with standard token).

Also runs the METAR collector (AWC + NWS api.weather.gov) when enabled.

Usage:
    python -m services.synoptic.listener
    python -m services.synoptic.listener --config path/to/config.yaml
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from services.core.config import (
    load_config,
    get_event_series,
    get_synoptic_token,
    build_synoptic_ws_url,
    standard_argparser,
    configure_logging,
)
from services.core.service import AsyncService, MetarCollectorMixin
from services.core.storage import ParquetStorage
from services.synoptic.station_registry import synoptic_stations_for_series
from services.synoptic.ws import SynopticWSMixin

logger = logging.getLogger(__name__)


class SynopticLiveCollector(AsyncService, SynopticWSMixin, MetarCollectorMixin):
    """Ingests Synoptic ASOS 1-min data via streaming (WebSocket) or polling (REST)."""

    def __init__(self, config: dict, config_dir: Path):
        self.config = config

        scfg = config.get("synoptic", {})
        self._synoptic_enabled = scfg.get("enabled", True)

        self._synoptic_token = None
        self._stations = []
        self._synoptic_mode = "polling"
        self._poll_interval = 90
        self._poll_recent_minutes = 120
        self.synoptic_ws_url = ""

        if self._synoptic_enabled:
            self._synoptic_token = get_synoptic_token(config)
            stations = scfg.get("stations", None)
            if stations is None:
                stations = synoptic_stations_for_series(get_event_series(config, "synoptic_listener"))
            variables = scfg.get("vars", ["air_temp"])
            self._stations = stations
            self._synoptic_mode = scfg.get("mode", "polling")
            self._poll_interval = scfg.get("poll_interval_seconds", 90)
            self._poll_recent_minutes = scfg.get("poll_recent_minutes", 120)
            self.synoptic_ws_url = build_synoptic_ws_url(
                self._synoptic_token, stations, variables,
            )
            logger.info(
                "Synoptic mode=%s (stations=%s)",
                self._synoptic_mode, self._stations,
            )
        else:
            logger.info("Synoptic disabled; running METAR collector only")

        # Storage
        data_dir = (config_dir / config["storage"]["data_dir"]).resolve()
        self.storage = ParquetStorage(str(data_dir))
        self.flush_interval = config["storage"].get("flush_interval_seconds", 300)

        # State
        self._running = False
        self._buf: list[dict] = []
        self._last_synoptic_ob: dict[str, object] = {}

        # METAR collector (AWC + NWS)
        self._init_metar_collector(config, config_dir)

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

    async def _synoptic_poll_loop(self):
        """Poll Synoptic REST API and write new observations."""
        from services.synoptic.poll import fetch_synoptic_recent

        while self._running:
            try:
                rows = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: fetch_synoptic_recent(
                        self._stations,
                        self._synoptic_token,
                        recent_minutes=self._poll_recent_minutes,
                    ),
                )
                if rows:
                    new_rows = []
                    for r in rows:
                        stid = r["stid"]
                        ob_ts = r["ob_timestamp"]
                        last = self._last_synoptic_ob.get(stid)
                        if last is None or ob_ts > last:
                            self._last_synoptic_ob[stid] = ob_ts
                            new_rows.append(r)
                    if new_rows:
                        self.storage.write_synoptic_ws(new_rows)
                        logger.info(
                            "Synoptic poll: saved %d new obs (stations=%s)",
                            len(new_rows), list({r["stid"] for r in new_rows}),
                        )
            except Exception:
                if self._running:
                    logger.exception("Synoptic poll error")
            await asyncio.sleep(self._poll_interval)

    # ------------------------------------------------------------------ #
    # AsyncService overrides                                               #
    # ------------------------------------------------------------------ #

    def _get_tasks(self) -> list:
        tasks = []
        if self._synoptic_enabled:
            if self._synoptic_mode == "streaming":
                tasks.extend([self.synoptic_ws_loop(), self._periodic_flush(self.flush_interval)])
            else:
                tasks.append(self._synoptic_poll_loop())
        tasks.extend(self._metar_collector_tasks())
        return tasks

    def _on_shutdown(self):
        if self._synoptic_enabled and self._synoptic_mode == "streaming":
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
