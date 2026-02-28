"""Async service lifecycle with graceful shutdown.

Provides ``AsyncService``, a base class for long-running async processes.
Subclasses override:
  - ``_get_tasks()``   → list of coroutines to run via ``asyncio.gather``
  - ``_on_shutdown()``  → cleanup logic (flush buffers, etc.)

Signal handling (SIGINT / SIGTERM) is wired automatically.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from pathlib import Path

logger = logging.getLogger(__name__)


class AsyncService:
    """Base class for long-running async services with graceful shutdown."""

    _running: bool = False

    def _get_tasks(self) -> list:
        """Return a list of coroutines to run via ``asyncio.gather``."""
        raise NotImplementedError

    def _on_shutdown(self) -> None:
        """Called after all tasks finish — override for cleanup."""

    def _flush(self) -> None:
        """Override to flush buffered data; called by ``_periodic_flush``."""

    async def _periodic_flush(self, interval: float) -> None:
        """Run ``self._flush()`` every *interval* seconds while running.

        Reusable loop — avoids duplicating the same flush-timer pattern
        across multiple listener subclasses.
        """
        last = time.monotonic()
        while self._running:
            await asyncio.sleep(1)
            if not self._running:
                break
            if time.monotonic() - last >= interval:
                self._flush()
                last = time.monotonic()

    async def run(self) -> None:
        """Main entry point — runs until SIGINT / SIGTERM."""
        self._running = True

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.shutdown)

        logger.info("Starting %s...", self.__class__.__name__)
        self._main_task = asyncio.current_task()
        try:
            await asyncio.gather(*self._get_tasks())
        except asyncio.CancelledError:
            logger.info("%s run task cancelled for shutdown.", self.__class__.__name__)
        finally:
            self._on_shutdown()
            logger.info("%s stopped.", self.__class__.__name__)

    def shutdown(self) -> None:
        """Signal-safe shutdown trigger."""
        logger.info("Shutdown signal received")
        self._running = False
        if hasattr(self, "_main_task") and self._main_task and not self._main_task.done():
            self._main_task.cancel()


class MetarCollectorMixin:
    """Mixin that adds METAR collector tasks to a listener service.

    Eliminates duplicated aviationweather collector setup in synoptic
    and wethr listeners.  Call ``_init_metar_collector`` from __init__
    and ``_metar_collector_tasks`` from ``_get_tasks``.
    """

    _metar_collector = None

    def _init_metar_collector(self, config: dict, config_dir: Path) -> None:
        awc_cfg = config.get("aviationweather_metar_collector", {})
        if awc_cfg.get("enabled", False):
            from services.weather.metar_collector import MetarCollector

            self._metar_collector = MetarCollector(
                config, config_dir, get_running=lambda: self._running
            )

    def _metar_collector_tasks(self) -> list:
        if self._metar_collector is None:
            return []
        return [
            self._metar_collector._awc_poll_loop(),
            self._metar_collector._nws_poll_loop(),
        ]
