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

logger = logging.getLogger(__name__)


class AsyncService:
    """Base class for long-running async services with graceful shutdown."""

    _running: bool = False

    def _get_tasks(self) -> list:
        """Return a list of coroutines to run via ``asyncio.gather``."""
        raise NotImplementedError

    def _on_shutdown(self) -> None:
        """Called after all tasks finish — override for cleanup."""

    async def run(self) -> None:
        """Main entry point — runs until SIGINT / SIGTERM."""
        self._running = True

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.shutdown)

        logger.info("Starting %s...", self.__class__.__name__)
        try:
            await asyncio.gather(*self._get_tasks())
        finally:
            self._on_shutdown()
            logger.info("%s stopped.", self.__class__.__name__)

    def shutdown(self) -> None:
        """Signal-safe shutdown trigger."""
        logger.info("Shutdown signal received")
        self._running = False
