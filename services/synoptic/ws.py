"""Synoptic WebSocket mixin — real-time weather observation streaming.

Provides ``SynopticWSMixin``, to be mixed into any class that needs
live Synoptic push data.  The mixin handles:
  - Authenticated WebSocket connection
  - Automatic reconnection on disconnect
  - Message parsing (data / auth / metadata)
  - A hook (``on_synoptic_observation``) for subclass-specific logic
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

import websockets

logger = logging.getLogger(__name__)


class SynopticWSMixin:
    """Reusable Synoptic WebSocket connection + message parsing.

    Subclasses must set:
        self.synoptic_ws_url : str
        self._running        : bool

    Override ``on_synoptic_observation(row)`` to handle each parsed observation.
    ``row`` is a dict with keys: received_ts, ob_timestamp, stid, sensor, value.
    """

    def on_synoptic_observation(self, row: dict) -> None:
        """Override in subclass to handle each parsed weather observation."""

    def _on_synoptic_auth(self, msg: dict) -> None:
        """Called on Synoptic auth messages."""
        logger.info("Synoptic Auth: %s", msg)
        if msg.get("code") == "failed":
            logger.error("Synoptic Auth Failed!")
            self._running = False

    async def synoptic_ws_loop(self) -> None:
        """WebSocket connection loop for Synoptic push data."""
        token = getattr(self, "_synoptic_token", "")
        safe_url = (
            self.synoptic_ws_url.replace(token, "<TOKEN>")
            if token
            else self.synoptic_ws_url
        )

        while self._running:
            try:
                logger.info("Connecting to Synoptic WS: %s", safe_url)
                async with websockets.connect(
                    self.synoptic_ws_url, ping_interval=None
                ) as ws:
                    logger.info("Synoptic WebSocket connected")

                    async for raw in ws:
                        if not self._running:
                            break
                        msg = json.loads(raw)
                        mtype = msg.get("type")

                        if mtype == "data":
                            received_ts = datetime.now(timezone.utc)
                            for d in msg.get("data", []):
                                try:
                                    ob_dt = datetime.strptime(
                                        d.get("date"), "%Y-%m-%d %H:%M:%S"
                                    )
                                    ob_ts = ob_dt.replace(tzinfo=timezone.utc)
                                    row = {
                                        "received_ts": received_ts,
                                        "ob_timestamp": ob_ts,
                                        "stid": d.get("stid", ""),
                                        "sensor": d.get("sensor", ""),
                                        "value": float(d.get("value")),
                                    }
                                    self.on_synoptic_observation(row)
                                except Exception as e:
                                    logger.warning(
                                        "Could not parse synoptic data row %s: %s",
                                        d, e,
                                    )
                        elif mtype == "auth":
                            self._on_synoptic_auth(msg)
                        elif mtype == "metadata":
                            logger.info("Synoptic Metadata: %s", msg)
                        else:
                            logger.debug("Unknown Synoptic message type: %s", msg)

            except websockets.ConnectionClosed as e:
                logger.warning(
                    "Synoptic WS disconnected: %s — reconnecting in 5s", e
                )
                await asyncio.sleep(5)
            except Exception as e:
                if not self._running:
                    break
                logger.error(
                    "Synoptic WS error: %s — reconnecting in 10s",
                    type(e).__name__,
                )
                await asyncio.sleep(10)
