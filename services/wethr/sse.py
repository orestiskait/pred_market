"""Wethr.net Push API SSE mixin — real-time weather data streaming.

Provides ``WethrSSEMixin``, to be mixed into any class that needs live
Wethr.net push data. The mixin handles:
  - Server-Sent Events (SSE) connection to Wethr.net Push API
  - Automatic reconnection with last_event_id replay
  - Parsing of all event types: observation, dsm, cli, new_high, new_low
  - Hooks for subclass-specific logic per event type

The Push API delivers:
  - observation: New METAR/HF-METAR/SPECI observation (every 1-5 min per station)
  - dsm: Daily Summary Message release
  - cli: Climate Report release
  - new_high: Temperature high alert
  - new_low: Temperature low alert
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

import aiohttp

logger = logging.getLogger(__name__)

WETHR_PUSH_BASE_URL = "https://wethr.net:3443/api/v2/stream"


class WethrSSEMixin:
    """Reusable Wethr.net Push API SSE connection + message parsing.

    Subclasses must set:
        self.wethr_api_key  : str
        self.wethr_stations : list[str]  (ICAO codes)
        self._running       : bool

    Override hooks to handle each event type:
        on_wethr_observation(data, received_ts)
        on_wethr_dsm(data, received_ts)
        on_wethr_cli(data, received_ts)
        on_wethr_new_high(data, received_ts)
        on_wethr_new_low(data, received_ts)
    """

    def on_wethr_observation(self, data: dict, received_ts: datetime) -> None:
        """Override in subclass to handle observation events."""

    def on_wethr_dsm(self, data: dict, received_ts: datetime) -> None:
        """Override in subclass to handle DSM events."""

    def on_wethr_cli(self, data: dict, received_ts: datetime) -> None:
        """Override in subclass to handle CLI events."""

    def on_wethr_new_high(self, data: dict, received_ts: datetime) -> None:
        """Override in subclass to handle new_high events."""

    def on_wethr_new_low(self, data: dict, received_ts: datetime) -> None:
        """Override in subclass to handle new_low events."""

    def _build_wethr_url(self) -> str:
        stations = ",".join(getattr(self, "wethr_stations", []))
        api_key = getattr(self, "wethr_api_key", "")
        return f"{WETHR_PUSH_BASE_URL}?stations={stations}&api_key={api_key}"

    async def wethr_sse_loop(self) -> None:
        """SSE connection loop for Wethr.net Push API."""
        last_event_id: str | None = None
        stations = getattr(self, "wethr_stations", [])
        safe_url = f"{WETHR_PUSH_BASE_URL}?stations={','.join(stations)}&api_key=<REDACTED>"

        while self._running:
            try:
                url = self._build_wethr_url()
                if last_event_id:
                    url += f"&last_event_id={last_event_id}"

                logger.info("Connecting to Wethr Push API: %s", safe_url)

                timeout = aiohttp.ClientTimeout(total=None, sock_read=90)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    async with session.get(url) as resp:
                        if resp.status != 200:
                            body = await resp.text()
                            logger.error(
                                "Wethr Push API returned %d: %s", resp.status, body[:200]
                            )
                            await asyncio.sleep(10)
                            continue

                        logger.info("Wethr Push API connected (stations=%s)", stations)

                        event_type = None
                        data_lines: list[str] = []

                        async for line_bytes in resp.content:
                            if not self._running:
                                break

                            line = line_bytes.decode("utf-8").rstrip("\n\r")

                            if line.startswith("event:"):
                                event_type = line[6:].strip()
                                data_lines = []
                            elif line.startswith("data:"):
                                data_lines.append(line[5:].strip())
                            elif line.startswith("id:"):
                                last_event_id = line[3:].strip()
                            elif line == "":
                                if event_type and data_lines:
                                    received_ts = datetime.now(timezone.utc)
                                    raw = "\n".join(data_lines)
                                    self._dispatch_wethr_event(
                                        event_type, raw, received_ts
                                    )
                                event_type = None
                                data_lines = []

            except aiohttp.ClientError as e:
                if not self._running:
                    break
                logger.warning(
                    "Wethr Push API connection error: %s — reconnecting in 5s",
                    e,
                )
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._running:
                    break
                logger.error(
                    "Wethr Push API error: %s — reconnecting in 10s",
                    type(e).__name__,
                )
                await asyncio.sleep(10)

    def _dispatch_wethr_event(
        self, event_type: str, raw: str, received_ts: datetime
    ) -> None:
        """Parse JSON and route to the appropriate handler."""
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.warning("Could not parse Wethr event data: %.200s", raw)
            return

        handler = {
            "observation": self.on_wethr_observation,
            "dsm": self.on_wethr_dsm,
            "cli": self.on_wethr_cli,
            "new_high": self.on_wethr_new_high,
            "new_low": self.on_wethr_new_low,
        }.get(event_type)

        if handler:
            try:
                handler(data, received_ts)
            except Exception:
                logger.exception("Error in Wethr %s handler", event_type)
        elif event_type == "heartbeat":
            logger.debug("Wethr heartbeat received")
        elif event_type == "connected":
            logger.info("Wethr connected event: %s", data)
        else:
            logger.debug("Unknown Wethr event type: %s", event_type)
