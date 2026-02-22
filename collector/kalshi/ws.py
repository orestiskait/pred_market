"""Kalshi WebSocket mixin — orderbook maintenance and reconnection logic.

Provides ``KalshiWSMixin``, to be mixed into any class that needs
live Kalshi orderbook data.  The mixin handles:
  - Authenticated WebSocket connection
  - Automatic reconnection on disconnect
  - In-memory orderbook snapshot + delta application
  - A hook (``on_kalshi_message``) for subclass-specific logic
"""

from __future__ import annotations

import asyncio
import json
import logging

import websockets

logger = logging.getLogger(__name__)


class KalshiWSMixin:
    """Reusable Kalshi WebSocket connection + orderbook maintenance.

    Subclasses must set:
        self.kalshi_auth   : KalshiAuth
        self.kalshi_ws_url : str
        self._running      : bool
        self.orderbooks    : dict
        self.market_tickers: list

    Override ``on_kalshi_message(mtype, data)`` for extra processing.

    When ``_kalshi_ws`` is set (by the connection loop), call
    ``request_kalshi_reconnect()`` to force a reconnect with updated
    subscriptions (e.g., after periodic re-discovery finds new tickers).
    """

    def request_kalshi_reconnect(self) -> None:
        """Request a reconnect so subscriptions use the current ticker list.
        Call after updating market_tickers / _kalshi_subscribe_tickers."""
        self._kalshi_reconnect_requested = True
        ws = getattr(self, "_kalshi_ws", None)
        if ws is not None and not ws.closed:
            asyncio.create_task(self._close_kalshi_ws())

    async def _close_kalshi_ws(self) -> None:
        """Close the WebSocket from another task to trigger reconnect."""
        ws = getattr(self, "_kalshi_ws", None)
        if ws is not None and not ws.closed:
            await ws.close()

    # Orderbook apply helpers ─────────────────────────────────────────

    def apply_orderbook_snapshot(self, data: dict) -> None:
        """Replace the in-memory orderbook with a full WS snapshot."""
        tk = data.get("market_ticker", "")
        ob = {"yes": {}, "no": {}}
        for side in ("yes", "no"):
            for price, qty in data.get(side, []):
                ob[side][int(price)] = qty
        self.orderbooks[tk] = ob

    def apply_orderbook_delta(self, data: dict) -> None:
        """Incrementally update the in-memory orderbook."""
        tk = data.get("market_ticker", "")
        if tk not in self.orderbooks:
            return
        for side in ("yes", "no"):
            for price, qty in data.get(side, []):
                p = int(price)
                if qty <= 0:
                    self.orderbooks[tk][side].pop(p, None)
                else:
                    self.orderbooks[tk][side][p] = qty

    # Hook for subclass-specific processing ───────────────────────────

    def on_kalshi_message(self, mtype: str, data: dict) -> None:
        """Override in subclass for additional message handling."""

    # Connection loop ─────────────────────────────────────────────────

    async def kalshi_ws_loop(self) -> None:
        """WebSocket connection loop with reconnection and orderbook maintenance."""
        channels = getattr(self, "_kalshi_channels", ["orderbook_delta"])

        while self._running:
            self._kalshi_reconnect_requested = False
            subscribe_tickers = (
                getattr(self, "_kalshi_subscribe_tickers", None)
                or self.market_tickers
            )
            try:
                headers = self.kalshi_auth.ws_headers()
                async with websockets.connect(
                    self.kalshi_ws_url, additional_headers=headers
                ) as ws:
                    self._kalshi_ws = ws
                    logger.info("Kalshi WebSocket connected")

                    for msg_id, channel in enumerate(channels, 1):
                        sub = {
                            "id": msg_id,
                            "cmd": "subscribe",
                            "params": {
                                "channels": [channel],
                                "market_tickers": subscribe_tickers,
                            },
                        }
                        await ws.send(json.dumps(sub))
                    logger.info(
                        "Subscribed to %d markets on %s",
                        len(subscribe_tickers), channels,
                    )

                    async for raw in ws:
                        if not self._running:
                            break
                        if getattr(self, "_kalshi_reconnect_requested", False):
                            logger.info("Reconnecting for updated ticker subscriptions")
                            break
                        msg = json.loads(raw)
                        mtype = msg.get("type")
                        data = msg.get("msg", {})

                        if mtype == "orderbook_snapshot":
                            self.apply_orderbook_snapshot(data)
                        elif mtype == "orderbook_delta":
                            self.apply_orderbook_delta(data)

                        # Always forward to subclass hook
                        self.on_kalshi_message(mtype, data)

            except websockets.ConnectionClosed as e:
                logger.warning("Kalshi WS disconnected: %s — reconnecting in 5s", e)
                await asyncio.sleep(5)
            except Exception as e:
                if not self._running:
                    break
                logger.error(
                    "Kalshi WS error: %s — reconnecting in 10s", type(e).__name__
                )
                await asyncio.sleep(10)
            finally:
                self._kalshi_ws = None
