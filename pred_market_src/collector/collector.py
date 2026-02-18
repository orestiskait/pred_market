"""Live WebSocket-based Kalshi market data collector.

Usage:
    pred_env/bin/python pred_market_src/collector/collector.py
    pred_env/bin/python pred_market_src/collector/collector.py --config path/to/config.yaml
"""

import asyncio
import json
import logging
import os
import signal
import time
from pathlib import Path

from dotenv import load_dotenv

# Load .env from collector directory so KALSHI_API_KEY_ID etc. are available
load_dotenv(Path(__file__).resolve().parent / ".env")

from datetime import datetime, timezone
from typing import Dict, List
from zoneinfo import ZoneInfo

import websockets
import yaml

# Support both package and script-level imports.
try:
    from .kalshi_client import KalshiAuth, KalshiRestClient
    from .storage import ParquetStorage
except ImportError:
    from kalshi_client import KalshiAuth, KalshiRestClient
    from storage import ParquetStorage

logger = logging.getLogger(__name__)


class LiveCollector:
    """Streams Kalshi WebSocket data and periodically snapshots state to parquet."""

    def __init__(self, config: dict, config_dir: Path):
        self.config = config

        # Auth
        kcfg = config["kalshi"]
        api_key_id = kcfg.get("api_key_id") or os.environ.get("KALSHI_API_KEY_ID", "")
        pk_path = kcfg.get("private_key_path") or os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")
        self.auth = KalshiAuth(api_key_id, pk_path)
        self.rest = KalshiRestClient(kcfg["base_url"], self.auth)
        self.ws_url = kcfg["ws_url"]

        # Storage
        data_dir = config_dir / config["storage"]["data_dir"]
        self.storage = ParquetStorage(str(data_dir))
        self.flush_interval = config["storage"].get("flush_interval_seconds", 300)

        # Collection schedule
        ccfg = config["collection"]
        self.default_interval = ccfg["default_interval_seconds"]
        self.time_schedules = ccfg.get("time_schedules", [])

        # In-memory state
        self.market_tickers: List[str] = []
        self.market_info: Dict[str, dict] = {}
        self.orderbooks: Dict[str, dict] = {}
        self.ticker_data: Dict[str, dict] = {}

        # Buffers
        self._market_buf: List[dict] = []
        self._ob_buf: List[dict] = []
        self._running = False

    # ------------------------------------------------------------------ #
    # Market discovery                                                     #
    # ------------------------------------------------------------------ #

    def discover_markets(self):
        """Fetch all contract tickers for the configured events via REST."""
        for event_ticker in self.config["events"]:
            logger.info("Discovering markets for %s", event_ticker)
            markets = self.rest.get_markets_for_event(event_ticker)
            for m in markets:
                tk = m["ticker"]
                self.market_tickers.append(tk)
                self.market_info[tk] = {
                    "event_ticker": event_ticker,
                    "subtitle": m.get("subtitle", ""),
                    "yes_bid": m.get("yes_bid", 0),
                    "yes_ask": m.get("yes_ask", 0),
                    "last_price": m.get("last_price", 0),
                    "volume": m.get("volume", 0),
                    "open_interest": m.get("open_interest", 0),
                }
                self.orderbooks[tk] = {"yes": {}, "no": {}}
            logger.info("  %d contracts found", len(markets))
        logger.info("Tracking %d total contracts", len(self.market_tickers))

    # ------------------------------------------------------------------ #
    # Dynamic snapshot interval                                            #
    # ------------------------------------------------------------------ #

    def _current_interval(self) -> float:
        """Return snapshot interval for the current time of day."""
        for sched in self.time_schedules:
            tz = ZoneInfo(sched.get("timezone", "UTC"))
            now = datetime.now(tz).time()
            start = datetime.strptime(sched["start"], "%H:%M").time()
            end = datetime.strptime(sched["end"], "%H:%M").time()
            if start <= now <= end:
                return sched["interval_seconds"]
        return self.default_interval

    # ------------------------------------------------------------------ #
    # WebSocket message handling                                           #
    # ------------------------------------------------------------------ #

    def _handle_message(self, raw: str):
        msg = json.loads(raw)
        mtype = msg.get("type")
        data = msg.get("msg", {})

        if mtype == "orderbook_snapshot":
            tk = data.get("market_ticker", "")
            ob = {"yes": {}, "no": {}}
            for side in ("yes", "no"):
                for price, qty in data.get(side, []):
                    ob[side][price] = qty
            self.orderbooks[tk] = ob

        elif mtype == "orderbook_delta":
            tk = data.get("market_ticker", "")
            if tk in self.orderbooks:
                for side in ("yes", "no"):
                    for price, qty in data.get(side, []):
                        if qty <= 0:
                            self.orderbooks[tk][side].pop(price, None)
                        else:
                            self.orderbooks[tk][side][price] = qty

        elif mtype in ("ticker", "ticker_v2"):
            tk = data.get("market_ticker", "")
            self.ticker_data[tk] = data
            if tk in self.market_info:
                for f in ("yes_bid", "yes_ask", "last_price", "volume", "open_interest"):
                    if f in data:
                        self.market_info[tk][f] = data[f]

        elif mtype == "error":
            logger.error("WS error: %s", data)
        elif mtype == "subscribed":
            logger.info("Subscribed: sid=%s", data.get("sid"))
        else:
            logger.debug("Unhandled WS message type: %s", mtype)

    # ------------------------------------------------------------------ #
    # Snapshot and flush                                                   #
    # ------------------------------------------------------------------ #

    def _take_snapshot(self):
        """Capture current in-memory state into buffers."""
        ts = datetime.now(timezone.utc)

        for tk in self.market_tickers:
            info = self.market_info.get(tk, {})
            self._market_buf.append({
                "snapshot_ts": ts,
                "event_ticker": info.get("event_ticker", ""),
                "market_ticker": tk,
                "subtitle": info.get("subtitle", ""),
                "yes_bid": info.get("yes_bid", 0),
                "yes_ask": info.get("yes_ask", 0),
                "last_price": info.get("last_price", 0),
                "volume": info.get("volume", 0),
                "open_interest": info.get("open_interest", 0),
            })

            ob = self.orderbooks.get(tk, {"yes": {}, "no": {}})
            for side in ("yes", "no"):
                for price, qty in ob[side].items():
                    if qty > 0:
                        self._ob_buf.append({
                            "snapshot_ts": ts,
                            "market_ticker": tk,
                            "side": side,
                            "price_cents": int(price),
                            "quantity": float(qty),
                        })

        logger.info(
            "Snapshot @ %s | mkt_rows=%d ob_rows=%d | interval=%.0fs",
            ts.strftime("%H:%M:%S"),
            len(self._market_buf),
            len(self._ob_buf),
            self._current_interval(),
        )

    def _flush(self):
        """Write buffered data to parquet and clear buffers."""
        if self._market_buf:
            self.storage.write_market_snapshots(self._market_buf)
            self._market_buf.clear()
        if self._ob_buf:
            self.storage.write_orderbook_snapshots(self._ob_buf)
            self._ob_buf.clear()

    # ------------------------------------------------------------------ #
    # Async loops                                                          #
    # ------------------------------------------------------------------ #

    async def _ws_loop(self):
        """WebSocket connection loop with automatic reconnection."""
        while self._running:
            try:
                headers = self.auth.ws_headers()
                async with websockets.connect(
                    self.ws_url, additional_headers=headers
                ) as ws:
                    logger.info("WebSocket connected")

                    # Subscribe: orderbook_delta (private) + ticker (public)
                    for msg_id, channel in enumerate(["orderbook_delta", "ticker"], 1):
                        sub = {
                            "id": msg_id,
                            "cmd": "subscribe",
                            "params": {
                                "channels": [channel],
                                "market_tickers": self.market_tickers,
                            },
                        }
                        await ws.send(json.dumps(sub))

                    logger.info("Subscribed to %d markets", len(self.market_tickers))

                    async for raw in ws:
                        if not self._running:
                            break
                        self._handle_message(raw)

            except websockets.ConnectionClosed as e:
                logger.warning("WS disconnected: %s  — reconnecting in 5s", e)
                await asyncio.sleep(5)
            except Exception as e:
                logger.error("WS error: %s  — reconnecting in 10s", e)
                await asyncio.sleep(10)

    async def _snapshot_loop(self):
        """Periodically snapshot state and flush buffers."""
        last_flush = time.monotonic()
        while self._running:
            interval = self._current_interval()
            await asyncio.sleep(interval)
            if not self._running:
                break
            self._take_snapshot()
            if time.monotonic() - last_flush >= self.flush_interval:
                self._flush()
                last_flush = time.monotonic()

    async def run(self):
        """Main entry point — runs until SIGINT / SIGTERM."""
        self._running = True
        self.discover_markets()
        if not self.market_tickers:
            logger.error("No markets found. Check config 'events' list.")
            return

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._shutdown)

        logger.info("Starting live collector...")
        try:
            await asyncio.gather(self._ws_loop(), self._snapshot_loop())
        finally:
            self._flush()
            logger.info("Collector stopped. Buffers flushed.")

    def _shutdown(self):
        logger.info("Shutdown signal received")
        self._running = False


# ------------------------------------------------------------------ #
# CLI                                                                  #
# ------------------------------------------------------------------ #

def main():
    import argparse

    parser = argparse.ArgumentParser(description="Kalshi live market data collector")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).parent / "config.yaml"),
        help="Path to config.yaml",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    config_path = Path(args.config)
    with open(config_path) as f:
        config = yaml.safe_load(f)

    collector = LiveCollector(config, config_dir=config_path.parent)
    asyncio.run(collector.run())


if __name__ == "__main__":
    main()
