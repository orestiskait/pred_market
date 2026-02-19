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
        api_key_id = os.environ.get("KALSHI_API_KEY_ID") or kcfg.get("api_key_id", "")
        pk_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH") or kcfg.get("private_key_path", "")
        self.auth = KalshiAuth(api_key_id, pk_path)
        self.rest = KalshiRestClient(kcfg["base_url"], self.auth)
        self.ws_url = kcfg["ws_url"]

        # Storage
        data_dir = config_dir / config["storage"]["data_dir"]
        self.storage = ParquetStorage(str(data_dir))
        self.flush_interval = config["storage"].get("flush_interval_seconds", 300)

        # Collection schedule
        ccfg = config["collection"]
        self.snapshot_interval = ccfg.get("interval_seconds", 60)
        self.spike_threshold = ccfg.get("spike_threshold_cents", 3)
        self.spike_cooldown = ccfg.get("spike_cooldown_seconds", 2)
        self.max_ob_depth = ccfg.get("max_orderbook_depth", 0)
        self.baseline_every = ccfg.get("baseline_every_n_snapshots", 60)

        # In-memory state
        self.market_tickers: List[str] = []
        self.market_info: Dict[str, dict] = {}
        self.orderbooks: Dict[str, dict] = {}
        self.ticker_data: Dict[str, dict] = {}

        # Buffers
        self._market_buf: List[dict] = []
        self._ob_buf: List[dict] = []
        self._running = False

        # Spike detection: previous prices for delta comparison
        self._prev_prices: Dict[str, dict] = {}
        self._last_event_snapshot: float = 0

        # Delta compression: track which OB levels changed since last snapshot
        self._snapshot_count = 0
        self._last_ob: Dict[str, Dict[str, Dict[int, float]]] = {}
        self._dirty_levels: Dict[str, Dict[str, set]] = {}  # tk -> side -> {prices}

    # ------------------------------------------------------------------ #
    # Market discovery                                                     #
    # ------------------------------------------------------------------ #

    def _resolve_event_tickers(self) -> list[str]:
        """Return the list of event tickers to track.

        Supports two config keys (can be combined):
        - ``event_series``: list of series prefixes (e.g. ``KXHIGHCHI``).
          The collector queries the API for currently-open events and picks
          the one with the latest ``close_time`` for each series.
        - ``events``: list of exact event tickers (legacy / override).
        """
        tickers: list[str] = []

        for series in self.config.get("event_series", []):
            logger.info("Resolving series %s → open events", series)
            events = self.rest.get_events_for_series(series, status="open")
            if not events:
                logger.warning("  No open events found for series %s", series)
                continue
            # Pick the event whose close_time is soonest in the future (i.e.
            # the one actively trading today).  Fall back to ticker sort.
            events.sort(key=lambda e: e.get("close_time") or e.get("ticker", ""))
            chosen = events[0]["event_ticker"]
            logger.info("  → %s (%d open event(s) found)", chosen, len(events))
            tickers.append(chosen)

        tickers.extend(self.config.get("events", []))
        return tickers

    def discover_markets(self):
        """Fetch all contract tickers for the configured events via REST."""
        for event_ticker in self._resolve_event_tickers():
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

        # Seed previous prices for spike detection (from REST initial state)
        for tk, info in self.market_info.items():
            self._prev_prices[tk] = {
                "yes_bid": info.get("yes_bid", 0),
                "yes_ask": info.get("yes_ask", 0),
                "last_price": info.get("last_price", 0),
            }

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
            self._mark_all_dirty(tk)

        elif mtype == "orderbook_delta":
            tk = data.get("market_ticker", "")
            if tk in self.orderbooks:
                for side in ("yes", "no"):
                    for price, qty in data.get(side, []):
                        if qty <= 0:
                            self.orderbooks[tk][side].pop(price, None)
                        else:
                            self.orderbooks[tk][side][price] = qty
                        self._dirty_levels.setdefault(tk, {}).setdefault(side, set()).add(int(price))

        elif mtype in ("ticker", "ticker_v2"):
            tk = data.get("market_ticker", "")
            self.ticker_data[tk] = data
            if tk in self.market_info:
                for f in ("yes_bid", "yes_ask", "last_price", "volume", "open_interest"):
                    if f in data:
                        self.market_info[tk][f] = data[f]

                # Event-driven snapshot on sharp price move (spike detection)
                if self.spike_threshold > 0:
                    self._maybe_snapshot_on_spike(tk, data)

        elif mtype == "error":
            logger.error("WS error: %s", data)
        elif mtype == "subscribed":
            logger.info("Subscribed: sid=%s", data.get("sid"))
        else:
            logger.debug("Unhandled WS message type: %s", mtype)

    def _maybe_snapshot_on_spike(self, tk: str, data: dict):
        """Snapshot immediately when price moves ≥ spike_threshold since last snapshot.

        _prev_prices is only updated when a snapshot is taken (periodic or here),
        so cumulative moves during cooldown are never silently absorbed.
        """
        prev = self._prev_prices.get(tk)
        if prev is None:
            return

        now_mono = time.monotonic()
        if now_mono - self._last_event_snapshot < self.spike_cooldown:
            return

        for key in ("yes_bid", "yes_ask", "last_price"):
            old_val = prev.get(key, 0) or 0
            new_val = data.get(key) if data.get(key) is not None else old_val
            if abs(new_val - old_val) >= self.spike_threshold:
                logger.info(
                    "Spike on %s: %s %d → %d (Δ%d)",
                    tk, key, old_val, new_val, abs(new_val - old_val),
                )
                self._take_snapshot(trigger="spike")
                self._last_event_snapshot = now_mono
                return

    # ------------------------------------------------------------------ #
    # Orderbook delta helpers                                              #
    # ------------------------------------------------------------------ #

    def _mark_all_dirty(self, tk: str):
        """Mark every level of a ticker dirty (used after a WS full snapshot)."""
        ob = self.orderbooks.get(tk, {"yes": {}, "no": {}})
        self._dirty_levels[tk] = {
            side: {int(p) for p in ob[side]} for side in ("yes", "no")
        }

    def _trim_ob(self, levels: list[tuple[int, float]]) -> list[tuple[int, float]]:
        """Sort by best price and apply max_ob_depth."""
        levels.sort(key=lambda x: x[0], reverse=True)
        if self.max_ob_depth:
            levels = levels[: self.max_ob_depth]
        return levels

    # ------------------------------------------------------------------ #
    # Snapshot and flush                                                   #
    # ------------------------------------------------------------------ #

    def _take_snapshot(self, trigger: str = "periodic"):
        """Capture current in-memory state into buffers.

        trigger: "periodic" (timer) or "spike" (event-driven).

        Every `baseline_every` snapshots the full orderbook is written (snapshot_type
        = "baseline"); in between, only levels that changed since the previous snapshot
        are written (snapshot_type = "delta"), with quantity=0 for removed levels.
        """
        ts = datetime.now(timezone.utc)

        self._snapshot_count += 1
        is_baseline = (
            self.baseline_every <= 1
            or self._snapshot_count % self.baseline_every == 1
        )
        snapshot_type = "baseline" if is_baseline else "delta"

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
                "trigger": trigger,
            })

            ob = self.orderbooks.get(tk, {"yes": {}, "no": {}})

            if is_baseline:
                for side in ("yes", "no"):
                    levels = self._trim_ob(
                        [(int(p), float(q)) for p, q in ob[side].items() if q > 0]
                    )
                    for price, qty in levels:
                        self._ob_buf.append({
                            "snapshot_ts": ts,
                            "market_ticker": tk,
                            "side": side,
                            "price_cents": price,
                            "quantity": qty,
                            "snapshot_type": "baseline",
                        })
                # Reset reference for next delta cycle
                self._last_ob[tk] = {
                    side: {int(p): float(q) for p, q in ob[side].items() if q > 0}
                    for side in ("yes", "no")
                }
            else:
                dirty = self._dirty_levels.get(tk, {})
                prev_ob = self._last_ob.get(tk, {"yes": {}, "no": {}})
                for side in ("yes", "no"):
                    changed_prices = dirty.get(side, set())
                    cur = {int(p): float(q) for p, q in ob[side].items() if q > 0}

                    # Also detect levels that existed in prev but are now gone
                    removed = set(prev_ob.get(side, {}).keys()) - set(cur.keys())
                    changed_prices = changed_prices | removed

                    delta_levels: list[tuple[int, float]] = []
                    for price in changed_prices:
                        qty = cur.get(price, 0.0)
                        old_qty = prev_ob.get(side, {}).get(price, 0.0)
                        if qty != old_qty:
                            delta_levels.append((price, qty))

                    delta_levels = self._trim_ob(delta_levels)
                    for price, qty in delta_levels:
                        self._ob_buf.append({
                            "snapshot_ts": ts,
                            "market_ticker": tk,
                            "side": side,
                            "price_cents": price,
                            "quantity": qty,  # 0.0 = level removed
                            "snapshot_type": "delta",
                        })

                # Update reference for next delta
                self._last_ob[tk] = {
                    side: {int(p): float(q) for p, q in ob[side].items() if q > 0}
                    for side in ("yes", "no")
                }

            # Spike detection baseline
            self._prev_prices[tk] = {
                "yes_bid": info.get("yes_bid", 0),
                "yes_ask": info.get("yes_ask", 0),
                "last_price": info.get("last_price", 0),
            }

        # Clear dirty set for next cycle
        self._dirty_levels.clear()

        logger.info(
            "Snapshot [%s/%s] @ %s | mkt_rows=%d ob_rows=%d",
            trigger, snapshot_type, ts.strftime("%H:%M:%S"),
            len(self._market_buf),
            len(self._ob_buf),
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
        """Periodic baseline snapshots + buffer flush."""
        last_flush = time.monotonic()
        while self._running:
            await asyncio.sleep(self.snapshot_interval)
            if not self._running:
                break
            self._take_snapshot(trigger="periodic")
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
