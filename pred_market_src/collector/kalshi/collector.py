"""Live WebSocket-based Kalshi market data collector.

Streams real-time Kalshi market data (orderbooks + tickers) and
periodically snapshots state to Parquet.

Usage:
    python -m pred_market_src.collector.kalshi.collector
    python -m pred_market_src.collector.kalshi.collector --config path/to/config.yaml
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

from ..core import (
    AsyncService,
    ParquetStorage,
    load_config,
    make_kalshi_clients,
    standard_argparser,
    configure_logging,
)
from ..markets import resolve_event_tickers, discover_markets
from .ws import KalshiWSMixin

logger = logging.getLogger(__name__)


class LiveCollector(AsyncService, KalshiWSMixin):
    """Streams Kalshi WebSocket data and periodically snapshots state to parquet."""

    def __init__(self, config: dict, config_dir: Path):
        self.config = config

        # Kalshi API (shared factory)
        self.kalshi_auth, self.rest = make_kalshi_clients(config)
        self.kalshi_ws_url = config["kalshi"]["ws_url"]

        # Tell the mixin to subscribe to both channels
        self._kalshi_channels = ["orderbook_delta", "ticker"]

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

    def _discover(self):
        """Resolve events from config and fetch contract metadata."""
        event_tickers = resolve_event_tickers(self.rest, self.config)
        self.market_tickers, self.market_info = discover_markets(self.rest, event_tickers)

        # Seed previous prices for spike detection (from REST initial state)
        for tk, info in self.market_info.items():
            self.orderbooks[tk] = {"yes": {}, "no": {}}
            self._prev_prices[tk] = {
                "yes_bid": info.get("yes_bid", 0),
                "yes_ask": info.get("yes_ask", 0),
                "last_price": info.get("last_price", 0),
            }

    # ------------------------------------------------------------------ #
    # Kalshi message hook (extends base mixin)                             #
    # ------------------------------------------------------------------ #

    def on_kalshi_message(self, mtype: str, data: dict):
        """Handle ticker updates and spike detection on top of base OB tracking."""
        if mtype == "orderbook_snapshot":
            # Mark all levels dirty for delta compression
            tk = data.get("market_ticker", "")
            self._mark_all_dirty(tk)

        elif mtype == "orderbook_delta":
            # Track dirty levels for delta compression
            tk = data.get("market_ticker", "")
            for side in ("yes", "no"):
                for price, _qty in data.get(side, []):
                    self._dirty_levels.setdefault(tk, {}).setdefault(side, set()).add(int(price))

        elif mtype in ("ticker", "ticker_v2"):
            tk = data.get("market_ticker", "")
            self.ticker_data[tk] = data
            if tk in self.market_info:
                for f in ("yes_bid", "yes_ask", "last_price", "volume", "open_interest"):
                    if f in data:
                        self.market_info[tk][f] = data[f]

                if self.spike_threshold > 0:
                    self._maybe_snapshot_on_spike(tk, data)

        elif mtype == "error":
            logger.error("WS error: %s", data)
        elif mtype == "subscribed":
            logger.info("Subscribed: sid=%s", data.get("sid"))

    # ------------------------------------------------------------------ #
    # Spike detection                                                      #
    # ------------------------------------------------------------------ #

    def _maybe_snapshot_on_spike(self, tk: str, data: dict):
        """Snapshot immediately when price moves ≥ spike_threshold since last snapshot."""
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

    # ------------------------------------------------------------------ #
    # AsyncService overrides                                               #
    # ------------------------------------------------------------------ #

    def _get_tasks(self) -> list:
        return [self.kalshi_ws_loop(), self._snapshot_loop()]

    def _on_shutdown(self):
        self._flush()
        logger.info("Buffers flushed.")

    async def run(self):
        self._running = True
        self._discover()
        if not self.market_tickers:
            logger.error("No markets found. Check config 'events' / 'event_series'.")
            return
        await super().run()


# ------------------------------------------------------------------ #
# CLI                                                                  #
# ------------------------------------------------------------------ #

def main():
    parser = standard_argparser("Kalshi live market data collector")
    args = parser.parse_args()

    configure_logging(args.log_level)

    config, config_path = load_config(args.config)
    collector = LiveCollector(config, config_dir=config_path.parent)
    asyncio.run(collector.run())


if __name__ == "__main__":
    main()
