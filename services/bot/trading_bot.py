"""Generic Kalshi Trading Bot — exchange-agnostic strategy host.

Provides the core bot lifecycle:
  - Kalshi WebSocket (orderbook feed)
  - EventBus (in-memory pub/sub for strategies)
  - StrategyManager (config-driven strategy loading)
  - ExecutionManager (risk guardrails + paper/live sweep)
  - Periodic market re-discovery

Domain-specific data feeds (weather, NWP, etc.) are added by subclasses.
For a pure market-data bot (e.g. probability velocity), this class is
sufficient on its own.

Usage:
    # As standalone (market-data-only bot):
    python -m services.bot.trading_bot
    python -m services.bot.trading_bot --config config.yaml
    python -m services.bot.trading_bot --series KXHIGHCHI

    # Or subclass for domain-specific feeds — see weather_bot.py.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

from services.core.config import (
    load_config,
    get_event_series,
    make_kalshi_clients,
    configure_logging,
    standard_argparser,
)
from services.core.service import AsyncService
from services.kalshi.ws import KalshiWSMixin
from services.markets.kalshi_registry import KALSHI_MARKET_REGISTRY
from services.markets.ticker import discover_markets, resolve_event_tickers

from services.bot.events import EventBus, OrderbookUpdateEvent, MarketDiscoveryEvent
from services.bot.managers.execution import ExecutionManager
from services.bot.managers.strategy_manager import StrategyManager


logger = logging.getLogger("TradingBot")


def _collect_strategy_targets(config: dict) -> list[str]:
    """Extract the union of all series targets from bot.strategies config."""
    targets: set[str] = set()
    for sdef in config.get("bot", {}).get("strategies", []):
        for t in sdef.get("targets", []):
            targets.add(t)
    return sorted(targets)


class TradingBot(AsyncService, KalshiWSMixin):
    """Generic Kalshi trading bot host.

    Responsible for:
      - Kalshi WebSocket lifecycle (orderbook feed)
      - Publishing OrderbookUpdateEvent and MarketDiscoveryEvent to EventBus
      - Periodic market re-discovery (event rollover)

    Subclasses (e.g. WeatherBot) add domain-specific data feeds by
    overriding ``_setup_feeds()``, ``_get_feed_tasks()``, and
    ``_on_feed_shutdown()``.
    """

    def __init__(self, config: dict, config_path: Path, series_filter: list[str] | None = None):
        self.config = config
        self._config_path = config_path

        # Event Bus & Managers
        self.event_bus = EventBus()
        self.execution_manager = ExecutionManager(self.event_bus, config, config_path)
        self.strategy_manager = StrategyManager(self.event_bus, config)

        # Target series: union of configured event_series and all strategy targets
        es_series = set(get_event_series(config, self._event_series_consumer()))
        strat_series = set(_collect_strategy_targets(config))
        all_series = sorted(es_series | strat_series)

        if series_filter:
            self._target_series = [s for s in series_filter if s in all_series or s in KALSHI_MARKET_REGISTRY]
        else:
            self._target_series = all_series

        if not self._target_series:
            raise ValueError("No event_series configured or matched by --series filter")

        # Kalshi
        self.kalshi_auth, self.kalshi_rest = make_kalshi_clients(config)
        self.kalshi_ws_url = config["kalshi"]["ws_url"]
        self._kalshi_channels = ["orderbook_delta"]

        # Event rollover
        rollover = config.get("event_rollover", {})
        self.rediscover_interval = rollover.get("rediscover_interval_seconds", 300)

        # State required by KalshiWSMixin
        self._running = False
        self.market_tickers: list[str] = []
        self.orderbooks: dict[str, dict] = {}
        self._kalshi_subscribe_tickers: list[str] = []

        # Allow subclasses to set up domain-specific feeds
        self._setup_feeds()
        self._log_startup_banner()

    # ------------------------------------------------------------------
    # Extension points for subclasses
    # ------------------------------------------------------------------

    def _event_series_consumer(self) -> str:
        """Config key under event_series to use. Override in subclass."""
        return "trading_bot"

    def _setup_feeds(self) -> None:
        """Override to initialize domain-specific data feeds (weather, NWP, etc.)."""

    def _get_feed_tasks(self) -> list:
        """Override to return additional async tasks for domain-specific feeds."""
        return []

    def _on_feed_shutdown(self) -> None:
        """Override to clean up domain-specific feeds on shutdown."""

    # ------------------------------------------------------------------
    # Startup banner
    # ------------------------------------------------------------------

    def _log_startup_banner(self):
        logger.info(
            "%s: series=%s",
            self.__class__.__name__, self._target_series,
        )
        for sid, strat in self.strategy_manager.strategies.items():
            mode = "PAPER" if strat.params.get("paper_mode", True) else "LIVE"
            logger.info("  %s [%s]: %s", sid, mode, strat.targets)

    # -------------------------------------------------------------------------
    # Market discovery
    # -------------------------------------------------------------------------

    def _discover(self):
        """Resolve events and publish MarketDiscoveryEvent to the bus."""
        event_tickers = resolve_event_tickers(
            self.kalshi_rest, self.config, consumer=self._event_series_consumer(),
        )
        if not event_tickers:
            return
        tickers, info = discover_markets(self.kalshi_rest, event_tickers)

        self.market_tickers = tickers

        for tk in tickers:
            if tk not in self.orderbooks:
                self.orderbooks[tk] = {"yes": {}, "no": {}}

        self.event_bus.publish(MarketDiscoveryEvent(
            market_tickers=tickers,
            market_info=info,
        ))

        self._kalshi_subscribe_tickers = self.market_tickers

    # -------------------------------------------------------------------------
    # KalshiWSMixin hook
    # -------------------------------------------------------------------------

    def on_kalshi_message(self, mtype: str, data: dict) -> None:
        """Forward orderbook state to the EventBus after the mixin applies it."""
        if mtype in ("orderbook_snapshot", "orderbook_delta"):
            tk = data.get("market_ticker")
            if tk and tk in self.orderbooks:
                ob = self.orderbooks[tk]
                self.event_bus.publish(OrderbookUpdateEvent(
                    market_ticker=tk,
                    orderbook={
                        "yes": dict(ob["yes"]),
                        "no": dict(ob["no"]),
                    },
                ))

    # -------------------------------------------------------------------------
    # Async loops
    # -------------------------------------------------------------------------

    async def _rediscover_loop(self):
        """Periodic re-discovery of event tickers (handles market rollover)."""
        if self.rediscover_interval <= 0:
            return
        while self._running:
            await asyncio.sleep(self.rediscover_interval)
            if not self._running:
                break
            try:
                event_tickers = resolve_event_tickers(
                    self.kalshi_rest, self.config,
                    consumer=self._event_series_consumer(),
                )
                if not event_tickers:
                    continue
                tickers, info = discover_markets(self.kalshi_rest, event_tickers)
                if set(tickers) != set(self.market_tickers):
                    logger.info(
                        "Event rollover: %s → %s",
                        sorted(self.market_tickers)[:3],
                        sorted(tickers)[:3],
                    )
                    self._discover()
                    self.request_kalshi_reconnect()
            except Exception as e:
                logger.exception("Rediscover failed: %s", e)

    # -------------------------------------------------------------------------
    # AsyncService overrides
    # -------------------------------------------------------------------------

    def _get_tasks(self) -> list:
        tasks = [
            self.kalshi_ws_loop(),
        ]
        if self.rediscover_interval > 0:
            tasks.append(self._rediscover_loop())
        # Add domain-specific feed tasks from subclass
        tasks.extend(self._get_feed_tasks())
        return tasks

    def _on_shutdown(self) -> None:
        self._on_feed_shutdown()

    async def run(self):
        self._running = True
        self._discover()
        logger.info("%s ready.", self.__class__.__name__)
        await super().run()

    def shutdown(self):
        super().shutdown()


# ------------------------------------------------------------------ #
# CLI — standalone generic bot                                         #
# ------------------------------------------------------------------ #

def main():
    parser = standard_argparser("Kalshi Trading Bot (generic — market data only)")
    parser.add_argument(
        "--series", nargs="+", default=None,
        help="Limit to specific event series (e.g. KXHIGHCHI KXHIGHNY). "
             "Default: all series from config.yaml strategies + event_series.",
    )
    args = parser.parse_args()

    configure_logging(args.log_level)

    config, config_path = load_config(args.config)
    bot = TradingBot(config, config_path, series_filter=args.series)
    asyncio.run(bot.run())


if __name__ == "__main__":
    main()
