"""
Kalshi Information Arbitrage Weather Bot (Multi-Strategy Architecture)

This is the main entry point / Feed Manager.
It handles WebSocket connections, parses incoming data, and routes them as events
to the EventBus. Execution and Strategy logic are fully decoupled.

Usage:
    python -m services.bot.weather_bot
    python -m services.bot.weather_bot --config config.yaml
    python -m services.bot.weather_bot --series KXHIGHCHI
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
    get_synoptic_token,
    build_synoptic_ws_url,
    configure_logging,
    standard_argparser,
)
from services.core.service import AsyncService
from services.kalshi.ws import KalshiWSMixin
from services.synoptic.ws import SynopticWSMixin
from services.markets.kalshi_registry import KalshiMarketConfig, KALSHI_MARKET_REGISTRY
from services.synoptic.station_registry import synoptic_stations_for_series
from services.markets.ticker import discover_markets, resolve_event_tickers

# Event Driven Architecture
from services.bot.events import EventBus, WeatherObservationEvent, OrderbookUpdateEvent, MarketDiscoveryEvent
from services.bot.managers.execution import ExecutionManager
from services.bot.managers.strategy_manager import StrategyManager


logger = logging.getLogger("WeatherBot")

class WeatherBot(AsyncService, KalshiWSMixin, SynopticWSMixin):
    """
    Feed Manager / Bot Host for paper-trading weather bot.
    Responsible for websocket lifecycles and event dispatch.
    """

    def __init__(self, config: dict, config_path: Path, series_filter: list[str] | None = None):
        self.config = config
        self._config_path = config_path

        # Event Bus & Sub-managers
        self.event_bus = EventBus()
        self.execution_manager = ExecutionManager(self.event_bus, config, config_path)
        self.strategy_manager = StrategyManager(self.event_bus, config)

        # Determine which series to target
        all_series = get_event_series(config, "weather_bot")
        if series_filter:
            self._target_series = [s for s in series_filter if s in all_series or s in KALSHI_MARKET_REGISTRY]
        else:
            self._target_series = all_series

        if not self._target_series:
            raise ValueError("No event_series configured or matched by --series filter")

        # Build market configs for targeted series
        self._market_configs: dict[str, KalshiMarketConfig] = {}
        for s in self._target_series:
            if s in KALSHI_MARKET_REGISTRY:
                self._market_configs[s] = KALSHI_MARKET_REGISTRY[s]
            else:
                logger.warning("Series %s not in KALSHI_MARKET_REGISTRY, skipping", s)

        # Kalshi
        self.kalshi_auth, self.kalshi_rest = make_kalshi_clients(config)
        self.kalshi_ws_url = config["kalshi"]["ws_url"]

        # Synoptic — subscribe only to stations we care about
        self._synoptic_token = get_synoptic_token(config)
        synoptic_stations = synoptic_stations_for_series(self._target_series)
        self.synoptic_ws_url = build_synoptic_ws_url(
            self._synoptic_token, synoptic_stations, ["air_temp"],
        )

        # Event rollover
        rollover = config.get("event_rollover", {})
        self.rediscover_interval = rollover.get("rediscover_interval_seconds", 300)

        # State required by mixins
        self._running = False
        self.market_tickers: list[str] = []
        self.orderbooks: dict[str, dict] = {}
        self._kalshi_subscribe_tickers: list[str] = []

    # -------------------------------------------------------------------------
    # Market discovery (config-driven, multi-market)
    # -------------------------------------------------------------------------

    def _discover(self):
        """Resolve events and publish to EventBus."""
        event_tickers = resolve_event_tickers(self.kalshi_rest, self.config, consumer="weather_bot")
        if not event_tickers:
            return
        tickers, info = discover_markets(self.kalshi_rest, event_tickers)
        
        self.market_tickers = tickers
        
        for tk in tickers:
            if tk not in self.orderbooks:
                self.orderbooks[tk] = {"yes": {}, "no": {}}
                
        # Send out event so strategies can build their ladders etc.
        self.event_bus.publish(MarketDiscoveryEvent(
            market_tickers=tickers,
            market_info=info
        ))

        # Subscribe to all discovered tickers
        self._kalshi_subscribe_tickers = self.market_tickers

    # -------------------------------------------------------------------------
    # SynopticWSMixin hook — react to weather observations
    # -------------------------------------------------------------------------

    def on_synoptic_observation(self, row: dict):
        """Each 1-minute ASOS observation is parsed and routed via EventBus."""
        station = row["stid"]
        temp = row["value"]
        ob_time_str = row["ob_timestamp"]

        try:
            ob_time = datetime.fromisoformat(ob_time_str.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            logger.warning("Could not parse timestamp: %s", ob_time_str)
            return

        logger.info("🌡️ [%s] %.1f°F at %s", station, temp, ob_time_str)
        
        self.event_bus.publish(WeatherObservationEvent(
            station=station,
            temp=temp,
            ob_time=ob_time
        ))

    # -------------------------------------------------------------------------
    # KalshiWSMixin hook - react to live orderbooks
    # -------------------------------------------------------------------------
    
    def on_kalshi_message(self, mtype: str, data: dict) -> None:
        """Forward orderbook updates to the EventBus."""
        if mtype in ("orderbook_snapshot", "orderbook_delta"):
            tk = data.get("market_ticker")
            if tk and tk in self.orderbooks:
                # We publish the latest full orderbook state (which the mixin applied in-place to self.orderbooks[tk])
                self.event_bus.publish(OrderbookUpdateEvent(
                    market_ticker=tk,
                    orderbook=self.orderbooks[tk]
                ))

    # -------------------------------------------------------------------------
    # Async Loops
    # -------------------------------------------------------------------------

    async def _rediscover_loop(self):
        """Periodic re-discovery of event tickers."""
        if self.rediscover_interval <= 0:
            return
        while self._running:
            await asyncio.sleep(self.rediscover_interval)
            if not self._running:
                break
            try:
                event_tickers = resolve_event_tickers(self.kalshi_rest, self.config, consumer="weather_bot")
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
        tasks = [self.kalshi_ws_loop(), self.synoptic_ws_loop()]
        if self.rediscover_interval > 0:
            tasks.append(self._rediscover_loop())
        return tasks

    async def run(self):
        self._running = True
        self._discover()
        logger.info("Bot fully initialized and entering event loop.")
        await super().run()

    def shutdown(self):
        super().shutdown()


# ------------------------------------------------------------------ #
# CLI                                                                  #
# ------------------------------------------------------------------ #

def main():
    parser = standard_argparser("Kalshi Weather Arbitrage Bot (Multi-Strategy)")
    parser.add_argument(
        "--series", nargs="+", default=None,
        help="Limit to specific event series (e.g. KXHIGHCHI KXHIGHNY). "
             "Default: all series in config.yaml.",
    )
    args = parser.parse_args()

    configure_logging(args.log_level)

    config, config_path = load_config(args.config)
    bot = WeatherBot(config, config_path, series_filter=args.series)
    asyncio.run(bot.run())


if __name__ == "__main__":
    main()
