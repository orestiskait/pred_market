"""Ladder Strategy — config-driven, per-city tunable.

When a monitored station reports N consecutive 1-minute ASOS observations
at or above a contract's cap_strike, emits an OrderIntent to buy the NO side.

This strategy is instantiated once per config entry in ``bot.strategies``.
Each instance only tracks the series listed in its ``targets`` list, and
uses the ``params`` dict for tuning (consecutive_obs, max_price_cents).
"""

from __future__ import annotations

import logging
from collections import deque

from services.bot.events import EventBus, OrderIntent
from services.bot.events import WeatherObservationEvent, OrderbookUpdateEvent, MarketDiscoveryEvent
from services.bot.strategies.base import BaseStrategy
from services.markets.kalshi_registry import KALSHI_MARKET_REGISTRY
from services.markets.ticker import nws_observation_period

logger = logging.getLogger("LadderStrategy")


class LadderStrategy(BaseStrategy):
    """Ladder strategy: buy NO when consecutive obs confirm the high is reached."""

    def __init__(
        self,
        strategy_id: str,
        event_bus: EventBus,
        targets: list[str],
        params: dict,
        full_config: dict,
    ):
        super().__init__(strategy_id, event_bus, targets, params, full_config)

        # Per-instance params (from config.yaml → bot.strategies[].params)
        self.consecutive_obs_required = params.get("consecutive_obs", 2)
        self.max_price_cents = params.get("max_price_cents", 95)

        # Build market configs for THIS instance's targets only
        self._market_configs = {
            s: KALSHI_MARKET_REGISTRY[s]
            for s in self.targets
            if s in KALSHI_MARKET_REGISTRY
        }

        # Isolated state — each strategy instance owns its own history + ladder
        self.weather_history: dict[str, deque] = {}
        for mc in self._market_configs.values():
            if mc.synoptic_station and mc.synoptic_station not in self.weather_history:
                self.weather_history[mc.synoptic_station] = deque(maxlen=10)

        self.ladder: dict[str, dict] = {}

        logger.info(
            "[%s] Initialized — targets=%s, consecutive_obs=%d, max_price=%d¢",
            self.strategy_id, self.targets, self.consecutive_obs_required, self.max_price_cents,
        )

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    async def on_market_discovery(self, event: MarketDiscoveryEvent):
        """Rebuild the ladder for the new event day.

        Blocker 2 fix: clear weather_history on every rediscovery so stale
        observations from yesterday's NWS window cannot bleed into today's
        trigger evaluation.  The deques are re-initialised for all stations
        this instance targets so nothing is lost for currently-active cities.
        """
        self.ladder.clear()

        # Clear stale history — yesterday's temps are irrelevant for today's
        # NWS window.  Re-initialise so new observations can flow straight in.
        self.weather_history.clear()
        for mc in self._market_configs.values():
            if mc.synoptic_station:
                self.weather_history[mc.synoptic_station] = deque(maxlen=10)
        logger.info(
            "[%s] Weather history cleared for new event day — stations: %s",
            self.strategy_id, list(self.weather_history.keys()),
        )

        for tk in event.market_tickers:
            info = event.market_info.get(tk, {})
            cap_strike = info.get("cap_strike")
            if cap_strike is None:
                continue

            event_ticker = info.get("event_ticker", "")
            # Only process markets that belong to this instance's targets
            series = next(
                (s for s in self._market_configs if event_ticker.startswith(s)),
                None,
            )
            if series is None:
                continue

            mc = self._market_configs[series]
            trigger_temp = float(cap_strike)
            nws_start_utc, nws_end_utc = nws_observation_period(event_ticker, mc.tz)

            self.ladder[tk] = {
                "trigger_temp": trigger_temp,
                "subtitle": info.get("subtitle"),
                "executed": False,
                "series": series,
                "station": mc.synoptic_station,
                "nws_start": nws_start_utc,
                "nws_end": nws_end_utc,
            }
            logger.info(
                "  [%s] Tracking: %s '%s' triggers at >= %.1f°F",
                self.strategy_id, tk, info.get("subtitle"), trigger_temp,
            )

        if self.ladder:
            logger.info("[%s] Ladder built with %d contracts", self.strategy_id, len(self.ladder))

    async def on_orderbook_update(self, event: OrderbookUpdateEvent):
        # Ladder evaluation is purely weather-driven; no OB processing needed.
        pass

    async def on_weather_observation(self, event: WeatherObservationEvent):
        station = event.station

        if station not in self.weather_history:
            return

        self.weather_history[station].append((event.ob_time, event.temp))
        history = self.weather_history[station]

        for tk, info in self.ladder.items():
            if info["executed"]:
                continue
            if info["station"] != station:
                continue

            threshold = info["trigger_temp"]
            nws_start = info["nws_start"]
            nws_end = info["nws_end"]

            valid_obs = [t for (dt, t) in history if nws_start <= dt <= nws_end]
            if len(valid_obs) < self.consecutive_obs_required:
                continue

            recent_valid = valid_obs[-self.consecutive_obs_required:]

            if all(t >= threshold for t in recent_valid):
                logger.warning(
                    "🚨 [%s] TRIGGERED! [%s] Last %d valid obs: %s >= %.1f°F!",
                    self.strategy_id, station, self.consecutive_obs_required,
                    recent_valid, threshold,
                )
                logger.warning(
                    "   [%s] Targeting contract: %s ('%s')",
                    self.strategy_id, tk, info["subtitle"],
                )
                self.ladder[tk]["executed"] = True

                self.event_bus.publish(OrderIntent(
                    strategy_id=self.strategy_id,
                    market_ticker=tk,
                    side="no",
                    max_price_cents=self.max_price_cents,
                    station=station,
                    series=info["series"],
                ))
