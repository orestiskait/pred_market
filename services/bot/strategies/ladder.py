import logging
from collections import deque
from services.bot.events import EventBus, OrderIntent
from services.bot.events import WeatherObservationEvent, OrderbookUpdateEvent, MarketDiscoveryEvent
from services.bot.strategies.base import BaseStrategy
from services.markets.kalshi_registry import KALSHI_MARKET_REGISTRY
from services.markets.ticker import nws_observation_period

logger = logging.getLogger("LadderStrategy")

class LadderStrategy(BaseStrategy):
    """
    Ladder Strategy implementation.
    Watches consecutive observations against an event's cap_strike.
    """
    def __init__(self, strategy_id: str, event_bus: EventBus, config: dict):
        super().__init__(strategy_id, event_bus, config)
        
        bot_cfg = config.get("bot", {})
        self.consecutive_obs_required = bot_cfg.get("consecutive_obs", 2)
        self.max_price_cents = bot_cfg.get("max_price_cents", 95)
        
        # State
        self.ladder: dict[str, dict] = {}
        self.weather_history: dict[str, deque] = {}
        
        # Build market configs for targeted series
        self._target_series = config.get("event_series", {}).get("weather_bot", [])
        self._market_configs = {
            s: KALSHI_MARKET_REGISTRY[s] 
            for s in self._target_series if s in KALSHI_MARKET_REGISTRY
        }
        for mc in self._market_configs.values():
            if mc.synoptic_station and mc.synoptic_station not in self.weather_history:
                self.weather_history[mc.synoptic_station] = deque(maxlen=10)

    async def on_market_discovery(self, event: MarketDiscoveryEvent):
        self.ladder.clear()
        
        for tk in event.market_tickers:
            info = event.market_info.get(tk, {})
            cap_strike = info.get("cap_strike")
            if cap_strike is None:
                continue
                
            event_ticker = info.get("event_ticker", "")
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
                "  [Ladder] Tracking: %s '%s' triggers at >= %.1f°F",
                tk, info.get("subtitle"), trigger_temp,
            )

    async def on_orderbook_update(self, event: OrderbookUpdateEvent):
        # Ladder doesn't strictly need orderbooks for its evaluation loop, 
        # it just fires OrderIntents when conditions are met. 
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
                    "🚨 [Ladder] TRIGGERED! [%s] Last %d valid obs: %s >= %.1f°F!",
                    station, self.consecutive_obs_required, recent_valid, threshold,
                )
                logger.warning("   [Ladder] Targeting contract: %s ('%s')", tk, info["subtitle"])
                self.ladder[tk]["executed"] = True
                
                # Emit order intent instead of executing directly
                intent = OrderIntent(
                    strategy_id=self.strategy_id,
                    market_ticker=tk,
                    side="no",
                    max_price_cents=self.max_price_cents,
                    station=station,
                    series=info["series"]
                )
                self.event_bus.publish(intent)
