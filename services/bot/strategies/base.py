"""Base strategy interface for the multi-strategy trading bot.

All strategies must subclass BaseStrategy and implement the three event hooks.
Each strategy instance receives its own isolated config (targets + params)
and auto-subscribes to the EventBus on construction.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from services.bot.events import EventBus, WeatherObservationEvent, OrderbookUpdateEvent, MarketDiscoveryEvent


class BaseStrategy(ABC):
    """Abstract base for all trading strategies.

    Args:
        strategy_id: Unique identifier for this strategy instance (from config).
        event_bus: Shared event bus for publishing OrderIntents.
        targets: List of Kalshi series prefixes this instance cares about
                 (e.g. ["KXHIGHCHI"]).
        params: Strategy-specific parameters (e.g. consecutive_obs, max_price_cents).
        full_config: The full application config dict (for registry lookups etc.).
    """

    def __init__(
        self,
        strategy_id: str,
        event_bus: EventBus,
        targets: list[str],
        params: dict,
        full_config: dict,
    ):
        self.strategy_id = strategy_id
        self.event_bus = event_bus
        self.targets = targets
        self.params = params
        self.full_config = full_config

        # Auto-subscribe to all relevant events
        self.event_bus.subscribe(WeatherObservationEvent, self.on_weather_observation)
        self.event_bus.subscribe(OrderbookUpdateEvent, self.on_orderbook_update)
        self.event_bus.subscribe(MarketDiscoveryEvent, self.on_market_discovery)

    @abstractmethod
    async def on_weather_observation(self, event: WeatherObservationEvent):
        """Called on every 1-minute ASOS observation from Synoptic."""

    @abstractmethod
    async def on_orderbook_update(self, event: OrderbookUpdateEvent):
        """Called on every Kalshi orderbook snapshot or delta."""

    @abstractmethod
    async def on_market_discovery(self, event: MarketDiscoveryEvent):
        """Called when markets are (re-)discovered from the Kalshi API."""
