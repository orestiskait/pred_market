from abc import ABC, abstractmethod
from services.bot.events import EventBus, WeatherObservationEvent, OrderbookUpdateEvent, MarketDiscoveryEvent

class BaseStrategy(ABC):
    def __init__(self, strategy_id: str, event_bus: EventBus, config: dict):
        self.strategy_id = strategy_id
        self.event_bus = event_bus
        self.config = config

        self.event_bus.subscribe(WeatherObservationEvent, self.on_weather_observation)
        self.event_bus.subscribe(OrderbookUpdateEvent, self.on_orderbook_update)
        self.event_bus.subscribe(MarketDiscoveryEvent, self.on_market_discovery)

    @abstractmethod
    async def on_weather_observation(self, event: WeatherObservationEvent):
        pass

    @abstractmethod
    async def on_orderbook_update(self, event: OrderbookUpdateEvent):
        pass

    @abstractmethod
    async def on_market_discovery(self, event: MarketDiscoveryEvent):
        pass
