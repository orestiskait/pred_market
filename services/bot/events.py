import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, Coroutine, Dict, List

@dataclass
class WeatherObservationEvent:
    station: str
    temp: float
    ob_time: datetime

@dataclass
class OrderbookUpdateEvent:
    market_ticker: str
    orderbook: dict

@dataclass
class OrderIntent:
    strategy_id: str
    market_ticker: str
    side: str
    max_price_cents: int
    max_spend_cents: int = 0          # per-(strategy, event) budget in cents; 0 = uncapped
    paper_mode: bool = True
    station: str = ""
    series: str = ""
    event_ticker: str = ""

@dataclass
class MarketDiscoveryEvent:
    market_tickers: list[str]
    market_info: dict[str, dict]

class EventBus:
    def __init__(self):
        self._subscribers: Dict[type, List[Callable[[Any], Coroutine]]] = {}

    def subscribe(self, event_type: type, handler: Callable[[Any], Coroutine]):
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(handler)

    def publish(self, event: Any):
        event_type = type(event)
        if event_type in self._subscribers:
            for handler in self._subscribers[event_type]:
                asyncio.create_task(handler(event))
