import logging
from typing import Dict
from services.bot.events import EventBus
from services.bot.strategies.base import BaseStrategy
from services.bot.strategies.ladder import LadderStrategy

logger = logging.getLogger("StrategyManager")

class StrategyManager:
    """
    Instantiates and manages strategy lifecycle.
    Passes the shared event_bus to them so they can subscribe/publish.
    """
    def __init__(self, event_bus: EventBus, config: dict):
        self.event_bus = event_bus
        self.config = config
        self.strategies: Dict[str, BaseStrategy] = {}
        
        # Load strategies dynamically (hardcoded to ladder for now, but easily expandable)
        self.load_strategies()
        
    def load_strategies(self):
        # In the future, this could use importlib to dynamically load based on config
        ladder = LadderStrategy(strategy_id="ladder_strategy", event_bus=self.event_bus, config=self.config)
        self.strategies[ladder.strategy_id] = ladder
        logger.info("Loaded strategy: %s", ladder.strategy_id)
