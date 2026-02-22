"""Strategy Manager — config-driven dynamic loading of strategy instances.

Reads ``bot.strategies`` from config.yaml and instantiates the correct
strategy class for each entry. Each strategy receives its own targets
and params, enabling per-city tuning without code changes.

Supports dynamically reloading strategies when config changes.
"""

from __future__ import annotations

import importlib
import logging
from typing import Dict

from services.bot.events import EventBus
from services.bot.strategies.base import BaseStrategy

logger = logging.getLogger("StrategyManager")

# Maps class_name strings from config → (module_path, class_name)
# Add new strategy classes here as you create them.
STRATEGY_CLASS_REGISTRY: dict[str, tuple[str, str]] = {
    "LadderStrategy": ("services.bot.strategies.ladder", "LadderStrategy"),
}


class StrategyManager:
    """Loads and manages strategy instances from config.yaml."""

    def __init__(self, event_bus: EventBus, config: dict):
        self.event_bus = event_bus
        self.config = config
        self.strategies: Dict[str, BaseStrategy] = {}

        self._load_strategies()

    def _load_strategies(self):
        """Parse bot.strategies from config and instantiate each one."""
        bot_cfg = self.config.get("bot", {})
        strategy_defs = bot_cfg.get("strategies", [])

        if not strategy_defs:
            logger.warning("No strategies defined in bot.strategies config — bot will not trade.")
            return

        for sdef in strategy_defs:
            s_id = sdef.get("id")
            class_name = sdef.get("class_name")
            targets = sdef.get("targets", [])
            params = sdef.get("params", {})

            if not s_id or not class_name:
                logger.error("Strategy definition missing 'id' or 'class_name': %s", sdef)
                continue

            if s_id in self.strategies:
                logger.error("Duplicate strategy id '%s' — skipping", s_id)
                continue

            # Resolve the class via the registry
            if class_name not in STRATEGY_CLASS_REGISTRY:
                logger.error(
                    "Unknown strategy class '%s' for id '%s'. "
                    "Available: %s",
                    class_name, s_id, list(STRATEGY_CLASS_REGISTRY.keys()),
                )
                continue

            module_path, cls_name = STRATEGY_CLASS_REGISTRY[class_name]
            try:
                module = importlib.import_module(module_path)
                strategy_cls = getattr(module, cls_name)
            except (ImportError, AttributeError) as e:
                logger.exception("Failed to load strategy class %s: %s", class_name, e)
                continue

            # Instantiate with per-instance config
            try:
                instance = strategy_cls(
                    strategy_id=s_id,
                    event_bus=self.event_bus,
                    targets=targets,
                    params=params,
                    full_config=self.config,
                )
                self.strategies[s_id] = instance
                logger.info(
                    "✅ Loaded strategy: id=%s class=%s targets=%s",
                    s_id, class_name, targets,
                )
            except Exception as e:
                logger.exception("Failed to instantiate strategy '%s': %s", s_id, e)

        logger.info("Strategy Manager: %d strategies loaded", len(self.strategies))
