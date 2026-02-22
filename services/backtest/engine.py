"""Backtesting engine — replays SimEvents through production strategy code.

Reuses the EXACT same components as the live bot:
  - EventBus          (services.bot.events)
  - StrategyManager   (services.bot.managers.strategy_manager)
  - ExecutionManager   (services.bot.managers.execution)   — paper sweep logic
  - LadderStrategy     (services.bot.strategies.ladder)    — any strategy

The engine does NOT subclass or monkey-patch any of these.  Instead it:
  1. Instantiates them with the same config the live bot uses.
  2. Replays SimEvents by publishing typed events to the EventBus.
  3. Captures trade fills from the ExecutionManager for analysis.

LATENCY & DATA LEAKAGE
========================
The timeline produced by DataLoader already orders events by wall-clock
(received_ts for weather, snapshot_ts for orderbooks).  The engine simply
iterates in order — no future data can leak.

The Synoptic ``ob_timestamp`` is embedded inside the WeatherObservationEvent
payload, so the strategy's NWS-window filtering still works correctly (it
checks whether ``ob_time`` falls within the NWS observation window), but
the EVENT ARRIVAL ORDER is governed by received_ts.

DESIGN DECISION — Synchronous Replay
======================================
The live bot is async (WebSocket event loop + asyncio.create_task).
The backtester replaces ``asyncio.create_task`` with immediate synchronous
execution.  This is both deterministic and fast — no race conditions,
no event reordering.  We achieve this by using a ``SyncEventBus`` that
calls handlers directly instead of scheduling tasks.
"""

from __future__ import annotations

import copy
import csv
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

from services.bot.events import (
    EventBus,
    MarketDiscoveryEvent,
    OrderbookUpdateEvent,
    OrderIntent,
    WeatherObservationEvent,
)
from services.bot.strategies.base import BaseStrategy
from services.backtest.data_loader import DataLoader, SimEvent, SimEventType

logger = logging.getLogger("backtest.engine")


# ======================================================================
# SyncEventBus — deterministic, immediate handler dispatch
# ======================================================================

class SyncEventBus:
    """Drop-in replacement for EventBus that calls handlers synchronously.

    In the live bot, ``EventBus.publish()`` calls ``asyncio.create_task(handler(event))``.
    In backtesting, we call each handler directly (await-free) for determinism.
    Since all strategy/execution handlers are ``async def``, we call them via
    a trivial coroutine runner.
    """

    def __init__(self):
        self._subscribers: dict[type, list[Callable]] = {}

    def subscribe(self, event_type: type, handler: Callable):
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(handler)

    def publish(self, event: Any):
        event_type = type(event)
        if event_type in self._subscribers:
            for handler in self._subscribers[event_type]:
                # Run the async handler synchronously for determinism
                import asyncio
                coro = handler(event)
                if coro is not None:
                    # Use existing loop if available, else run directly
                    try:
                        loop = asyncio.get_running_loop()
                        # We're already in an async context — shouldn't happen
                        # in backtest, but handle gracefully
                        loop.run_until_complete(coro)
                    except RuntimeError:
                        asyncio.run(coro)


# ======================================================================
# BacktestExecutionManager — captures fills instead of logging to disk
# ======================================================================

@dataclass
class Fill:
    """One executed paper trade captured during backtesting."""
    wall_clock: datetime           # when this trade would have executed
    strategy_id: str
    event_ticker: str
    series: str
    station: str
    market_ticker: str
    side: str
    contracts_filled: int
    avg_fill_price_cents: float
    total_cost_cents: int
    strategy_event_spent_cents: int


class BacktestExecutionManager:
    """Execution manager for backtesting — same sweep logic, captures fills.

    This is a slimmed-down version of services.bot.managers.execution.ExecutionManager.
    The sweep algorithm is IDENTICAL: iterate over opposing side levels sorted
    by price, buy up to max_price_cents and budget.

    Instead of writing CSVs / parquet during the run, it stores fills in memory.
    """

    def __init__(self, event_bus: SyncEventBus, config: dict):
        self.event_bus = event_bus
        self.config = config

        self.orderbooks: dict[str, dict] = {}
        self.market_info: dict[str, dict] = {}
        self._spent: dict[tuple[str, str], int] = defaultdict(int)
        self.fills: list[Fill] = []

        # Track wall clock from the engine so fills get accurate timestamps
        self._current_wall_clock: datetime | None = None

        self.event_bus.subscribe(OrderIntent, self.on_order_intent)
        self.event_bus.subscribe(OrderbookUpdateEvent, self.on_orderbook_update)
        self.event_bus.subscribe(MarketDiscoveryEvent, self.on_market_discovery)

        logger.info("BacktestExecutionManager initialized")

    async def on_market_discovery(self, event: MarketDiscoveryEvent):
        self.market_info = event.market_info
        for tk in event.market_tickers:
            if tk not in self.orderbooks:
                self.orderbooks[tk] = {"yes": {}, "no": {}}

    async def on_orderbook_update(self, event: OrderbookUpdateEvent):
        self.orderbooks[event.market_ticker] = event.orderbook

    async def on_order_intent(self, intent: OrderIntent):
        """Identical sweep logic to ExecutionManager.on_order_intent."""

        budget = self._remaining(intent)
        if budget == 0:
            logger.debug(
                "[%s] Budget exhausted for %s — skipping %s",
                intent.strategy_id, intent.event_ticker, intent.market_ticker,
            )
            return

        ob = self.orderbooks.get(intent.market_ticker)
        if not ob:
            logger.debug(
                "[%s] No orderbook for %s — skipping",
                intent.strategy_id, intent.market_ticker,
            )
            return

        # Build available levels — EXACT same logic as ExecutionManager
        available_levels = []
        if intent.side.lower() == "no":
            for price, qty in ob["yes"].items():
                if qty > 0:
                    available_levels.append((100 - price, qty))
        else:
            for price, qty in ob["no"].items():
                if qty > 0:
                    available_levels.append((100 - price, qty))

        available_levels.sort(key=lambda x: x[0])

        total_contracts_bought = 0
        total_cost = 0

        for price, qty in available_levels:
            if price > intent.max_price_cents:
                break

            if budget >= 0:
                budget_left = budget - total_cost
                if budget_left <= 0:
                    break
                max_by_budget = budget_left // price
            else:
                max_by_budget = qty

            affordable_qty = min(qty, max_by_budget)
            if affordable_qty > 0:
                total_contracts_bought += affordable_qty
                cost = int(affordable_qty * price)
                total_cost += cost

        if total_contracts_bought > 0:
            avg_price = total_cost / total_contracts_bought
            key = (intent.strategy_id, intent.event_ticker)
            self._spent[key] += total_cost

            fill = Fill(
                wall_clock=self._current_wall_clock or datetime.now(timezone.utc),
                strategy_id=intent.strategy_id,
                event_ticker=intent.event_ticker,
                series=intent.series,
                station=intent.station,
                market_ticker=intent.market_ticker,
                side=intent.side,
                contracts_filled=int(total_contracts_bought),
                avg_fill_price_cents=round(avg_price, 2),
                total_cost_cents=total_cost,
                strategy_event_spent_cents=self._spent[key],
            )
            self.fills.append(fill)

            logger.info(
                "✅ [BT] [%s] Filled %d %s (%s) @ avg %.2f¢ — cost $%.2f",
                intent.strategy_id, total_contracts_bought,
                intent.market_ticker, intent.side.upper(), avg_price,
                total_cost / 100,
            )
        else:
            logger.debug(
                "❌ [BT] [%s] No fills for %s under %d¢",
                intent.strategy_id, intent.market_ticker, intent.max_price_cents,
            )

    def _remaining(self, intent: OrderIntent) -> int:
        if intent.max_spend_cents <= 0:
            return -1
        key = (intent.strategy_id, intent.event_ticker)
        return max(0, intent.max_spend_cents - self._spent.get(key, 0))


# ======================================================================
# BacktestEngine — the main replay loop
# ======================================================================

class BacktestEngine:
    """Replay historical SimEvents through production strategy code.

    Parameters
    ----------
    config : dict
        Full config.yaml dict (same format as live bot).
    data_dir : str
        Path to the data/ directory.
    start_date, end_date : date
        Date range to backtest.
    series_filter : list[str] | None
        Optionally limit to specific series.
    latency_model : str
        Passed through to DataLoader: ``"actual"`` (default) or ``"fixed_N"``.
    """

    def __init__(
        self,
        config: dict,
        data_dir: str,
        start_date: date,
        end_date: date,
        series_filter: list[str] | None = None,
        latency_model: str = "actual",
    ):
        self.config = config
        self.data_dir = data_dir
        self.start_date = start_date
        self.end_date = end_date
        self.series_filter = series_filter
        self.latency_model = latency_model

        # Wire up components with SYNC event bus
        self.event_bus = SyncEventBus()

        # Execution Manager (backtest variant — captures fills in-memory)
        self.execution_manager = BacktestExecutionManager(self.event_bus, config)

        # Strategy Manager — loads the exact same strategies as live bot
        # We need to temporarily patch the strategy base class to use our SyncEventBus
        self._strategies = self._load_strategies()

        # Data loader
        self.loader = DataLoader(
            data_dir=data_dir,
            start_date=start_date,
            end_date=end_date,
            series_filter=series_filter,
            latency_model=latency_model,
        )

    def _load_strategies(self) -> dict[str, BaseStrategy]:
        """Instantiate strategies from config using our SyncEventBus.

        Mirrors StrategyManager._load_strategies() but uses our bus.
        """
        import importlib
        from services.bot.managers.strategy_manager import STRATEGY_CLASS_REGISTRY

        strategies: dict[str, BaseStrategy] = {}
        bot_cfg = self.config.get("bot", {})
        strategy_defs = bot_cfg.get("strategies", [])

        for sdef in strategy_defs:
            s_id = sdef.get("id")
            class_name = sdef.get("class_name")
            targets = sdef.get("targets", [])
            params = sdef.get("params", {})

            if not s_id or not class_name:
                continue
            if class_name not in STRATEGY_CLASS_REGISTRY:
                logger.error("Unknown strategy class: %s", class_name)
                continue

            # Filter by series if needed
            if self.series_filter:
                targets = [t for t in targets if t in self.series_filter]
                if not targets:
                    continue

            module_path, cls_name = STRATEGY_CLASS_REGISTRY[class_name]
            module = importlib.import_module(module_path)
            strategy_cls = getattr(module, cls_name)

            instance = strategy_cls(
                strategy_id=s_id,
                event_bus=self.event_bus,
                targets=targets,
                params=params,
                full_config=self.config,
            )
            strategies[s_id] = instance
            logger.info("Loaded backtest strategy: %s (%s) targets=%s", s_id, class_name, targets)

        return strategies

    def run(self) -> BacktestResult:
        """Run the full backtesting simulation.

        Returns a BacktestResult containing all fills and summary statistics.
        """
        timeline = self.loader.load_timeline()
        if not timeline:
            logger.warning("Empty timeline — nothing to backtest.")
            return BacktestResult(fills=[], timeline_length=0)

        logger.info("=" * 60)
        logger.info("BACKTEST START")
        logger.info("  Date range : %s → %s", self.start_date, self.end_date)
        logger.info("  Strategies : %s", list(self._strategies.keys()))
        logger.info("  Latency    : %s", self.latency_model)
        logger.info("  Timeline   : %d events", len(timeline))
        logger.info("=" * 60)

        n_discovery = 0
        n_orderbook = 0
        n_weather = 0

        for i, sim_event in enumerate(timeline):
            # Set wall clock so fills get accurate timestamps
            self.execution_manager._current_wall_clock = sim_event.wall_clock

            if sim_event.event_type == SimEventType.MARKET_DISCOVERY:
                self.event_bus.publish(MarketDiscoveryEvent(
                    market_tickers=sim_event.payload["market_tickers"],
                    market_info=sim_event.payload["market_info"],
                ))
                n_discovery += 1

            elif sim_event.event_type == SimEventType.ORDERBOOK_UPDATE:
                self.event_bus.publish(OrderbookUpdateEvent(
                    market_ticker=sim_event.payload["market_ticker"],
                    orderbook=sim_event.payload["orderbook"],
                ))
                n_orderbook += 1

            elif sim_event.event_type == SimEventType.WEATHER_OBSERVATION:
                p = sim_event.payload
                ob_time = datetime.fromisoformat(
                    p["ob_timestamp"].replace("Z", "+00:00")
                    if isinstance(p["ob_timestamp"], str)
                    else p["ob_timestamp"].isoformat()
                )
                self.event_bus.publish(WeatherObservationEvent(
                    station=p["stid"],
                    temp=p["value"],
                    ob_time=ob_time,
                ))
                n_weather += 1

            # Progress logging every 10% of the timeline
            if (i + 1) % max(1, len(timeline) // 10) == 0:
                pct = 100 * (i + 1) / len(timeline)
                logger.info(
                    "  [%.0f%%] Processed %d/%d events — %d fills so far",
                    pct, i + 1, len(timeline), len(self.execution_manager.fills),
                )

        logger.info("=" * 60)
        logger.info("BACKTEST COMPLETE")
        logger.info("  Events : %d discovery, %d orderbook, %d weather",
                     n_discovery, n_orderbook, n_weather)
        logger.info("  Fills  : %d total", len(self.execution_manager.fills))
        logger.info("=" * 60)

        result = BacktestResult(
            fills=self.execution_manager.fills,
            timeline_length=len(timeline),
            start_date=self.start_date,
            end_date=self.end_date,
            latency_model=self.latency_model,
        )
        result.log_summary()
        return result


# ======================================================================
# BacktestResult — structured output
# ======================================================================

@dataclass
class BacktestResult:
    """Container for backtesting results with convenience analysis methods."""
    fills: list[Fill]
    timeline_length: int = 0
    start_date: date | None = None
    end_date: date | None = None
    latency_model: str = "actual"

    @property
    def total_cost_cents(self) -> int:
        return sum(f.total_cost_cents for f in self.fills)

    @property
    def total_contracts(self) -> int:
        return sum(f.contracts_filled for f in self.fills)

    @property
    def n_fills(self) -> int:
        return len(self.fills)

    def fills_by_strategy(self) -> dict[str, list[Fill]]:
        out: dict[str, list[Fill]] = defaultdict(list)
        for f in self.fills:
            out[f.strategy_id].append(f)
        return dict(out)

    def fills_by_day(self) -> dict[date, list[Fill]]:
        out: dict[date, list[Fill]] = defaultdict(list)
        for f in self.fills:
            out[f.wall_clock.date()].append(f)
        return dict(out)

    def to_dataframe(self):
        """Convert fills to a pandas DataFrame for analysis."""
        import pandas as pd
        if not self.fills:
            return pd.DataFrame()
        return pd.DataFrame([
            {
                "wall_clock": f.wall_clock,
                "strategy_id": f.strategy_id,
                "event_ticker": f.event_ticker,
                "series": f.series,
                "station": f.station,
                "market_ticker": f.market_ticker,
                "side": f.side,
                "contracts_filled": f.contracts_filled,
                "avg_fill_price_cents": f.avg_fill_price_cents,
                "total_cost_cents": f.total_cost_cents,
                "strategy_event_spent_cents": f.strategy_event_spent_cents,
            }
            for f in self.fills
        ])

    def log_summary(self):
        """Print a human-readable summary."""
        logger.info("-" * 60)
        logger.info("BACKTEST SUMMARY")
        logger.info("-" * 60)
        logger.info("  Date range     : %s → %s", self.start_date, self.end_date)
        logger.info("  Latency model  : %s", self.latency_model)
        logger.info("  Total fills    : %d", self.n_fills)
        logger.info("  Total contracts: %d", self.total_contracts)
        logger.info("  Total cost     : $%.2f", self.total_cost_cents / 100)

        by_strat = self.fills_by_strategy()
        for sid, fills in by_strat.items():
            cost = sum(f.total_cost_cents for f in fills)
            contracts = sum(f.contracts_filled for f in fills)
            logger.info(
                "  Strategy %-25s : %d fills, %d contracts, $%.2f",
                sid, len(fills), contracts, cost / 100,
            )

        by_day = self.fills_by_day()
        for d in sorted(by_day):
            fills = by_day[d]
            cost = sum(f.total_cost_cents for f in fills)
            logger.info("  %s : %d fills, $%.2f", d, len(fills), cost / 100)
        logger.info("-" * 60)

    def to_csv(self, path: str):
        """Export fills to CSV."""
        if not self.fills:
            return
        keys = [
            "wall_clock", "strategy_id", "event_ticker", "series", "station",
            "market_ticker", "side", "contracts_filled", "avg_fill_price_cents",
            "total_cost_cents", "strategy_event_spent_cents",
        ]
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            for fill in self.fills:
                writer.writerow({k: getattr(fill, k) for k in keys})
        logger.info("Exported %d fills to %s", len(self.fills), path)
