"""Centralized Execution Manager with per-strategy, per-event risk guardrails.

Listens for OrderIntents from strategies, enforces spend limits, and
simulates market sweep orders against the live shared orderbook state.

All order execution is centralized here to prevent strategies from
double-trading or exceeding risk limits.

GUARDRAIL MODEL:
  Each strategy declares its own ``max_spend_per_event`` (in its config
  params).  Spend is tracked per (strategy_id, event_ticker) pair — e.g.
  ("chicago_fast_ladder", "KXHIGHCHI-26FEB22").  When the event rolls to
  the next day the ticker changes, so the budget resets automatically.
  The bot never permanently halts.

PAPER / LIVE EQUIVALENCE:
  Paper and live modes MUST use identical execution logic:
  - Same orderbook source (Kalshi WS orderbook_delta → snapshot + deltas)
  - Same sweep algorithm (buy at each level up to max_price_cents)
  - Same risk checks (per-strategy-event spend cap)
  When adding live order placement, keep this module as the single source
  of truth: live should call Kalshi API to place the same orders this
  sweep would simulate.
"""

from __future__ import annotations

import csv
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from services.bot.events import EventBus, OrderIntent, OrderbookUpdateEvent, MarketDiscoveryEvent
from services.core.storage import ParquetStorage

logger = logging.getLogger("ExecutionManager")


class ExecutionManager:
    """Paper-trading execution engine with per-strategy, per-event risk guardrails."""

    def __init__(self, event_bus: EventBus, config: dict, config_path: Path):
        self.event_bus = event_bus
        self.config = config

        # Tracking: (strategy_id, event_ticker) → total cents spent
        self.orderbooks: dict[str, dict] = {}
        self.market_info: dict[str, dict] = {}
        self._spent: dict[tuple[str, str], int] = defaultdict(int)

        self.event_bus.subscribe(OrderIntent, self.on_order_intent)
        self.event_bus.subscribe(OrderbookUpdateEvent, self.on_orderbook_update)
        self.event_bus.subscribe(MarketDiscoveryEvent, self.on_market_discovery)

        # Paper trade persistence: CSV (legacy) + Parquet (primary)
        if Path("/app/data").exists():
            data_dir = Path("/app/data")
        else:
            data_dir = (config_path.parent / config.get("storage", {}).get("data_dir", "../data")).resolve()

        self._data_dir = data_dir
        self._parquet_storage = ParquetStorage(str(data_dir))

        self.csv_log = data_dir / "weather_bot_paper_trades" / "paper_trades.csv"
        self.csv_log.parent.mkdir(parents=True, exist_ok=True)
        self._init_csv()

        logger.info("ExecutionManager initialized (paper_mode is per-strategy)")

    # ------------------------------------------------------------------
    # CSV logging
    # ------------------------------------------------------------------

    def _init_csv(self):
        if not self.csv_log.exists():
            with open(self.csv_log, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "execution_timestamp_utc",
                    "strategy_id",
                    "event_ticker",
                    "series",
                    "station",
                    "market_ticker",
                    "side",
                    "contracts_filled",
                    "avg_fill_price_cents",
                    "total_cost_cents",
                    "strategy_event_spent_cents",
                ])

    def _log_trade(self, intent: OrderIntent, filled: int, avg_price: float, total_cost: int):
        key = (intent.strategy_id, intent.event_ticker)
        now = datetime.now(timezone.utc)
        with open(self.csv_log, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                now.isoformat(), intent.strategy_id, intent.event_ticker,
                intent.series, intent.station, intent.market_ticker,
                intent.side, filled, round(avg_price, 2), total_cost,
                self._spent[key],
            ])

        row = {
            "execution_ts": now,
            "strategy_id": intent.strategy_id,
            "event_ticker": intent.event_ticker,
            "series": intent.series,
            "station": intent.station,
            "market_ticker": intent.market_ticker,
            "side": intent.side,
            "contracts_filled": int(filled),
            "avg_fill_price_cents": round(avg_price, 2),
            "total_cost_cents": total_cost,
            "strategy_event_spent_cents": self._spent[key],
        }
        self._parquet_storage.write_paper_trades([row])

    # ------------------------------------------------------------------
    # Event handlers
    # ------------------------------------------------------------------

    async def on_market_discovery(self, event: MarketDiscoveryEvent):
        self.market_info = event.market_info
        for tk in event.market_tickers:
            if tk not in self.orderbooks:
                self.orderbooks[tk] = {"yes": {}, "no": {}}

    async def on_orderbook_update(self, event: OrderbookUpdateEvent):
        self.orderbooks[event.market_ticker] = event.orderbook

    # ------------------------------------------------------------------
    # Order execution
    # ------------------------------------------------------------------

    def _remaining(self, intent: OrderIntent) -> int:
        """Cents remaining for this (strategy, event) pair, or -1 if uncapped."""
        if intent.max_spend_cents <= 0:
            return -1
        key = (intent.strategy_id, intent.event_ticker)
        return max(0, intent.max_spend_cents - self._spent.get(key, 0))

    async def on_order_intent(self, intent: OrderIntent):
        """Receive an OrderIntent, check per-strategy-event budget, execute paper sweep."""

        budget = self._remaining(intent)
        if budget == 0:
            key = (intent.strategy_id, intent.event_ticker)
            logger.warning(
                "[%s] Budget exhausted for event %s ($%.2f spent) — skipping %s",
                intent.strategy_id, intent.event_ticker,
                self._spent.get(key, 0) / 100,
                intent.market_ticker,
            )
            return

        logger.info(
            "[%s] Received intent for %s %s up to %d¢",
            intent.strategy_id, intent.market_ticker, intent.side.upper(), intent.max_price_cents,
        )

        ob = self.orderbooks.get(intent.market_ticker)
        if not ob:
            logger.error(
                "[%s] Cannot execute: Orderbook missing for %s",
                intent.strategy_id, intent.market_ticker,
            )
            return

        # Build available levels
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
                    logger.warning(
                        "   [%s] Event %s budget exhausted mid-sweep",
                        intent.strategy_id, intent.event_ticker,
                    )
                    break
                max_by_budget = budget_left // price
            else:
                max_by_budget = qty

            affordable_qty = min(qty, max_by_budget)
            if affordable_qty > 0:
                total_contracts_bought += affordable_qty
                cost = affordable_qty * price
                total_cost += cost
                logger.info(
                    "   [%s] Filled: %d contracts @ %d¢",
                    intent.strategy_id, affordable_qty, price,
                )

        if total_contracts_bought > 0:
            avg_price = total_cost / total_contracts_bought
            key = (intent.strategy_id, intent.event_ticker)
            self._spent[key] += total_cost

            cap_str = "$%.2f" % (intent.max_spend_cents / 100) if intent.max_spend_cents > 0 else "uncapped"
            mode_tag = "PAPER" if intent.paper_mode else "LIVE"
            logger.info(
                "✅ [%s] %s TRADE COMPLETED: Bought %d %s (%s) at avg %.2f¢",
                intent.strategy_id, mode_tag, total_contracts_bought,
                intent.market_ticker, intent.side.upper(), avg_price,
            )
            logger.info(
                "   Cost: $%.2f | %s / %s spent: $%.2f/%s",
                total_cost / 100,
                intent.strategy_id, intent.event_ticker,
                self._spent[key] / 100, cap_str,
            )

            self._log_trade(intent, total_contracts_bought, avg_price, total_cost)
        else:
            logger.warning(
                "❌ [%s] PAPER TRADE FAILED: No liquidity under %d¢ or budget capped.",
                intent.strategy_id, intent.max_price_cents,
            )
