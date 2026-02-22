"""Centralized Execution Manager with risk guardrails.

Listens for OrderIntents from strategies, enforces risk limits
(global drawdown, per-series allocation caps), and simulates
market sweep orders against the live shared orderbook state.

All order execution is centralized here to prevent strategies from
double-trading, exceeding balance, or breaching risk limits.
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
    """Paper-trading execution engine with per-series and global risk guardrails."""

    def __init__(self, event_bus: EventBus, config: dict, config_path: Path):
        self.event_bus = event_bus
        self.config = config

        # Paper vs real mode: paper = no capital limits; real = enforce guardrails
        bot_cfg = config.get("bot", {})
        self.paper_mode = bot_cfg.get("paper_mode", True)

        # Parse guardrails from config
        guardrails = bot_cfg.get("execution_guardrails", {})
        starting = guardrails.get("starting_balance_cents", 100_000)
        self.max_total_drawdown = guardrails.get("max_total_drawdown_cents", 0)  # 0 = disabled
        self.max_allocation_per_series = guardrails.get("max_allocation_per_series_cents", 0)  # 0 = disabled

        if self.paper_mode:
            # Paper mode: no capital limits — use high balance, disable all guardrails
            self.paper_balance = 100_000_000  # $1M simulated balance
            self.starting_balance = self.paper_balance
            self.max_total_drawdown = 0
            self.max_allocation_per_series = 0
        else:
            # Real mode: enforce guardrails
            self.paper_balance = starting
            self.starting_balance = starting

        # Tracking
        self.orderbooks: dict[str, dict] = {}
        self.market_info: dict[str, dict] = {}
        self._series_spent: dict[str, int] = defaultdict(int)  # series → total cents spent
        self._halted = False  # global kill switch

        # Subscribe to events
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

        mode = "PAPER (no capital limits)" if self.paper_mode else "LIVE (guardrails enforced)"
        logger.info(
            "ExecutionManager initialized — mode=%s, balance=$%.2f, max_drawdown=$%.2f, max_per_series=$%.2f",
            mode,
            self.paper_balance / 100,
            self.max_total_drawdown / 100,
            self.max_allocation_per_series / 100,
        )

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
                    "series",
                    "station",
                    "market_ticker",
                    "side",
                    "contracts_filled",
                    "avg_fill_price_cents",
                    "total_cost_cents",
                    "remaining_balance_cents",
                    "series_allocation_cents",
                ])

    def _log_trade(self, strategy_id, series, station, ticker, side, filled, avg_price, total_cost):
        now = datetime.now(timezone.utc)
        with open(self.csv_log, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                now.isoformat(), strategy_id, series, station,
                ticker, side, filled,
                round(avg_price, 2), total_cost,
                self.paper_balance,
                self._series_spent[series],
            ])

        # Parquet output (primary for analysis)
        row = {
            "execution_ts": now,
            "strategy_id": strategy_id,
            "series": series,
            "station": station,
            "market_ticker": ticker,
            "side": side,
            "contracts_filled": int(filled),
            "avg_fill_price_cents": round(avg_price, 2),
            "total_cost_cents": total_cost,
            "remaining_balance_cents": self.paper_balance,
            "series_allocation_cents": self._series_spent[series],
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
    # Risk checks
    # ------------------------------------------------------------------

    def _check_drawdown(self) -> bool:
        """Return True if global drawdown limit is breached."""
        if self.max_total_drawdown <= 0:
            return False
        total_spent = self.starting_balance - self.paper_balance
        if total_spent >= self.max_total_drawdown:
            logger.error(
                "🛑 GLOBAL DRAWDOWN LIMIT HIT: spent $%.2f >= limit $%.2f — HALTING ALL TRADING",
                total_spent / 100, self.max_total_drawdown / 100,
            )
            self._halted = True
            return True
        return False

    def _check_series_allocation(self, series: str, proposed_cost: int) -> bool:
        """Return True if per-series allocation would be breached."""
        if self.max_allocation_per_series <= 0:
            return False
        projected = self._series_spent[series] + proposed_cost
        if projected > self.max_allocation_per_series:
            logger.warning(
                "⚠️ [%s] Series allocation limit: already $%.2f + proposed $%.2f > limit $%.2f — BLOCKING",
                series,
                self._series_spent[series] / 100,
                proposed_cost / 100,
                self.max_allocation_per_series / 100,
            )
            return True
        return False

    # ------------------------------------------------------------------
    # Order execution
    # ------------------------------------------------------------------

    async def on_order_intent(self, intent: OrderIntent):
        """Receive an OrderIntent, check risk, execute paper sweep."""
        if self._halted:
            logger.warning(
                "🛑 [%s] Trading halted — ignoring intent for %s",
                intent.strategy_id, intent.market_ticker,
            )
            return

        if self._check_drawdown():
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

        if self.paper_balance <= 0:
            logger.error("[%s] Cannot execute: Zero paper balance", intent.strategy_id)
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
            if self.paper_balance < price:
                break

            # Per-series allocation check: estimate max we can spend on this level
            if self.max_allocation_per_series > 0:
                series_remaining = self.max_allocation_per_series - self._series_spent.get(intent.series, 0) - total_cost
                if series_remaining <= 0:
                    logger.warning(
                        "   [%s] Series %s allocation exhausted mid-sweep",
                        intent.strategy_id, intent.series,
                    )
                    break
                max_by_series = series_remaining // price
            else:
                max_by_series = qty

            affordable_qty = min(qty, self.paper_balance // price, max_by_series)
            if affordable_qty > 0:
                total_contracts_bought += affordable_qty
                cost = affordable_qty * price
                total_cost += cost
                self.paper_balance -= cost
                logger.info(
                    "   [%s] Filled: %d contracts @ %d¢",
                    intent.strategy_id, affordable_qty, price,
                )

            if self.paper_balance < price:
                break

        if total_contracts_bought > 0:
            avg_price = total_cost / total_contracts_bought
            self._series_spent[intent.series] += total_cost

            logger.info(
                "✅ [%s] PAPER TRADE COMPLETED: Bought %d %s (%s) at avg %.2f¢",
                intent.strategy_id, total_contracts_bought,
                intent.market_ticker, intent.side.upper(), avg_price,
            )
            logger.info(
                "   Total Layout: $%.2f | Balance: $%.2f | Series %s alloc: $%.2f/$%.2f",
                total_cost / 100, self.paper_balance / 100,
                intent.series,
                self._series_spent[intent.series] / 100,
                self.max_allocation_per_series / 100 if self.max_allocation_per_series > 0 else float("inf"),
            )

            self._log_trade(
                intent.strategy_id, intent.series, intent.station,
                intent.market_ticker, intent.side,
                total_contracts_bought, avg_price, total_cost,
            )

            # Check drawdown after the trade
            self._check_drawdown()
        else:
            logger.warning(
                "❌ [%s] PAPER TRADE FAILED: No liquidity under %d¢, balance exhausted, or allocation capped.",
                intent.strategy_id, intent.max_price_cents,
            )
