import csv
import logging
from datetime import datetime, timezone
from pathlib import Path

from services.bot.events import EventBus, OrderIntent, OrderbookUpdateEvent, MarketDiscoveryEvent

logger = logging.getLogger("ExecutionManager")

class ExecutionManager:
    """
    Centralized Execution Manager.
    Listens for OrderIntents from strategies, checks risk guardrails,
    and simulated market sweeps against the live shared orderbook state.
    """
    def __init__(self, event_bus: EventBus, config: dict, config_path: Path):
        self.event_bus = event_bus
        self.config = config
        
        bot_cfg = config.get("bot", {})
        self.paper_balance = bot_cfg.get("starting_balance_cents", 100_000)
        
        # State
        self.orderbooks: dict[str, dict] = {}
        self.market_info: dict[str, dict] = {}
        
        # Subs
        self.event_bus.subscribe(OrderIntent, self.on_order_intent)
        self.event_bus.subscribe(OrderbookUpdateEvent, self.on_orderbook_update)
        self.event_bus.subscribe(MarketDiscoveryEvent, self.on_market_discovery)

        # Setup paper trading log
        if Path("/app/data").exists():
            data_dir = Path("/app/data")
        else:
            data_dir = (config_path.parent / config.get("storage", {}).get("data_dir", "../data")).resolve()
        
        self.csv_log = data_dir / "weather_bot_paper_trades" / "paper_trades.csv"
        self.csv_log.parent.mkdir(parents=True, exist_ok=True)
        self._init_csv()

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
                ])

    def _log_trade(self, strategy_id, series, station, ticker, side, filled, avg_price, total_cost):
        now = datetime.now(timezone.utc).isoformat()
        with open(self.csv_log, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                now, strategy_id, series, station,
                ticker, side, filled,
                round(avg_price, 2), total_cost,
                self.paper_balance,
            ])

    async def on_market_discovery(self, event: MarketDiscoveryEvent):
        self.market_info = event.market_info
        # Pre-initialize orderbooks for new markets
        for tk in event.market_tickers:
            if tk not in self.orderbooks:
                self.orderbooks[tk] = {"yes": {}, "no": {}}

    async def on_orderbook_update(self, event: OrderbookUpdateEvent):
        self.orderbooks[event.market_ticker] = event.orderbook

    async def on_order_intent(self, intent: OrderIntent):
        logger.info(
            "[%s] Received intent for %s %s up to %d¢",
            intent.strategy_id, intent.market_ticker, intent.side.upper(), intent.max_price_cents,
        )

        ob = self.orderbooks.get(intent.market_ticker)
        if not ob:
            logger.error("[%s] Cannot execute: Orderbook state is missing for %s", intent.strategy_id, intent.market_ticker)
            return

        # Centralized Risk Check: Balance limits etc. Could be expanded.
        if self.paper_balance <= 0:
            logger.error("[%s] Cannot execute: Insufficient paper balance", intent.strategy_id)
            return

        # Simulate Sweep
        available_levels = []
        if intent.side.lower() == "no":
            # To buy NO, cross spread and hit resting YES bids (100 - price)
            for price, qty in ob["yes"].items():
                if qty > 0:
                    available_levels.append((100 - price, qty))
        else:
            # To buy YES, cross spread and hit resting NO bids (100 - price)
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

            affordable_qty = min(qty, self.paper_balance // price)
            if affordable_qty > 0:
                total_contracts_bought += affordable_qty
                cost = affordable_qty * price
                total_cost += cost
                self.paper_balance -= cost
                logger.info("   [%s] Filled: %d contracts @ %d¢", intent.strategy_id, affordable_qty, price)

            if self.paper_balance < price:
                break

        if total_contracts_bought > 0:
            avg_price = total_cost / total_contracts_bought
            logger.info(
                "✅ [%s] PAPER TRADE COMPLETED: Bought %d %s (%s) at average %.2f¢",
                intent.strategy_id, total_contracts_bought, intent.market_ticker, intent.side.upper(), avg_price,
            )
            logger.info(
                "   Total Layout: $%.2f | Remaining Balance: $%.2f",
                total_cost / 100, self.paper_balance / 100,
            )

            self._log_trade(
                intent.strategy_id,
                intent.series,
                intent.station,
                intent.market_ticker,
                intent.side,
                total_contracts_bought,
                avg_price,
                total_cost,
            )
        else:
            logger.warning(
                "❌ [%s] PAPER TRADE FAILED: No liquidity available under %d¢ or out of balance.",
                intent.strategy_id, intent.max_price_cents,
            )
