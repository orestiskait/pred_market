"""
Kalshi Information Arbitrage Weather Bot (Paper Trading)

Strategy:
  When a monitored station reports two consecutive 1-minute ASOS observations
  at or above a contract's cap_strike, the bot identifies that the daily high
  has already been reached and sweeps the "NO" side of the orderbook up to
  MAX_PRICE_CENTS (buying NO = betting the high will NOT be below that
  strike, which is already proven true).

Configuration:
  The bot reads `event_series` from config.yaml to determine which markets
  to trade and which Synoptic stations to monitor. Each market's station
  and timezone are looked up from the market registry, so adding a new
  city requires zero code changes — just add it to the registry and config.

Usage:
    python -m pred_market_src.collector.bot.weather_bot
    python -m pred_market_src.collector.bot.weather_bot --config config.yaml
    python -m pred_market_src.collector.bot.weather_bot --series KXHIGHCHI
"""

from __future__ import annotations

import asyncio
import csv
import logging
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from ..core.config import (
    load_config,
    make_kalshi_clients,
    get_synoptic_token,
    build_synoptic_ws_url,
    configure_logging,
    standard_argparser,
)
from ..core.service import AsyncService
from ..kalshi.ws import KalshiWSMixin
from ..synoptic.ws import SynopticWSMixin
from ..markets.registry import MarketConfig, MARKET_REGISTRY, all_synoptic_stations
from ..markets.ticker import discover_markets

logger = logging.getLogger("WeatherBot")

# =============================================================================
# STRATEGY DEFAULTS (overridable via config.yaml → bot section)
# =============================================================================

DEFAULT_CONSECUTIVE_OBS = 2
DEFAULT_MAX_PRICE_CENTS = 95
DEFAULT_STARTING_BALANCE_CENTS = 100_000  # $1,000.00


# =============================================================================
# BOT
# =============================================================================

class WeatherBot(AsyncService, KalshiWSMixin, SynopticWSMixin):
    """Paper-trading weather bot that combines Kalshi orderbook tracking
    and Synoptic 1-minute ASOS observation streaming via shared mixins.

    Config-driven: reads ``event_series`` from config.yaml to discover
    which markets / stations / timezones to target.
    """

    def __init__(self, config: dict, config_path: Path, series_filter: list[str] | None = None):
        self.config = config
        self._config_path = config_path

        # Determine which series to target
        all_series = config.get("event_series", [])
        if series_filter:
            self._target_series = [s for s in series_filter if s in all_series or s in MARKET_REGISTRY]
        else:
            self._target_series = all_series

        if not self._target_series:
            raise ValueError("No event_series configured or matched by --series filter")

        # Build market configs for targeted series
        self._market_configs: dict[str, MarketConfig] = {}
        for s in self._target_series:
            if s in MARKET_REGISTRY:
                self._market_configs[s] = MARKET_REGISTRY[s]
            else:
                logger.warning("Series %s not in MARKET_REGISTRY, skipping", s)

        # Kalshi
        self.kalshi_auth, self.kalshi_rest = make_kalshi_clients(config)
        self.kalshi_ws_url = config["kalshi"]["ws_url"]

        # Synoptic — subscribe only to stations we care about
        self._synoptic_token = get_synoptic_token()
        synoptic_stations = all_synoptic_stations(self._target_series)
        self.synoptic_ws_url = build_synoptic_ws_url(
            self._synoptic_token, synoptic_stations, ["air_temp"],
        )

        # Build a reverse map: synoptic station id → series prefix
        self._synoptic_to_series: dict[str, str] = {}
        for s, mc in self._market_configs.items():
            if mc.synoptic_station:
                self._synoptic_to_series[mc.synoptic_station] = s

        # Strategy config (from config.yaml → bot section, or defaults)
        bot_cfg = config.get("bot", {})
        self.consecutive_obs_required = bot_cfg.get("consecutive_obs", DEFAULT_CONSECUTIVE_OBS)
        self.max_price_cents = bot_cfg.get("max_price_cents", DEFAULT_MAX_PRICE_CENTS)
        starting_balance = bot_cfg.get("starting_balance_cents", DEFAULT_STARTING_BALANCE_CENTS)

        # State
        self._running = False
        # Per-station weather history: station_id → deque of temps
        self.weather_history: dict[str, deque] = {
            mc.synoptic_station: deque(maxlen=10)
            for mc in self._market_configs.values()
            if mc.synoptic_station
        }

        # Populated by _discover()
        self.market_tickers: list[str] = []
        self.market_info: dict[str, dict] = {}
        self.orderbooks: dict[str, dict] = {}

        # Ladder of contracts to target: tk → {trigger_temp, subtitle, executed, series}
        self.ladder: dict[str, dict] = {}

        # Paper Trading
        self.paper_balance = starting_balance
        if Path("/app/data").exists():
            data_dir = Path("/app/data")  # Docker: volume mount
        else:
            data_dir = config_path.parent / config.get("storage", {}).get("data_dir", "data")
        self.csv_log = data_dir / "weather_bot" / "paper_trades.csv"
        self.csv_log.parent.mkdir(parents=True, exist_ok=True)
        self._init_csv()

    # -------------------------------------------------------------------------
    # CSV logging
    # -------------------------------------------------------------------------

    def _init_csv(self):
        if not self.csv_log.exists():
            with open(self.csv_log, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "execution_timestamp_utc",
                    "series",
                    "station",
                    "trigger_temp",
                    "market_ticker",
                    "side",
                    "contracts_filled",
                    "avg_fill_price_cents",
                    "total_cost_cents",
                    "remaining_balance_cents",
                ])

    def _log_trade(self, series, station, trigger_temp, ticker, side, filled, avg_price, total_cost):
        now = datetime.now(timezone.utc).isoformat()
        with open(self.csv_log, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                now, series, station, trigger_temp,
                ticker, side, filled,
                round(avg_price, 2), total_cost,
                self.paper_balance,
            ])

    # -------------------------------------------------------------------------
    # Market discovery (config-driven, multi-market)
    # -------------------------------------------------------------------------

    def _discover(self):
        """Resolve today's events and build the contract ladder for all targeted series."""
        for series, mc in self._market_configs.items():
            logger.info("Resolving active events for series: %s (%s)", series, mc.city)
            events = self.kalshi_rest.get_events_for_series(series, status="open")
            if not events:
                logger.warning("No open events for %s", series)
                continue

            events.sort(key=lambda e: e.get("strike_date") or e.get("event_ticker", ""))
            active_event = events[0]["event_ticker"]
            logger.info("  → %s", active_event)

            tickers, info = discover_markets(self.kalshi_rest, [active_event])
            self.market_tickers.extend(tickers)
            self.market_info.update(info)

            # Initialize orderbooks and build ladder
            for tk in tickers:
                self.orderbooks[tk] = {"yes": {}, "no": {}}
                cap_strike = info[tk].get("cap_strike")
                if cap_strike is None:
                    continue
                trigger_temp = float(cap_strike)
                self.ladder[tk] = {
                    "trigger_temp": trigger_temp,
                    "subtitle": info[tk].get("subtitle"),
                    "executed": False,
                    "series": series,
                    "station": mc.synoptic_station,
                }
                logger.info(
                    "  Ladder: %s '%s' triggers at >= %.1f°F",
                    tk, info[tk].get("subtitle"), trigger_temp,
                )

        if not self.ladder:
            logger.warning("No valid ladder contracts found across any series.")
        else:
            logger.info(
                "Tracking %d contracts total, %d ladder targets across %d series",
                len(self.market_tickers), len(self.ladder), len(self._market_configs),
            )

        # Tell the Kalshi mixin to subscribe only to ladder tickers
        self._kalshi_subscribe_tickers = (
            list(self.ladder.keys()) if self.ladder else self.market_tickers
        )

    # -------------------------------------------------------------------------
    # SynopticWSMixin hook — react to weather observations
    # -------------------------------------------------------------------------

    def on_synoptic_observation(self, row: dict):
        """Each 1-minute ASOS observation triggers strategy evaluation."""
        station = row["stid"]
        temp = row["value"]
        ob_time = row["ob_timestamp"]

        if station not in self.weather_history:
            # Observation from a station we're not tracking
            return

        logger.info("🌡️ [%s] %.1f°F at %s", station, temp, ob_time)
        self.weather_history[station].append(temp)
        asyncio.ensure_future(self.evaluate_strategy(station, temp))

    # -------------------------------------------------------------------------
    # Strategy Engine
    # -------------------------------------------------------------------------

    async def evaluate_strategy(self, station: str, latest_temp: float):
        """Check if recent observations for *station* trigger any ladder contracts."""
        history = self.weather_history.get(station)
        if not history or len(history) < self.consecutive_obs_required:
            return

        recent_obs = list(history)[-self.consecutive_obs_required:]

        for tk, info in self.ladder.items():
            if info["executed"]:
                continue
            if info["station"] != station:
                continue

            threshold = info["trigger_temp"]
            if all(t >= threshold for t in recent_obs):
                logger.warning(
                    "🚨 LADDER TRIGGERED! [%s] Last %d obs: %s >= %.1f°F!",
                    station, self.consecutive_obs_required, recent_obs, threshold,
                )
                logger.warning("   Targeting contract: %s ('%s')", tk, info["subtitle"])
                self.ladder[tk]["executed"] = True
                await self.execute_paper_trade(tk, side="no", station=station, series=info["series"])

    # -------------------------------------------------------------------------
    # Paper Execution Engine
    # -------------------------------------------------------------------------

    async def execute_paper_trade(self, market_ticker: str, side: str, station: str = "", series: str = ""):
        """Simulate a market sweep order against the live orderbook."""
        logger.info(
            "Executing PAPER SWEEP for %s %s up to %d¢",
            market_ticker, side.upper(), self.max_price_cents,
        )

        ob = self.orderbooks.get(market_ticker)
        if not ob:
            logger.error("Cannot execute: Orderbook state is missing!")
            return

        # To buy NO, we cross the spread and hit resting YES bids.
        available_levels = []
        for price, qty in ob["yes"].items():
            if qty > 0:
                available_levels.append((100 - price, qty))

        available_levels.sort(key=lambda x: x[0])  # cheapest NO ask first

        total_contracts_bought = 0
        total_cost = 0

        for price, qty in available_levels:
            if price > self.max_price_cents:
                break
            if self.paper_balance < price:
                break

            affordable_qty = min(qty, self.paper_balance // price)
            if affordable_qty > 0:
                total_contracts_bought += affordable_qty
                cost = affordable_qty * price
                total_cost += cost
                self.paper_balance -= cost
                logger.info("   Filled: %d contracts @ %d¢", affordable_qty, price)

            if self.paper_balance < price:
                break

        if total_contracts_bought > 0:
            avg_price = total_cost / total_contracts_bought
            trigger_temp = self.ladder.get(market_ticker, {}).get("trigger_temp", 0)
            logger.info(
                "✅ PAPER TRADE COMPLETED: Bought %d %s (%s) at average %.2f¢",
                total_contracts_bought, market_ticker, side.upper(), avg_price,
            )
            logger.info(
                "   Total Layout: $%.2f | Remaining Balance: $%.2f",
                total_cost / 100, self.paper_balance / 100,
            )
            self._log_trade(
                series, station, trigger_temp,
                market_ticker, side, total_contracts_bought,
                avg_price, total_cost,
            )
        else:
            logger.warning(
                "❌ PAPER TRADE FAILED: No liquidity available under %d¢ or out of balance.",
                self.max_price_cents,
            )

    # -------------------------------------------------------------------------
    # AsyncService overrides
    # -------------------------------------------------------------------------

    def _get_tasks(self) -> list:
        return [self.kalshi_ws_loop(), self.synoptic_ws_loop()]

    async def run(self):
        self._running = True
        self._discover()
        if not self.ladder:
            logger.warning("No ladder markets identified! The bot will listen but not trade.")
        logger.info("Bot fully initialized and entering event loop.")
        await super().run()

    def shutdown(self):
        super().shutdown()


# ------------------------------------------------------------------ #
# CLI                                                                  #
# ------------------------------------------------------------------ #

def main():
    parser = standard_argparser("Kalshi Weather Arbitrage Bot (Paper Trading)")
    parser.add_argument(
        "--series", nargs="+", default=None,
        help="Limit to specific event series (e.g. KXHIGHCHI KXHIGHNY). "
             "Default: all series in config.yaml.",
    )
    args = parser.parse_args()

    configure_logging(args.log_level)

    config, config_path = load_config(args.config)
    bot = WeatherBot(config, config_path, series_filter=args.series)
    asyncio.run(bot.run())


if __name__ == "__main__":
    main()
