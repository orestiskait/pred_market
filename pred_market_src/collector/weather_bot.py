"""
Kalshi Information Arbitrage Weather Bot (Paper Trading)

Strategy:
  When Chicago Midway (KMDW1M) reports two consecutive 1-minute ASOS observations
  strictly greater than a configured TARGET_TEMP (e.g., > 35.0), the bot identifies
  the active market contract representing "High Temperature < 35°F" (or the relevant bin)
  and sweeps the "NO" side of the orderbook up to a MAX_PRICE cents (e.g., 96¢).
  
  Since Kalshi's settlement utilizes official CLI data (derived from these same readings),
  if the temperature has *already* reached 35.0+, the daily high cannot mathematically
  be below 35.0. Therefore, buying "NO" on any contract that claims the high will be 
  < 35.0 is an information arbitrage play with a guaranteed payout (minus fees and edge cases).
  
Usage:
  pred_env/bin/python pred_market_src/bot/weather_bot.py
"""

import asyncio
import csv
import json
import logging
import os
import re
import signal
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
import websockets

from kalshi_client import KalshiAuth, KalshiRestClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s"
)
logger = logging.getLogger("WeatherBot")

# =============================================================================
# STRATEGY CONFIGURATION
# =============================================================================

# The station to monitor and its active series on Kalshi
STATION = "KMDW1M"
EVENT_SERIES = "KXHIGHCHI"

# Our trigger threshold logic is dynamic now (ladder approach).
# For example, if contract says "34° or below", trigger is > 35.0.
# If contract says "35° to 36°", trigger is > 37.0.

# Number of consecutive 1-minute observations required above TARGET_TEMP
CONSECUTIVE_OBS_REQUIRED = 2

# Maximum price (in cents) we are willing to pay for a "NO" contract.
# Kalshi settles at 100 cents. Buying at 95 cents = 5 cents profit per contract.
# Note: Kalshi fees apply, so 95-96 is a realistic upper bound for positive EV.
MAX_PRICE_CENTS = 95

# Starting paper balance for the simulation (cents)
STARTING_BALANCE_CENTS = 100000  # $1,000.00


# =============================================================================
# BOT ARCHITECTURE
# =============================================================================

class WeatherBot:
    def __init__(self):
        self.running = False
        
        # 1. API Initialization
        env_path = Path(__file__).resolve().parent / ".env"
        load_dotenv(env_path)
        
        self.kalshi_api_key_id = os.environ.get("KALSHI_API_KEY_ID")
        self.kalshi_private_key_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH")
        self.synoptic_token = os.environ.get("SYNOPTIC_API_TOKEN")
        
        if not all([self.kalshi_api_key_id, self.kalshi_private_key_path, self.synoptic_token]):
            raise ValueError("Missing required API credentials in .env")

        self.kalshi_auth = KalshiAuth(self.kalshi_api_key_id, self.kalshi_private_key_path)
        self.kalshi_rest = KalshiRestClient("https://api.elections.kalshi.com/trade-api/v2", self.kalshi_auth)
        self.kalshi_ws_url = "wss://api.elections.kalshi.com/trade-api/ws/v2"
        self.synoptic_ws_url = f"wss://push.synopticdata.com/feed/{self.synoptic_token}/?units=english&stid={STATION}&vars=air_temp"

        # 2. State Management
        # Keep the last N weather observations
        self.weather_history = deque(maxlen=10)
        
        # Map of market_ticker -> dict of current orderbook { 'yes': {price: qty}, 'no': {price: qty} }
        self.orderbooks = {}
        
        # Active WebSocket connections
        self.active_kalshi_ws = None
        self.active_synoptic_ws = None
        
        # Map of event_ticker -> list of active market_tickers
        self.active_event_ticker = None
        self.market_tickers = []
        
        # Ladder of contracts we want to short
        # Map of market_ticker -> {"trigger_temp": float, "subtitle": str, "executed": bool}
        self.ladder = {}

        # 3. Paper Trading State
        self.paper_balance = STARTING_BALANCE_CENTS
        # Write to shared data volume (collector-data/weather_bot) for fetch sync
        if Path("/app/data").exists():
            data_dir = Path("/app/data")  # Docker: volume mount
        else:
            data_dir = Path(__file__).resolve().parent / "data"  # Local dev
        self.csv_log = data_dir / "weather_bot" / "paper_trades.csv"
        self.csv_log.parent.mkdir(parents=True, exist_ok=True)
        self._init_csv()

    def _init_csv(self):
        if not self.csv_log.exists():
            with open(self.csv_log, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "execution_timestamp_utc", 
                    "trigger_temp", 
                    "market_ticker", 
                    "side", 
                    "contracts_filled", 
                    "avg_fill_price_cents", 
                    "total_cost_cents", 
                    "remaining_balance_cents"
                ])

    # -------------------------------------------------------------------------
    # REST: Discovery 
    # -------------------------------------------------------------------------

    def discover_markets(self):
        """Find the active event for today and map its contracts."""
        logger.info(f"Resolving active events for series: {EVENT_SERIES}")
        events = self.kalshi_rest.get_events_for_series(EVENT_SERIES, status="open")
        
        if not events:
            logger.error("No open events found today.")
            return

        # Sort by earliest event_ticker to get today's active event
        events.sort(key=lambda e: e.get("strike_date") or e.get("event_ticker", ""))
        self.active_event_ticker = events[0]["event_ticker"]
        logger.info(f"Targeting active event: {self.active_event_ticker}")

        markets = self.kalshi_rest.get_markets_for_event(self.active_event_ticker)
        
        for m in markets:
            tk = m["ticker"]
            self.market_tickers.append(tk)
            self.orderbooks[tk] = {"yes": {}, "no": {}}
            
            # Identify the specific market we want to short.
            # We skip "or above" contracts because rising temperatures do not invalidate them.
            cap_strike = m.get("cap_strike")
            if cap_strike is None:
                continue
                
            trigger_temp = float(cap_strike)
            
            self.ladder[tk] = {
                "trigger_temp": trigger_temp,
                "subtitle": m.get("subtitle"),
                "executed": False
            }
            logger.info(f" Ladder active for {tk} ('{m.get('subtitle')}'). Triggers at >= {trigger_temp}°F")

        target_tickers = list(self.ladder.keys())
        if not target_tickers:
            logger.warning(f"Could not parse any valid ladder contracts with cap_strikes.")
        else:
            logger.info(f"Tracking {len(self.market_tickers)} total contracts. Ladder targets: {len(target_tickers)}")

    # -------------------------------------------------------------------------
    # WebSocket 1: Kalshi Orderbook Listener 
    # -------------------------------------------------------------------------

    async def kalshi_ws_loop(self):
        """Maintain a 0-latency live replica of the Kalshi orderbook in memory."""
        logger.info("Starting Kalshi WebSocket listener...")
        while self.running:
            try:
                headers = self.kalshi_auth.ws_headers()
                async with websockets.connect(self.kalshi_ws_url, additional_headers=headers) as ws:
                    self.active_kalshi_ws = ws
                    logger.info("Kalshi WebSocket connected")

                    # Subscribe to orderbook changes
                    sub = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["orderbook_delta"],
                            "market_tickers": list(self.ladder.keys()) if self.ladder else self.market_tickers,
                        },
                    }
                    await ws.send(json.dumps(sub))

                    async for raw in ws:
                        if not self.running: break
                        msg = json.loads(raw)
                        
                        mtype = msg.get("type")
                        data = msg.get("msg", {})

                        if mtype == "orderbook_snapshot":
                            tk = data.get("market_ticker", "")
                            # Flush state fully on new snapshot (handles reconnection integrity)
                            self.orderbooks[tk] = {"yes": {}, "no": {}}
                            for side in ("yes", "no"):
                                for price, qty in data.get(side, []):
                                    self.orderbooks[tk][side][int(price)] = qty

                        elif mtype == "orderbook_delta":
                            tk = data.get("market_ticker", "")
                            if tk in self.orderbooks:
                                for side in ("yes", "no"):
                                    for price, qty in data.get(side, []):
                                        p = int(price)
                                        if qty <= 0:
                                            self.orderbooks[tk][side].pop(p, None)
                                        else:
                                            self.orderbooks[tk][side][p] = qty

            except Exception as e:
                logger.error(f"Kalshi WS error: {e}")
                await asyncio.sleep(5)

    # -------------------------------------------------------------------------
    # WebSocket 2: Synoptic Weather Listener
    # -------------------------------------------------------------------------

    async def synoptic_ws_loop(self):
        """Listen for new 1-minute ASOS temperatures and evaluate the trading rule."""
        safe_url = self.synoptic_ws_url.replace(self.synoptic_token, "<TOKEN>")
        logger.info(f"Starting Synoptic WebSocket listener... ({STATION})")
        
        while self.running:
            try:
                async with websockets.connect(self.synoptic_ws_url, ping_interval=None) as ws:
                    self.active_synoptic_ws = ws
                    logger.info("Synoptic WebSocket connected")

                    async for raw in ws:
                        if not self.running: break
                        msg = json.loads(raw)
                        
                        if msg.get("type") == "data":
                            for d in msg.get("data", []):
                                temp = float(d.get("value"))
                                ob_time_str = d.get("date")
                                logger.info(f"🌡️ New Obs: {temp}°F at {ob_time_str}")
                                
                                self.weather_history.append(temp)
                                await self.evaluate_strategy(temp)
                                
            except Exception as e:
                logger.error(f"Synoptic WS error: {type(e).__name__}")
                await asyncio.sleep(5)

    # -------------------------------------------------------------------------
    # Strategy Engine
    # -------------------------------------------------------------------------

    async def evaluate_strategy(self, latest_temp: float):
        """
        Check if the last `CONSECUTIVE_OBS_REQUIRED` temperatures are > any of our ladder triggers.
        If yes, and not yet executed, trigger the sweep for that contract.
        """
        if len(self.weather_history) < CONSECUTIVE_OBS_REQUIRED:
            return

        recent_obs = list(self.weather_history)[-CONSECUTIVE_OBS_REQUIRED:]
        
        for tk, info in self.ladder.items():
            if info["executed"]:
                continue
                
            threshold = info["trigger_temp"]
            # Trigger requires >= because cap_strike means anything exactly matching or above resolves the bin to NO
            all_above_threshold = all(t >= threshold for t in recent_obs)
            
            if all_above_threshold:
                logger.warning(f"🚨 LADDER TRIGGERED! Last {CONSECUTIVE_OBS_REQUIRED} obs: {recent_obs} >= {threshold}°F!")
                logger.warning(f"   Targeting contract: {tk} ('{info['subtitle']}')")
                self.ladder[tk]["executed"] = True
                await self.execute_paper_trade(tk, side="no")


    # -------------------------------------------------------------------------
    # Paper Execution Engine
    # -------------------------------------------------------------------------

    async def execute_paper_trade(self, market_ticker: str, side: str):
        """
        Simulate a market sweep order against the live orderbook up to MAX_PRICE_CENTS.
        Note: For LIVE execution, calculating depth dynamically in Python introduces latency.
        Instead, you would immediately send a 'Fill or Kill' (FOK) or 'Immediate or Cancel' (IOC)
        Market Order payload to `<base_url>/portfolio/orders` specifying MAX_PRICE_CENTS.
        Kalshi's internal matching engine guarantees atomicity and latency optimization.
        """
        logger.info(f"Executing PAPER SWEEP for {market_ticker} {side.upper()} up to {MAX_PRICE_CENTS}¢")
        
        ob = self.orderbooks.get(market_ticker)
        if not ob:
            logger.error("Cannot execute: Orderbook state is missing!")
            return

        # Kalshi V2 WebSockets provide `side="yes"` for YES bids, and `side="no"` for NO bids.
        # To strictly buy NO, we must cross the spread and hit resting YES bids (i.e. implied NO asks).
        # We calculate the implied NO Ask Price = 100 - YES Bid Price.
        
        target_bids = "yes"  # Since we are buying "NO", we match against resting "YES" bids
        
        available_levels = []
        for price, qty in ob[target_bids].items():
            if qty > 0:
                implied_no_price = 100 - price
                available_levels.append((implied_no_price, qty))
                
        available_levels.sort(key=lambda x: x[0])  # Sort ascending (cheapest theoretical NO Ask first)
        
        total_contracts_bought = 0
        total_cost = 0
        
        for price, qty in available_levels:
            if price > MAX_PRICE_CENTS:
                break # Reached our max willingness to pay
                
            if self.paper_balance < price:
                break # Out of money!
                
            # How many can we afford at this price layer?
            affordable_qty = min(qty, self.paper_balance // price)
            
            if affordable_qty > 0:
                total_contracts_bought += affordable_qty
                cost = affordable_qty * price
                total_cost += cost
                self.paper_balance -= cost
                
                logger.info(f"   Filled: {affordable_qty} contracts @ {price}¢")
            
            if self.paper_balance < price:
                break

        if total_contracts_bought > 0:
            avg_price = total_cost / total_contracts_bought
            logger.info(f"✅ PAPER TRADE COMPLETED: Bought {total_contracts_bought} {market_ticker} ({side.upper()}) at average {avg_price:.2f}¢")
            logger.info(f"   Total Layout: ${total_cost / 100:.2f} | Remaining Balance: ${self.paper_balance / 100:.2f}")
            self._log_trade(market_ticker, side, total_contracts_bought, avg_price, total_cost)
        else:
            logger.warning(f"❌ PAPER TRADE FAILED: No liquidity available under {MAX_PRICE_CENTS}¢ or out of balance.")

    def _log_trade(self, ticker, side, filled, avg_price, total_cost):
        now = datetime.now(timezone.utc).isoformat()
        with open(self.csv_log, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([
                now,
                list(self.weather_history)[-1],
                ticker,
                side,
                filled,
                round(avg_price, 2),
                total_cost,
                self.paper_balance
            ])

    # -------------------------------------------------------------------------
    # Application State
    # -------------------------------------------------------------------------

    async def run(self):
        self.running = True
        
        # 1. Discover the exact markets first
        self.discover_markets()
        if not self.ladder:
            logger.warning("No ladder markets identified! The bot will listen but not trade.")
            
        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self.shutdown)

        logger.info("Bot fully initialized and entering event loop.")
        try:
            await asyncio.gather(
                self.kalshi_ws_loop(),
                self.synoptic_ws_loop()
            )
        finally:
            logger.info("Bot shutting down gracefully.")

    def shutdown(self):
        logger.info("Shutdown signal received")
        self.running = False
        
        # Forcefully terminate active websockets to break generators
        if self.active_kalshi_ws:
            asyncio.create_task(self.active_kalshi_ws.close())
        if self.active_synoptic_ws:
            asyncio.create_task(self.active_synoptic_ws.close())


if __name__ == "__main__":
    bot = WeatherBot()
    asyncio.run(bot.run())
