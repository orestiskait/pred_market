#!/usr/bin/env python3
"""
Fetch Kalshi odds for the NYC highest temperature market.

Usage:
    python fetch_kalshi.py [ticker]
    python fetch_kalshi.py           # uses kxhighny-26feb11
    python fetch_kalshi.py kxhighny-26feb10  # historical day

Saves raw JSON to data/raw/ and prints a summary.
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import requests

# Add parent for config
sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    DATA_RAW_DIR,
    KALSHI_API_BASE,
    NYC_TEMP_MARKET_TICKER,
)


def fetch_market(ticker: str) -> dict | None:
    """Fetch market details from Kalshi API."""
    url = f"{KALSHI_API_BASE}/markets/{ticker}"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json().get("market") or r.json()
    except requests.RequestException as e:
        print(f"Error fetching market: {e}")
        return None


def fetch_orderbook(ticker: str) -> dict | None:
    """Fetch orderbook (bids/asks) from Kalshi API."""
    url = f"{KALSHI_API_BASE}/markets/{ticker}/orderbook"
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        print(f"Error fetching orderbook: {e}")
        return None


def format_odds(price_cents: int) -> str:
    """Convert cents (1-99) to probability %. Kalshi uses 1-99 scale."""
    return f"{price_cents}¢ ({price_cents}%)"


def summarize_market(market: dict, orderbook: dict | None) -> None:
    """Print human-readable summary of market and odds."""
    print("\n" + "=" * 60)
    print(f"Market: {market.get('title', 'N/A')}")
    print(f"Ticker: {market.get('ticker', 'N/A')}")
    print(f"Status: {market.get('status', 'N/A')}")
    print("=" * 60)

    # Contract outcomes (temperature ranges)
    contracts = market.get("contracts", []) or []
    if not contracts and "yes_sub_title" in market:
        # Single-contract market
        contracts = [{"outcome": market.get("subtitle", "N/A"), "yes_bid": market.get("yes_bid"), "yes_ask": market.get("yes_ask")}]

    for c in contracts:
        outcome = c.get("outcome", c.get("title", "?"))
        yes_bid = c.get("yes_bid")
        yes_ask = c.get("yes_ask")
        if yes_bid is not None or yes_ask is not None:
            mid = (yes_bid + yes_ask) / 2 if (yes_bid and yes_ask) else (yes_bid or yes_ask or 0)
            print(f"  {outcome}: bid={format_odds(yes_bid)} ask={format_odds(yes_ask)} mid≈{format_odds(int(mid))}")
        else:
            print(f"  {outcome}: (no quotes)")

    if orderbook:
        ob = orderbook.get("orderbook", orderbook)
        asks = ob.get("yes", []) or ob.get("asks", [])
        bids = ob.get("no", []) or ob.get("bids", [])
        if asks or bids:
            print("\nOrderbook sample:")
            if asks:
                print(f"  Top asks (yes): {asks[:5]}")
            if bids:
                print(f"  Top bids: {bids[:5]}")


def save_raw(ticker: str, market: dict | None, orderbook: dict | None) -> None:
    """Save raw API responses to data/raw/."""
    base = Path(__file__).resolve().parent
    raw_dir = base / DATA_RAW_DIR
    raw_dir.mkdir(parents=True, exist_ok=True)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    prefix = f"{ticker}_{ts}"

    if market:
        path = raw_dir / f"{prefix}_market.json"
        with open(path, "w") as f:
            json.dump(market, f, indent=2)
        print(f"\nSaved market: {path}")

    if orderbook:
        path = raw_dir / f"{prefix}_orderbook.json"
        with open(path, "w") as f:
            json.dump(orderbook, f, indent=2)
        print(f"Saved orderbook: {path}")


def main():
    ticker = sys.argv[1] if len(sys.argv) > 1 else NYC_TEMP_MARKET_TICKER
    print(f"Fetching Kalshi market: {ticker}")

    market = fetch_market(ticker)
    if not market:
        sys.exit(1)

    orderbook = fetch_orderbook(ticker)

    summarize_market(market, orderbook)
    save_raw(ticker, market, orderbook)


if __name__ == "__main__":
    main()
