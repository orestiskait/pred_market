#!/usr/bin/env python3
"""
Fetch Kalshi odds for historical NYC temperature markets.

Kalshi ticker format: KXHIGHNY-{YY}{MON}{DD}  (e.g. KXHIGHNY-26FEB11)
API requires uppercase. Each date has multiple contracts (temperature thresholds).

Usage:
    python fetch_historical.py --days 7          # last 7 days
    python fetch_historical.py --start 2026-02-01 --end 2026-02-11
"""

import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DATA_RAW_KALSHI, KALSHI_API_BASE, KXHIGHNY_SERIES


def date_to_ticker(d: datetime) -> str:
    """Convert date to Kalshi ticker suffix: 26FEB11 (API uses uppercase)."""
    return d.strftime("%y%b%d").upper()


def ticker_for_date(d: datetime) -> str:
    """Full event ticker for NYC high temp market on date d (KXHIGHNY-26FEB11)."""
    return f"KXHIGHNY-{date_to_ticker(d)}"


def fetch_market(ticker: str) -> dict | None:
    url = f"{KALSHI_API_BASE}/markets/{ticker}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json().get("market") or r.json()
    except requests.RequestException:
        return None


def fetch_orderbook(ticker: str) -> dict | None:
    url = f"{KALSHI_API_BASE}/markets/{ticker}/orderbook"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()
    except requests.RequestException:
        return None


def get_event_markets(event_ticker: str) -> list[dict]:
    """Fetch all markets for an event (e.g. kxhighny-26feb11 has multiple contracts).

    Kalshi uses GET /events/{event_ticker} which returns event + markets.
    For multivariate events (e.g. weather), fall back to GET /markets?event_ticker=.
    """
    # Try GET /events/{event_ticker} (returns event with nested markets)
    url = f"{KALSHI_API_BASE}/events/{event_ticker}"
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            data = r.json()
            markets = data.get("markets") or data.get("event", {}).get("markets", [])
            if markets:
                return markets
        if r.status_code == 404:
            # Multivariate events (e.g. kxhighny weather) use different API
            return _get_markets_by_event_ticker(event_ticker)
        r.raise_for_status()
        return []
    except requests.RequestException:
        return _get_markets_by_event_ticker(event_ticker)


def _get_markets_by_event_ticker(event_ticker: str) -> list[dict]:
    """Fallback: GET /markets?event_ticker= or GET /markets?series_ticker=KXHIGHNY (filter by event)."""
    url = f"{KALSHI_API_BASE}/markets"
    try:
        r = requests.get(url, params={"event_ticker": event_ticker}, timeout=10)
        if r.status_code != 200:
            return _get_markets_by_series(event_ticker)
        data = r.json()
        markets = data.get("markets", [])
        if markets:
            return markets
        return _get_markets_by_series(event_ticker)
    except requests.RequestException:
        return _get_markets_by_series(event_ticker)


def _get_markets_by_series(event_ticker: str) -> list[dict]:
    """Fallback: GET /markets?series_ticker=KXHIGHNY - Kalshi docs use this for KXHIGHNY."""
    url = f"{KALSHI_API_BASE}/markets"
    all_markets = []
    cursor = None
    try:
        for _ in range(20):  # paginate up to 20 pages
            params = {"series_ticker": KXHIGHNY_SERIES, "limit": 200}
            if cursor:
                params["cursor"] = cursor
            r = requests.get(url, params=params, timeout=10)
            if r.status_code != 200:
                break
            data = r.json()
            markets = data.get("markets", [])
            all_markets.extend(m for m in markets if m.get("event_ticker") == event_ticker)
            cursor = data.get("cursor") or ""
            if not cursor:
                break
        return all_markets
    except requests.RequestException:
        return []


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7, help="Number of past days to fetch")
    ap.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    ap.add_argument("--end", type=str, help="End date YYYY-MM-DD")
    ap.add_argument("--delay", type=float, default=0.5, help="Delay between API calls (sec)")
    args = ap.parse_args()

    if args.start and args.end:
        start = datetime.strptime(args.start, "%Y-%m-%d").date()
        end = datetime.strptime(args.end, "%Y-%m-%d").date()
    else:
        end = datetime.now().date()
        start = end - timedelta(days=args.days)

    base = Path(__file__).resolve().parent
    raw_dir = base / DATA_RAW_KALSHI
    raw_dir.mkdir(parents=True, exist_ok=True)

    results = []
    d = start
    while d <= end:
        event_ticker = ticker_for_date(datetime.combine(d, datetime.min.time()))
        print(f"Fetching {event_ticker} ({d})...", end=" ")

        # Try event-based fetch first (multiple outcome contracts per day)
        markets = get_event_markets(event_ticker)
        time.sleep(args.delay)

        if markets:
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            with open(raw_dir / f"{event_ticker}_{ts}_event_markets.json", "w") as f:
                json.dump({"event_ticker": event_ticker, "date": str(d), "markets": markets}, f, indent=2)
            # Fetch orderbook for each market
            for m in markets:
                mt = m.get("ticker", "")
                ob = fetch_orderbook(mt)
                time.sleep(args.delay)
                if ob:
                    with open(raw_dir / f"{mt}_{ts}_orderbook.json", "w") as f:
                        json.dump(ob, f, indent=2)
            print(f"OK ({len(markets)} contracts)")
            results.append({"date": str(d), "event_ticker": event_ticker, "markets": markets})
        else:
            # Fallback: single market ticker
            market = fetch_market(event_ticker)
            time.sleep(args.delay)
            orderbook = fetch_orderbook(event_ticker) if market else None

            if market:
                ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
                with open(raw_dir / f"{event_ticker}_{ts}_market.json", "w") as f:
                    json.dump(market, f, indent=2)
                if orderbook:
                    with open(raw_dir / f"{event_ticker}_{ts}_orderbook.json", "w") as f:
                        json.dump(orderbook, f, indent=2)
                print("OK (single market)")
                results.append({"date": str(d), "ticker": event_ticker, "market": market})
            else:
                print("not found")

        d += timedelta(days=1)
        time.sleep(args.delay)

    print(f"\nFetched {len(results)} markets. Raw data in {raw_dir}")
    return results


if __name__ == "__main__":
    main()
