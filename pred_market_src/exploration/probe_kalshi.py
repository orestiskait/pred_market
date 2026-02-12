#!/usr/bin/env python3
"""Probe Kalshi API to debug market/event structure for kxhighny."""
import json
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import KALSHI_API_BASE, KXHIGHNY_SERIES

def main():
    print(f"API Base: {KALSHI_API_BASE}\n")

    # 1. GET /series/KXHIGHNY
    print("1. GET /series/KXHIGHNY")
    r = requests.get(f"{KALSHI_API_BASE}/series/{KXHIGHNY_SERIES}", timeout=10)
    print(f"   Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(f"   Series: {json.dumps(data.get('series', {}), indent=2)[:500]}...")
    else:
        print(f"   Body: {r.text[:200]}")
    print()

    # 2. GET /markets?series_ticker=KXHIGHNY
    print("2. GET /markets?series_ticker=KXHIGHNY (limit=5)")
    r = requests.get(f"{KALSHI_API_BASE}/markets", params={"series_ticker": KXHIGHNY_SERIES, "limit": 5}, timeout=10)
    print(f"   Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        markets = data.get("markets", [])
        print(f"   Markets count: {len(markets)}")
        for m in markets[:3]:
            print(f"   - ticker={m.get('ticker')} event_ticker={m.get('event_ticker')}")
    else:
        print(f"   Body: {r.text[:200]}")
    print()

    # 3. GET /events/kxhighny-26feb11
    print("3. GET /events/kxhighny-26feb11")
    r = requests.get(f"{KALSHI_API_BASE}/events/kxhighny-26feb11", timeout=10)
    print(f"   Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        print(f"   Keys: {list(data.keys())}")
        markets = data.get("markets", [])
        print(f"   Markets: {len(markets)}")
    else:
        print(f"   Body: {r.text[:200]}")
    print()

    # 4. GET /markets?event_ticker=kxhighny-26feb11
    print("4. GET /markets?event_ticker=kxhighny-26feb11")
    r = requests.get(f"{KALSHI_API_BASE}/markets", params={"event_ticker": "kxhighny-26feb11"}, timeout=10)
    print(f"   Status: {r.status_code}")
    if r.status_code == 200:
        data = r.json()
        markets = data.get("markets", [])
        print(f"   Markets count: {len(markets)}")
    else:
        print(f"   Body: {r.text[:200]}")
    print()

    # 5. GET /markets/kxhighny-26feb11
    print("5. GET /markets/kxhighny-26feb11")
    r = requests.get(f"{KALSHI_API_BASE}/markets/kxhighny-26feb11", timeout=10)
    print(f"   Status: {r.status_code}")
    if r.status_code == 200:
        print("   OK")
    else:
        print(f"   Body: {r.text[:200]}")

if __name__ == "__main__":
    main()
