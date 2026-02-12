#!/usr/bin/env python3
"""
Compare Kalshi odds (from saved raw data) vs NWS actual high temps.

For each past day:
1. Load saved Kalshi market/orderbook from data/raw/
2. Load NWS actual high from data/processed/actuals.csv (or NWS API)
3. Compute: which contract resolved YES, implied prob vs binary outcome
4. Output analysis for calibration, Brier score, etc.

Usage:
    python compare_odds_vs_actual.py
"""

import json
from pathlib import Path

# Paths relative to exploration/
EXPLORATION_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = EXPLORATION_DIR / "data" / "raw"
PROCESSED_DIR = EXPLORATION_DIR / "data" / "processed"


def load_raw_markets() -> list[dict]:
    """Load all saved market JSON files from data/raw/."""
    markets = []
    for p in RAW_DIR.glob("*_market.json"):
        with open(p) as f:
            m = json.load(f)
        markets.append(m)
    return markets


def extract_contract_odds(market: dict) -> list[dict]:
    """
    Extract contract outcomes and yes-bid/yes-ask from market.
    Returns list of {outcome, yes_bid, yes_ask, ticker}.
    """
    contracts = market.get("contracts", [])
    if not contracts:
        return []

    result = []
    for c in contracts:
        result.append({
            "outcome": c.get("outcome", c.get("title", "?")),
            "yes_bid": c.get("yes_bid"),
            "yes_ask": c.get("yes_ask"),
            "ticker": c.get("ticker", ""),
        })
    return result


def parse_temp_range(outcome: str) -> tuple[float, float] | None:
    """
    Parse outcome string like "40-45°F" or "Under 40°F" into (low, high).
    Returns None if not parseable.
    """
    # TODO: Implement parsing for Kalshi NYC temp outcome strings
    return None


def which_contract_wins(actual_high_f: float, contracts: list[dict]) -> str | None:
    """
    Given actual high (F) and list of contracts, return ticker of winning contract.
    """
    for c in contracts:
        r = parse_temp_range(c["outcome"])
        if r and r[0] <= actual_high_f < r[1]:
            return c["ticker"]
    return None


def main():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    markets = load_raw_markets()
    print(f"Loaded {len(markets)} market snapshots")

    # TODO: Load actuals from data/processed/actuals.csv or NWS
    actuals_path = PROCESSED_DIR / "actuals.csv"
    if not actuals_path.exists():
        print(f"\nNo actuals file at {actuals_path}")
        print("Create it with columns: date,high_f,station")
        print("Example:\n  2026-02-10,42,KNYC\n  2026-02-09,38,KNYC")
        return

    # Placeholder analysis loop
    for m in markets:
        ticker = m.get("ticker", "?")
        contracts = extract_contract_odds(m)
        print(f"\n{ticker}: {len(contracts)} contracts")
        for c in contracts[:5]:
            print(f"  {c['outcome']}: bid={c['yes_bid']} ask={c['yes_ask']}")


if __name__ == "__main__":
    main()
