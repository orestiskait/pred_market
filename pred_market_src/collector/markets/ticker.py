"""Event-ticker resolution and market discovery via the Kalshi REST API.

These helpers query the Kalshi API at startup to find today's active
event tickers and their associated contracts.  They are deliberately
stateless — call them to get fresh data whenever needed.
"""

from __future__ import annotations

import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from .registry import MarketConfig, MARKET_REGISTRY, market_for_series

logger = logging.getLogger(__name__)


def resolve_event_tickers(
    rest_client,
    config: dict,
) -> list[str]:
    """Return event tickers from ``event_series`` and/or ``events`` config keys.

    For each series prefix, queries the Kalshi API for open events and picks
    the one with the earliest close_time (actively trading today).

    Timezone awareness
    ------------------
    Different markets settle on different *local* days.  The ``MarketConfig.tz``
    field ensures we can always tell which local day an event belongs to.
    This function doesn't need to use local tz itself (the Kalshi API returns
    globally-sorted events), but callers can use the registry to convert.
    """
    tickers: list[str] = []

    for series in config.get("event_series", []):
        logger.info("Resolving series %s → open events", series)
        events = rest_client.get_events_for_series(series, status="open")
        if not events:
            logger.warning("  No open events found for series %s", series)
            continue
        events.sort(key=lambda e: e.get("close_time") or e.get("event_ticker", ""))
        chosen = events[0]["event_ticker"]
        logger.info("  → %s (%d open event(s) found)", chosen, len(events))
        tickers.append(chosen)

    # Allow explicit overrides
    tickers.extend(config.get("events", []))
    return tickers


def discover_markets(
    rest_client,
    event_tickers: list[str],
) -> tuple[list[str], dict[str, dict]]:
    """Fetch contracts for each event ticker.

    Returns ``(market_tickers, market_info)`` where *market_info* maps each
    ticker to its metadata dict (event_ticker, subtitle, yes_bid, etc.).
    """
    market_tickers: list[str] = []
    market_info: dict[str, dict] = {}

    for event_ticker in event_tickers:
        logger.info("Discovering markets for %s", event_ticker)
        markets = rest_client.get_markets_for_event(event_ticker)
        for m in markets:
            tk = m["ticker"]
            market_tickers.append(tk)
            market_info[tk] = {
                "event_ticker": event_ticker,
                "subtitle": m.get("subtitle", ""),
                "yes_bid": m.get("yes_bid", 0),
                "yes_ask": m.get("yes_ask", 0),
                "last_price": m.get("last_price", 0),
                "volume": m.get("volume", 0),
                "open_interest": m.get("open_interest", 0),
                "cap_strike": m.get("cap_strike"),
            }
        logger.info("  %d contracts found", len(markets))

    logger.info("Tracking %d total contracts", len(market_tickers))
    return market_tickers, market_info


def local_date_for_market(series_prefix: str) -> str:
    """Return today's date in the market's local timezone (YYYY-MM-DD).

    Useful when constructing event ticker suffixes that use the local date
    (e.g. ``KXHIGHCHI-26FEB21`` where 21 is the local CST day).
    """
    mc = market_for_series(series_prefix)
    tz = ZoneInfo(mc.tz)
    return datetime.now(tz).strftime("%Y-%m-%d")
