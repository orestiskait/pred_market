"""Event-ticker resolution and market discovery via the Kalshi REST API.

These helpers query the Kalshi API at startup (and periodically) to find
active event tickers and their associated contracts.  They are deliberately
stateless — call them to get fresh data whenever needed.

Event selection strategies
--------------------------
- ``active``: Pick the event with earliest close_time (today's market in that
  timezone). Best for same-day trading.
- ``next``: Pick the event whose strike_date is the next local calendar day
  (or today). Enables trading tomorrow's market when it opens (e.g., Feb 22
  market tradeable on Feb 21). Uses each market's NWS-aligned timezone.
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from zoneinfo import ZoneInfo

from services.markets.registry import MarketConfig, MARKET_REGISTRY, market_for_series

logger = logging.getLogger(__name__)


def _parse_strike_date(strike_date: str | None) -> date | None:
    """Parse strike_date from API (ISO date or datetime) to date."""
    if not strike_date:
        return None
    try:
        if "T" in strike_date:
            return datetime.fromisoformat(strike_date.replace("Z", "+00:00")).date()
        return date.fromisoformat(strike_date[:10])
    except (ValueError, TypeError):
        return None


def _select_event_for_series(
    events: list[dict],
    series: str,
    strategy: str,
) -> list[str]:
    """Pick event(s) from the list based on strategy. Returns a list of event_tickers."""
    if not events:
        return []
    mc = market_for_series(series)
    tz = ZoneInfo(mc.tz)
    today_local = datetime.now(tz).date()

    if strategy == "next":
        # Pick event whose strike_date is >= today (local). Enables pre-trading
        # tomorrow's market when it opens (e.g., Feb 22 tradeable on Feb 21).
        candidates = []
        for e in events:
            sd = _parse_strike_date(e.get("strike_date"))
            if sd is None:
                continue
            if sd >= today_local:
                candidates.append((e, sd))
        if candidates:
            candidates.sort(key=lambda x: (x[1], x[0].get("event_ticker", "")))
            chosen = candidates[0][0]["event_ticker"]
            logger.info("  [next] %s → %s (strike_date >= %s)", series, chosen, today_local)
            return [chosen]
        # Fall through to active if no future events

    if strategy == "consecutive":
        # Return up to 2 active/open events (today and tomorrow) to track simultaneously
        events.sort(key=lambda e: (
            e.get("close_time") or "",
            e.get("strike_date") or "",
            e.get("event_ticker", ""),
        ))
        chosen = [e["event_ticker"] for e in events[:2]]
        logger.info("  [consecutive] %s → %s", series, chosen)
        return chosen

    # active: earliest close_time (or strike_date, or event_ticker)
    events.sort(key=lambda e: (
        e.get("close_time") or "",
        e.get("strike_date") or "",
        e.get("event_ticker", ""),
    ))
    return [events[0]["event_ticker"]]


def resolve_event_tickers(
    rest_client,
    config: dict,
    event_selection: str | None = None,
    consumer: str | None = None,
) -> list[str]:
    """Return event tickers from ``event_series`` and/or ``events`` config keys.

    For each series prefix, queries the Kalshi API for open events and picks
    one based on ``event_selection`` (or config event_rollover.event_selection):
    - ``active``: earliest close_time (today's market)
    - ``next``: strike_date >= today in market's local tz (enables pre-trading)

    consumer: kalshi_listener, weather_bot, etc. Used to select the right
    event_series list when config uses per-consumer keys.

    Timezone awareness
    ------------------
    Different markets settle on different *local* days (NWS standard). The
    ``MarketConfig.tz`` field ensures correct local-day logic per market.
    """
    from services.core.config import get_event_series

    strategy = (
        event_selection
        or config.get("event_rollover", {}).get("event_selection", "consecutive")
    )
    tickers: list[str] = []

    series_list = get_event_series(config, consumer or "default")
    for series in series_list:
        logger.info("Resolving series %s → open events (strategy=%s)", series, strategy)
        events = rest_client.get_events_for_series(series, status="open")
        if not events:
            logger.warning("  No open events found for series %s", series)
            continue
        chosen = _select_event_for_series(events, series, strategy)
        if chosen:
            logger.info("  → %s (from %d open event(s))", chosen, len(events))
            tickers.extend(chosen)

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


def nws_observation_period(event_ticker: str, tz_name: str) -> tuple[datetime, datetime]:
    """Return the exact NWS observation period (start_utc, end_utc) for this event.

    NWS records daily high from midnight LST to midnight LST. Since LST does
    not observe Daylight Saving Time, during the summer the observation bounds
    actually map to 1:00 AM to 12:59 AM local time.
    """
    from datetime import timedelta, timezone
    # e.g. KXHIGHCHI-26FEB21 -> '26FEB21' (length 7)
    suffix = event_ticker.split("-")[-1]
    target_date = datetime.strptime(suffix[:7], "%y%b%d").date()

    # Find the Standard Time offset for this timezone by checking Jan 1
    jan1 = datetime(target_date.year, 1, 1, tzinfo=ZoneInfo(tz_name))
    std_offset = jan1.utcoffset()
    if std_offset is None:
        raise ValueError(f"Could not determine standard offset for {tz_name}")

    # LST is effectively a fixed-offset timezone exactly matching the standard offset
    lst_tz = timezone(std_offset)
    
    start_lst = datetime(target_date.year, target_date.month, target_date.day, 0, 0, 0, tzinfo=lst_tz)
    end_lst = start_lst + timedelta(days=1) - timedelta(microseconds=1)

    return start_lst.astimezone(timezone.utc), end_lst.astimezone(timezone.utc)
