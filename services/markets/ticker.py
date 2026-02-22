"""Event-ticker resolution and market discovery via the Kalshi REST API.

These helpers query the Kalshi API at startup (and periodically) to find
active event tickers and their associated contracts.  They are deliberately
stateless — call them to get fresh data whenever needed.

Event selection strategies
--------------------------
- ``active``: Among all open events, pick the one with earliest close_time
  (then strike_date, then event_ticker). Typically today's market.
- ``next``: Among events with strike_date >= today (market's local tz), pick
  the one with earliest strike_date. Excludes past-dated events. When both
  today and tomorrow are open, picks today. When only tomorrow is open,
  picks tomorrow (pre-trading). Falls back to active if no future events.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

from services.markets.kalshi_registry import KalshiMarketConfig, KALSHI_MARKET_REGISTRY, market_for_series

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
) -> str | None:
    """Pick one event from the list based on strategy. Returns event_ticker or None."""
    if not events:
        return None
    mc = market_for_series(series)
    tz = ZoneInfo(mc.tz)
    today_local = datetime.now(tz).date()

    if strategy == "next":
        # Among events with strike_date >= today (local), pick earliest.
        # Excludes past-dated events. When only tomorrow is open, picks it.
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
            return chosen
        # Fall through to active if no future events

    # active: earliest close_time (or strike_date, or event_ticker)
    events.sort(key=lambda e: (
        e.get("close_time") or "",
        e.get("strike_date") or "",
        e.get("event_ticker", ""),
    ))
    return events[0]["event_ticker"]


def resolve_event_tickers(
    rest_client,
    config: dict,
    event_selection: str | None = None,
    consumer: str | None = None,
) -> list[str]:
    """Return event tickers from ``event_series`` and/or ``events`` config keys.

    For each series prefix, queries the Kalshi API for open events and picks
    one based on ``event_selection`` (or config event_rollover.event_selection):
    - ``active``: earliest close_time among all open events
    - ``next``: earliest strike_date among events with strike_date >= today
      (market's local tz); excludes past-dated events; picks tomorrow when
      today's market has closed

    consumer: kalshi_listener, weather_bot, etc. Used to select the right
    event_series list when config uses per-consumer keys.

    Timezone awareness
    ------------------
    Different markets settle on different *local* days (NWS standard). The
    ``KalshiMarketConfig.tz`` field ensures correct local-day logic per market.
    """
    from services.core.config import get_event_series

    strategy = (
        event_selection
        or config.get("event_rollover", {}).get("event_selection", "active")
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
            yes_bid = m.get("yes_bid") or 0
            yes_ask = m.get("yes_ask") or 0
            no_bid = m.get("no_bid")
            no_ask = m.get("no_ask")
            if no_bid is None:
                no_bid = int(round(100 - yes_ask))
            if no_ask is None:
                no_ask = int(round(100 - yes_bid))
            market_info[tk] = {
                "event_ticker": event_ticker,
                "subtitle": m.get("subtitle", ""),
                "yes_bid": int(round(yes_bid)),
                "yes_ask": int(round(yes_ask)),
                "no_bid": int(round(no_bid)),
                "no_ask": int(round(no_ask)),
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
    """Return the NWS climate-day observation window in UTC for an event ticker.

    The NWS records climate data in **Local Standard Time (LST)** year-round.
    The climate day runs midnight-to-midnight LST (see docs/events/kalshi_settlement_rules.md).

    Parameters
    ----------
    event_ticker : str
        E.g. ``"KXHIGHCHI-26FEB21"`` — the date suffix is parsed to determine
        which calendar day the market covers.
    tz_name : str
        IANA timezone for the station (e.g. ``"America/Chicago"``).  The
        Standard Time UTC offset is derived from this (winter Jan 15 trick).

    Returns
    -------
    (nws_start_utc, nws_end_utc) : tuple[datetime, datetime]
        The UTC times corresponding to midnight-to-midnight in Local Standard Time
        for the event's calendar day.

    Example
    -------
    >>> nws_observation_period("KXHIGHCHI-26FEB21", "America/Chicago")
    (datetime(2026, 2, 21, 6, 0, tzinfo=UTC), datetime(2026, 2, 22, 6, 0, tzinfo=UTC))
    # CST = UTC-6, so midnight CST = 06:00 UTC
    """
    # Parse the date from the event ticker suffix (e.g. "26FEB21" → Feb 21, 2026)
    parts = event_ticker.split("-")
    if len(parts) >= 2:
        date_suffix = parts[-1]  # e.g. "26FEB21"
        # Format: YYMMMDD — but actually the Kalshi format looks like "26FEB21"
        # which is century + month + day.  Let's parse more carefully.
        # Could be "T42" or "B39.5" for contract market tickers — need to find the date part.
        # Walk backwards through parts until we find one that looks like a date.
        date_part = None
        for p in parts[1:]:
            # Date suffix has 3-letter month embedded, e.g. "26FEB21"
            import re
            m = re.match(r'^(\d{2})([A-Z]{3})(\d{2})$', p)
            if m:
                date_part = p
                break
        if date_part is None:
            # Fallback: use today
            logger.warning("Cannot parse date from event ticker %s; using today", event_ticker)
            tz = ZoneInfo(tz_name)
            event_date = datetime.now(tz).date()
        else:
            # Parse "26FEB21" → 2026-02-21
            century_prefix = date_part[:2]  # "26"
            month_str = date_part[2:5]      # "FEB"
            day_str = date_part[5:7]        # "21"
            month_map = {
                "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
                "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
            }
            year = 2000 + int(century_prefix)
            month = month_map.get(month_str, 1)
            day = int(day_str)
            event_date = date(year, month, day)
    else:
        logger.warning("Cannot parse event ticker %s; using today", event_ticker)
        tz = ZoneInfo(tz_name)
        event_date = datetime.now(tz).date()

    # Compute the Standard Time UTC offset.
    # NWS always uses LST, even during DST.  The "Jan 15 trick":
    # use a winter date to get the standard offset (no DST).
    tz = ZoneInfo(tz_name)
    winter_dt = datetime(event_date.year, 1, 15, 12, 0, tzinfo=tz)
    lst_utc_offset = winter_dt.utcoffset()  # e.g. -6h for CST

    # Climate day: midnight LST on event_date → midnight LST on event_date + 1
    from datetime import timedelta
    midnight_lst = datetime(event_date.year, event_date.month, event_date.day, 0, 0, 0)
    nws_start_utc = (midnight_lst - lst_utc_offset).replace(tzinfo=timezone.utc)
    nws_end_utc = nws_start_utc + timedelta(hours=24)

    return nws_start_utc, nws_end_utc
