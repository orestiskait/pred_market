"""Historical data loader for backtesting.

Reads parquet files and produces chronologically-ordered SimEvent objects
that the BacktestEngine replays.  Each SimEvent carries the timestamp at
which the data WOULD HAVE BEEN RECEIVED by the live bot — not the raw
observation time — so latency is baked in.

DATA LEAKAGE PREVENTION
========================
* Weather observations are ordered by ``received_ts`` (when Synoptic pushed
  the data).  The ob_timestamp is included in the event payload but is NOT
  used for timeline ordering — this mirrors production where the bot only
  reacts when the WS message arrives.

* Orderbook snapshots are ordered by ``snapshot_ts`` (when the Kalshi WS
  sent the snapshot).

* Market discovery events are synthesised from ``kalshi_market_snapshots``:
  the first snapshot per event_ticker on a given day defines the contracts.
  Backtesting does NOT call the Kalshi REST API.

* Events are merged into a single sorted timeline.  The engine processes
  them strictly in this order, so a strategy can never see futures data.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum, auto
from typing import Any

import pandas as pd

from services.core.storage import ParquetStorage

logger = logging.getLogger("backtest.data_loader")


# ======================================================================
# SimEvent — timeline atom
# ======================================================================

class SimEventType(Enum):
    """Types of events the backtester can replay."""
    MARKET_DISCOVERY = auto()
    WEATHER_OBSERVATION = auto()
    ORDERBOOK_UPDATE = auto()


@dataclass
class SimEvent:
    """One atomic event on the backtesting timeline.

    Attributes:
        wall_clock: The wall-clock time this event would have been RECEIVED
                    by the live bot.  Used for timeline ordering only.
        event_type: What kind of event this is.
        payload:    Type-specific data dict.
    """
    wall_clock: datetime
    event_type: SimEventType
    payload: dict = field(default_factory=dict)


# ======================================================================
# DataLoader
# ======================================================================

class DataLoader:
    """Load historical parquet data and build a unified SimEvent timeline.

    Parameters
    ----------
    data_dir : str
        Root data directory (same as ``storage.data_dir`` in config.yaml).
    start_date, end_date : date
        Inclusive range of calendar dates to load.
    series_filter : list[str] | None
        If given, only include tickers matching these series prefixes.
    latency_model : str
        How to model Synoptic latency:
        - ``"actual"``: use ``received_ts`` from the parquet (default).
        - ``"fixed_N"``: shift ``ob_timestamp`` by N seconds (e.g. ``"fixed_180"``).
          Useful when testing sensitivity to latency assumptions.
    """

    def __init__(
        self,
        data_dir: str,
        start_date: date,
        end_date: date,
        series_filter: list[str] | None = None,
        latency_model: str = "actual",
    ):
        self.storage = ParquetStorage(data_dir)
        self.start_date = start_date
        self.end_date = end_date
        self.series_filter = series_filter
        self.latency_model = latency_model

    # ------------------------------------------------------------------
    # Internal loaders
    # ------------------------------------------------------------------

    def _load_weather_events(self) -> list[SimEvent]:
        """Load Synoptic weather observations → SimEvents."""
        df = self.storage.read_parquets("synoptic_ws", self.start_date, self.end_date)
        if df.empty:
            logger.warning("No Synoptic data for %s → %s", self.start_date, self.end_date)
            return []

        events: list[SimEvent] = []
        for _, row in df.iterrows():
            # Wall-clock = when the bot received the message
            if self.latency_model == "actual":
                wall_clock = row["received_ts"]
            elif self.latency_model.startswith("fixed_"):
                seconds = int(self.latency_model.split("_")[1])
                wall_clock = row["ob_timestamp"] + pd.Timedelta(seconds=seconds)
            else:
                wall_clock = row["received_ts"]

            events.append(SimEvent(
                wall_clock=wall_clock.to_pydatetime(),
                event_type=SimEventType.WEATHER_OBSERVATION,
                payload={
                    "stid": row["stid"],
                    "value": float(row["value"]),
                    "ob_timestamp": row["ob_timestamp"].isoformat(),
                },
            ))
        logger.info("Loaded %d weather observations", len(events))
        return events

    def _load_orderbook_events(self) -> list[SimEvent]:
        """Load reconstructed orderbook snapshots → SimEvents.

        Uses ``reconstruct_orderbooks()`` so baseline+delta compression is
        transparent — the strategy sees the same full orderbook state it
        would in production after the mixin applies deltas.
        """
        df = self.storage.reconstruct_orderbooks(self.start_date, self.end_date)
        if df.empty:
            logger.warning("No orderbook data for %s → %s", self.start_date, self.end_date)
            return []

        if self.series_filter:
            mask = pd.Series(False, index=df.index)
            for s in self.series_filter:
                mask |= df["market_ticker"].str.startswith(s)
            df = df[mask]

        events: list[SimEvent] = []
        for ts, group in df.groupby("snapshot_ts"):
            # Build per-ticker orderbook dicts
            ob_by_ticker: dict[str, dict] = {}
            for _, row in group.iterrows():
                tk = row["market_ticker"]
                if tk not in ob_by_ticker:
                    ob_by_ticker[tk] = {"yes": {}, "no": {}}
                ob_by_ticker[tk][row["side"]][int(row["price_cents"])] = float(row["quantity"])

            # Emit one SimEvent per ticker (same as live bot — one OrderbookUpdateEvent per ticker)
            for tk, ob in ob_by_ticker.items():
                events.append(SimEvent(
                    wall_clock=ts.to_pydatetime() if hasattr(ts, "to_pydatetime") else ts,
                    event_type=SimEventType.ORDERBOOK_UPDATE,
                    payload={
                        "market_ticker": tk,
                        "orderbook": ob,
                    },
                ))
        logger.info("Loaded %d orderbook update events", len(events))
        return events

    def _load_market_discovery_events(self) -> list[SimEvent]:
        """Synthesise MarketDiscoveryEvents from market snapshots.

        For each unique event_ticker on each day, discover the full set of
        market tickers and their cap_strike / subtitle metadata from the
        FIRST snapshot of the day.  This mimics the bot's _discover() at
        startup.
        """
        df = self.storage.read_parquets("market", self.start_date, self.end_date)
        if df.empty:
            logger.warning("No market snapshot data for %s → %s", self.start_date, self.end_date)
            return []

        if self.series_filter:
            mask = pd.Series(False, index=df.index)
            for s in self.series_filter:
                mask |= df["market_ticker"].str.startswith(s)
            df = df[mask]

        events: list[SimEvent] = []
        df = df.sort_values("snapshot_ts")

        # Group by calendar date (UTC) to emit one discovery per day
        df["_date"] = df["snapshot_ts"].dt.date
        for d, day_df in df.groupby("_date"):
            first_ts = day_df["snapshot_ts"].min()
            # Deduplicate tickers — keep first occurrence
            seen: set[str] = set()
            market_tickers: list[str] = []
            market_info: dict[str, dict] = {}

            for _, row in day_df[day_df["snapshot_ts"] == first_ts].iterrows():
                tk = row["market_ticker"]
                if tk in seen:
                    continue
                seen.add(tk)
                market_tickers.append(tk)

                # Parse cap_strike from subtitle (e.g. "43° or above" → 43, "39° to 40°" → 40)
                subtitle = row.get("subtitle", "")
                cap_strike = self._parse_cap_strike(subtitle)

                market_info[tk] = {
                    "event_ticker": row.get("event_ticker", ""),
                    "subtitle": subtitle,
                    "yes_bid": int(row.get("yes_bid", 0)),
                    "yes_ask": int(row.get("yes_ask", 0)),
                    "no_bid": int(row.get("no_bid", 0) if "no_bid" in row.index else 100 - int(row.get("yes_ask", 0))),
                    "no_ask": int(row.get("no_ask", 0) if "no_ask" in row.index else 100 - int(row.get("yes_bid", 0))),
                    "last_price": int(row.get("last_price", 0)),
                    "volume": int(row.get("volume", 0)),
                    "open_interest": int(row.get("open_interest", 0)),
                    "cap_strike": cap_strike,
                }

            if market_tickers:
                events.append(SimEvent(
                    wall_clock=first_ts.to_pydatetime() if hasattr(first_ts, "to_pydatetime") else first_ts,
                    event_type=SimEventType.MARKET_DISCOVERY,
                    payload={
                        "market_tickers": market_tickers,
                        "market_info": market_info,
                    },
                ))
        logger.info("Loaded %d market discovery events", len(events))
        return events

    @staticmethod
    def _parse_cap_strike(subtitle: str) -> float | None:
        """Parse cap_strike from Kalshi contract subtitle.

        Examples:
            "43° or above" → 43.0
            "39° to 40°"   → 40.0   (cap of the bracket)
            "Below 30°"    → 30.0
        """
        if not subtitle:
            return None
        import re
        # "X° or above" patterns
        m = re.match(r"(\d+(?:\.\d+)?)°?\s+or\s+above", subtitle, re.IGNORECASE)
        if m:
            return float(m.group(1))
        # "X° to Y°" patterns → cap is Y
        m = re.match(r"(\d+(?:\.\d+)?)°?\s+to\s+(\d+(?:\.\d+)?)°?", subtitle, re.IGNORECASE)
        if m:
            return float(m.group(2))
        # "Below X°"
        m = re.match(r"below\s+(\d+(?:\.\d+)?)°?", subtitle, re.IGNORECASE)
        if m:
            return float(m.group(1))
        # Fallback: try to find any number
        nums = re.findall(r"(\d+(?:\.\d+)?)", subtitle)
        return float(nums[-1]) if nums else None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def load_timeline(self) -> list[SimEvent]:
        """Load all data and return a merged, chronologically-sorted timeline.

        Returns a list of SimEvent sorted by wall_clock.  Ties are broken by
        event type priority: MARKET_DISCOVERY < ORDERBOOK_UPDATE < WEATHER_OBS
        so that the orderbook state is always current before a strategy trigger.

        STARTUP ORDERING:
            In production, the bot calls ``_discover()`` BEFORE starting the
            WebSocket feeds.  To replicate this, each day's first market
            discovery event is timestamped to the earliest event on that day,
            guaranteeing the ladder is built before any weather data flows in.
        """
        weather = self._load_weather_events()
        orderbook = self._load_orderbook_events()
        discovery = self._load_market_discovery_events()

        # ── Ensure discovery precedes all other events on each day ──
        # In production, _discover() runs synchronously before any WS feeds.
        # The market snapshot may timestamp a few seconds after the first
        # weather push.  Fix: set each discovery's wall_clock to the MINIMUM
        # of all events on that calendar date.
        all_data_events = orderbook + weather
        if all_data_events and discovery:
            from collections import defaultdict
            earliest_by_date: dict[date, datetime] = {}
            for e in all_data_events:
                d = e.wall_clock.date()
                if d not in earliest_by_date or e.wall_clock < earliest_by_date[d]:
                    earliest_by_date[d] = e.wall_clock
            for disc in discovery:
                d = disc.wall_clock.date()
                if d in earliest_by_date:
                    disc.wall_clock = earliest_by_date[d]

        all_events = discovery + all_data_events

        # Sort: primary = wall_clock, secondary = event type priority
        # Discovery must come first, then OB, then weather (matching live startup order)
        type_priority = {
            SimEventType.MARKET_DISCOVERY: 0,
            SimEventType.ORDERBOOK_UPDATE: 1,
            SimEventType.WEATHER_OBSERVATION: 2,
        }
        all_events.sort(key=lambda e: (e.wall_clock, type_priority[e.event_type]))

        logger.info(
            "Timeline built: %d events (%d discovery, %d orderbook, %d weather) "
            "over %s → %s",
            len(all_events), len(discovery), len(orderbook), len(weather),
            self.start_date, self.end_date,
        )
        return all_events
