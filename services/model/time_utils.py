"""LST (Local Standard Time) timezone utilities.

LST deliberately ignores DST year-round, which matches how Kalshi and NWS
define the climate day boundary.  For example, Chicago is always UTC−6 in
LST regardless of whether CDT is in effect.

All public functions are pure / deterministic — no external API calls.
"""

from __future__ import annotations

from datetime import datetime, timezone, timedelta, date
from typing import Optional

import pandas as pd


# ──────────────────────────────────────────────────────────────────────
# LST offset table  (standard offset, ignoring DST year-round)
# ──────────────────────────────────────────────────────────────────────

_LST_OFFSETS: dict[str, int] = {
    # UTC offset in hours (negative = west)
    "America/New_York":    -5,
    "America/Chicago":     -6,
    "America/Denver":      -7,
    "America/Los_Angeles": -8,
    "America/Phoenix":     -7,  # Arizona never observes DST
}


def lst_offset_hours(tz: str) -> int:
    """Return the standard UTC offset (hours) for a given IANA timezone.

    Uses a static table rather than a live tz library so the LST concept
    is explicit and never influenced by real DST transitions.

    Raises ValueError for unsupported timezones.
    """
    if tz not in _LST_OFFSETS:
        supported = ", ".join(sorted(_LST_OFFSETS))
        raise ValueError(
            f"Unsupported timezone {tz!r} for LST conversion. "
            f"Supported: {supported}"
        )
    return _LST_OFFSETS[tz]


def utc_to_lst(dt_utc: datetime, tz: str) -> datetime:
    """Convert a UTC datetime to LST (no DST adjustment).

    Parameters
    ----------
    dt_utc: timezone-aware UTC datetime
    tz:     IANA timezone string (must be in _LST_OFFSETS)

    Returns a *naive* datetime representing LST wall-clock time.
    """
    offset = timedelta(hours=lst_offset_hours(tz))
    return dt_utc.astimezone(timezone.utc).replace(tzinfo=None) + offset


def lst_climate_date(dt_utc: datetime, tz: str) -> date:
    """Return the LST climate date for a UTC timestamp.

    The climate date is the calendar date of the LST midnight-to-midnight
    window.  Two UTC timestamps on opposite sides of the LST midnight
    boundary belong to DIFFERENT climate dates even if they share the
    same UTC calendar date.
    """
    return utc_to_lst(dt_utc, tz).date()


def lst_midnight_utc(climate_date: date, tz: str) -> datetime:
    """Return the UTC timestamp corresponding to midnight LST on *climate_date*.

    This is the START of the climate day (inclusive lower bound).
    """
    offset = timedelta(hours=lst_offset_hours(tz))
    midnight_lst = datetime(climate_date.year, climate_date.month, climate_date.day)
    return (midnight_lst - offset).replace(tzinfo=timezone.utc)


def climate_day_end_utc(climate_date: date, tz: str) -> datetime:
    """Return the UTC timestamp of the last second of an LST climate day.

    This equals midnight_utc of the *next* climate day (exclusive upper bound).
    """
    from datetime import timedelta as td
    from datetime import date as _date
    next_date = date.fromordinal(climate_date.toordinal() + 1)
    return lst_midnight_utc(next_date, tz)


def hours_since_midnight_lst(dt_utc: datetime, tz: str) -> float:
    """Return decimal hours elapsed since midnight LST for a UTC timestamp."""
    lst_dt = utc_to_lst(dt_utc, tz)
    return lst_dt.hour + lst_dt.minute / 60.0 + lst_dt.second / 3600.0


def series_to_lst_climate_date(
    ts_series: "pd.Series",
    tz: str,
) -> "pd.Series":
    """Vectorised: convert a UTC-aware pandas Series to LST climate date strings.

    Returns a Series of date strings (YYYY-MM-DD) in LST.
    """
    offset = pd.Timedelta(hours=lst_offset_hours(tz))
    lst_series = ts_series.dt.tz_localize(None) if ts_series.dt.tz is not None else ts_series
    # Remove tz info, add LST offset
    lst_naive = ts_series.dt.tz_convert("UTC").dt.tz_localize(None) + offset
    return lst_naive.dt.date.astype(str)


def get_latest_record_per_date(df: "pd.DataFrame") -> "pd.DataFrame":
    """Return one row per ``for_date_lst`` — the row with the latest ``received_ts_utc``.

    Designed for CLI/DSM parquet DataFrames where multiple records may be
    stored for the same LST date (e.g. a correction issued later in the day).
    The row whose ``received_ts_utc`` is greatest is kept; all others are dropped.

    Safe to call on an empty DataFrame or one missing either key column — the
    original DataFrame is returned unchanged in those cases.
    """
    if (
        df.empty
        or "for_date_lst" not in df.columns
        or "received_ts_utc" not in df.columns
    ):
        return df
    # Sort ascending so drop_duplicates(keep="last") retains the most-recent row.
    return (
        df.sort_values("received_ts_utc")
        .drop_duplicates("for_date_lst", keep="last")
        .reset_index(drop=True)
    )
