"""Timezone utilities — single source of truth for project-wide UTC convention.

RULE: All datetime values in this project are stored and compared in UTC.
      Local timezones are ONLY used at the boundary when computing
      "local day" for station-specific weather observations.

This module provides convenience helpers so no file ever needs to call
  datetime.now()     (naive — system-local, FORBIDDEN)
  date.today()       (naive — system-local, FORBIDDEN)

Instead, import from here:
  from collector.tz import utc_now, utc_today
"""

from datetime import date, datetime, timezone


def utc_now() -> datetime:
    """Return the current moment as a timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def utc_today() -> date:
    """Return today's date in UTC (not system-local time)."""
    return datetime.now(timezone.utc).date()
