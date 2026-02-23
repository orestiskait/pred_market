"""Synoptic REST API polling — alternative to WebSocket streaming.

Fetches recent observations from Synoptic Time Series API.
Uses same storage schema as WebSocket (synoptic_weather_observations/).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta

import requests

logger = logging.getLogger(__name__)

SYNOPTIC_TIMESERIES_URL = "https://api.synopticdata.com/v2/stations/timeseries"


def fetch_synoptic_recent(
    stations: list[str],
    token: str,
    recent_minutes: int = 120,
    timeout: int = 30,
) -> list[dict]:
    """Fetch recent Synoptic observations via REST API.

    Returns rows for write_synoptic_ws schema: received_ts, ob_timestamp,
    stid, sensor, value, source="live".
    """
    if not stations:
        return []

    received_ts = datetime.now(timezone.utc)
    params = {
        "token": token,
        "stid": ",".join(stations),
        "recent": recent_minutes,
        "vars": "air_temp",
        "units": "english",
        "obtimezone": "UTC",
    }

    try:
        resp = requests.get(SYNOPTIC_TIMESERIES_URL, params=params, timeout=timeout)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("Synoptic REST fetch failed: %s", e)
        return []

    if "STATION" not in data or not data["STATION"]:
        return []

    rows = []
    for stn in data["STATION"]:
        stid = stn.get("STID", "")
        if not stid:
            continue
        obs = stn.get("OBSERVATIONS", {})
        times = obs.get("date_time", [])
        temp_key = next((k for k in obs if k.startswith("air_temp_set_")), None)
        if not temp_key or not times:
            continue

        temps = obs[temp_key]
        for t, val in zip(times, temps):
            if val is None:
                continue
            try:
                ob_ts = datetime.fromisoformat(t.replace("Z", "+00:00"))
                rows.append({
                    "received_ts": received_ts,
                    "ob_timestamp": ob_ts,
                    "stid": stid,
                    "sensor": temp_key,
                    "value": float(val),
                    "source": "live",
                })
            except (ValueError, TypeError):
                continue

    return rows
