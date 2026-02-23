"""Aviation Weather METAR collector: AWC API + NWS api.weather.gov.

Polls Aviation Weather Center (aviationweather.gov) and NWS api.weather.gov
(stations/observations — same data that powers weather.gov/wrh/LowTimeseries).
No WebSocket available; uses short-interval polling (90–120s) to catch new obs quickly.
Uses HTTP conditional GET (If-None-Match/ETag) for AWC when supported.
Sends identifiable User-Agent per API best practices.

Tracks: saved_ts, ob_time_utc (data reference), temp, raw_ob, RMK portion,
6hr and 24hr min/max temperature. Runs as a concurrent task in synoptic-listener.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests

from services.weather.aviationweather_storage import AviationWeatherMetarStorage
from services.weather.metar_parser import MetarParser

logger = logging.getLogger(__name__)

AWC_METAR_URL = "https://aviationweather.gov/api/data/metar"
NWS_OBSERVATIONS_URL = "https://api.weather.gov/stations/{station}/observations"
DEFAULT_USER_AGENT = "WeatherCollectionResearch/1.0 (weather data collection;)"


def _c_to_f(celsius: float | None) -> float | None:
    if celsius is None:
        return None
    return celsius * 9.0 / 5.0 + 32.0


def _fetch_awc_metar(
    stations: list[str],
    hours: int = 2,
    *,
    etag: str | None = None,
    user_agent: str | None = None,
) -> tuple[list[dict], str | None]:
    """Fetch METAR from AWC API. Uses conditional GET (If-None-Match) when etag provided.

    Returns (rows, new_etag). On 304 Not Modified, returns ([], etag) — no body to parse.
    """
    if not stations:
        return [], None
    params = {"ids": ",".join(stations), "format": "json", "hours": hours}
    headers = {}
    if user_agent:
        headers["User-Agent"] = user_agent
    if etag:
        headers["If-None-Match"] = etag
    try:
        resp = requests.get(AWC_METAR_URL, params=params, headers=headers or None, timeout=15)
        if resp.status_code == 304:
            logger.debug("AWC METAR 304 Not Modified (ETag unchanged)")
            return [], etag
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("AWC METAR fetch failed: %s", e)
        return [], None

    if not data:
        return [], resp.headers.get("ETag")

    rows = []
    for obs in data:
        try:
            report_time = pd.to_datetime(obs.get("reportTime"), utc=True)
        except Exception:
            continue

        raw_ob = obs.get("rawOb", "")
        stid = obs.get("icaoId") or obs.get("stationId", "")
        if not stid:
            continue

        parsed = MetarParser.parse(raw_ob)
        temp_c = parsed.temp_c if parsed.temp_high_accuracy else obs.get("temp")

        rows.append({
            "ob_time_utc": report_time,
            "station": stid,
            "source": "awc_metar",
            "temp_c": temp_c,
            "temp_f": _c_to_f(temp_c),
            "raw_ob": raw_ob or None,
            "rmk": parsed.rmk,
        })
    new_etag = resp.headers.get("ETag")
    return rows, new_etag


def _fetch_nws_observations(
    station: str,
    limit: int = 50,
    *,
    user_agent: str | None = None,
) -> list[dict]:
    """Fetch observations from api.weather.gov (powers weather.gov/wrh/LowTimeseries).

    NWS API does not support ETag/Last-Modified for observations; full response each time.
    Returns Cache-Control: max-age=292 (≈5 min) — consider aligning poll interval.
    """
    url = NWS_OBSERVATIONS_URL.format(station=station)
    headers = {"User-Agent": user_agent or DEFAULT_USER_AGENT}
    try:
        resp = requests.get(
            url, params={"limit": limit}, headers=headers, timeout=15
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("NWS observations fetch failed for %s: %s", station, e)
        return []

    features = data.get("features", [])
    rows = []
    for f in features:
        props = f.get("properties", {})
        try:
            ts = props.get("timestamp")
            if not ts:
                continue
            ob_time = pd.to_datetime(ts, utc=True)
        except Exception:
            continue

        temp_obj = props.get("temperature", {})
        temp_c = temp_obj.get("value") if temp_obj else None

        max24 = props.get("maxTemperatureLast24Hours", {})
        min24 = props.get("minTemperatureLast24Hours", {})
        temp_24hr_max_c = max24.get("value") if max24 else None
        temp_24hr_min_c = min24.get("value") if min24 else None

        rows.append({
            "ob_time_utc": ob_time,
            "station": station,
            "source": "nws_observations",
            "temp_c": temp_c,
            "temp_f": _c_to_f(temp_c),
            "raw_ob": props.get("rawMessage"),
            "rmk": None,  # NWS API doesn't provide raw METAR with RMK
            "temp_24hr_min_c": temp_24hr_min_c,
            "temp_24hr_max_c": temp_24hr_max_c,
        })
    return rows


def _compute_rolling_minmax(
    df: pd.DataFrame, ob_col: str, temp_col: str, hours: int
) -> tuple[pd.Series, pd.Series]:
    """Compute rolling min and max over the last `hours` for each row."""
    if df.empty or temp_col not in df.columns or ob_col not in df.columns:
        return pd.Series(dtype=float), pd.Series(dtype=float)

    df = df.sort_values(ob_col).copy()
    df = df.set_index(ob_col)
    window = f"{hours}h"
    rolling = df[temp_col].rolling(window=window, min_periods=1)
    return rolling.min(), rolling.max()


def _add_rolling_minmax(
    df: pd.DataFrame,
    storage: AviationWeatherMetarStorage,
    source: str,
    station: str,
) -> pd.DataFrame:
    """Load recent data, compute 6hr/24hr min/max, add to df."""
    if df.empty or station is None:
        return df

    # Load last 25 hours for this station+source to compute rolling
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=25)
    try:
        existing = storage.read(source, station, start.date(), now.date())
    except Exception:
        existing = pd.DataFrame()

    if existing.empty:
        combined = df.copy()
    else:
        combined = pd.concat([existing, df], ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["ob_time_utc"], keep="last"
    ).sort_values("ob_time_utc")

    ob_col = "ob_time_utc"
    temp_col = "temp_c"
    if temp_col not in combined.columns:
        df["temp_6hr_min_c"] = None
        df["temp_6hr_max_c"] = None
        df["temp_24hr_min_c"] = None
        df["temp_24hr_max_c"] = None
        return df

    for hours, min_col, max_col in [
        (6, "temp_6hr_min_c", "temp_6hr_max_c"),
        (24, "temp_24hr_min_c", "temp_24hr_max_c"),
    ]:
        rmin, rmax = _compute_rolling_minmax(
            combined, ob_col, temp_col, hours
        )
        df[min_col] = df[ob_col].map(lambda t: rmin.loc[t] if t in rmin.index else None)
        df[max_col] = df[ob_col].map(lambda t: rmax.loc[t] if t in rmax.index else None)

    return df


class AviationWeatherMetarCollector:
    """Polls AWC and NWS APIs, saves with latency tracking and rolling min/max."""

    def __init__(self, config: dict, config_dir: Path, get_running: callable):
        self.config = config
        self.config_dir = config_dir
        self._get_running: Callable[[], bool] = get_running

        cfg = config.get("aviationweather_metar_collector", {})
        self.stations = cfg.get("stations", ["KMDW"])
        self.awc_poll_interval = cfg.get("awc_poll_interval_seconds", 90)
        self.nws_poll_interval = cfg.get("nws_poll_interval_seconds", 300)
        self.nws_stations = cfg.get("nws_stations", None)  # None = same as stations

        data_dir = (config_dir / config["storage"]["data_dir"]).resolve()
        self.storage = AviationWeatherMetarStorage(data_dir)

        self._last_awc: dict[str, datetime] = {}  # station -> last ob_time
        self._last_nws: dict[str, datetime] = {}
        self._awc_etag: str | None = None  # for conditional GET
        self._user_agent = cfg.get("user_agent") or DEFAULT_USER_AGENT

        logger.info(
            "AviationWeather METAR collector: stations=%s, awc_poll=%ds nws_poll=%ds",
            self.stations, self.awc_poll_interval, self.nws_poll_interval,
        )

    async def _awc_poll_loop(self) -> None:
        """Poll AWC METAR at awc_poll_interval_seconds."""
        min_dt = datetime.min.replace(tzinfo=timezone.utc)
        while self._get_running():
            try:
                awc_rows, new_etag = _fetch_awc_metar(
                    self.stations,
                    etag=self._awc_etag,
                    user_agent=self._user_agent,
                )
                if new_etag:
                    self._awc_etag = new_etag
                if awc_rows:
                    df = pd.DataFrame(awc_rows)
                    for st in df["station"].unique():
                        st_df = df[df["station"] == st].copy()
                        last = self._last_awc.get(st, min_dt)
                        st_df = st_df[st_df["ob_time_utc"] > last]
                        if st_df.empty:
                            continue
                        self._last_awc[st] = st_df["ob_time_utc"].max()
                        st_df = _add_rolling_minmax(
                            st_df, self.storage, "awc_metar", st
                        )
                        self.storage.save(st_df, "awc_metar")
            except Exception:
                if self._get_running():
                    logger.exception("AWC poll error")
            await asyncio.sleep(self.awc_poll_interval)

    async def _nws_poll_loop(self) -> None:
        """Poll NWS observations at nws_poll_interval_seconds."""
        nws_stations = self.nws_stations or self.stations
        min_dt = datetime.min.replace(tzinfo=timezone.utc)
        while self._get_running():
            try:
                for station in nws_stations:
                    nws_rows = _fetch_nws_observations(
                        station, user_agent=self._user_agent
                    )
                    if not nws_rows:
                        continue
                    df = pd.DataFrame(nws_rows)
                    last = self._last_nws.get(station, min_dt)
                    df = df[df["ob_time_utc"] > last]
                    if df.empty:
                        continue
                    self._last_nws[station] = df["ob_time_utc"].max()
                    df = _add_rolling_minmax(
                        df, self.storage, "nws_observations", station
                    )
                    self.storage.save(df, "nws_observations")
            except Exception:
                if self._get_running():
                    logger.exception("NWS poll error")
            await asyncio.sleep(self.nws_poll_interval)
