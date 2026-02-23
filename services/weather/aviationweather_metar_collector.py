"""Aviation Weather METAR collector: AWC API + NWS api.weather.gov.

Polls Aviation Weather Center (aviationweather.gov) and NWS api.weather.gov
(stations/observations — same data that powers weather.gov/wrh/LowTimeseries).
No WebSocket available; uses short-interval polling (90–120s) to catch new obs quickly.

Tracks: saved_ts, ob_time_utc (data reference), temp, raw_ob, RMK portion,
6hr and 24hr min/max temperature. Runs as a concurrent task in the nwp-listener.
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
NWS_USER_AGENT = "PredMarket/1.0 (weather data collection)"


def _c_to_f(celsius: float | None) -> float | None:
    if celsius is None:
        return None
    return celsius * 9.0 / 5.0 + 32.0


def _fetch_awc_metar(stations: list[str], hours: int = 2) -> list[dict]:
    """Fetch METAR from AWC API. Returns list of row dicts."""
    if not stations:
        return []
    params = {"ids": ",".join(stations), "format": "json", "hours": hours}
    try:
        resp = requests.get(AWC_METAR_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.warning("AWC METAR fetch failed: %s", e)
        return []

    if not data:
        return []

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
    return rows


def _fetch_nws_observations(station: str, limit: int = 50) -> list[dict]:
    """Fetch observations from api.weather.gov (powers weather.gov/wrh/LowTimeseries)."""
    url = NWS_OBSERVATIONS_URL.format(station=station)
    headers = {"User-Agent": NWS_USER_AGENT}
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
        self.poll_interval = cfg.get("poll_interval_seconds", 90)
        self.nws_stations = cfg.get("nws_stations", None)  # None = same as stations

        data_dir = (config_dir / config["storage"]["data_dir"]).resolve()
        self.storage = AviationWeatherMetarStorage(data_dir)

        self._last_awc: dict[str, datetime] = {}  # station -> last ob_time
        self._last_nws: dict[str, datetime] = {}

        logger.info(
            "AviationWeather METAR collector: stations=%s, poll=%ds",
            self.stations, self.poll_interval,
        )

    async def _poll_loop(self) -> None:
        """Main poll loop: fetch AWC and NWS, dedupe, save."""
        nws_stations = self.nws_stations or self.stations
        min_dt = datetime.min.replace(tzinfo=timezone.utc)

        while self._get_running():
            try:
                # AWC METAR
                awc_rows = _fetch_awc_metar(self.stations)
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

                # NWS observations (api.weather.gov — powers weather.gov/wrh/LowTimeseries)
                for station in nws_stations:
                    nws_rows = _fetch_nws_observations(station)
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
                    logger.exception("AviationWeather poll error")

            await asyncio.sleep(self.poll_interval)
