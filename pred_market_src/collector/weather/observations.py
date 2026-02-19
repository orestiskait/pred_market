"""Orchestrator that coordinates all weather fetchers.

Loads station configuration from config.yaml and provides a single
`collect_all()` method to fetch ASOS 1-min, METAR, and daily climate
data for all configured stations.

Usage:
    from pred_market_src.collector.weather import WeatherObservations

    obs = WeatherObservations.from_config("pred_market_src/collector/config.yaml")
    results = obs.collect_all(date(2026, 2, 18))
    # results["asos_1min"] → pd.DataFrame
    # results["metar"]     → pd.DataFrame
    # results["daily_climate"] → pd.DataFrame

Or target specific stations/sources:
    obs.fetch_metar("KNYC", date(2026, 2, 18))
    obs.fetch_asos_1min("KMDW", date(2026, 2, 18))
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from pred_market_src.collector.weather.asos_1min import ASOS1MinFetcher
from pred_market_src.collector.weather.daily_climate import DailyClimateFetcher
from pred_market_src.collector.weather.metar import METARFetcher
from pred_market_src.collector.weather.stations import (
    STATION_REGISTRY,
    StationInfo,
    station_for_icao,
    stations_for_series,
)

logger = logging.getLogger(__name__)


class WeatherObservations:
    """Unified interface for fetching weather observations.

    Instantiate via from_config() to auto-populate stations from your
    config.yaml, or construct directly with a list of StationInfo objects.
    """

    def __init__(
        self,
        stations: list[StationInfo],
        data_dir: Path | str | None = None,
    ):
        self.stations = stations
        self.data_dir = Path(data_dir) if data_dir else None

        # Initialize fetchers
        base = self.data_dir
        self.asos = ASOS1MinFetcher(data_dir=base)
        self.metar = METARFetcher(data_dir=base)
        self.climate = DailyClimateFetcher(data_dir=base)

    @classmethod
    def from_config(cls, config_path: str | Path) -> WeatherObservations:
        """Create from a config.yaml file.

        Reads `event_series` and/or `weather_stations` from config to
        determine which stations to track.  Falls back to all stations
        in STATION_REGISTRY if neither is specified.
        """
        config_path = Path(config_path)
        with open(config_path) as f:
            cfg = yaml.safe_load(f)

        # Determine data directory
        storage = cfg.get("storage", {})
        rel_data_dir = storage.get("data_dir", "data")
        data_dir = config_path.parent / rel_data_dir / "weather_obs"

        # Resolve stations from event_series or explicit weather_stations
        stations: list[StationInfo] = []

        # Option 1: Explicit weather_stations list (ICAO codes)
        explicit = cfg.get("weather_stations", [])
        if explicit:
            for icao in explicit:
                try:
                    stations.append(station_for_icao(icao))
                except KeyError:
                    logger.warning("Unknown station ICAO: %s", icao)

        # Option 2: Derive from event_series
        if not stations:
            series_list = cfg.get("event_series", [])
            for series in series_list:
                if series in STATION_REGISTRY:
                    info = STATION_REGISTRY[series]
                    if info not in stations:
                        stations.append(info)

        # Fallback: all configured stations
        if not stations:
            stations = list({v for v in STATION_REGISTRY.values()})
            logger.info("No stations configured; using all %d from registry",
                        len(stations))

        logger.info("Weather stations: %s", [s.icao for s in stations])
        return cls(stations=stations, data_dir=data_dir)

    # ------------------------------------------------------------------
    # Convenience: fetch by ICAO string
    # ------------------------------------------------------------------

    def _resolve_station(self, station: str | StationInfo) -> StationInfo:
        """Accept either a StationInfo or an ICAO string."""
        if isinstance(station, StationInfo):
            return station
        return station_for_icao(station)

    def _resolve_stations(
        self, stations: list[str | StationInfo] | None = None
    ) -> list[StationInfo]:
        """Resolve station list; defaults to self.stations."""
        if stations is None:
            return self.stations
        return [self._resolve_station(s) for s in stations]

    # ------------------------------------------------------------------
    # Individual fetchers (convenience wrappers)
    # ------------------------------------------------------------------

    def fetch_asos_1min(
        self,
        station: str | StationInfo,
        target_date: date,
        save: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch ASOS 1-minute data for one station."""
        stn = self._resolve_station(station)
        df = self.asos.fetch(stn, target_date, **kwargs)
        if save and not df.empty:
            self.asos.save_parquet(df, stn, target_date)
        return df

    def fetch_metar(
        self,
        station: str | StationInfo,
        target_date: date,
        save: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch METAR data for one station."""
        stn = self._resolve_station(station)
        df = self.metar.fetch(stn, target_date, **kwargs)
        if save and not df.empty:
            self.metar.save_parquet(df, stn, target_date)
        return df

    def fetch_daily_climate(
        self,
        station: str | StationInfo,
        target_date: date,
        save: bool = True,
        **kwargs,
    ) -> pd.DataFrame:
        """Fetch NWS Daily Climate Report for one station."""
        stn = self._resolve_station(station)
        df = self.climate.fetch(stn, target_date, **kwargs)
        if save and not df.empty:
            self.climate.save_parquet(df, stn, target_date)
        return df

    # ------------------------------------------------------------------
    # Bulk operations
    # ------------------------------------------------------------------

    def collect_all(
        self,
        target_date: date,
        stations: list[str | StationInfo] | None = None,
        save: bool = True,
        sources: list[str] | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Fetch all three data sources for all configured stations.

        Parameters
        ----------
        target_date : date
            Date to fetch.
        stations : list, optional
            Override station list (ICAO strings or StationInfo).
        save : bool
            Whether to auto-save parquet files.
        sources : list[str], optional
            Which sources to fetch.  Defaults to all three:
            ["asos_1min", "metar", "daily_climate"].

        Returns
        -------
        dict mapping source name → concatenated DataFrame for all stations.
        """
        stns = self._resolve_stations(stations)
        if sources is None:
            sources = ["asos_1min", "metar", "daily_climate"]

        results: dict[str, pd.DataFrame] = {}

        if "asos_1min" in sources:
            df = self.asos.fetch_many(stns, target_date)
            if save and not df.empty:
                for stn in stns:
                    mask = df["station"] == stn.icao
                    if mask.any():
                        self.asos.save_parquet(df[mask], stn, target_date)
            results["asos_1min"] = df

        if "metar" in sources:
            df = self.metar.fetch_many(stns, target_date)
            if save and not df.empty:
                for stn in stns:
                    mask = df["station"] == stn.icao
                    if mask.any():
                        self.metar.save_parquet(df[mask], stn, target_date)
            results["metar"] = df

        if "daily_climate" in sources:
            df = self.climate.fetch_many(stns, target_date)
            if save and not df.empty:
                for stn in stns:
                    mask = df["station"] == stn.icao
                    if mask.any():
                        self.climate.save_parquet(df[mask], stn, target_date)
            results["daily_climate"] = df

        for source, df in results.items():
            logger.info("%s: %d rows across %d stations",
                        source, len(df), df["station"].nunique() if not df.empty else 0)

        return results

    def collect_date_range(
        self,
        start_date: date,
        end_date: date,
        stations: list[str | StationInfo] | None = None,
        save: bool = True,
        sources: list[str] | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Fetch all sources for a date range (for backfilling).

        Returns concatenated results across all dates.
        """
        from datetime import timedelta

        all_results: dict[str, list[pd.DataFrame]] = {}

        current = start_date
        while current <= end_date:
            day_results = self.collect_all(current, stations, save, sources)
            for source, df in day_results.items():
                if not df.empty:
                    all_results.setdefault(source, []).append(df)
            current += timedelta(days=1)

        return {
            source: pd.concat(frames, ignore_index=True)
            for source, frames in all_results.items()
            if frames
        }
