"""Kalshi Weather Arbitrage Bot — trading + wethr ingest + NWP model data.

Extends the generic TradingBot with weather-specific data feeds:
  - Wethr.net Push API SSE (real-time obs → trading signals + parquet storage)
  - NWP model data ingest via AWS SNS/SQS (HRRR, RRFS, NBM → parquet storage)

Architecture:
    WeatherBot(TradingBot)
    ├── EventBus (in-memory pub/sub)          ← inherited
    ├── StrategyManager                       ← inherited
    │   └── LadderStrategy("chicago_fast_ladder", targets=["KXHIGHCHI"])
    ├── ExecutionManager (risk guardrails)     ← inherited
    ├── Kalshi WS (orderbook feed)            ← inherited
    ├── Wethr SSE (real-time observations)    ← weather-specific
    └── NWPSNSListener (HRRR/RRFS/NBM)       ← weather-specific

Usage:
    python -m services.bot.weather_bot
    python -m services.bot.weather_bot --config config.yaml
    python -m services.bot.weather_bot --series KXHIGHCHI
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

# Import rasterio first to ensure its bundled libcurl is loaded before pyarrow/pandas
# loads theirs, preventing GDAL CPLE_AppDefined curl version mismatch errors.
import rasterio

import pandas as pd

from services.core.config import (
    load_config,
    _read_credential,
    configure_logging,
    standard_argparser,
)
from services.wethr.sse import WethrSSEMixin
from services.wethr.storage import WethrPushStorage
from services.wethr.station_registry import wethr_stations_for_series
from services.markets.kalshi_registry import KalshiMarketConfig, KALSHI_MARKET_REGISTRY

from services.bot.events import WeatherObservationEvent
from services.bot.trading_bot import TradingBot


logger = logging.getLogger("WeatherBot")


def _nested_get(d: dict, *keys) -> object:
    """Safely traverse nested dicts."""
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
        if d is None:
            return None
    return d


def _parse_iso_ts(raw: str) -> datetime | None:
    """Parse an ISO timestamp string, returning None on failure."""
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


class WeatherBot(TradingBot, WethrSSEMixin):
    """Weather-specific trading bot.

    Extends TradingBot with:
      - Wethr.net Push API SSE (real-time obs → trading signals + parquet)
      - NWP model ingest (HRRR, RRFS, NBM → parquet via composed NWPSNSListener)
    """

    # ------------------------------------------------------------------
    # TradingBot extension points
    # ------------------------------------------------------------------

    def _event_series_consumer(self) -> str:
        return "weather_bot"

    def _setup_feeds(self) -> None:
        """Initialize weather-specific data feeds (Wethr SSE + NWP)."""
        config = self.config
        config_path = self._config_path

        # Market configs for weather-specific lookups
        self._market_configs: dict[str, KalshiMarketConfig] = {}
        for s in self._target_series:
            if s in KALSHI_MARKET_REGISTRY:
                self._market_configs[s] = KALSHI_MARKET_REGISTRY[s]
            else:
                logger.warning("Series %s not in KALSHI_MARKET_REGISTRY, skipping", s)

        # Wethr
        wethr_cfg = config.get("wethr", {})
        self.wethr_api_key = _read_credential(config, "wethr_api_key")
        stations_override = wethr_cfg.get("stations")
        self.wethr_stations = stations_override if stations_override else wethr_stations_for_series(self._target_series)

        # Wethr storage (persists all SSE event types to parquet)
        data_dir = (config_path.parent / config["storage"]["data_dir"]).resolve()
        self._wethr_storage = WethrPushStorage(str(data_dir))
        self._wethr_buffers: dict[str, list[dict]] = {
            et: [] for et in WethrPushStorage.EVENT_TYPES
        }
        self.flush_interval = config["storage"].get("flush_interval_seconds", 300)

        # NWP model ingest (HRRR, RRFS, NBM) — composed as a separate service
        self._nwp_listener = self._build_nwp_listener(config, config_path.parent)

    def _get_feed_tasks(self) -> list:
        """Return weather-specific async tasks."""
        tasks = [
            self.wethr_sse_loop(),
            self._flush_loop(),
        ]
        if self._nwp_listener is not None:
            try:
                tasks.extend(self._nwp_listener._get_tasks())
            except Exception:
                logger.exception("NWP listener task setup failed; NWP ingest disabled")
        return tasks

    def _on_feed_shutdown(self) -> None:
        """Flush weather buffers and shut down NWP listener."""
        self._flush()
        logger.info("Wethr flush complete.")
        if self._nwp_listener is not None:
            self._nwp_listener._on_shutdown()

    # ------------------------------------------------------------------
    # NWP listener builder
    # ------------------------------------------------------------------

    def _build_nwp_listener(self, config: dict, config_dir: Path):
        """Build NWPSNSListener if NWP is configured, else return None."""
        import os
        if os.environ.get("ENABLE_NWP", "").lower() not in ["1", "true", "yes"]:
            logger.info("NWP ingest disabled by default. Set ENABLE_NWP=1 runtime environment variable to enable.")
            return None

        nwp_cfg = config.get("nwp", {})
        if not nwp_cfg or nwp_cfg.get("enabled") is False:
            return None
        enabled_models = [
            m for m, mc in nwp_cfg.get("models", {}).items()
            if mc.get("enabled", True)
        ]
        if not enabled_models:
            return None
        try:
            from services.weather.nwp_listener import NWPSNSListener
            return NWPSNSListener(config, config_dir)
        except Exception:
            logger.exception("Failed to initialize NWP listener; NWP ingest disabled")
            return None

    # ------------------------------------------------------------------
    # Startup banner override
    # ------------------------------------------------------------------

    def _log_startup_banner(self):
        nwp_status = "enabled" if self._nwp_listener else "disabled"
        logger.info(
            "WeatherBot: series=%s stations=%s NWP=%s",
            self._target_series, self.wethr_stations, nwp_status,
        )
        for sid, strat in self.strategy_manager.strategies.items():
            mode = "PAPER" if strat.params.get("paper_mode", True) else "LIVE"
            logger.info("  %s [%s]: %s", sid, mode, strat.targets)

    # -------------------------------------------------------------------------
    # WethrSSEMixin hooks — trading signals + parquet storage
    # -------------------------------------------------------------------------

    def on_wethr_observation(self, data: dict, received_ts: datetime):
        """Parse observation → WeatherObservationEvent (trading) + buffer for storage."""
        station = data.get("station_code", "")
        temp_f = data.get("temperature_fahrenheit")
        ob_time_str = data.get("observation_time_utc", "")

        if data.get("suspect_temperature"):
            logger.warning("Suspect temperature at %s — skipping", station)
            return

        if temp_f is not None:
            try:
                if not ob_time_str.endswith("Z") and "+" not in ob_time_str:
                    ob_time_str += "Z"
                ob_time = datetime.fromisoformat(ob_time_str.replace("Z", "+00:00"))
                logger.info("Obs %s %.1f°F %s", station, temp_f, ob_time_str.replace(" ", "T"))
                self.event_bus.publish(WeatherObservationEvent(
                    station=station,
                    temp=temp_f,
                    ob_time=ob_time,
                ))
            except (ValueError, TypeError):
                logger.warning("Could not parse timestamp: %s", ob_time_str)

        ob_time = _parse_iso_ts(ob_time_str)
        self._wethr_buffers["observations"].append({
            "station_code": station,
            "observation_time_utc": ob_time,
            "received_ts_utc": received_ts.astimezone(timezone.utc),
            "product": data.get("product", ""),
            "temperature_celsius": data.get("temperature_celsius"),
            "temperature_fahrenheit": temp_f,
            "dew_point_celsius": data.get("dew_point_celsius"),
            "dew_point_fahrenheit": data.get("dew_point_fahrenheit"),
            "relative_humidity": data.get("relative_humidity"),
            "wind_direction": data.get("wind_direction", ""),
            "wind_speed_mph": data.get("wind_speed_mph"),
            "wind_gust_mph": data.get("wind_gust_mph"),
            "visibility_miles": data.get("visibility_miles"),
            "altimeter_inhg": data.get("altimeter_inhg"),
            "wethr_high_nws_f": _nested_get(data, "wethr_high", "nws", "value_f"),
            "wethr_high_wu_f": _nested_get(data, "wethr_high", "wu", "value_f"),
            "wethr_low_nws_f": _nested_get(data, "wethr_low", "nws", "value_f"),
            "wethr_low_wu_f": _nested_get(data, "wethr_low", "wu", "value_f"),
            "anomaly": data.get("anomaly", False),
            "event_id": data.get("id", ""),
        })

    def on_wethr_dsm(self, data: dict, received_ts: datetime) -> None:
        row = {
            "station_code": data.get("station_code", ""),
            "for_date_lst": data.get("for_date", ""),
            "received_ts_utc": received_ts.astimezone(timezone.utc),
            "observation_time_utc": _parse_iso_ts(data.get("timestamp", "")),
            "high_f": data.get("high_f"),
            "high_c": data.get("high_c"),
            "high_time_utc": _parse_iso_ts(data.get("high_time_utc", "")),
            "low_f": data.get("low_f"),
            "low_c": data.get("low_c"),
            "low_time_utc": _parse_iso_ts(data.get("low_time_utc", "")),
            "anomaly": data.get("anomaly", False),
            "event_id": data.get("id", ""),
        }
        self._wethr_buffers["dsm"].append(row)
        logger.info(
            "DSM [%s] for %s: high=%s°F low=%s°F",
            row["station_code"], row["for_date_lst"], row["high_f"], row["low_f"],
        )

    def on_wethr_cli(self, data: dict, received_ts: datetime) -> None:
        row = {
            "station_code": data.get("station_code", ""),
            "for_date_lst": data.get("for_date", ""),
            "received_ts_utc": received_ts.astimezone(timezone.utc),
            "observation_time_utc": _parse_iso_ts(data.get("timestamp", "")),
            "high_f": data.get("high_f"),
            "high_c": data.get("high_c"),
            "high_time_utc": _parse_iso_ts(data.get("high_time_utc", "")),
            "low_f": data.get("low_f"),
            "low_c": data.get("low_c"),
            "low_time_utc": _parse_iso_ts(data.get("low_time_utc", "")),
            "anomaly": data.get("anomaly", False),
            "event_id": data.get("id", ""),
        }
        self._wethr_buffers["cli"].append(row)
        logger.info(
            "CLI [%s] for %s: high=%s°F low=%s°F",
            row["station_code"], row["for_date_lst"], row["high_f"], row["low_f"],
        )

    def _on_wethr_extreme(self, event_type: str, data: dict, received_ts: datetime) -> None:
        ob_time = _parse_iso_ts(data.get("observation_time_utc", ""))
        row = {
            "station_code": data.get("station_code", ""),
            "observation_time_utc": ob_time,
            "received_ts_utc": received_ts.astimezone(timezone.utc),
            "logic": data.get("logic", ""),
            "value_f": data.get("value_f"),
            "value_c": data.get("value_c"),
            "prev_value_f": data.get("prev_value_f"),
            "prev_value_c": data.get("prev_value_c"),
            "event_id": data.get("id", ""),
        }
        self._wethr_buffers[event_type].append(row)
        label = "NEW HIGH" if event_type == "new_high" else "NEW LOW"
        logger.info(
            "%s [%s] (%s): %s°F (was %s°F)",
            label, row["station_code"], row["logic"], row["value_f"], row["prev_value_f"],
        )

    def on_wethr_new_high(self, data: dict, received_ts: datetime) -> None:
        self._on_wethr_extreme("new_high", data, received_ts)

    def on_wethr_new_low(self, data: dict, received_ts: datetime) -> None:
        self._on_wethr_extreme("new_low", data, received_ts)

    # -------------------------------------------------------------------------
    # Flush logic
    # -------------------------------------------------------------------------

    def _flush(self) -> None:
        """Write all buffered wethr data to parquet (runs in thread executor)."""
        sizes = {et: len(buf) for et, buf in self._wethr_buffers.items()}
        total = sum(sizes.values())
        if total > 0:
            logger.info(
                "Wethr flush: persisting obs=%d dsm=%d cli=%d new_high=%d new_low=%d",
                sizes.get("observations", 0),
                sizes.get("dsm", 0),
                sizes.get("cli", 0),
                sizes.get("new_high", 0),
                sizes.get("new_low", 0),
            )
        for event_type, buf in self._wethr_buffers.items():
            if buf:
                df = pd.DataFrame(buf)
                self._wethr_storage.save(df, event_type)
                buf.clear()

    async def _flush_loop(self) -> None:
        """Periodic wethr parquet flush, offloaded to a thread so parquet I/O
        never blocks the Kalshi WS or wethr SSE event loop tasks."""
        import time
        last = time.monotonic()
        loop = asyncio.get_event_loop()
        while self._running:
            await asyncio.sleep(1)
            if not self._running:
                break
            if time.monotonic() - last >= self.flush_interval:
                await loop.run_in_executor(None, self._flush)
                last = time.monotonic()

    # -------------------------------------------------------------------------
    # Lifecycle overrides
    # -------------------------------------------------------------------------

    async def run(self):
        if self._nwp_listener is not None:
            self._nwp_listener._running = True
        await super().run()

    def shutdown(self):
        super().shutdown()
        if self._nwp_listener is not None:
            self._nwp_listener._running = False


# ------------------------------------------------------------------ #
# CLI                                                                  #
# ------------------------------------------------------------------ #

def main():
    parser = standard_argparser("Kalshi Weather Arbitrage Bot (trading + wethr + NWP)")
    parser.add_argument(
        "--series", nargs="+", default=None,
        help="Limit to specific event series (e.g. KXHIGHCHI KXHIGHNY). "
             "Default: all series from config.yaml strategies + event_series.",
    )
    args = parser.parse_args()

    configure_logging(args.log_level)

    config, config_path = load_config(args.config)
    bot = WeatherBot(config, config_path, series_filter=args.series)
    asyncio.run(bot.run())


if __name__ == "__main__":
    main()
