"""Wethr.net Push API listener: real-time weather data ingest via SSE.

Streams observations, DSM/CLI releases, and temperature extreme alerts
from the Wethr.net Push API. Each event type is stored in a separate
parquet file under data/wethr_push/<event_type>/.

Also runs the METAR collector (AWC + NWS) when enabled.

Usage:
    python -m services.wethr.listener
    python -m services.wethr.listener --config path/to/config.yaml
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from services.core.config import (
    load_config,
    get_event_series,
    standard_argparser,
    configure_logging,
)
from services.core.service import AsyncService, MetarCollectorMixin
from services.wethr.sse import WethrSSEMixin
from services.wethr.storage import WethrPushStorage
from services.wethr.station_registry import wethr_stations_for_series

logger = logging.getLogger(__name__)


def _get_wethr_api_key(config: dict) -> str:
    """Read Wethr API key from credentials file."""
    from services.core.config import _read_credential
    return _read_credential(config, "wethr_api_key")


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


class WethrPushCollector(AsyncService, WethrSSEMixin, MetarCollectorMixin):
    """Ingests Wethr.net Push API data via SSE and stores to parquet."""

    def __init__(self, config: dict, config_dir: Path):
        self.config = config

        wcfg = config.get("wethr", {})
        self._wethr_enabled = wcfg.get("enabled", True)

        self.wethr_api_key = ""
        self.wethr_stations: list[str] = []

        if self._wethr_enabled:
            self.wethr_api_key = _get_wethr_api_key(config)
            stations = wcfg.get("stations", None)
            if stations is None:
                stations = wethr_stations_for_series(
                    get_event_series(config, "weather_bot")
                )
            self.wethr_stations = stations
            logger.info("Wethr Push API stations: %s", self.wethr_stations)
        else:
            logger.info("Wethr Push API disabled; running METAR collector only")

        # Storage
        data_dir = (config_dir / config["storage"]["data_dir"]).resolve()
        self.storage = WethrPushStorage(str(data_dir))
        self.flush_interval = config["storage"].get("flush_interval_seconds", 300)

        # Buffered rows per event type
        self._buffers: dict[str, list[dict]] = {
            et: [] for et in WethrPushStorage.EVENT_TYPES
        }
        self._buf_lock = asyncio.Lock()

        # State
        self._running = False

        # METAR collector (AWC + NWS)
        self._init_metar_collector(config, config_dir)

    # ------------------------------------------------------------------ #
    # WethrSSEMixin hooks                                                  #
    # ------------------------------------------------------------------ #

    def on_wethr_observation(self, data: dict, received_ts: datetime) -> None:
        """Buffer observation events for periodic flush."""
        if data.get("suspect_temperature"):
            logger.warning(
                "Suspect temperature at %s: %s",
                data.get("station_code"),
                data["suspect_temperature"],
            )

        ob_time = _parse_iso_ts(data.get("observation_time_utc", ""))

        row = {
            "station_code": data.get("station_code", ""),
            "observation_time_utc": ob_time,
            "received_ts": received_ts,
            "product": data.get("product", ""),
            "temperature_celsius": data.get("temperature_celsius"),
            "temperature_fahrenheit": data.get("temperature_fahrenheit"),
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
        }
        self._buffers["observations"].append(row)

        logger.info(
            "[%s] %.1f°F (product=%s) at %s",
            row["station_code"],
            row["temperature_fahrenheit"] or 0.0,
            row["product"],
            data.get("observation_time_utc", ""),
        )

    def on_wethr_dsm(self, data: dict, received_ts: datetime) -> None:
        row = {
            "station_code": data.get("station_code", ""),
            "for_date": data.get("for_date", ""),
            "received_ts": received_ts,
            "high_f": data.get("high_f"),
            "high_c": data.get("high_c"),
            "high_time_utc": data.get("high_time_utc", ""),
            "low_f": data.get("low_f"),
            "low_c": data.get("low_c"),
            "low_time_utc": data.get("low_time_utc", ""),
            "anomaly": data.get("anomaly", False),
            "event_id": data.get("id", ""),
        }
        self._buffers["dsm"].append(row)
        logger.info(
            "DSM [%s] for %s: high=%s°F low=%s°F",
            row["station_code"], row["for_date"], row["high_f"], row["low_f"],
        )

    def on_wethr_cli(self, data: dict, received_ts: datetime) -> None:
        row = {
            "station_code": data.get("station_code", ""),
            "for_date": data.get("for_date", ""),
            "received_ts": received_ts,
            "high_f": data.get("high_f"),
            "high_c": data.get("high_c"),
            "low_f": data.get("low_f"),
            "low_c": data.get("low_c"),
            "anomaly": data.get("anomaly", False),
            "event_id": data.get("id", ""),
        }
        self._buffers["cli"].append(row)
        logger.info(
            "CLI [%s] for %s: high=%s°F low=%s°F",
            row["station_code"], row["for_date"], row["high_f"], row["low_f"],
        )

    def _on_wethr_extreme(self, event_type: str, data: dict, received_ts: datetime) -> None:
        """Shared handler for new_high and new_low events."""
        ob_time = _parse_iso_ts(data.get("observation_time_utc", ""))
        row = {
            "station_code": data.get("station_code", ""),
            "observation_time_utc": ob_time,
            "received_ts": received_ts,
            "logic": data.get("logic", ""),
            "value_f": data.get("value_f"),
            "value_c": data.get("value_c"),
            "prev_value_f": data.get("prev_value_f"),
            "prev_value_c": data.get("prev_value_c"),
            "event_id": data.get("id", ""),
        }
        self._buffers[event_type].append(row)
        label = "NEW HIGH" if event_type == "new_high" else "NEW LOW"
        logger.info(
            "%s [%s] (%s): %s°F (was %s°F)",
            label, row["station_code"], row["logic"], row["value_f"], row["prev_value_f"],
        )

    def on_wethr_new_high(self, data: dict, received_ts: datetime) -> None:
        self._on_wethr_extreme("new_high", data, received_ts)

    def on_wethr_new_low(self, data: dict, received_ts: datetime) -> None:
        self._on_wethr_extreme("new_low", data, received_ts)

    # ------------------------------------------------------------------ #
    # Flush logic                                                          #
    # ------------------------------------------------------------------ #

    def _flush(self) -> None:
        """Write all buffered data to parquet and clear buffers."""
        for event_type, buf in self._buffers.items():
            if buf:
                df = pd.DataFrame(buf)
                logger.info("Flushing %d Wethr %s rows to parquet", len(buf), event_type)
                self.storage.save(df, event_type)
                buf.clear()

    # ------------------------------------------------------------------ #
    # AsyncService overrides                                               #
    # ------------------------------------------------------------------ #

    def _get_tasks(self) -> list:
        tasks = []
        if self._wethr_enabled:
            tasks.append(self.wethr_sse_loop())
            tasks.append(self._periodic_flush(self.flush_interval))
        tasks.extend(self._metar_collector_tasks())
        return tasks

    def _on_shutdown(self) -> None:
        if self._wethr_enabled:
            self._flush()
            logger.info("Wethr buffers flushed.")


# ------------------------------------------------------------------ #
# CLI                                                                  #
# ------------------------------------------------------------------ #

def main():
    parser = standard_argparser("Wethr.net Push API listener (real-time weather ingest)")
    args = parser.parse_args()

    configure_logging(args.log_level)

    config, config_path = load_config(args.config)
    svc = WethrPushCollector(config, config_dir=config_path.parent)
    asyncio.run(svc.run())


if __name__ == "__main__":
    main()
