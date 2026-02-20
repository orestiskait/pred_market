"""Live WebSocket-based Synoptic Data collector.

Usage:
    pred_env/bin/python pred_market_src/collector/synoptic_listener.py
    pred_env/bin/python pred_market_src/collector/synoptic_listener.py --config path/to/config.yaml
"""

import asyncio
import json
import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
import websockets
import yaml

# Load .env from collector directory so SYNOPTIC_API_TOKEN is available
load_dotenv(Path(__file__).resolve().parent / ".env")

try:
    from .storage import ParquetStorage
except ImportError:
    from storage import ParquetStorage

logger = logging.getLogger(__name__)


class SynopticLiveCollector:
    """Streams Synoptic WebSocket data and periodically snapshots state to parquet."""

    def __init__(self, config: dict, config_dir: Path):
        self.config = config

        # Collect Synoptic Token
        self.token = os.environ.get("SYNOPTIC_API_TOKEN")
        if not self.token:
            raise ValueError("SYNOPTIC_API_TOKEN must be set in .env")

        # Storage setup
        data_dir = config_dir / config["storage"]["data_dir"]
        self.storage = ParquetStorage(str(data_dir))
        self.flush_interval = config["storage"].get("flush_interval_seconds", 300)

        # Connection config
        # Defaulting to midway for the time being or whatever is in config
        ccfg = config.get("synoptic", {})
        self.stations = ccfg.get("stations", ["KMDW1M"])
        self.vars = ccfg.get("vars", ["air_temp"])
        
        stid_str = ",".join(self.stations)
        vars_str = ",".join(self.vars)
        self.ws_url = f"wss://push.synopticdata.com/feed/{self.token}/?units=english&stid={stid_str}&vars={vars_str}"

        # State
        self._running = False
        self._buf = []

    def _handle_message(self, raw: str):
        msg = json.loads(raw)
        mtype = msg.get("type")

        if mtype == "data":
            # Example message:
            # {
            #   "type": "data",
            #   "data": [
            #     {
            #       "stid": "KMDW1M",
            #       "sensor": "air_temp",
            #       "set": 1,
            #       "date": "2026-02-20 21:44:00",
            #       "value": 30.2,
            #       "qc": [],
            #       "match": false
            #     }
            #   ]
            # }
            
            received_ts = datetime.now(timezone.utc)
            for d in msg.get("data", []):
                try:
                    # Synoptic times are typically UTC ("2026-02-20 21:44:00")
                    # Assuming it is UTC because the docs say UTC or local.
                    # As push streaming, it pushes in UTC by default unless obtimezone=local is used.
                    ob_dt = datetime.strptime(d.get("date"), "%Y-%m-%d %H:%M:%S")
                    ob_ts = ob_dt.replace(tzinfo=timezone.utc)
                    
                    row = {
                        "received_ts": received_ts,
                        "ob_timestamp": ob_ts,
                        "stid": d.get("stid", ""),
                        "sensor": d.get("sensor", ""),
                        "value": float(d.get("value")),
                    }
                    self._buf.append(row)
                except Exception as e:
                    logger.warning("Could not parse synoptic data row %s: %s", d, e)
        elif mtype == "auth":
            logger.info("Synoptic Auth: %s", msg)
            if msg.get("code") == "failed":
                logger.error("Synoptic Auth Failed! Exiting...")
                self._running = False
        elif mtype == "metadata":
            logger.info("Synoptic Metadata: %s", msg)
        else:
            logger.info("Unknown Synoptic message type: %s", msg)

    def _flush(self):
        """Write buffered data to parquet and clear buffers."""
        if self._buf:
            logger.info("Flushing %d Synoptic observations to parquet", len(self._buf))
            self.storage.write_synoptic_ws(self._buf)
            self._buf.clear()

    async def _ws_loop(self):
        """WebSocket connection loop with automatic reconnection."""
        # Replace token for safe logging
        safe_url = self.ws_url.replace(self.token, "<TOKEN>")
        
        while self._running:
            try:
                logger.info("Connecting to Synoptic WS: %s", safe_url)
                # Need a larger ping_timeout or ping_interval if Synoptic is quiet
                async with websockets.connect(self.ws_url, ping_interval=None) as ws:
                    logger.info("Synoptic WebSocket connected")

                    async for raw in ws:
                        if not self._running:
                            break
                        self._handle_message(raw)

            except websockets.ConnectionClosed as e:
                logger.warning("Synoptic WS disconnected: %s  — reconnecting in 5s", e)
                await asyncio.sleep(5)
            except Exception as e:
                if not self._running:
                    break
                logger.error("Synoptic WS error: %s  — reconnecting in 10s", type(e).__name__)
                await asyncio.sleep(10)

    async def _snapshot_loop(self):
        """Periodic buffer flush."""
        last_flush = time.monotonic()
        while self._running:
            await asyncio.sleep(1)
            if not self._running:
                break
            
            if time.monotonic() - last_flush >= self.flush_interval:
                self._flush()
                last_flush = time.monotonic()

    async def run(self):
        """Main entry point — runs until SIGINT / SIGTERM."""
        self._running = True

        loop = asyncio.get_event_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._shutdown)

        logger.info("Starting Synoptic live collector...")
        try:
            await asyncio.gather(self._ws_loop(), self._snapshot_loop())
        finally:
            self._flush()
            logger.info("Synoptic Collector stopped. Buffers flushed.")

    def _shutdown(self):
        logger.info("Shutdown signal received")
        self._running = False


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Synoptic Data live WebSocket collector")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).parent / "config.yaml"),
        help="Path to config.yaml",
    )
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper()),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    )

    config_path = Path(args.config)
    with open(config_path) as f:
        config = yaml.safe_load(f)

    collector = SynopticLiveCollector(config, config_dir=config_path.parent)
    asyncio.run(collector.run())


if __name__ == "__main__":
    main()
