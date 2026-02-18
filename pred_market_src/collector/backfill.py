"""Backfill historical candlestick and trade data from the Kalshi REST API.

Usage:
    pred_env/bin/python pred_market_src/collector/backfill.py --start 2026-02-01
    pred_env/bin/python pred_market_src/collector/backfill.py --start 2026-02-01 --end 2026-02-11 --events KXHIGHCHI-26FEB11
"""

import argparse
import logging
import os
import time
from pathlib import Path

from dotenv import load_dotenv

# Load .env from collector directory so KALSHI_API_KEY_ID etc. are available
load_dotenv(Path(__file__).resolve().parent / ".env")

from datetime import datetime, timezone

import yaml

try:
    from .kalshi_client import KalshiAuth, KalshiRestClient
    from .storage import ParquetStorage
except ImportError:
    from kalshi_client import KalshiAuth, KalshiRestClient
    from storage import ParquetStorage

logger = logging.getLogger(__name__)


def backfill_event(
    client: KalshiRestClient,
    storage: ParquetStorage,
    event_ticker: str,
    start_ts: int,
    end_ts: int,
    period_interval: int = 60,
):
    """Backfill candlesticks and trades for every market in *event_ticker*."""
    markets = client.get_markets_for_event(event_ticker)
    # Derive series ticker: KXHIGHCHI-26FEB11 -> KXHIGHCHI
    series_ticker = event_ticker.rsplit("-", 1)[0]
    logger.info(
        "Backfilling %d markets for %s (series=%s)",
        len(markets), event_ticker, series_ticker,
    )

    # ---- Candlesticks --------------------------------------------------
    candle_rows = []
    for m in markets:
        tk = m["ticker"]
        logger.info("  Candlesticks: %s", tk)
        try:
            candles = client.get_candlesticks(
                series_ticker, tk, start_ts, end_ts, period_interval,
            )
        except Exception as e:
            logger.warning("    Failed: %s", e)
            continue

        for c in candles:
            # The API may use different field names; handle common variants.
            ts_raw = (
                c.get("end_period_ts")
                or c.get("period_end_ts")
                or c.get("timestamp", 0)
            )
            if isinstance(ts_raw, (int, float)):
                ts = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
            else:
                ts = ts_raw

            candle_rows.append({
                "timestamp": ts,
                "event_ticker": event_ticker,
                "market_ticker": tk,
                "open_price": c.get("open", c.get("yes_open", 0)),
                "close_price": c.get("close", c.get("yes_close", 0)),
                "high_price": c.get("high", c.get("yes_high", 0)),
                "low_price": c.get("low", c.get("yes_low", 0)),
                "volume": c.get("volume", 0),
            })
        time.sleep(0.2)  # rate-limit

    if candle_rows:
        storage.write_candlesticks(candle_rows, event_ticker)
        logger.info("  Saved %d candlestick rows", len(candle_rows))

    # ---- Trades --------------------------------------------------------
    trade_rows = []
    for m in markets:
        tk = m["ticker"]
        logger.info("  Trades: %s", tk)
        cursor = None
        while True:
            try:
                resp = client.get_trades(
                    ticker=tk, min_ts=start_ts, max_ts=end_ts, cursor=cursor,
                )
            except Exception as e:
                logger.warning("    Failed: %s", e)
                break

            trades = resp.get("trades", [])
            if not trades:
                break

            for t in trades:
                ts_raw = t.get("created_time") or t.get("ts", 0)
                if isinstance(ts_raw, str):
                    ts = datetime.fromisoformat(ts_raw.replace("Z", "+00:00"))
                elif isinstance(ts_raw, (int, float)):
                    ts = datetime.fromtimestamp(ts_raw, tz=timezone.utc)
                else:
                    ts = ts_raw

                trade_rows.append({
                    "timestamp": ts,
                    "event_ticker": event_ticker,
                    "market_ticker": tk,
                    "trade_id": str(t.get("trade_id", "")),
                    "price": t.get("yes_price", t.get("price", 0)),
                    "count": t.get("count", t.get("contracts", 0)),
                    "taker_side": t.get("taker_side", ""),
                })

            cursor = resp.get("cursor")
            if not cursor:
                break
            time.sleep(0.2)

    if trade_rows:
        storage.write_trades(trade_rows, event_ticker)
        logger.info("  Saved %d trade rows", len(trade_rows))


# ------------------------------------------------------------------ #
# CLI                                                                  #
# ------------------------------------------------------------------ #

def main():
    parser = argparse.ArgumentParser(description="Backfill historical Kalshi market data")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).parent / "config.yaml"),
        help="Path to config.yaml",
    )
    parser.add_argument("--events", nargs="+", help="Event tickers (default: from config)")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD (default: now)")
    parser.add_argument(
        "--period", type=int, default=60,
        help="Candlestick interval in minutes: 1 | 60 | 1440",
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

    kcfg = config["kalshi"]
    api_key = kcfg.get("api_key_id") or os.environ.get("KALSHI_API_KEY_ID", "")
    pk_path = kcfg.get("private_key_path") or os.environ.get("KALSHI_PRIVATE_KEY_PATH", "")
    auth = KalshiAuth(api_key, pk_path)
    client = KalshiRestClient(kcfg["base_url"], auth)
    storage = ParquetStorage(str(config_path.parent / config["storage"]["data_dir"]))

    events = args.events or config.get("events", [])
    start_ts = int(
        datetime.strptime(args.start, "%Y-%m-%d")
        .replace(tzinfo=timezone.utc)
        .timestamp()
    )
    end_ts = (
        int(
            datetime.strptime(args.end, "%Y-%m-%d")
            .replace(tzinfo=timezone.utc)
            .timestamp()
        )
        if args.end
        else int(datetime.now(timezone.utc).timestamp())
    )

    for ev in events:
        backfill_event(client, storage, ev, start_ts, end_ts, args.period)

    logger.info("Backfill complete.")


if __name__ == "__main__":
    main()
