"""Parquet storage for Kalshi market data - live snapshots and historical backfill."""

import logging
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Schemas

MARKET_SNAPSHOT_SCHEMA = pa.schema([
    ("snapshot_ts",   pa.timestamp("us", tz="UTC")),
    ("event_ticker",  pa.string()),
    ("market_ticker", pa.string()),
    ("subtitle",      pa.string()),
    ("yes_bid",       pa.int32()),
    ("yes_ask",       pa.int32()),
    ("last_price",    pa.int32()),
    ("volume",        pa.int64()),
    ("open_interest", pa.int64()),
    ("trigger",       pa.string()),
])

ORDERBOOK_SNAPSHOT_SCHEMA = pa.schema([
    ("snapshot_ts",   pa.timestamp("us", tz="UTC")),
    ("market_ticker", pa.string()),
    ("side",          pa.string()),
    ("price_cents",   pa.int32()),
    ("quantity",      pa.float64()),
])

CANDLESTICK_SCHEMA = pa.schema([
    ("timestamp",     pa.timestamp("us", tz="UTC")),
    ("event_ticker",  pa.string()),
    ("market_ticker", pa.string()),
    ("open_price",    pa.float64()),
    ("close_price",   pa.float64()),
    ("high_price",    pa.float64()),
    ("low_price",     pa.float64()),
    ("volume",        pa.int64()),
])

TRADE_SCHEMA = pa.schema([
    ("timestamp",     pa.timestamp("us", tz="UTC")),
    ("event_ticker",  pa.string()),
    ("market_ticker", pa.string()),
    ("trade_id",      pa.string()),
    ("price",         pa.int32()),
    ("count",         pa.int64()),
    ("taker_side",    pa.string()),
])


class ParquetStorage:
    """Append-friendly parquet I/O organized by date (live) or event (historical)."""

    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self.dirs = {
            "market":       self.data_dir / "market_snapshots",
            "orderbook":    self.data_dir / "orderbook_snapshots",
            "candlesticks": self.data_dir / "historical" / "candlesticks",
            "trades":       self.data_dir / "historical" / "trades",
        }
        for d in self.dirs.values():
            d.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _append(path: Path, table: pa.Table):
        """Write table to path, appending to an existing file if present."""
        if path.exists():
            existing = pq.read_table(path)
            table = pa.concat_tables([existing, table])
        pq.write_table(table, path)

    def _write(self, kind: str, filename: str, rows: List[Dict], schema: pa.Schema):
        if not rows:
            return
        df = pd.DataFrame(rows)
        table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
        path = self.dirs[kind] / filename
        self._append(path, table)
        logger.info("Wrote %d rows to %s", len(rows), path)

    # -- live snapshots ---------------------------------------------------

    def write_market_snapshots(self, rows: List[Dict], dt: Optional[date] = None):
        dt = dt or date.today()
        self._write("market", f"{dt.isoformat()}.parquet", rows, MARKET_SNAPSHOT_SCHEMA)

    def write_orderbook_snapshots(self, rows: List[Dict], dt: Optional[date] = None):
        dt = dt or date.today()
        self._write("orderbook", f"{dt.isoformat()}.parquet", rows, ORDERBOOK_SNAPSHOT_SCHEMA)

    # -- historical backfill ----------------------------------------------

    def write_candlesticks(self, rows: List[Dict], event_ticker: str):
        self._write("candlesticks", f"{event_ticker}.parquet", rows, CANDLESTICK_SCHEMA)

    def write_trades(self, rows: List[Dict], event_ticker: str):
        self._write("trades", f"{event_ticker}.parquet", rows, TRADE_SCHEMA)

    # -- reading ----------------------------------------------------------

    def read_parquets(
        self,
        kind: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> pd.DataFrame:
        """Read and concatenate parquet files.

        kind: "market" | "orderbook" | "candlesticks" | "trades"
        """
        base = self.dirs[kind]
        files = sorted(base.glob("*.parquet"))
        if start_date:
            files = [f for f in files if f.stem >= start_date.isoformat()]
        if end_date:
            files = [f for f in files if f.stem <= end_date.isoformat()]
        if not files:
            return pd.DataFrame()
        return pa.concat_tables(
            [pq.read_table(f) for f in files],
            promote_options="default",
        ).to_pandas()
