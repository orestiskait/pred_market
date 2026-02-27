import os
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import sys

# Add project root to path
PROJECT_ROOT = Path("/home/kaitores/projects/pred_market")
sys.path.insert(0, str(PROJECT_ROOT))

from services.core.storage import MARKET_SNAPSHOT_SCHEMA, ORDERBOOK_SNAPSHOT_SCHEMA

def get_data_dir():
    # Allow override via environment variable (useful on OCI VM)
    env_dir = os.environ.get("KALSHI_DATA_DIR")
    if env_dir:
        return Path(env_dir)
    return PROJECT_ROOT / "data"

def migrate_market_snapshots():
    data_dir = get_data_dir()
    base_dir = data_dir / "kalshi" / "market_snapshots"
    if not base_dir.exists():
        print(f"Directory {base_dir} does not exist, skipping.")
        return
    files = sorted(base_dir.glob("*.parquet"))
    
    print(f"Found {len(files)} market snapshot files.")
    
    for f in files:
        print(f"Processing {f.name}...")
        try:
            df = pd.read_parquet(f)
            
            # 0. Rename snapshot_ts if needed
            if 'snapshot_ts' in df.columns:
                df.rename(columns={'snapshot_ts': 'snapshot_ts_utc'}, inplace=True)
            
            # 1. Add missing columns with logic
            if 'no_bid' not in df.columns:
                df['no_bid'] = df['yes_ask'].apply(lambda x: 100.0 - float(x) if pd.notnull(x) else 0.0)
            
            if 'no_ask' not in df.columns:
                df['no_ask'] = df['yes_bid'].apply(lambda x: 100.0 - float(x) if pd.notnull(x) else 0.0)
            
            if 'is_data_live' not in df.columns:
                df['is_data_live'] = True
            
            # 2. Ensure all columns in schema are present
            for field in MARKET_SNAPSHOT_SCHEMA.names:
                if field not in df.columns:
                    print(f"  Adding missing column: {field} to {f.name}")
                    df[field] = 0
            
            # 3. Reorder and cast to schema types
            df = df[MARKET_SNAPSHOT_SCHEMA.names]
            type_map = {
                "yes_bid": "float64", "yes_ask": "float64", "no_bid": "float64", "no_ask": "float64",
                "last_price": "float64", "volume": "float64", "open_interest": "float64",
            }
            for col, dtype in type_map.items():
                df[col] = df[col].fillna(0.0).astype(dtype)
            
            df['is_data_live'] = df['is_data_live'].astype(bool)
            
            table = pa.Table.from_pandas(df, schema=MARKET_SNAPSHOT_SCHEMA, preserve_index=False)
            pq.write_table(table, f)
            print(f"  Successfully migrated {f.name}")
        except Exception as e:
            print(f"  Error processing {f.name}: {e}")

def migrate_orderbook_snapshots():
    data_dir = get_data_dir()
    base_dir = data_dir / "kalshi" / "orderbook_snapshots"
    if not base_dir.exists():
        print(f"Directory {base_dir} does not exist, skipping.")
        return
    files = sorted(base_dir.glob("*.parquet"))
    
    print(f"\nFound {len(files)} orderbook snapshot files.")
    
    for f in files:
        print(f"Processing {f.name}...")
        try:
            df = pd.read_parquet(f)
            
            # Rename snapshot_ts if needed
            if 'snapshot_ts' in df.columns:
                df.rename(columns={'snapshot_ts': 'snapshot_ts_utc'}, inplace=True)
                
            if 'is_data_live' not in df.columns:
                df['is_data_live'] = True

            # Ensure all columns in schema are present
            for field in ORDERBOOK_SNAPSHOT_SCHEMA.names:
                if field not in df.columns:
                    print(f"  Adding missing column: {field} to {f.name}")
                    if field in ["market_ticker", "side", "snapshot_type"]:
                        df[field] = ""
                    elif field == "is_data_live":
                        df[field] = True
                    else:
                        df[field] = 0.0
            
            # Reorder and cast
            df = df[ORDERBOOK_SNAPSHOT_SCHEMA.names]
            type_map = {
                "price_cents": "float64",
                "quantity": "float64",
            }
            for col, dtype in type_map.items():
                df[col] = df[col].fillna(0.0).astype(dtype)
            
            df['is_data_live'] = df['is_data_live'].astype(bool)
                
            table = pa.Table.from_pandas(df, schema=ORDERBOOK_SNAPSHOT_SCHEMA, preserve_index=False)
            pq.write_table(table, f)
            print(f"  Successfully migrated {f.name}")
        except Exception as e:
            print(f"  Error processing {f.name}: {e}")

if __name__ == "__main__":
    migrate_market_snapshots()
    migrate_orderbook_snapshots()

