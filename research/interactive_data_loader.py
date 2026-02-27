# %% [markdown]
# Interactive Data Loader
# 
# This script provides a structured way to load and explore various data sources
# in the `pred_market` project. It uses `# %%` cell markers for interactive use
# (e.g., in VS Code, PyCharm, or Jupyter).

# %%
import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date, datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns

# Add project root to sys.path so we can import 'services'
PROJECT_ROOT = Path("/home/kaitores/projects/pred_market")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
DATA_DIR = PROJECT_ROOT / "data"

# Setup plotting
sns.set_theme(style="whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)

# %% [markdown]
# ## Helper Functions

# %%
def load_kalshi_market(start_date=None, end_date=None):
    """Load Kalshi market snapshots."""
    from services.core.storage import ParquetStorage
    storage = ParquetStorage(str(DATA_DIR))
    return storage.read_parquets("market", start_date, end_date)

def load_kalshi_orderbook(start_date=None, end_date=None, reconstruct=True):
    """Load Kalshi orderbook snapshots."""
    from services.core.storage import ParquetStorage
    storage = ParquetStorage(str(DATA_DIR))
    if reconstruct:
        return storage.reconstruct_orderbooks(start_date, end_date)
    return storage.read_parquets("orderbook", start_date, end_date)

def load_nwp_data(model, station=None, start_date=None, end_date=None):
    """Load NWP realtime data (HRRR, NBM, RRFS)."""
    from services.weather.storage import NWPRealtimeStorage
    storage = NWPRealtimeStorage(DATA_DIR)
    return storage.read(model, station, start_date, end_date)

def load_wethr_push(event_type, station=None, start_date=None, end_date=None):
    """Load Wethr.net push data (observations, dsm, cli, new_high, new_low)."""
    from services.wethr.storage import WethrPushStorage
    storage = WethrPushStorage(DATA_DIR)
    return storage.read(event_type, station, start_date, end_date)

def load_madis_data(source, station=None, start_date=None, end_date=None):
    """Load MADIS realtime data (metar, omo)."""
    from services.weather.storage import MADISRealtimeStorage
    storage = MADISRealtimeStorage(DATA_DIR)
    return storage.read(source, station, start_date, end_date)

def load_iem_asos(station, start_date=None, end_date=None):
    """Load historical IEM ASOS 1-min data."""
    base_path = DATA_DIR / "iem_asos_1min"
    files = sorted(base_path.glob(f"{station}_*.parquet"))
    if not files: return pd.DataFrame()
    
    frames = []
    for f in files:
        # Expected format: <ICAO>_YYYY-MM-DD.parquet
        try:
            f_date = date.fromisoformat(f.stem.split('_')[1])
            if start_date and f_date < start_date: continue
            if end_date and f_date > end_date: continue
            frames.append(pd.read_parquet(f))
        except (ValueError, IndexError):
            continue
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

# %% [markdown]
# ## 1. Load Kalshi Data
# Choose a date range and load market snapshots or reconstructed orderbooks.

# %%
# Example: Load today's Kalshi market data
# target_date = date.today()
target_date = date(2026, 2, 20) # Or pick a specific date

df_market = load_kalshi_market(start_date=target_date, end_date=target_date)

print(f"Loaded {len(df_market)} market snapshots.")
if not df_market.empty:
    print(df_market.head())
    # Quick plot of last price for a specific market
    if "market_ticker" in df_market.columns:
        ticker = df_market["market_ticker"].unique()[0]
        sub = df_market[df_market["market_ticker"] == ticker]
        plt.figure()
        plt.plot(sub["snapshot_ts_utc"], sub["last_price"], label=ticker)
        plt.title(f"Last Price: {ticker}")
        plt.xlabel("Time (UTC)")
        plt.ylabel("Price (cents)")
        plt.legend()
        plt.show()

# %% [markdown]
# ## 2. Load NWP Model Data
# Models available: `hrrr`, `nbm`, `rrfs`.

# %%
model_name = "hrrr"  # or "nbm", "rrfs"
station_icao = "KMDW" # Chicago Midway
target_date = date.today() - timedelta(days=1)

df_nwp = load_nwp_data(model_name, station=station_icao, start_date=target_date)

print(f"Loaded {len(df_nwp)} rows of {model_name} data for {station_icao}.")
if not df_nwp.empty:
    print(df_nwp.columns.tolist())
    print(df_nwp.head())

# %% [markdown]
# ## 3. Load Wethr Push Data
# Event types: `observations`, `dsm`, `cli`, `new_high`, `new_low`.

# %%
event_type = "observations"
station_icao = "KMDW"
target_date = date.today()

df_push = load_wethr_push(event_type, station=station_icao, start_date=target_date)

print(f"Loaded {len(df_push)} {event_type} from Wethr Push.")
if not df_push.empty:
    print(df_push.head())

# %% [markdown]
# ## 4. Load MADIS / METAR
# Sources: `metar`, `omo`.

# %%
source = "metar"
station_icao = "KNYC"
target_date = date.today()

df_madis = load_madis_data(source, station=station_icao, start_date=target_date)

print(f"Loaded {len(df_madis)} {source} records from MADIS.")
if not df_madis.empty:
    print(df_madis.head())

# %% [markdown]
# ## 5. Performance / Latency Analysis
# Compare notification time vs saved time or observation time.

# %%
if not df_nwp.empty and "notification_ts_utc" in df_nwp.columns:
    df_nwp["ingest_latency"] = (df_nwp["saved_ts_utc"] - df_nwp["notification_ts_utc"]).dt.total_seconds()
    print(f"Mean ingest latency: {df_nwp['ingest_latency'].mean():.2f} seconds")
    
    plt.figure()
    sns.histplot(df_nwp["ingest_latency"].dropna(), bins=30, kde=True)
    plt.title("NWP Ingest Latency Distribution (seconds)")
    plt.show()
