"""Compare ASOS 1-min and METAR daily max vs NWS official high (CLI).

Workflow:
  1. Fetch and save data via IEMAWCDataCollector (iem_asos_1min, awc_metar, iem_daily_climate).
  2. Load saved parquet files.
  3. Compute daily max from ASOS 1-min (IEM) and METAR (AWC) using LST windows.
  4. Compare against CLI (IEM) official highs.

Config:
  - 50-day window ending yesterday.
  - Stations: KNYC, KMDW.
"""

import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

# Ensure project root is on sys.path
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from research.weather.iem_awc_data_collector import IEMAWCDataCollector
from research.weather.iem_awc_station_registry import lst_offset_hours, station_for_icao

# ── Configuration ──────────────────────────────────────────────────────────
END_DATE   = date(2026, 2, 18)
NUM_DAYS   = 50
START_DATE = END_DATE - timedelta(days=NUM_DAYS - 1)

# ── Helpers ────────────────────────────────────────────────────────────────

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%H:%M:%S",
    )
    # Silence requests/urllib3
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def compute_daily_stats(df: pd.DataFrame, time_col: str, val_col: str, 
                        offset_hours: int, start: date, end: date) -> pd.DataFrame:
    """Compute daily stats using Local Standard Time (LST).
    
    The NWS Climate Day is Midnight-to-Midnight LST.
    """
    if df.empty:
        return pd.DataFrame()

    work_df = df.copy()
    work_df = work_df.dropna(subset=[val_col])
    
    # 1. Convert to LST
    work_df["lst_time"] = work_df[time_col] + pd.Timedelta(hours=offset_hours)
    work_df["lst_date"] = work_df["lst_time"].dt.date
    
    # 2. Filter to analysis window (based on LST date)
    work_df = work_df[(work_df["lst_date"] >= start) & (work_df["lst_date"] <= end)]
    
    # 3. Aggregations
    # We want: Max, Min, Count
    # Potential future refinement: filtering outliers?
    daily = work_df.groupby("lst_date").agg(
        day_max=(val_col, "max"),
        day_min=(val_col, "min"),
        day_count=(val_col, "count"),
    ).reset_index().rename(columns={"lst_date": "valid_date"})
    
    return daily


def analyze_station(collector: IEMAWCDataCollector, icao: str):
    print(f"\n{'='*70}")
    print(f"  ANALYSIS: {icao}")
    print(f"{'='*70}")

    try:
        station = station_for_icao(icao)
        offset = lst_offset_hours(station.tz)
    except KeyError:
        print(f"Skipping {icao} (not in station registry)")
        return

    # 1. Load Parquet Data
    print("Loading data from parquet...")
    asos_df = collector.asos.read_all(start_date=START_DATE, end_date=END_DATE)
    metar_df = collector.metar.read_all(start_date=START_DATE, end_date=END_DATE)
    cli_df = collector.climate.read_all(start_date=START_DATE, end_date=END_DATE)
    
    # Filter by station (asos/metar/climate are fetcher attributes)
    if not asos_df.empty:
        asos_df = asos_df[asos_df["station"] == icao]
    if not metar_df.empty:
        metar_df = metar_df[metar_df["station"] == icao]
    if not cli_df.empty:
        cli_df = cli_df[cli_df["station"] == icao]
        
    print(f"  ASOS Rows:  {len(asos_df)}")
    print(f"  METAR Rows: {len(metar_df)}")
    print(f"  CLI Rows:   {len(cli_df)}")

    # 2. Compute Daily Maxes (LST)
    print("\nComputing daily stats (LST)...")
    asos_daily = compute_daily_stats(asos_df, "valid_utc", "tmpf", offset, START_DATE, END_DATE)
    
    metar_daily = pd.DataFrame()
    if not metar_df.empty:
         metar_daily = compute_daily_stats(metar_df, "valid_utc", "temp_f", offset, START_DATE, END_DATE)

    # 3. Merge & Compare
    # CLI data is already daily and 'valid_date' is the local date.
    # Note: CLI 'high_f' is the target.
    
    # Rename columns for merge
    if not asos_daily.empty:
        asos_daily = asos_daily.rename(columns={
            "day_max": "asos_max", "day_count": "asos_count"
        })
    
    if not metar_daily.empty:
        metar_daily = metar_daily.rename(columns={
            "day_max": "metar_max", "day_count": "metar_count"
        })

    merged = cli_df[["valid_date", "high_f"]].copy()
    # Normalize valid_date types
    merged["valid_date"] = pd.to_datetime(merged["valid_date"]).dt.date
    
    if not asos_daily.empty:
        merged = pd.merge(merged, asos_daily, on="valid_date", how="left")
    if not metar_daily.empty:
        merged = pd.merge(merged, metar_daily, on="valid_date", how="left")
        
    merged = merged.sort_values("valid_date").reset_index(drop=True)
    
    # 4. Discrepancy Calc
    merged["diff_asos"] = merged["asos_max"] - merged["high_f"]
    merged["diff_metar"] = merged["metar_max"] - merged["high_f"]
    
    # 5. Output Results
    print(f"\n{'─'*70}")
    print(f"RESULTS: {icao}")
    print(f"{'─'*70}")
    print(f"{'Date':<12} {'CLI':<6} {'ASOS':<6} {'Diff':<6} {'Count':<6} {'METAR':<6} {'Diff':<6}")
    print(f"{'':<12} {'High':<6} {'Max':<6} {'':<6} {'':<6} {'Max':<6} {'':<6}")
    print("─" * 70)
    
    for _, row in merged.iterrows():
        d = row["valid_date"]
        cli = f"{row['high_f']:.0f}" if pd.notna(row['high_f']) else "?"
        
        a_max = f"{row['asos_max']:.0f}" if pd.notna(row['asos_max']) else "?"
        a_diff = f"{row['diff_asos']:+.0f}" if pd.notna(row['diff_asos']) else "?"
        a_cnt = f"{row['asos_count']:.0f}" if pd.notna(row['asos_count']) else "?"
        
        m_max = f"{row['metar_max']:.1f}" if pd.notna(row['metar_max']) else "?"
        m_diff = f"{row['diff_metar']:+.1f}" if pd.notna(row['diff_metar']) else "?"
        
        print(f"{str(d):<12} {cli:<6} {a_max:<6} {a_diff:<6} {a_cnt:<6} {m_max:<6} {m_diff:<6}")

    # Summary Stats
    if "diff_asos" in merged.columns and not merged["diff_asos"].dropna().empty:
        diffs = merged["diff_asos"].dropna()
        print(f"\nSUMMARY ({icao}) - ASOS vs CLI:")
        print(f"  Exact Match:        {(diffs == 0).sum()}/{len(diffs)} ({100*(diffs==0).mean():.1f}%)")
        print(f"  Within ±1°F:        {(diffs.abs() <= 1).sum()}/{len(diffs)} ({100*(diffs.abs()<=1).mean():.1f}%)")
        print(f"  MAE:                {diffs.abs().mean():.2f}°F")
        print(f"  Mean Bias:          {diffs.mean():.2f}°F (Negative = ASOS under)")


def main():
    setup_logging()
    
    print("=" * 70)
    print(f"WEATHER DISCREPANCY ANALYSIS (via Parquet)")
    print(f"Period: {START_DATE} -> {END_DATE}")
    print("=" * 70)
    
    config_path = _project_root / "services" / "config.yaml"
    collector = IEMAWCDataCollector.from_config(config_path)

    # 1. FETCH & SAVE (Production Pipeline)
    print("\n[Step 1] Fetching and saving data...")
    # This will loop day-by-day, fetch from APIs, and save to parquet
    # matching the production services logic.
    collector.collect_date_range(START_DATE, END_DATE)
    
    # 2. ANALYZE
    print("\n[Step 2] Analyzing saved data...")
    for station in collector.stations:
        analyze_station(collector, station.icao)


if __name__ == "__main__":
    main()
