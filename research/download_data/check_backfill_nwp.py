"""Script to verify the integrity and completeness of the downloaded NWP backfill data."""

import pandas as pd
from pathlib import Path

def check_model_directory(model: str, data_dir: Path):
    model_dir = data_dir / model
    if not model_dir.exists():
        print(f"\n[!] Model `{model}` directory not found at {model_dir}. Skipping...")
        return
    
    files = list(model_dir.glob("*.parquet"))
    if not files:
        print(f"\n[!] No parquet files found for {model}.")
        return

    print(f"\n=== Report for {model.upper()} ({len(files)} files) ===")
    
    total_rows = 0
    files_with_missing_data = 0
    total_missing_values = 0
    total_cycles_seen = 0
    total_expected_cycles_sum = 0
    
    for file in sorted(files):
        try:
            df = pd.read_parquet(file)
        except Exception as e:
            print(f"  [ERROR] reading {file.name}: {e}")
            continue
            
        total_rows += len(df)
        
        # Critical columns to check for NaNs/nulls
        critical_cols = ["model_run_time_utc", "lead_time_minutes", "forecast_target_time_utc", "tmp_2m_f"]
        cols_to_check = [c for c in critical_cols if c in df.columns]
        
        na_counts = df[cols_to_check].isna().sum()
        if na_counts.sum() > 0:
            files_with_missing_data += 1
            total_missing_values += na_counts.sum()
            print(f"  [WARN] Missing data in {file.name}:")
            for col, count in na_counts[na_counts > 0].items():
                print(f"         - {col}: {count} rows")
            
        if "model_run_time_utc" in df.columns:
            cycles = df['model_run_time_utc'].dt.hour.unique()
            total_cycles_seen += len(cycles)
            total_expected_cycles_sum += 24  # Usually 24 cycles per day per station file
            
            # Simple heuristic alert for very few cycles
            if len(cycles) < 12:
                print(f"  [INFO] Low cycle count ({len(cycles)}/24) in {file.name}")
                
    # Summary for the model
    print(f"  -> Scanned rows: {total_rows:,}")
    print(f"  -> Cycles coverage roughly: {total_cycles_seen} / {total_expected_cycles_sum} expected")
    if total_missing_values > 0:
        print(f"  -> [FAIL] Found {total_missing_values} missing critical values across {files_with_missing_data} files.")
    else:
        print("  -> [PASS] No missing critical data found! ✓")

def main():
    print("Starting NWP Backfill Data Check...")
    
    # Path resolution: we are in research/download_data/
    script_dir = Path(__file__).parent.resolve()
    data_dir = script_dir.parent.parent / "data" / "weather" / "nwp_realtime"
    
    models_to_check = ["hrrr", "nbm", "rrfs"]
    for model in models_to_check:
        check_model_directory(model, data_dir)
        
    print("\nCheck complete.\n")

if __name__ == "__main__":
    main()
