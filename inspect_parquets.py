import os
import pandas as pd
import glob

def print_head(path):
    print(f"\n--- Head of {path} ---")
    try:
        df = pd.read_parquet(path)
        print(df.head())
        print(f"Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
    except Exception as e:
        print(f"Error reading {path}: {e}")

base_dirs = [
    "/home/kaitores/projects/pred_market/data/weather/wethr_push",
    "/home/kaitores/projects/pred_market/data/kalshi"
]

for base_dir in base_dirs:
    print(f"\n{'='*20} Exploring {base_dir} {'='*20}")
    for root, dirs, files in os.walk(base_dir):
        # Look for the first parquet file in each directory
        parquet_files = [f for f in files if f.endswith('.parquet')]
        if parquet_files:
            # We found files in this directory. Just take the first one as representative.
            file_path = os.path.join(root, parquet_files[0])
            print_head(file_path)
