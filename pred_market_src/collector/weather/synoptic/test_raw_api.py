"""Test Synoptic API via raw HTTP requests.

Fetches recent observations and computes observation delay (useful for
validating real-time data latency). Uses KMDW1M by default.
Run: python -m pred_market_src.collector.weather.synoptic.test_raw_api
"""

import datetime
import os
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

# Load .env from collector directory
dotenv_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path)

STATION = "KMDW1M"


def main():
    token = os.environ.get("SYNOPTIC_API_TOKEN")
    if not token:
        raise SystemExit("Synoptic API Token not found. Set SYNOPTIC_API_TOKEN in .env")

    url = "https://api.synopticdata.com/v2/stations/timeseries"
    params = {
        "token": token,
        "stid": STATION,
        "recent": 60,  # Minutes
        "units": "english",
        "obtimezone": "local",
    }

    response = requests.get(url, params=params)
    data = response.json()

    if "STATION" not in data or len(data["STATION"]) == 0:
        print("No STATION data returned.")
        print("Response data:", data)
        return

    obs = data["STATION"][0]["OBSERVATIONS"]
    df = pd.DataFrame(obs)
    df["date_time"] = pd.to_datetime(df["date_time"])

    now_utc = datetime.datetime.now(datetime.timezone.utc)
    latest_obs = df["date_time"].iloc[-1].to_pydatetime()
    latest_obs_utc = latest_obs.astimezone(datetime.timezone.utc)
    delay = now_utc - latest_obs_utc

    print(f"Current UTC time: {now_utc}")
    print(f"Latest observation UTC time: {latest_obs_utc}")
    print(f"Total rows: {len(df)}")
    print(f"Delay: {delay}")


if __name__ == "__main__":
    main()
