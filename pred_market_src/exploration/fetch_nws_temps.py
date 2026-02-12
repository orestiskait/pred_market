#!/usr/bin/env python3
"""
Fetch NWS hourly observations for NYC Central Park (KNYC).

Source: https://forecast.weather.gov/data/obhistory/KNYC.html
Returns all columns by hour for each day (past ~3 days).

Usage:
    python fetch_nws_temps.py
    python fetch_nws_temps.py --output csv
"""

import argparse
import csv
import json
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DATA_RAW_NWS, NWS_STATION_NYC

# Column order from NWS obs-history table
COLUMNS = [
    "date_day",
    "time_est",
    "wind_mph",
    "vis_mi",
    "weather",
    "sky_cond",
    "temp_air_f",
    "temp_dwpt_f",
    "temp_6hr_max_f",
    "temp_6hr_min_f",
    "relative_humidity_pct",
    "wind_chill_f",
    "heat_index_f",
    "pressure_altimeter_in",
    "pressure_sea_level_mb",
    "precip_1hr_in",
    "precip_3hr_in",
    "precip_6hr_in",
]


def fetch_obhistory(station: str = NWS_STATION_NYC) -> str:
    """Fetch raw HTML from NWS obhistory page."""
    url = f"https://forecast.weather.gov/data/obhistory/{station}.html"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.text


def parse_obhistory(html: str) -> list[dict]:
    """Parse NWS obs-history table into list of hourly observations (all columns)."""
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table", class_="obs-history")
    if not table:
        return []

    rows = []
    tbody = table.find("tbody")
    if not tbody:
        return []

    for tr in tbody.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 10:
            continue
        try:
            date_day = _clean(cells[0])
            time_est = _clean(cells[1])
            if not date_day.isdigit() or not time_est:
                continue
            obs = {
                "date_day": int(date_day),
                "time_est": time_est,
                "wind_mph": _clean(cells[2]),
                "vis_mi": _parse_num(cells[3]),
                "weather": _clean(cells[4]),
                "sky_cond": _clean(cells[5]),
                "temp_air_f": _parse_num(cells[6]),
                "temp_dwpt_f": _parse_num(cells[7]),
                "temp_6hr_max_f": _parse_num(cells[8]),
                "temp_6hr_min_f": _parse_num(cells[9]),
                "relative_humidity_pct": _parse_num(cells[10], strip_pct=True),
                "wind_chill_f": _parse_num(cells[11]),
                "heat_index_f": _parse_num(cells[12]),
                "pressure_altimeter_in": _parse_num(cells[13]),
                "pressure_sea_level_mb": _parse_num(cells[14]),
                "precip_1hr_in": _parse_num(cells[15]),
                "precip_3hr_in": _parse_num(cells[16]),
                "precip_6hr_in": _parse_num(cells[17]) if len(cells) > 17 else None,
            }
            rows.append(obs)
        except (ValueError, IndexError):
            continue
    return rows


def _clean(cell) -> str:
    """Get normalized text from cell."""
    return " ".join(cell.get_text(strip=True).split())


def _parse_num(cell, strip_pct: bool = False) -> float | str | None:
    """Parse numeric value or return original string."""
    s = _clean(cell)
    if not s:
        return None
    if strip_pct and s.endswith("%"):
        s = s[:-1]
    m = re.match(r"^(-?\d+(?:\.\d+)?)", s)
    if m:
        return float(m.group(1))
    return s if s else None


def add_datetime(rows: list[dict], ref_date: datetime) -> list[dict]:
    """Add datetime_est (YYYY-MM-DD HH:MM) to each row."""
    today_day = ref_date.day
    for r in rows:
        day = r["date_day"]
        if day > today_day:
            d = ref_date - timedelta(days=32)
            d = d.replace(day=day)
        else:
            d = ref_date.replace(day=day)
        time_str = r["time_est"]
        # time is HH:MM or H:MM
        dt_str = f"{d.strftime('%Y-%m-%d')} {time_str}"
        r["datetime_est"] = dt_str
    return rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--station", default=NWS_STATION_NYC, help="NWS station code")
    ap.add_argument("--output", choices=["json", "csv", "both"], default="both")
    args = ap.parse_args()

    base = Path(__file__).resolve().parent
    raw_dir = base / DATA_RAW_NWS
    raw_dir.mkdir(parents=True, exist_ok=True)

    print(f"Fetching NWS obhistory for {args.station}...")
    html = fetch_obhistory(args.station)
    rows = parse_obhistory(html)
    if not rows:
        print("No observations parsed; page structure may have changed.")
        sys.exit(1)

    ref_date = datetime.now()
    rows = add_datetime(rows, ref_date)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    prefix = f"{args.station}_obhistory_{ts}"

    # Save raw HTML
    with open(raw_dir / f"{prefix}.html", "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Saved raw HTML: {raw_dir / f'{prefix}.html'}")

    out_data = {
        "station": args.station,
        "fetched_at": datetime.utcnow().isoformat() + "Z",
        "source": f"https://forecast.weather.gov/data/obhistory/{args.station}.html",
        "columns": COLUMNS + ["datetime_est"],
        "observations": rows,
    }

    if args.output in ("json", "both"):
        path = raw_dir / f"{prefix}_hourly.json"
        with open(path, "w") as f:
            json.dump(out_data, f, indent=2)
        print(f"Saved hourly JSON: {path}")

    if args.output in ("csv", "both"):
        path = raw_dir / f"{prefix}_hourly.csv"
        csv_columns = ["datetime_est"] + COLUMNS
        with open(path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=csv_columns, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow(r)
        print(f"Saved hourly CSV: {path}")

    print(f"\nParsed {len(rows)} hourly observations")
    print("Columns:", ", ".join(csv_columns))
    return rows


if __name__ == "__main__":
    main()
