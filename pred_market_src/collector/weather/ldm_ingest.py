#!/usr/bin/env python3
"""LDM ingest handler for real-time ASOS / METAR surface observations.

This script is called by LDM's pqact (pattern-action) system.  LDM pipes
raw METAR text products to stdin; we decode, filter to stations of interest,
and append observations to parquet files on disk.

Invocation (from pqact.conf):
    IDS|DDPLUS  ^S[AP]...K
        PIPE    /path/to/python /path/to/ldm_ingest.py

The script processes one WMO product per invocation.  Each product may
contain multiple METAR/SPECI reports.  Only stations listed in
STATION_REGISTRY (via their ICAO codes) are kept; the rest are discarded.

Data is stored in the same parquet layout as the existing weather fetchers:
    data/weather_obs/ldm_surface/<ICAO>_<date>.parquet

This gives us near-real-time 1-minute resolution surface observations
(temp, dewpoint, wind, pressure, visibility, weather) for all Kalshi-tracked
stations, with ~30 second latency from the LDM feed.
"""

from __future__ import annotations

import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Bootstrap: make sure project root is importable regardless of how LDM
# invokes us (cwd may be /home/ldm or anywhere else).
# ---------------------------------------------------------------------------
_SCRIPT_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _SCRIPT_DIR.parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from pred_market_src.collector.weather.stations import STATION_REGISTRY, StationInfo

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Where to write parquet files.  Overridable via env var for Docker flexibility.
DATA_DIR = Path(
    os.environ.get(
        "LDM_WEATHER_DATA_DIR",
        str(_SCRIPT_DIR.parent / "data" / "weather_obs" / "ldm_surface"),
    )
)

# Only ingest observations from stations we care about.
TRACKED_ICAO: set[str] = {info.icao for info in STATION_REGISTRY.values()}

# Set up logging — LDM captures stderr, so we log there.
LOG_LEVEL = os.environ.get("LDM_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(name)s %(levelname)-5s %(message)s",
    stream=sys.stderr,
)
logger = logging.getLogger("ldm_ingest")


# ---------------------------------------------------------------------------
# METAR parsing (lightweight, no external dependency)
# ---------------------------------------------------------------------------

# Regex to extract a METAR/SPECI report line.
# Matches: METAR KMDW 191753Z ... or SPECI KNYC 191755Z ...
_METAR_RE = re.compile(
    r"^(METAR|SPECI)\s+"
    r"(?:COR\s+)?"          # optional COR (correction)
    r"([A-Z]{4})\s+"         # ICAO station ID
    r"(\d{6})Z\s+"           # day-hour-minute Z
    r"(.+)",                 # rest of the report
    re.MULTILINE,
)

# Temperature group: M prefix = negative, e.g. 24/18 or M02/M05
_TEMP_RE = re.compile(r"\b(M?\d{2})/(M?\d{2})\b")

# Wind: 18010KT or 18010G20KT or VRB05KT
_WIND_RE = re.compile(r"\b(\d{3}|VRB)(\d{2,3})(?:G(\d{2,3}))?KT\b")

# Visibility (statute miles): 10SM, 3SM, 1/2SM, 1 1/2SM, P6SM
_VIS_RE = re.compile(r"\b(?:P)?(\d+\s)?(\d+/?\.?\d*)?SM\b")

# Altimeter setting: A2992
_ALT_RE = re.compile(r"\bA(\d{4})\b")

# Sea-level pressure from remarks: SLP142 (= 1014.2 mb)
_SLP_RE = re.compile(r"\bSLP(\d{3})\b")


def _parse_temp_c(raw: str) -> float | None:
    """Parse a METAR temp token like '24' or 'M02' to °C."""
    if raw is None:
        return None
    val = raw.replace("M", "-")
    try:
        return float(val)
    except ValueError:
        return None


def _c_to_f(celsius: float | None) -> float | None:
    if celsius is None:
        return None
    return round(celsius * 9.0 / 5.0 + 32.0, 1)


def _parse_visibility(raw_report: str) -> float | None:
    """Extract visibility in statute miles from report text."""
    m = _VIS_RE.search(raw_report)
    if not m:
        return None
    whole = (m.group(1) or "").strip()
    frac = m.group(2) or ""
    try:
        if "/" in frac:
            num, den = frac.split("/")
            val = float(num) / float(den)
        else:
            val = float(frac) if frac else 0.0
        if whole:
            val += float(whole)
        return val
    except (ValueError, ZeroDivisionError):
        return None


def _parse_slp(raw_report: str) -> float | None:
    """Extract sea-level pressure from SLP remark (millibars)."""
    m = _SLP_RE.search(raw_report)
    if not m:
        return None
    raw = int(m.group(1))
    # SLP convention: add 1000 if < 500, else 900
    if raw < 500:
        return 1000.0 + raw / 10.0
    else:
        return 900.0 + raw / 10.0


def parse_metar_product(text: str) -> list[dict]:
    """Parse a WMO surface-observation product into observation dicts.

    Only returns observations for stations in TRACKED_ICAO.
    """
    rows: list[dict] = []
    now_utc = datetime.now(timezone.utc)

    for match in _METAR_RE.finditer(text):
        metar_type = match.group(1)   # METAR or SPECI
        icao = match.group(2)
        ddhhmm = match.group(3)
        body = match.group(4)

        if icao not in TRACKED_ICAO:
            continue

        # Parse observation time
        try:
            day = int(ddhhmm[:2])
            hour = int(ddhhmm[2:4])
            minute = int(ddhhmm[4:6])
            # Construct UTC time using current month/year
            obs_time = now_utc.replace(
                day=day, hour=hour, minute=minute, second=0, microsecond=0
            )
            # Handle month boundary wrap
            if obs_time > now_utc:
                # Observation day is in the future → belongs to previous month
                if obs_time.month == 1:
                    obs_time = obs_time.replace(year=obs_time.year - 1, month=12)
                else:
                    obs_time = obs_time.replace(month=obs_time.month - 1)
        except (ValueError, OverflowError):
            logger.warning("Bad datetime in METAR: %s", ddhhmm)
            continue

        full_report = f"{metar_type} {icao} {ddhhmm}Z {body}"

        # Temperature / Dewpoint
        temp_c = dewp_c = None
        tm = _TEMP_RE.search(body)
        if tm:
            temp_c = _parse_temp_c(tm.group(1))
            dewp_c = _parse_temp_c(tm.group(2))

        # Wind
        wdir = None
        wspd_kt = None
        gust_kt = None
        wm = _WIND_RE.search(body)
        if wm:
            wdir = wm.group(1)
            wspd_kt = float(wm.group(2))
            if wm.group(3):
                gust_kt = float(wm.group(3))

        # Visibility
        vis_sm = _parse_visibility(body)

        # Altimeter
        alt_inhg = None
        am = _ALT_RE.search(body)
        if am:
            alt_inhg = float(am.group(1)) / 100.0

        # SLP
        slp_mb = _parse_slp(full_report)

        row = {
            "station": icao,
            "valid_utc": obs_time,
            "metar_type": metar_type,
            "temp_c": temp_c,
            "dewp_c": dewp_c,
            "temp_f": _c_to_f(temp_c),
            "dewp_f": _c_to_f(dewp_c),
            "wdir": wdir,
            "wspd_kt": wspd_kt,
            "gust_kt": gust_kt,
            "visibility_sm": vis_sm,
            "altimeter_inhg": alt_inhg,
            "slp_mb": slp_mb,
            "raw_ob": full_report.strip(),
            "ingest_ts": now_utc,
            "source": "ldm",
        }
        rows.append(row)

    return rows


# ---------------------------------------------------------------------------
# Parquet persistence
# ---------------------------------------------------------------------------

def append_to_parquet(rows: list[dict]) -> None:
    """Append observation rows to date-partitioned parquet files.

    File naming: <ICAO>_<YYYY-MM-DD>.parquet (matches existing convention).
    Deduplicates by (station, valid_utc) — keeps newest ingest.
    """
    if not rows:
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)

    # Group by station + date to write to correct files
    df["_date"] = df["valid_utc"].dt.date
    for (icao, obs_date), group in df.groupby(["station", "_date"]):
        path = DATA_DIR / f"{icao}_{obs_date.isoformat()}.parquet"
        group = group.drop(columns=["_date"])

        if path.exists():
            existing = pd.read_parquet(path)
            combined = pd.concat([existing, group], ignore_index=True)
            combined = combined.drop_duplicates(
                subset=["station", "valid_utc"], keep="last"
            )
            combined = combined.sort_values("valid_utc").reset_index(drop=True)
            combined.to_parquet(path, index=False)
            logger.debug("Appended %d rows → %s (total %d)", len(group), path, len(combined))
        else:
            group = group.sort_values("valid_utc").reset_index(drop=True)
            group.to_parquet(path, index=False)
            logger.info("Created %s with %d rows", path, len(group))


# ---------------------------------------------------------------------------
# Main entry point — called by LDM pqact PIPE action
# ---------------------------------------------------------------------------

def main():
    """Read a WMO product from stdin, parse, filter, and persist."""
    try:
        raw_text = sys.stdin.read()
    except Exception:
        logger.exception("Failed to read stdin")
        sys.exit(1)

    if not raw_text.strip():
        return  # empty product, nothing to do

    rows = parse_metar_product(raw_text)
    if rows:
        logger.info(
            "Ingested %d obs: %s",
            len(rows),
            ", ".join(f"{r['station']}@{r['valid_utc'].strftime('%H:%MZ')}" for r in rows),
        )
        append_to_parquet(rows)
    else:
        logger.debug("No tracked stations in product")


if __name__ == "__main__":
    main()
