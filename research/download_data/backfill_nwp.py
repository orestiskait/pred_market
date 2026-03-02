"""Backfill NWP model data (HRRR / NBM / RRFS) with parallel downloads and LST-day clamping.

Saves to the same directory as the live production system:
  data/weather/nwp_realtime/{model}/{ICAO}_{YYYY-MM-DD}.parquet

Schema matches the base fetcher output exactly, with three extra metadata columns
appended to distinguish backfilled rows from live-ingested rows:
  - notification_ts_utc : hardcoded to cycle_time + P95 latency (see constants below)
  - saved_ts_utc        : actual wall-clock time when the row was written
  - is_live             : False  (distinguishes backfill from live SNS ingest)

Deduplication key (same as NWPRealtimeStorage):
  [station, model_run_time_utc, lead_time_minutes, model]

LST-day clamping:
  HRRR/NBM/RRFS forecasts are only kept if their forecast_target_date_lst equals
  the cycle's own LST climate day. Forecasts spilling past midnight LST are
  discarded rather than saved to the next day's file.

P95 notification latency constants (measured from live production data):
  HRRR : 5135s  (~85 min after cycle init — S3 notification to SQS arrival)
  NBM  : 4048s  (~67 min)
  RRFS : ~3600s (~60 min, estimated — insufficient live data to measure precisely)

Usage:
  python -m research.download_data.backfill_nwp
"""

from __future__ import annotations

import logging
import math
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

_project_root = Path(__file__).resolve().parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from services.weather.station_registry import NWPStation, nwp_station_for_icao

logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════
# P95 notification latency per model (seconds, from live production data)
# Used to synthesise notification_ts_utc for backfilled rows so the
# column is meaningful for calibration and feature engineering.
# ══════════════════════════════════════════════════════════════════════

P95_NOTIFICATION_LATENCY_S: dict[str, float] = {
    "hrrr": 5135.0,   # measured: P95 of (notification_ts - cycle_time) over ~6800 live rows
    "nbm":  4048.0,   # measured: P95 over ~197 live rows
    "rrfs": 3600.0,   # estimated: insufficient live data (RRFS paused Dec 2024)
}

# ══════════════════════════════════════════════════════════════════════
# Configuration — edit these constants before running
# ══════════════════════════════════════════════════════════════════════
# python3 -m research.download_data.check_backfill_nwp.py
# python3 -m research.download_data.backfill_nwp

MODELS = ["nbm", "rrfs"]           # "hrrr" | "nbm" | "rrfs" — or any combination
START_DATE = date(2025, 12, 15)
END_DATE = date(2025, 12, 17)  # inclusive
STATIONS = ["KMDW"]           # ICAO codes; multi-station supported

# Cycles to fetch (UTC hours). None = model default.
#   HRRR/RRFS: all 24 by default
#   NBM: all 24 by default
# Tip: use [0, 6, 12, 18] for a much faster (4×) run during testing.
CYCLES = None

# Max forecast hour override. None = model default (18 HRRR/RRFS, 36 NBM).
# LST clamping further reduces this per-cycle automatically.
MAX_FXX = None

# Parallel download workers (fxx files downloaded concurrently per cycle)
MAX_WORKERS = 16

# Parallel concurrent days per model
MAX_PARALLEL_DAYS = 6

# Skip cycles already present in the target parquet (safe resume after crash)
SKIP_EXISTING = True

# Config path for data_dir resolution
CONFIG_PATH = _project_root / "services" / "config.yaml"

# Storage root — same as live production system
DATA_ROOT = _project_root / "data"
NWP_REALTIME_DIR = DATA_ROOT / "weather" / "nwp_realtime"

# Deduplication key (must match NWPRealtimeStorage.DEDUP_COLS)
DEDUP_COLS = ["station", "model_run_time_utc", "lead_time_minutes", "model"]
SORT_COLS  = ["model_run_time_utc", "lead_time_minutes"]


# ══════════════════════════════════════════════════════════════════════
# LST Climate Day Utilities
# ══════════════════════════════════════════════════════════════════════

def lst_offset_hours(tz: str) -> float:
    """UTC offset for Local Standard Time (ignores DST).

    Uses a January date so DST is never in effect. NWS climate days
    run midnight-to-midnight LST, ignoring DST year-round.
    """
    dt = datetime(2025, 1, 15, 12, 0, 0, tzinfo=ZoneInfo(tz))
    return dt.utcoffset().total_seconds() / 3600


def compute_max_useful_fxx(
    cycle_utc: datetime, tz: str, model_max_fxx: int
) -> int:
    """Maximum fxx that still targets the cycle's own LST climate day.

    Avoids downloading forecast files whose valid time falls past midnight
    LST — those rows would be discarded anyway by filter_to_lst_day().

    Examples (KMDW, CST = UTC-6):
      18Z → 12:00 CST, midnight = 06:00+1 UTC → remaining = 12h → max_fxx = 12
      23Z → 17:00 CST, midnight = 06:00+1 UTC → remaining =  7h → max_fxx = 7
      03Z → 21:00 CST prev day,  midnight = 06:00 UTC → remaining = 3h → max_fxx = 3
    """
    offset = lst_offset_hours(tz)
    cycle_lst = cycle_utc + timedelta(hours=offset)
    climate_day = cycle_lst.date()
    # Midnight ending the climate day, in UTC
    next_midnight_lst = datetime(climate_day.year, climate_day.month, climate_day.day) + timedelta(days=1)
    next_midnight_utc = next_midnight_lst - timedelta(hours=offset)
    hours_remaining = (next_midnight_utc - cycle_utc).total_seconds() / 3600
    return max(0, min(math.ceil(hours_remaining), model_max_fxx))


def filter_to_lst_day(
    df: pd.DataFrame, cycle_utc: datetime, tz: str
) -> pd.DataFrame:
    """Keep only rows whose forecast target falls on the cycle's LST climate day.

    Precise row-level trim applied after download. The coarser fxx-level
    pre-filter (compute_max_useful_fxx) already limits downloads, but for
    sub-hourly models (HRRR 15-min steps) a few rows may still spill over.
    """
    if df.empty:
        return df

    offset = lst_offset_hours(tz)
    cycle_lst = cycle_utc + timedelta(hours=offset)
    climate_day = cycle_lst.date()

    # forecast_target_date_lst is a Python date object (added by _add_time_columns)
    if "forecast_target_date_lst" in df.columns:
        mask = df["forecast_target_date_lst"].apply(lambda d: d == climate_day)
    elif "forecast_target_time_utc" in df.columns:
        # Fallback: derive LST date from UTC column
        lst_times = df["forecast_target_time_utc"].dt.tz_localize(None) + pd.Timedelta(hours=offset)
        mask = lst_times.dt.date == climate_day
    else:
        return df

    filtered = df[mask].copy()
    dropped = len(df) - len(filtered)
    if dropped:
        logger.debug("LST clamp: dropped %d/%d rows outside climate day %s (tz=%s)",
                     dropped, len(df), climate_day, tz)
    return filtered


# ══════════════════════════════════════════════════════════════════════
# Metadata helpers
# ══════════════════════════════════════════════════════════════════════

def add_backfill_metadata(
    df: pd.DataFrame, model_name: str, cycle_utc: datetime
) -> pd.DataFrame:
    """Append the three extra columns that distinguish backfill from live rows.

    notification_ts_utc: cycle_time + P95 latency for this model.
    saved_ts_utc:        actual wall-clock timestamp of this write.
    is_live:             False (for all backfilled rows).
    """
    p95 = P95_NOTIFICATION_LATENCY_S.get(model_name, 4500.0)
    cycle_ts = pd.Timestamp(cycle_utc, tz="UTC") if cycle_utc.tzinfo is None else pd.Timestamp(cycle_utc)
    notif_ts = cycle_ts + pd.Timedelta(seconds=p95)
    saved_ts  = pd.Timestamp.now(tz="UTC")

    df = df.copy()
    df["notification_ts_utc"] = notif_ts
    df["saved_ts_utc"]        = saved_ts
    df["is_live"]             = False
    return df


# ══════════════════════════════════════════════════════════════════════
# Parquet I/O — saves to data/weather/nwp_realtime/{model}/
# ══════════════════════════════════════════════════════════════════════

def _model_dir(model_name: str) -> Path:
    d = NWP_REALTIME_DIR / model_name
    d.mkdir(parents=True, exist_ok=True)
    return d


def save_to_nwp_realtime(
    df: pd.DataFrame,
    model_name: str,
    station_icao: str,
    cycle_date: date,
) -> Path:
    """Append *df* to data/weather/nwp_realtime/{model}/{ICAO}_{date}.parquet.

    Deduplication key: [station, model_run_time_utc, lead_time_minutes, model].
    On collision from a previous partial run, the new row wins (keep='last').
    """
    path = _model_dir(model_name) / f"{station_icao}_{cycle_date.isoformat()}.parquet"

    def _enforce_schema(d: pd.DataFrame):
        for col in d.columns:
            if col.endswith("_utc") or col.endswith("_utc_ts"):
                d[col] = pd.to_datetime(d[col], errors="coerce", utc=True)
            elif col.endswith("_lst"):
                d[col] = pd.to_datetime(d[col], errors="coerce").dt.tz_localize(None)

    df = df.copy()
    _enforce_schema(df)

    if path.exists():
        existing = pd.read_parquet(path)
        _enforce_schema(existing)
        
        # Normalise UTC timestamps for safe concat
        for col in SORT_COLS:
            if col in existing.columns and pd.api.types.is_datetime64_any_dtype(existing[col]):
                existing[col] = pd.to_datetime(existing[col], utc=True)
            if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
                df[col] = pd.to_datetime(df[col], utc=True)
        combined = pd.concat([existing, df], ignore_index=True)
        dedup = [c for c in DEDUP_COLS if c in combined.columns]
        if dedup:
            combined = combined.drop_duplicates(subset=dedup, keep="last")
    else:
        combined = df

    sort = [c for c in SORT_COLS if c in combined.columns]
    if sort:
        combined = combined.sort_values(sort, ignore_index=True)

    combined.to_parquet(path, index=False)
    logger.debug("Saved %d rows → %s", len(combined), path)
    return path


def get_existing_cycles(model_name: str, station_icao: str, cycle_date: date) -> set[str]:
    """Return model_run_time_utc strings already stored for (model, station, date).

    Used by the resume logic to skip cycles already successfully downloaded.
    """
    path = _model_dir(model_name) / f"{station_icao}_{cycle_date.isoformat()}.parquet"
    if not path.exists():
        return set()
    try:
        df = pd.read_parquet(path, columns=["model_run_time_utc"])
        return set(df["model_run_time_utc"].astype(str).unique())
    except Exception:
        return set()


# ══════════════════════════════════════════════════════════════════════
# Fetcher factory
# ══════════════════════════════════════════════════════════════════════

def _create_fetcher(model_name: str):
    """Instantiate the correct fetcher, wired to a throwaway data_dir.

    We bypass the fetcher's built-in save logic entirely — the backfill
    script calls fetch_run() directly and handles saving itself so it can
    apply LST filtering and add metadata before writing.
    """
    tmp_dir = _project_root / "data" / "weather" / "nwp_realtime"

    if model_name == "hrrr":
        from services.weather.nwp.hrrr import HRRRFetcher
        fetcher = HRRRFetcher(data_dir=tmp_dir)
    elif model_name == "nbm":
        from services.weather.nwp.nbm_cog import NBMCOGFetcher
        fetcher = NBMCOGFetcher(data_dir=tmp_dir)
    elif model_name == "rrfs":
        from services.weather.nwp.rrfs import RRFSFetcher
        fetcher = RRFSFetcher(data_dir=tmp_dir)
    else:
        raise ValueError(f"Unknown model '{model_name}'. Choose: hrrr, nbm, rrfs")

    if MAX_FXX is not None:
        fetcher.max_forecast_hour = MAX_FXX

    return fetcher


# ══════════════════════════════════════════════════════════════════════
# Parallel fxx download
# ══════════════════════════════════════════════════════════════════════

def fetch_cycle_parallel(
    fetcher,
    cycle: datetime,
    stations: list[NWPStation],
    fxx_range: range,
    max_workers: int,
) -> pd.DataFrame:
    """Download all fxx values for one cycle concurrently.

    For NBMCOGFetcher: defers to its highly-performant native flat-parallel
    `fetch_cycle` method.
    For other fetchers: submits one task per fxx into a thread pool.
    """
    from services.weather.nwp.nbm_cog import NBMCOGFetcher

    # ── NBM natively supports flat-parallel execution across all fxx ──────
    if isinstance(fetcher, NBMCOGFetcher):
        return fetcher.fetch_cycle(cycle, stations, fxx_range, max_workers=max_workers)

    # ── Generic path (HRRR, RRFS) ─────────────────────────────────────────
    def _fetch_one(fxx: int) -> pd.DataFrame:
        try:
            return fetcher.fetch_run(cycle, fxx, stations)
        except Exception:
            logger.debug("%s fxx=%02d unavailable for %s",
                         fetcher.SOURCE_NAME, fxx, cycle.strftime("%Y-%m-%d %HZ"))
            return pd.DataFrame()

    frames: list[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=min(max_workers, len(fxx_range) or 1)) as pool:
        futures = {pool.submit(_fetch_one, fxx): fxx for fxx in fxx_range}
        for fut in as_completed(futures):
            try:
                df = fut.result()
                if df is not None and not df.empty:
                    frames.append(df)
            except Exception:
                logger.warning("%s fxx=%02d raised an exception (cycle=%s)",
                               fetcher.SOURCE_NAME, futures[fut],
                               cycle.strftime("%Y-%m-%d %HZ"), exc_info=True)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()



# ══════════════════════════════════════════════════════════════════════
# Main backfill orchestrator
# ══════════════════════════════════════════════════════════════════════

def backfill_model(
    model_name: str,
    stations: list[NWPStation],
    start_date: date,
    end_date: date,
    cycles: list[int] | None = None,
    max_workers: int = 8,
    skip_existing: bool = True,
    max_parallel_days: int = 3,
) -> None:
    fetcher = _create_fetcher(model_name)
    cycle_hours = cycles if cycles is not None else list(fetcher.DEFAULT_CYCLES)
    model_max_fxx = fetcher.max_forecast_hour

    total_days = (end_date - start_date).days + 1
    total_cycles = total_days * len(cycle_hours)

    print(f"\n{'=' * 64}")
    print(f"  Backfill: {model_name.upper()}")
    print(f"  Mode    : base-fetcher schema  (is_live=False)")
    print(f"  Storage : data/weather/nwp_realtime/{model_name}/")
    print(f"  Range   : {start_date} → {end_date}  ({total_days} days)")
    print(f"  Stations: {[s.icao for s in stations]}")
    print(f"  Cycles  : {cycle_hours} ({len(cycle_hours)}/day)")
    print(f"  Max fxx : {model_max_fxx} (pre-LST-clamp)")
    print(f"  Workers : {max_workers} fxx/cycle, {max_parallel_days} days in parallel")
    print(f"  Resume  : {'on' if skip_existing else 'off'}")
    print(f"  P95 lag : {P95_NOTIFICATION_LATENCY_S[model_name]:.0f}s  "
          f"({P95_NOTIFICATION_LATENCY_S[model_name]/60:.1f}m)")
    print(f"{'=' * 64}\n")

    completed = skipped = errors = total_rows = 0
    t0 = time.time()

    def _process_day(current_date: date) -> tuple[int, int, int, int]:
        """Download all cycles for one day and write ONE parquet at the end.

        Accumulating frames in memory and flushing once avoids the N×24
        read-concat-dedup-write pattern that was hammering the parquet file
        on every cycle.
        """
        d_completed = d_skipped = d_errors = d_total_rows = 0

        # Load existing cycle timestamps once per (station, day) for resume.
        existing_by_stn: dict[str, set[str]] = {}
        if skip_existing:
            for stn in stations:
                existing_by_stn[stn.icao] = get_existing_cycles(
                    model_name, stn.icao, current_date
                )

        # Accumulate new frames per station; write parquet ONCE at end of day.
        day_frames: dict[str, list[pd.DataFrame]] = {stn.icao: [] for stn in stations}

        for cycle_hour in cycle_hours:
            cycle_dt = datetime(current_date.year, current_date.month, current_date.day, cycle_hour)
            d_completed += 1

            if skip_existing:
                cycle_ts_str = str(pd.Timestamp(cycle_dt).tz_localize("UTC"))
                if all(
                    cycle_ts_str in existing_by_stn.get(stn.icao, set())
                    for stn in stations
                ):
                    d_skipped += 1
                    continue

            max_useful = max(
                (compute_max_useful_fxx(cycle_dt, stn.tz, model_max_fxx) for stn in stations),
                default=0,
            )
            if max_useful <= 0:
                d_skipped += 1
                continue

            fxx_range = range(0, max_useful + 1)
            logger.info(
                "%s [%s] | fxx 0–%d",
                model_name.upper(), cycle_dt.strftime("%Y-%m-%d %HZ"), max_useful,
            )

            try:
                df = fetch_cycle_parallel(fetcher, cycle_dt, stations, fxx_range, max_workers)
            except Exception:
                logger.exception("%s failed cycle %s",
                                 model_name, cycle_dt.strftime("%Y-%m-%d %HZ"))
                d_errors += 1
                continue

            if df.empty:
                continue

            for stn in stations:
                stn_df = df[df["station"] == stn.icao].copy()
                if stn_df.empty:
                    continue
                stn_df = filter_to_lst_day(stn_df, cycle_dt, stn.tz)
                if stn_df.empty:
                    continue
                stn_df = add_backfill_metadata(stn_df, model_name, cycle_dt)
                day_frames[stn.icao].append(stn_df)
                logger.debug("Queued %d rows (cycle %02d) for %s",
                             len(stn_df), cycle_hour, stn.icao)

        # --- Single write per station per day --------------------------------
        for stn in stations:
            frames = day_frames[stn.icao]
            if not frames:
                continue
            combined_new = pd.concat(frames, ignore_index=True)
            path = save_to_nwp_realtime(combined_new, model_name, stn.icao, current_date)
            n = len(combined_new)
            d_total_rows += n
            logger.info("Wrote %d rows → %s", n, path.name)

        print(
            f"  [{model_name.upper()}] {current_date}  "
            f"cycles={d_completed - d_skipped - d_errors} "
            f"skipped={d_skipped} err={d_errors} rows={d_total_rows}",
            flush=True,
        )
        return d_completed, d_skipped, d_errors, d_total_rows

    days_to_process = []
    curr = start_date
    while curr <= end_date:
        days_to_process.append(curr)
        curr += timedelta(days=1)

    with ThreadPoolExecutor(max_workers=max_parallel_days) as pool:
        futures = {pool.submit(_process_day, d): d for d in days_to_process}
        for fut in as_completed(futures):
            c, s, e, r = fut.result()
            completed += c
            skipped += s
            errors += e
            total_rows += r

    elapsed_total = time.time() - t0
    print(f"\n{'=' * 64}")
    print(f"  {model_name.upper()} backfill complete")
    print(f"  Rows saved  : {total_rows:,}")
    print(f"  Processed   : {completed - skipped}")
    print(f"  Skipped     : {skipped}")
    print(f"  Errors      : {errors}")
    print(f"  Elapsed     : {elapsed_total / 60:.1f} min")
    print(f"{'=' * 64}\n")


# ══════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    logging.getLogger("rasterio").setLevel(logging.ERROR)

    stations = [nwp_station_for_icao(icao) for icao in STATIONS]
    logger.info("Stations: %s", [(s.icao, s.city, s.tz) for s in stations])
    logger.info("Date range: %s → %s", START_DATE, END_DATE)

    with ProcessPoolExecutor(max_workers=len(MODELS)) as pool:
        futures = []
        for model in MODELS:
            futures.append(pool.submit(
                backfill_model,
                model_name=model,
                stations=stations,
                start_date=START_DATE,
                end_date=END_DATE,
                cycles=CYCLES,
                max_workers=MAX_WORKERS,
                skip_existing=SKIP_EXISTING,
                max_parallel_days=MAX_PARALLEL_DAYS,
            ))
        
        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                logger.error("A model backfill task failed: %s", e, exc_info=True)


if __name__ == "__main__":
    main()
