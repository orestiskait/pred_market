"""ASOS vs CLI High Plateau Analyzer — Synoptic ASOS 1-min vs NWS CLI daily high.

Compares Synoptic ASOS 1-minute temperatures with the official NWS Daily Climate Report
(CLI) daily high. Reads from synoptic_weather_observations/ (live + backfill).
Live data takes priority when deduplicating. Finds "stability plateaus" where
consecutive ASOS observations round to the same integer.

WHY THIS MATTERS
================
The NWS Daily Climate Report (CLI) publishes the official daily high, which
is what Kalshi uses for settlement.  The NWS appears to filter single-minute
ASOS spikes — if a temperature only appears for 1 minute and then drops,
it may not count as the official high.

This analyzer quantifies:
  - How often the "stable max" (peak of consecutive-same-round observations)
    matches the CLI high vs the raw ASOS max.
  - How many degrees the raw max overshoots the CLI high (spike magnitude).
  - Duration of each stability plateau (how long consecutive obs stayed at
    the same integer temperature).

TRADING IMPLICATIONS
====================
  - Tells you the minimum `consecutive_obs` parameter needed for your
    strategy to match the official high.
  - Quantifies the risk of trading on single-obs spikes — they likely
    won't settle in your favor.
  - Helps calibrate the latency window: if a stable plateau is only 2 min,
    and Synoptic latency is ~2.5 min, you might miss it entirely.

DATA SOURCES
============
  - ``synoptic_weather_observations/`` — Synoptic ASOS 1-min (live WebSocket + REST backfill)
  - ``iem_daily_climate/`` — NWS CLI official high/low
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import pyarrow.parquet as pq

from services.core.storage import ParquetStorage
from services.markets.kalshi_registry import synoptic_station_for_icao

logger = logging.getLogger("backtest.asos_cli_plateau_analyzer")


# ======================================================================
# Data classes
# ======================================================================

@dataclass
class StabilityPlateau:
    """One contiguous run of consecutive observations rounding to the same integer."""
    temp_rounded: int        # The integer temperature
    start_time: datetime     # Start of the plateau
    end_time: datetime       # End of the plateau (time of last obs in plateau)
    duration_minutes: int    # Number of minutes in the plateau
    max_raw_temp: float      # Max raw (unrounded) temp within the plateau
    n_obs: int               # Number of observations in the plateau


@dataclass
class DayAnalysis:
    """Complete analysis of one day's ASOS data vs CLI high."""
    station: str
    climate_date: date
    cli_high_f: int | None        # Official NWS CLI high; None if no CLI data
    asos_raw_max: float           # Highest raw ASOS temperature in NWS window
    stable_max: float | None      # Highest temp on a stability plateau (N consecutive same-round obs)
    stable_max_rounded: int | None
    raw_matches_cli: bool | None  # Does round(asos_raw_max) == cli_high?
    stable_matches_cli: bool | None  # Does stable_max_rounded == cli_high?
    spike_magnitude: int          # round(raw_max) - stable_max_rounded (degree overshoot)
    n_obs: int                    # Total ASOS observations in NWS window
    highest_plateau: StabilityPlateau | None  # The plateau reaching stable_max
    all_plateaus: list[StabilityPlateau] = field(default_factory=list)
    # NWS uses 2-min or 5-min averaging for official high. CLI reports whole °F.
    # All comparisons use rounded values: round(value) == cli_high_f.
    avg2_max: float | None = None  # Max of 2-min rolling average
    avg5_max: float | None = None  # Max of 5-min rolling average
    avg2_matches_cli: bool | None = None  # round(avg2_max) == cli_high_f
    avg5_matches_cli: bool | None = None  # round(avg5_max) == cli_high_f


# ======================================================================
# Core analysis functions
# ======================================================================

def find_plateaus(
    temps: list[float],
    times: list[datetime],
    min_consecutive: int = 2,
) -> list[StabilityPlateau]:
    """Find contiguous runs of consecutive observations that round to the same integer.

    Parameters
    ----------
    temps : list of float
        Temperature values in chronological order.
    times : list of datetime
        Corresponding timestamps.
    min_consecutive : int
        Minimum number of consecutive observations to count as a plateau.
        Default = 2 (Tₙ and Tₙ₊₁ round to same integer).

    Returns
    -------
    List of StabilityPlateau, sorted by max_raw_temp descending.
    """
    if len(temps) < min_consecutive:
        return []

    plateaus: list[StabilityPlateau] = []
    rounded = [round(t) for t in temps]

    # Walk through and find contiguous runs of same round value
    run_start = 0
    for i in range(1, len(rounded)):
        if rounded[i] != rounded[run_start]:
            # End of a run
            run_len = i - run_start
            if run_len >= min_consecutive:
                run_temps = temps[run_start:i]
                plateaus.append(StabilityPlateau(
                    temp_rounded=int(rounded[run_start]),
                    start_time=times[run_start],
                    end_time=times[i - 1],
                    duration_minutes=run_len,
                    max_raw_temp=max(run_temps),
                    n_obs=run_len,
                ))
            run_start = i

    # Final run
    run_len = len(rounded) - run_start
    if run_len >= min_consecutive:
        run_temps = temps[run_start:]
        plateaus.append(StabilityPlateau(
            temp_rounded=int(rounded[run_start]),
            start_time=times[run_start],
            end_time=times[-1],
            duration_minutes=run_len,
            max_raw_temp=max(run_temps),
            n_obs=run_len,
        ))

    # Sort by max_raw_temp descending
    plateaus.sort(key=lambda p: p.max_raw_temp, reverse=True)
    return plateaus


def nws_window_utc(
    climate_date: date,
    tz_name: str,
    lat: float | None = None,
) -> tuple[datetime, datetime]:
    """Return NWS climate-day boundaries in UTC.

    NWS uses Local Standard Time (LST) year-round for climate days, even during DST.
    During DST, the "Tuesday" climate day = 1:00 AM Tuesday to 12:59 AM Wednesday
    in local clock time. We use a winter date to get the LST offset (avoids DST).
    See docs/events/kalshi_settlement_rules.md.

    Parameters
    ----------
    climate_date : date
        The NWS climate day.
    tz_name : str
        IANA timezone (e.g. America/New_York, America/Chicago).
    lat : float, optional
        Station latitude (decimal degrees). If provided and < 0 (southern hemisphere),
        uses Jul 15 for winter; otherwise Jan 15. Enables correct LST for NYC (DST),
        Phoenix (no DST), and future southern-hemisphere stations.
    """
    from zoneinfo import ZoneInfo
    from datetime import timezone

    tz = ZoneInfo(tz_name)
    # Winter date: northern hemisphere = Jan, southern = Jul (LST = standard time)
    month, day = (7, 15) if (lat is not None and lat < 0) else (1, 15)
    winter_dt = datetime(climate_date.year, month, day, 12, 0, tzinfo=tz)
    lst_offset = winter_dt.utcoffset()

    midnight_lst = datetime(climate_date.year, climate_date.month, climate_date.day, 0, 0)
    start_utc = (midnight_lst - lst_offset).replace(tzinfo=timezone.utc)
    end_utc = start_utc + timedelta(hours=24)
    return start_utc, end_utc


def analyze_day(
    asos_df: pd.DataFrame,
    cli_high_f: int | None,
    station: str,
    climate_date: date,
    tz_name: str,
    min_consecutive: int = 2,
    lat: float | None = None,
) -> DayAnalysis:
    """Analyze one day of ASOS data against the CLI high.

    Parameters
    ----------
    asos_df : DataFrame
        ASOS 1-minute data with columns ['valid_utc', 'tmpf'].
    cli_high_f : int or None
        Official NWS CLI high.
    station : str
        ICAO station ID (e.g. "KMDW").
    climate_date : date
        The NWS climate day being analyzed.
    tz_name : str
        IANA timezone name.
    min_consecutive : int
        Minimum consecutive obs for a plateau (default 2).
    lat : float, optional
        Station latitude for LST hemisphere (north=Jan 15, south=Jul 15).
    """
    nws_start, nws_end = nws_window_utc(climate_date, tz_name, lat=lat)
    nws_obs = asos_df[
        (asos_df["valid_utc"] >= nws_start) & (asos_df["valid_utc"] < nws_end)
    ].sort_values("valid_utc").reset_index(drop=True)

    if nws_obs.empty:
        return DayAnalysis(
            station=station, climate_date=climate_date,
            cli_high_f=cli_high_f, asos_raw_max=float("nan"),
            stable_max=None, stable_max_rounded=None,
            raw_matches_cli=None, stable_matches_cli=None,
            spike_magnitude=0, n_obs=0, highest_plateau=None,
            avg2_max=None, avg5_max=None, avg2_matches_cli=None, avg5_matches_cli=None,
        )

    temps = nws_obs["tmpf"].tolist()
    times = nws_obs["valid_utc"].tolist()
    raw_max = max(temps)

    # NWS official high uses 2-min or 5-min averaging (see asos_temperature_resolution.md)
    avg2_max = max(
        sum(temps[i : i + 2]) / 2 for i in range(len(temps) - 1)
    ) if len(temps) >= 2 else None
    avg5_max = max(
        sum(temps[i : i + 5]) / 5 for i in range(len(temps) - 4)
    ) if len(temps) >= 5 else None

    avg2_matches = (round(avg2_max) == cli_high_f) if (cli_high_f is not None and avg2_max is not None) else None
    avg5_matches = (round(avg5_max) == cli_high_f) if (cli_high_f is not None and avg5_max is not None) else None

    plateaus = find_plateaus(temps, times, min_consecutive)
    if plateaus:
        highest = plateaus[0]
        stable_max = highest.max_raw_temp
        stable_rounded = highest.temp_rounded
    else:
        highest = None
        stable_max = None
        stable_rounded = None

    # CLI reports whole °F; compare rounded values only
    raw_rounded = round(raw_max)
    raw_matches = (raw_rounded == cli_high_f) if cli_high_f is not None else None
    stable_matches = (stable_rounded == cli_high_f) if (cli_high_f is not None and stable_rounded is not None) else None
    spike = (raw_rounded - stable_rounded) if stable_rounded is not None else 0

    return DayAnalysis(
        station=station,
        climate_date=climate_date,
        cli_high_f=cli_high_f,
        asos_raw_max=raw_max,
        stable_max=stable_max,
        stable_max_rounded=stable_rounded,
        raw_matches_cli=raw_matches,
        stable_matches_cli=stable_matches,
        spike_magnitude=spike,
        n_obs=len(nws_obs),
        highest_plateau=highest,
        all_plateaus=plateaus,
        avg2_max=avg2_max,
        avg5_max=avg5_max,
        avg2_matches_cli=avg2_matches,
        avg5_matches_cli=avg5_matches,
    )


# ======================================================================
# Batch analyzer
# ======================================================================

class AsosCliPlateauAnalyzer:
    """Compare Synoptic ASOS 1-min plateau temps vs NWS CLI daily high across days.

    Data sources: synoptic_weather_observations/ (ASOS) and iem_daily_climate/ (CLI).
    Deduplicates by (ob_timestamp, stid), preferring source=live over backfill.

    Parameters
    ----------
    data_dir : str
        Root data directory (contains synoptic_weather_observations/ and iem_daily_climate/).
    station : str
        ICAO station ID (e.g. "KMDW").
    tz_name : str
        IANA timezone for the station.
    min_consecutive : int
        Minimum consecutive same-round observations for a plateau.
    lat : float, optional
        Station latitude (decimal degrees). Used for LST hemisphere: north uses
        Jan 15, south uses Jul 15. Auto from registry when omitted.
    asos_source : str, optional
        "synoptic" (default) or "iem". IEM has full historical 1-min data.
    """

    def __init__(
        self,
        data_dir: str,
        station: str = "KMDW",
        tz_name: str = "America/Chicago",
        min_consecutive: int = 2,
        lat: float | None = None,
        asos_source: str = "synoptic",
    ):
        self.data_dir = Path(data_dir)
        self.station = station
        self.tz_name = tz_name
        self.min_consecutive = min_consecutive
        self.lat = lat
        self.asos_source = asos_source.lower()
        if self.asos_source not in ("synoptic", "iem"):
            raise ValueError(f"asos_source must be 'synoptic' or 'iem', got {asos_source!r}")

        self.storage = ParquetStorage(str(data_dir))
        self.cli_dir = self.data_dir / "iem_daily_climate"
        self._stid = synoptic_station_for_icao(station) if self.asos_source == "synoptic" else None
        if self.asos_source == "synoptic" and not self._stid:
            raise ValueError(f"No Synoptic station for {station}")

    def _available_dates(self) -> list[date]:
        """Dates where both ASOS and CLI data exist for this station."""
        asos_dates = set()
        if self.asos_source == "synoptic":
            for f in sorted(self.storage.dirs["synoptic_ws"].glob("*.parquet")):
                try:
                    file_date = date.fromisoformat(f.stem)
                except ValueError:
                    continue
                df = pq.read_table(str(f)).to_pandas()
                if "stid" in df.columns and (df["stid"] == self._stid).any():
                    asos_dates.add(file_date)
        else:
            iem_dir = self.data_dir / "iem_asos_1min"
            for f in iem_dir.glob(f"{self.station}_*.parquet"):
                m = re.search(r"_(\d{4}-\d{2}-\d{2})\.parquet$", f.name)
                if m:
                    asos_dates.add(date.fromisoformat(m.group(1)))

        cli_dates = set()
        for f in self.cli_dir.glob(f"{self.station}_*.parquet"):
            m = re.search(r"_(\d{4}-\d{2}-\d{2})\.parquet$", f.name)
            if m:
                cli_dates.add(date.fromisoformat(m.group(1)))

        overlap = sorted(asos_dates & cli_dates)
        src = self._stid if self.asos_source == "synoptic" else "iem"
        logger.info(
            "Station %s (%s): %d ASOS dates, %d CLI dates, %d overlap",
            self.station, src, len(asos_dates), len(cli_dates), len(overlap),
        )
        return overlap

    def _load_asos(self, d: date) -> pd.DataFrame:
        """Load ASOS data for climate day d.

        NWS climate day uses Local Standard Time (midnight-to-midnight LST).
        The UTC window may span 1–2 calendar dates depending on timezone.
        """
        nws_start, nws_end = nws_window_utc(d, self.tz_name, lat=self.lat)
        start_date = nws_start.date()
        end_date = (nws_end - timedelta(microseconds=1)).date()
        dates_to_load = [
            start_date + timedelta(days=i)
            for i in range((end_date - start_date).days + 1)
        ]

        frames = []
        if self.asos_source == "synoptic":
            for load_date in dates_to_load:
                path = self.storage.dirs["synoptic_ws"] / f"{load_date.isoformat()}.parquet"
                if not path.exists():
                    continue
                df = pq.read_table(str(path)).to_pandas()
                df = df[df["stid"] == self._stid].copy()
                if df.empty:
                    continue
                df = df[df["sensor"].str.startswith("air_temp", na=False)]
                if df.empty:
                    continue
                frames.append(df)
            if not frames:
                return pd.DataFrame()
            df = pd.concat(frames, ignore_index=True)
            df["_sort"] = df["source"].map({"live": 0, "backfill": 1})
            df = df.sort_values("_sort").drop_duplicates(subset=["ob_timestamp"], keep="first")
            df = df.drop(columns=["_sort"])
            df = df.rename(columns={"ob_timestamp": "valid_utc", "value": "tmpf"})
        else:
            iem_dir = self.data_dir / "iem_asos_1min"
            for load_date in dates_to_load:
                path = iem_dir / f"{self.station}_{load_date.isoformat()}.parquet"
                if not path.exists():
                    continue
                df = pq.read_table(str(path)).to_pandas()
                if df.empty or "tmpf" not in df.columns:
                    continue
                df = df[df["station"] == self.station][["valid_utc", "tmpf"]].copy()
                if df.empty:
                    continue
                frames.append(df)
            if not frames:
                return pd.DataFrame()
            df = pd.concat(frames, ignore_index=True)
            df = df.drop_duplicates(subset=["valid_utc"], keep="first")

        df = df[["valid_utc", "tmpf"]].sort_values("valid_utc").reset_index(drop=True)
        return df

    def _load_cli_high(self, d: date) -> int | None:
        path = self.cli_dir / f"{self.station}_{d.isoformat()}.parquet"
        if not path.exists():
            return None
        df = pq.read_table(str(path)).to_pandas()
        if df.empty or "high_f" not in df.columns:
            return None
        return int(df["high_f"].iloc[0])

    def run(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> StabilityReport:
        """Run the analysis over all available overlapping dates.

        Parameters
        ----------
        start_date, end_date : date, optional
            Filter to a specific date range.

        Returns
        -------
        StabilityReport with per-day analyses and aggregate statistics.
        """
        dates = self._available_dates()
        if start_date:
            dates = [d for d in dates if d >= start_date]
        if end_date:
            dates = [d for d in dates if d <= end_date]

        if not dates:
            logger.warning("No overlapping dates found for %s", self.station)
            return StabilityReport(station=self.station, days=[], min_consecutive=self.min_consecutive)

        days: list[DayAnalysis] = []
        for d in dates:
            asos_df = self._load_asos(d)
            cli_high = self._load_cli_high(d)
            analysis = analyze_day(
                asos_df, cli_high, self.station, d, self.tz_name, self.min_consecutive,
                lat=self.lat,
            )
            days.append(analysis)

        report = StabilityReport(
            station=self.station,
            days=days,
            min_consecutive=self.min_consecutive,
        )
        return report


# ======================================================================
# StabilityReport
# ======================================================================

@dataclass
class StabilityReport:
    """Aggregated results from the ASOS vs CLI plateau analysis."""
    station: str
    days: list[DayAnalysis]
    min_consecutive: int = 2

    @property
    def n_days(self) -> int:
        return len(self.days)

    @property
    def raw_match_rate(self) -> float:
        """Fraction of days where round(asos_raw_max) == cli_high."""
        matches = [d for d in self.days if d.raw_matches_cli is True]
        total = [d for d in self.days if d.raw_matches_cli is not None]
        return len(matches) / len(total) if total else 0.0

    @property
    def stable_match_rate(self) -> float:
        """Fraction of days where stable_max_rounded == cli_high."""
        matches = [d for d in self.days if d.stable_matches_cli is True]
        total = [d for d in self.days if d.stable_matches_cli is not None]
        return len(matches) / len(total) if total else 0.0

    @property
    def avg2_match_rate(self) -> float:
        """Fraction of days where round(2-min avg max) == cli_high."""
        matches = [d for d in self.days if d.avg2_matches_cli is True]
        total = [d for d in self.days if d.avg2_matches_cli is not None]
        return len(matches) / len(total) if total else 0.0

    @property
    def avg5_match_rate(self) -> float:
        """Fraction of days where round(5-min avg max) == cli_high."""
        matches = [d for d in self.days if d.avg5_matches_cli is True]
        total = [d for d in self.days if d.avg5_matches_cli is not None]
        return len(matches) / len(total) if total else 0.0

    @property
    def spike_days(self) -> list[DayAnalysis]:
        """Days where raw_max > stable_max (single-obs spikes exceed plateau)."""
        return [d for d in self.days if d.spike_magnitude > 0]

    def to_dataframe(self) -> pd.DataFrame:
        """Convert to a summary DataFrame. All comparisons use rounded °F vs CLI."""
        rows = []
        for d in self.days:
            raw_r = round(d.asos_raw_max) if d.n_obs > 0 else None
            avg2_r = round(d.avg2_max) if d.avg2_max is not None else None
            avg5_r = round(d.avg5_max) if d.avg5_max is not None else None
            rows.append({
                "date": d.climate_date,
                "cli_high_f": d.cli_high_f,
                "raw_rounded": raw_r,
                "avg2_rounded": avg2_r,
                "avg5_rounded": avg5_r,
                "stable_rounded": d.stable_max_rounded,
                "raw_matches_cli": d.raw_matches_cli,
                "avg2_matches_cli": d.avg2_matches_cli,
                "avg5_matches_cli": d.avg5_matches_cli,
                "stable_matches_cli": d.stable_matches_cli,
                "spike_deg": d.spike_magnitude,
                "plateau_duration_min": d.highest_plateau.duration_minutes if d.highest_plateau else None,
                "n_obs": d.n_obs,
            })
        return pd.DataFrame(rows)

    def log_summary(self):
        """Print a human-readable summary."""
        logger.info("=" * 70)
        logger.info("ASOS vs CLI HIGH PLATEAU ANALYSIS — %s", self.station)
        logger.info("=" * 70)
        logger.info("  Days analyzed       : %d", self.n_days)
        logger.info("  Min consecutive obs : %d", self.min_consecutive)
        logger.info("  Raw max == CLI high : %.0f%% (%d/%d)",
                     self.raw_match_rate * 100,
                     sum(1 for d in self.days if d.raw_matches_cli is True),
                     sum(1 for d in self.days if d.raw_matches_cli is not None))
        logger.info("  2-min avg max == CLI: %.0f%% (%d/%d)",
                     self.avg2_match_rate * 100,
                     sum(1 for d in self.days if d.avg2_matches_cli is True),
                     sum(1 for d in self.days if d.avg2_matches_cli is not None))
        logger.info("  5-min avg max == CLI: %.0f%% (%d/%d)",
                     self.avg5_match_rate * 100,
                     sum(1 for d in self.days if d.avg5_matches_cli is True),
                     sum(1 for d in self.days if d.avg5_matches_cli is not None))
        logger.info("  Stable max == CLI   : %.0f%% (%d/%d)",
                     self.stable_match_rate * 100,
                     sum(1 for d in self.days if d.stable_matches_cli is True),
                     sum(1 for d in self.days if d.stable_matches_cli is not None))
        logger.info("  Days with spikes    : %d (raw > stable)", len(self.spike_days))
        logger.info("-" * 70)

        # Per-day detail
        for d in self.days:
            plateau_str = ""
            if d.highest_plateau:
                plateau_str = f"plateau={d.highest_plateau.duration_minutes}min"
            else:
                plateau_str = "no plateau"

            raw_flag = "✅" if d.raw_matches_cli else "❌" if d.raw_matches_cli is False else "?"
            avg2_flag = "✅" if d.avg2_matches_cli else "❌" if d.avg2_matches_cli is False else "?"
            avg5_flag = "✅" if d.avg5_matches_cli else "❌" if d.avg5_matches_cli is False else "?"
            stable_flag = "✅" if d.stable_matches_cli else "❌" if d.stable_matches_cli is False else "?"

            logger.info(
                "  %s | CLI=%s | raw=%s %s | avg2=%s %s | avg5=%s %s | stable=%s %s | %s",
                d.climate_date,
                f"{d.cli_high_f:3d}" if d.cli_high_f is not None else "N/A",
                f"{round(d.asos_raw_max):3.0f}" if d.n_obs > 0 else "N/A",
                raw_flag,
                f"{round(d.avg2_max):3.0f}" if d.avg2_max is not None else "N/A",
                avg2_flag,
                f"{round(d.avg5_max):3.0f}" if d.avg5_max is not None else "N/A",
                avg5_flag,
                f"{d.stable_max_rounded:3d}" if d.stable_max_rounded is not None else "N/A",
                stable_flag,
                plateau_str,
            )
        logger.info("=" * 70)

    def print_table(self):
        """Print a clean table to stdout."""
        df = self.to_dataframe()
        if df.empty:
            print("No data.")
            return

        print(f"\n{'=' * 75}")
        print(f"ASOS vs CLI HIGH PLATEAU — {self.station} "
              f"(min_consecutive={self.min_consecutive})")
        print(f"{'=' * 75}")
        print(df.to_string(index=False))
        print(f"\nRaw max == CLI:     {self.raw_match_rate * 100:.0f}%")
        print(f"2-min avg max == CLI: {self.avg2_match_rate * 100:.0f}%")
        print(f"5-min avg max == CLI: {self.avg5_match_rate * 100:.0f}%")
        print(f"Stable max == CLI:  {self.stable_match_rate * 100:.0f}%")
        print(f"Spike days:         {len(self.spike_days)}/{self.n_days}")
        print(f"{'=' * 75}")
