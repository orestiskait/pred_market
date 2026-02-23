#!/usr/bin/env python3
"""Interactive comparison of weather data sources — last 14 days.

Single chart: weather (°F) from Synoptic, IEM, NWS CLI, AWC METAR, RTMA-RU,
HRRR, NBM, RRFS.

Run cells in VS Code/Cursor (Ctrl+Enter) or run the whole file.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import pyarrow.parquet as pq

# %% Setup: project root and imports
import sys

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
except NameError:
    # Interactive window: __file__ is undefined — find project root from cwd
    cwd = Path.cwd()
    for candidate in [cwd, cwd.parent, cwd.parent.parent]:
        if (candidate / "services").is_dir() and (candidate / "data").is_dir():
            PROJECT_ROOT = candidate
            break
    else:
        PROJECT_ROOT = cwd

sys.path.insert(0, str(PROJECT_ROOT))

from services.backtest.asos_cli_plateau_analyzer import nws_window_utc
from services.markets.kalshi_registry import synoptic_station_for_icao

# %% Config
station = "KMDW"
days = 14
data_dir = PROJECT_ROOT / "data"
end_date = date.today()
start_date = end_date - timedelta(days=days)

# %% Load functions
def load_synoptic_asos(
    data_dir: Path,
    station: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load Synoptic ASOS 1-min from synoptic_weather_observations/."""
    stid = synoptic_station_for_icao(station)
    if not stid:
        return pd.DataFrame()

    base = data_dir / "synoptic_weather_observations"
    frames = []
    d = start_date
    while d <= end_date:
        path = base / f"{d.isoformat()}.parquet"
        if path.exists():
            df = pq.read_table(str(path)).to_pandas()
            df = df[(df["stid"] == stid) & (df["sensor"].str.startswith("air_temp", na=False))]
            if not df.empty:
                df = df.rename(columns={"ob_timestamp": "valid_utc", "value": "tmpf"})
                if "source" in df.columns:
                    df["_sort"] = df["source"].map({"live": 0, "backfill": 1})
                    df = df.sort_values("_sort").drop_duplicates(subset=["valid_utc"], keep="first")
                frames.append(df[["valid_utc", "tmpf"]].copy())
        d += timedelta(days=1)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["valid_utc"], keep="first")
    out = out.sort_values("valid_utc").reset_index(drop=True)
    return out


def load_iem_asos(
    data_dir: Path,
    station: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load IEM ASOS 1-min from iem_asos_1min/."""
    base = data_dir / "iem_asos_1min"
    frames = []
    d = start_date
    while d <= end_date:
        path = base / f"{station}_{d.isoformat()}.parquet"
        if path.exists():
            df = pq.read_table(str(path)).to_pandas()
            if not df.empty and "tmpf" in df.columns:
                df = df[df["station"] == station][["valid_utc", "tmpf"]].copy()
                if not df.empty:
                    frames.append(df)
        d += timedelta(days=1)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["valid_utc"], keep="first")
    return out.sort_values("valid_utc").reset_index(drop=True)


def load_cli(
    data_dir: Path,
    station: str,
    start_date: date,
    end_date: date,
    tz_name: str = "America/Chicago",
    lat: float | None = 41.78417,
) -> pd.DataFrame:
    """Load NWS CLI daily high from iem_daily_climate/."""
    base = data_dir / "iem_daily_climate"
    rows = []
    d = start_date
    while d <= end_date:
        path = base / f"{station}_{d.isoformat()}.parquet"
        if path.exists():
            df = pq.read_table(str(path)).to_pandas()
            if not df.empty and "high_f" in df.columns:
                high = int(df["high_f"].iloc[0])
                nws_start, nws_end = nws_window_utc(d, tz_name, lat=lat)
                rows.append({
                    "valid_utc": nws_start,
                    "valid_utc_end": nws_end,
                    "high_f": high,
                    "climate_date": d,
                })
        d += timedelta(days=1)

    return pd.DataFrame(rows)


def load_rtma_ru(
    data_dir: Path,
    station: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load RTMA-RU 15-min analysis from rtma_ru/ (2.5km grid at station coords)."""
    base = data_dir / "rtma_ru"
    frames = []
    d = start_date
    while d <= end_date:
        path = base / f"{station}_{d.isoformat()}.parquet"
        if path.exists():
            df = pq.read_table(str(path)).to_pandas()
            if not df.empty and "tmp_2m_f" in df.columns:
                df = df[df["station"] == station][["valid_utc", "tmp_2m_f"]].copy()
                df = df.rename(columns={"tmp_2m_f": "tmpf"})
                if not df.empty:
                    frames.append(df)
        d += timedelta(days=1)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["valid_utc"], keep="first")
    return out.sort_values("valid_utc").reset_index(drop=True)


def load_hrrr(
    data_dir: Path,
    station: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load HRRR 15-min sub-hourly from hrrr/ (3km grid at station coords)."""
    base = data_dir / "hrrr"
    frames = []
    d = start_date
    while d <= end_date:
        path = base / f"{station}_{d.isoformat()}.parquet"
        if path.exists():
            df = pq.read_table(str(path)).to_pandas()
            if not df.empty and "tmp_2m_f" in df.columns:
                df = df[df["station"] == station][["valid_utc", "tmp_2m_f"]].copy()
                df = df.rename(columns={"tmp_2m_f": "tmpf"})
                if not df.empty:
                    frames.append(df)
        d += timedelta(days=1)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["valid_utc"], keep="first")
    return out.sort_values("valid_utc").reset_index(drop=True)


def load_nbm_f02(
    data_dir: Path,
    station: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load NBM 2h-prior forecast (f02) from nbm/. Fetches from start-1d for coverage."""
    base = data_dir / "nbm"
    frames = []
    fetch_start = start_date - timedelta(days=1)
    d = fetch_start
    while d <= end_date:
        path = base / f"{station}_{d.isoformat()}.parquet"
        if path.exists():
            df = pq.read_table(str(path)).to_pandas()
            if not df.empty and "forecast_minutes" in df.columns and "tmp_2m_f" in df.columns:
                df = df[(df["station"] == station) & (df["forecast_minutes"] == 120)]
                df = df[["valid_utc", "tmp_2m_f"]].copy()
                df = df.rename(columns={"tmp_2m_f": "tmpf"})
                if not df.empty:
                    frames.append(df)
        d += timedelta(days=1)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["valid_utc"], keep="first")
    out = out.sort_values("valid_utc").reset_index(drop=True)
    # Filter to requested valid_utc range
    start_ts = pd.Timestamp(start_date, tz="UTC")
    end_ts = pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1)
    out = out[(out["valid_utc"] >= start_ts) & (out["valid_utc"] < end_ts)]
    return out.reset_index(drop=True)


def load_rrfs_f02(
    data_dir: Path,
    station: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load RRFS 2h-prior forecast (f02) from rrfs/. Fetches from start-1d for coverage."""
    base = data_dir / "rrfs"
    frames = []
    fetch_start = start_date - timedelta(days=1)
    d = fetch_start
    while d <= end_date:
        path = base / f"{station}_{d.isoformat()}.parquet"
        if path.exists():
            df = pq.read_table(str(path)).to_pandas()
            if not df.empty and "forecast_minutes" in df.columns and "tmp_2m_f" in df.columns:
                df = df[(df["station"] == station) & (df["forecast_minutes"] == 120)]
                df = df[["valid_utc", "tmp_2m_f"]].copy()
                df = df.rename(columns={"tmp_2m_f": "tmpf"})
                if not df.empty:
                    frames.append(df)
        d += timedelta(days=1)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["valid_utc"], keep="first")
    out = out.sort_values("valid_utc").reset_index(drop=True)
    start_ts = pd.Timestamp(start_date, tz="UTC")
    end_ts = pd.Timestamp(end_date, tz="UTC") + pd.Timedelta(days=1)
    out = out[(out["valid_utc"] >= start_ts) & (out["valid_utc"] < end_ts)]
    return out.reset_index(drop=True)


def load_metar(
    data_dir: Path,
    station: str,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load AWC METAR from awc_metar/."""
    base = data_dir / "awc_metar"
    frames = []
    d = start_date
    while d <= end_date:
        path = base / f"{station}_{d.isoformat()}.parquet"
        if path.exists():
            df = pq.read_table(str(path)).to_pandas()
            if not df.empty and "temp_f" in df.columns:
                cols = ["valid_utc", "temp_f"]
                if "temp_high_accuracy" in df.columns:
                    cols.append("temp_high_accuracy")
                df = df[df["station"] == station][cols].copy()
                df = df.rename(columns={"temp_f": "tmpf"})
                if not df.empty:
                    frames.append(df)
        d += timedelta(days=1)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["valid_utc"], keep="first")
    return out.sort_values("valid_utc").reset_index(drop=True)


# %% Plot function
def plot_all(
    synoptic: pd.DataFrame,
    iem: pd.DataFrame,
    cli: pd.DataFrame,
    metar: pd.DataFrame,
    station: str,
    days: int,
    synoptic_as_tgroup: pd.DataFrame | None = None,
    rtma_ru: pd.DataFrame | None = None,
    hrrr: pd.DataFrame | None = None,
    nbm: pd.DataFrame | None = None,
    rrfs: pd.DataFrame | None = None,
) -> None:
    """Plot weather sources on one overlapping chart."""
    def ensure_utc(s: pd.Series) -> pd.Series:
        if s.dt.tz is None:
            return s.dt.tz_localize("UTC")
        return s.dt.tz_convert("UTC")

    all_ts = []
    series_list: list[tuple[pd.DataFrame, str]] = [
        (synoptic, "valid_utc"),
        (iem, "valid_utc"),
        (metar, "valid_utc"),
        (cli, "valid_utc"),
        (cli, "valid_utc_end"),
    ]
    if rtma_ru is not None and not rtma_ru.empty:
        series_list.append((rtma_ru, "valid_utc"))
    if hrrr is not None and not hrrr.empty:
        series_list.append((hrrr, "valid_utc"))
    if nbm is not None and not nbm.empty:
        series_list.append((nbm, "valid_utc"))
    if rrfs is not None and not rrfs.empty:
        series_list.append((rrfs, "valid_utc"))
    if synoptic_as_tgroup is not None and not synoptic_as_tgroup.empty:
        series_list.insert(3, (synoptic_as_tgroup, "valid_utc"))
    for df, col in series_list:
        if not df.empty and col in df.columns:
            all_ts.extend(df[col].dropna().tolist())
    if not all_ts:
        print("No data to plot.")
        return
    x_min = min(all_ts)
    x_max = max(all_ts)

    fig = go.Figure()

    def add_weather_trace(trace, **kwargs):
        fig.add_trace(trace, **kwargs)

    if not synoptic.empty:
        ts = ensure_utc(synoptic["valid_utc"])
        add_weather_trace(
            go.Scattergl(
                x=ts,
                y=synoptic["tmpf"],
                mode="markers",
                marker=dict(size=5, opacity=0.8),
                name="Synoptic ASOS 1-min",
            ),
        )

    if synoptic_as_tgroup is not None and not synoptic_as_tgroup.empty:
        ts = ensure_utc(synoptic_as_tgroup["valid_utc"])
        add_weather_trace(
            go.Scattergl(
                x=ts,
                y=synoptic_as_tgroup["tmpf"],
                mode="markers",
                marker=dict(size=8, opacity=0.7, color="#e377c2"),
                name="Synoptic → T-group (1-min)",
            ),
        )

    if not iem.empty:
        ts = ensure_utc(iem["valid_utc"])
        add_weather_trace(
            go.Scattergl(
                x=ts,
                y=iem["tmpf"],
                mode="markers",
                marker=dict(size=5, opacity=0.8, color="#ff7f0e"),
                name="IEM ASOS 1-min",
            ),
        )

    if not cli.empty:
        for i, (_, row) in enumerate(cli.iterrows()):
            climate_date = row["climate_date"].strftime("%Y-%m-%d") if hasattr(row["climate_date"], "strftime") else str(row["climate_date"])
            add_weather_trace(
                go.Scatter(
                    x=[row["valid_utc"], row["valid_utc_end"]],
                    y=[row["high_f"], row["high_f"]],
                    mode="lines",
                    line=dict(color="#2ca02c", width=2),
                    showlegend=(i == 0),
                    name="NWS CLI (daily high)",
                    legendgroup="CLI",
                    hovertemplate=f"NWS CLI: %{{y}}°F<br>Climate date: {climate_date}<extra></extra>",
                ),
            )

    if not metar.empty:
        has_t_group_flag = "temp_high_accuracy" in metar.columns
        t_group = metar[metar["temp_high_accuracy"] == True] if has_t_group_flag else pd.DataFrame()
        body_only = metar[metar["temp_high_accuracy"] == False] if has_t_group_flag else pd.DataFrame()
        if has_t_group_flag and not t_group.empty:
            ts = ensure_utc(t_group["valid_utc"])
            add_weather_trace(
                go.Scatter(
                    x=ts,
                    y=t_group["tmpf"],
                    mode="markers",
                    marker=dict(size=6, opacity=0.8, color="#d62728"),
                    name="AWC METAR (T-group)",
                ),
            )
        if has_t_group_flag and not body_only.empty:
            ts = ensure_utc(body_only["valid_utc"])
            add_weather_trace(
                go.Scatter(
                    x=ts,
                    y=body_only["tmpf"],
                    mode="markers",
                    marker=dict(size=6, opacity=0.8, color="#9467bd"),
                    name="AWC METAR (body)",
                ),
            )
        if not has_t_group_flag or (t_group.empty and body_only.empty):
            ts = ensure_utc(metar["valid_utc"])
            add_weather_trace(
                go.Scatter(
                    x=ts,
                    y=metar["tmpf"],
                    mode="markers",
                    marker=dict(size=6, opacity=0.8, color="#d62728"),
                    name="AWC METAR",
                ),
            )

    if rtma_ru is not None and not rtma_ru.empty:
        ts = ensure_utc(rtma_ru["valid_utc"])
        add_weather_trace(
            go.Scattergl(
                x=ts,
                y=rtma_ru["tmpf"],
                mode="markers",
                marker=dict(size=6, opacity=0.9, color="#17becf"),
                name="RTMA-RU (15-min)",
            ),
        )

    if hrrr is not None and not hrrr.empty:
        ts = ensure_utc(hrrr["valid_utc"])
        add_weather_trace(
            go.Scattergl(
                x=ts,
                y=hrrr["tmpf"],
                mode="markers",
                marker=dict(size=5, opacity=0.8, color="#8c564b"),
                name="HRRR (15-min)",
            ),
        )

    if nbm is not None and not nbm.empty:
        ts = ensure_utc(nbm["valid_utc"])
        add_weather_trace(
            go.Scattergl(
                x=ts,
                y=nbm["tmpf"],
                mode="markers",
                marker=dict(size=5, opacity=0.8, color="#bcbd22"),
                name="NBM (2h prior)",
            ),
        )

    if rrfs is not None and not rrfs.empty:
        ts = ensure_utc(rrfs["valid_utc"])
        add_weather_trace(
            go.Scattergl(
                x=ts,
                y=rrfs["tmpf"],
                mode="markers",
                marker=dict(size=5, opacity=0.8, color="#7f7f7f"),
                name="RRFS (2h prior)",
            ),
        )

    fig.update_layout(
        title=f"Weather — {station} — last {days} days",
        height=550,
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_xaxes(range=[x_min, x_max], title_text="Time (UTC)")
    fig.update_yaxes(title_text="°F")
    fig.show(renderer="notebook")


# %% Load data and plot
synoptic = load_synoptic_asos(data_dir, station, start_date, end_date)
iem = load_iem_asos(data_dir, station, start_date, end_date)
cli = load_cli(data_dir, station, start_date, end_date)
metar = load_metar(data_dir, station, start_date, end_date)
rtma_ru = load_rtma_ru(data_dir, station, start_date, end_date)
hrrr = load_hrrr(data_dir, station, start_date, end_date)
nbm = load_nbm_f02(data_dir, station, start_date, end_date)
rrfs = load_rrfs_f02(data_dir, station, start_date, end_date)
print(f"Synoptic: {len(synoptic)} | IEM: {len(iem)} | CLI: {len(cli)} days | METAR: {len(metar)} | RTMA-RU: {len(rtma_ru)} | HRRR: {len(hrrr)} | NBM: {len(nbm)} | RRFS: {len(rrfs)}")

# %% Build dict: AWC METAR T-Group → Synoptic ASOS 1-min (same timestamp)
def awc_tgroup_to_synoptic_dict(
    synoptic: pd.DataFrame,
    metar: pd.DataFrame,
) -> dict:
    """Map AWC METAR T-Group only values to Synoptic ASOS 1-min for same timestamp.

    Returns dict: {valid_utc: {"awc_t_group": float, "synoptic_1min": float}}
    """
    if metar.empty or synoptic.empty:
        return {}
    has_t_group = "temp_high_accuracy" in metar.columns
    t_group = metar[metar["temp_high_accuracy"] == True] if has_t_group else pd.DataFrame()
    if t_group.empty:
        return {}
    # Normalize timestamps for merge (floor to minute if needed)
    syn = synoptic[["valid_utc", "tmpf"]].copy()
    syn = syn.rename(columns={"tmpf": "synoptic_1min"})
    met = t_group[["valid_utc", "tmpf"]].copy()
    met = met.rename(columns={"tmpf": "awc_t_group"})
    merged = met.merge(syn, on="valid_utc", how="inner")
    return {
        row["valid_utc"]: {"awc_t_group": row["awc_t_group"], "synoptic_1min": row["synoptic_1min"]}
        for _, row in merged.iterrows()
    }


awc_to_synoptic = awc_tgroup_to_synoptic_dict(synoptic, metar)
print(f"AWC T-Group ↔ Synoptic (same timestamp): {len(awc_to_synoptic)} pairs")


def synoptic_to_tgroup_series(
    synoptic: pd.DataFrame,
    awc_to_synoptic: dict,
) -> tuple[pd.DataFrame, dict[float, float]]:
    """Convert each Synoptic value to equivalent T-group using 1-to-1 paired mapping.

    For each synoptic value X seen in paired data, map to the t_group value Y from that pair.
    All measurements of 60.8 → 60.1 (example). Unmapped values use nearest-neighbor lookup.
    Returns (per-minute DataFrame, value_map).
    """
    if synoptic.empty or not awc_to_synoptic:
        return pd.DataFrame(), {}
    # Build 1-1 map: synoptic_value (rounded 0.1°F) -> t_group_value (first occurrence)
    value_map: dict[float, float] = {}
    for v in awc_to_synoptic.values():
        x, y = round(v["synoptic_1min"], 1), v["awc_t_group"]
        if x not in value_map:
            value_map[x] = y
    if not value_map:
        return pd.DataFrame(), {}
    keys = np.array(list(value_map.keys()))

    def lookup(syn_val: float) -> float:
        x = round(float(syn_val), 1)
        if x in value_map:
            return value_map[x]
        idx = np.argmin(np.abs(keys - x))
        return value_map[keys[idx]]

    out = synoptic[["valid_utc", "tmpf"]].copy()
    out["tmpf"] = out["tmpf"].map(lookup)
    return out, value_map


synoptic_as_tgroup, value_map = synoptic_to_tgroup_series(synoptic, awc_to_synoptic)
samples = ", ".join(f"{k}→{v}" for k, v in list(value_map.items())[:5])
print(f"Synoptic → T-group: {len(synoptic_as_tgroup)} points (1-min), {len(value_map)} unique mappings (e.g. {samples})")
plot_all(
    synoptic, iem, cli, metar, station, days,
    synoptic_as_tgroup=synoptic_as_tgroup,
    rtma_ru=rtma_ru, hrrr=hrrr, nbm=nbm, rrfs=rrfs,
)
