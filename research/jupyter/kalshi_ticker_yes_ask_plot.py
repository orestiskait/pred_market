#!/usr/bin/env python3
"""Interactive Kalshi ticker yes-ask price plotter.

1. Run first cells to load data and print all available event tickers.
2. Set TICKER in the selection cell to the event ticker you want.
3. Run the plot cell to see yes_ask over time for each contract in that event.

Run cells in VS Code/Cursor (Ctrl+Enter) or run the whole file.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import pyarrow.parquet as pq

# %% Setup: project root and imports
import sys

try:
    PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
except NameError:
    cwd = Path.cwd()
    for candidate in [cwd, cwd.parent, cwd.parent.parent]:
        if (candidate / "services").is_dir() and (candidate / "data").is_dir():
            PROJECT_ROOT = candidate
            break
    else:
        PROJECT_ROOT = cwd

sys.path.insert(0, str(PROJECT_ROOT))

# %% Config: date range for loading snapshots
data_dir = PROJECT_ROOT / "data"
days = 30
end_date = date.today()
start_date = end_date - timedelta(days=days)

# %% Load Kalshi market snapshots
def load_all_kalshi_snapshots(
    data_dir: Path,
    start_date: date,
    end_date: date,
) -> pd.DataFrame:
    """Load all Kalshi market snapshots from kalshi/market_snapshots/."""
    base = data_dir / "kalshi" / "market_snapshots"
    frames = []
    d = start_date
    while d <= end_date:
        path = base / f"{d.isoformat()}.parquet"
        if path.exists():
            df = pq.read_table(str(path)).to_pandas()
            if not df.empty:
                frames.append(df)
        d += timedelta(days=1)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values("snapshot_ts_utc").reset_index(drop=True)


snapshots = load_all_kalshi_snapshots(data_dir, start_date, end_date)
print(f"Loaded {len(snapshots)} snapshots from {start_date} to {end_date}")

# %% Print list of all event tickers
if snapshots.empty:
    print("No Kalshi data found. Ensure kalshi/market_snapshots/ has parquet files.")
else:
    event_tickers = sorted(snapshots["event_ticker"].dropna().unique().tolist())
    print(f"Available event tickers ({len(event_tickers)}):")
    for i, tk in enumerate(event_tickers, 1):
        print(f"  {i:3}. {tk}")

# %% Select ticker (edit this and re-run to plot a different event)
TICKER = "KXHIGHCHI-26FEB20"  # <-- Change to any event ticker from the list above

# %% Plot yes_ask for each market (contract) in the selected event
def plot_yes_ask_for_event(
    snapshots: pd.DataFrame,
    event_ticker: str,
) -> None:
    """Plot yes_ask over time for each market_ticker in the given event."""
    sub = snapshots[snapshots["event_ticker"] == event_ticker].copy()
    if sub.empty:
        print(f"No data for event ticker: {event_ticker}")
        return

    if sub["snapshot_ts_utc"].dt.tz is None:
        sub["snapshot_ts_utc"] = sub["snapshot_ts_utc"].dt.tz_localize("UTC")

    market_tickers = sub["market_ticker"].unique().tolist()
    if not market_tickers:
        print(f"No markets found for event: {event_ticker}")
        return

    fig = go.Figure()
    colors = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
    ]
    for idx, mt in enumerate(sorted(market_tickers)):
        mt_df = sub[sub["market_ticker"] == mt].sort_values("snapshot_ts_utc")
        if mt_df.empty or "yes_ask" not in mt_df.columns:
            continue
        subtitle = (
            mt_df["subtitle"].dropna().iloc[0]
            if "subtitle" in mt_df.columns and mt_df["subtitle"].notna().any()
            else mt
        )
        fig.add_trace(
            go.Scattergl(
                x=mt_df["snapshot_ts_utc"],
                y=mt_df["yes_ask"],
                mode="lines+markers",
                line=dict(width=2, color=colors[idx % len(colors)]),
                marker=dict(size=4, opacity=0.8),
                name=subtitle[:50] + ("…" if len(str(subtitle)) > 50 else ""),
                hovertemplate=(
                    f"{mt}<br>yes_ask: %{{y}}¢<br>%{{x|%Y-%m-%d %H:%M}} UTC<extra></extra>"
                ),
            ),
        )

    fig.update_layout(
        title=f"Yes ask price — {event_ticker}",
        height=550,
        template="plotly_white",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_title="Time (UTC)",
        yaxis_title="yes_ask (¢)",
    )
    fig.show(renderer="notebook")


plot_yes_ask_for_event(snapshots, TICKER)
