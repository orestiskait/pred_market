"""Fetch NBM (National Blend of Models) data from COG bucket at station coordinates.

Uses noaa-nbm-pds (COG format) for percentage temperature and standard temperature.
Replaces GRIB2-based fetcher for lower latency and official COG bucket.

NBM COG specifics:
  - Path: blendv4.3/conus/YYYY/MM/DD/HH00/temp/blendv4.3_conus_temp_RUN_VALID.tif
  - Temp in Celsius (NBM COG standard)
  - Runs every hour (00Z–23Z)
  - One file per (run, valid) pair

Data source: NOAA NBM COG via AWS S3 (s3://noaa-nbm-pds), us-east-1, public, no auth.
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from botocore import UNSIGNED
from botocore.config import Config

from services.weather.station_registry import NWPStation
from services.weather.nwp.base import _add_time_columns

logger = logging.getLogger(__name__)

BUCKET = "noaa-nbm-pds"
REGION = "us-east-1"
COG_VERSION = "blendv4.3"
VARIABLE = "temp"


def _celsius_to_fahrenheit(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


class NBMCOGFetcher:
    """NBM CONUS point fetcher using COG (noaa-nbm-pds)."""

    SOURCE_NAME = "nbm"
    DEFAULT_MAX_FXX = 36
    DEFAULT_CYCLES = list(range(24))
    MODEL_VERSION = COG_VERSION.replace("blend", "")

    def __init__(
        self,
        data_dir: Path | str | None = None,
        max_forecast_hour: int | None = None,
    ):
        if data_dir is None:
            data_dir = Path(__file__).resolve().parent.parent.parent.parent / "data"
        self.data_dir = Path(data_dir) / self.SOURCE_NAME
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.max_forecast_hour = (
            max_forecast_hour if max_forecast_hour is not None else self.DEFAULT_MAX_FXX
        )

    @classmethod
    def from_config(cls, config_path: str | Path) -> "NBMCOGFetcher":
        import yaml

        config_path = Path(config_path)
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
        storage = cfg.get("storage", {})
        rel_data_dir = storage.get("data_dir", "../data")
        data_dir = (config_path.parent / rel_data_dir).resolve()
        model_cfg = cfg.get("nbm", {})
        max_fxx = model_cfg.get("max_forecast_hour", cls.DEFAULT_MAX_FXX)
        return cls(data_dir=data_dir, max_forecast_hour=max_fxx)

    def _cog_key(self, cycle: datetime, valid: datetime, variable: str = VARIABLE) -> str:
        """Build S3 key for temp COG: run_cycle, valid_time."""
        run = cycle.strftime("%Y-%m-%dT%H:00")
        v = valid.strftime("%Y-%m-%dT%H:00")
        hh = cycle.strftime("%H00")
        y, m, d = cycle.year, cycle.month, cycle.day
        return (
            f"{COG_VERSION}/conus/{y}/{m:02d}/{d:02d}/{hh}/{variable}/"
            f"{COG_VERSION}_conus_{variable}_{run}_{v}.tif"
        )

    def _read_cog_point(
        self, key: str, lat: float, lon: float
    ) -> tuple[float, float, float, float] | None:
        """Read COG and extract value at (lat, lon). Returns (temp_c, grid_lat, grid_lon) or None.

        NOAA buckets are public; uses anonymous (unsigned) S3 access only.
        """
        import boto3
        import rasterio
        from rasterio.env import Env

        s3_kwargs = {"region_name": REGION, "config": Config(signature_version=UNSIGNED)}
        env_kwargs = {"AWS_S3_ENDPOINT": "s3.us-east-1.amazonaws.com", "AWS_NO_SIGN_REQUEST": "YES"}

        s3 = boto3.client("s3", **s3_kwargs)
        url = f"s3://{BUCKET}/{key}"
        try:
            with Env(**env_kwargs):
                with rasterio.open(url) as src:
                    import rasterio.windows as rwin
                    from pyproj import Transformer

                    transformer = Transformer.from_crs(
                        "EPSG:4326", src.crs, always_xy=True
                    )
                    x, y = transformer.transform(lon, lat)
                    row, col = src.index(x, y)
                    if 0 <= row < src.height and 0 <= col < src.width:
                        window = rwin.Window(
                            max(0, col - 1), max(0, row - 1), 3, 3
                        )
                        data = src.read(1, window=window)
                        r = min(1, row - window.row_off)
                        c = min(1, col - window.col_off)
                        val = float(data[r, c])
                        # Grid center for this pixel
                        gx, gy = src.xy(row, col)
                        inv = Transformer.from_crs(src.crs, "EPSG:4326", always_xy=True)
                        glon, glat = inv.transform(gx, gy)
                        return (val, glat, glon)
        except Exception as e:
            if "does not exist" in str(e):
                logger.debug("NBM COG key does not exist: %s", key)
            else:
                import traceback
                logger.warning("NBM COG read failed %s: %s\n%s", key, e, traceback.format_exc())
        return None

    def fetch_run(
        self,
        cycle: datetime,
        fxx: int,
        stations: list[NWPStation],
    ) -> pd.DataFrame:
        """Fetch one (cycle, fxx) and extract point values from COG."""
        if cycle.tzinfo is None:
            cycle = cycle.replace(tzinfo=timezone.utc)
        valid = cycle + timedelta(hours=fxx)
        forecast_minutes = fxx * 60

        key_temp = self._cog_key(cycle, valid, "temp")
        key_tempstddev = self._cog_key(cycle, valid, "tempstddev")

        rows = []
        for stn in stations:
            result_temp = self._read_cog_point(key_temp, stn.lat, stn.lon)
            if result_temp is None:
                continue
            
            result_std = self._read_cog_point(key_tempstddev, stn.lat, stn.lon)
            
            temp_c, grid_lat, grid_lon = result_temp
            temp_f = _celsius_to_fahrenheit(temp_c)
            
            # standard deviation of temp (tempstddev is in K. delta T is same: 1K = 1C. 1C = 1.8F difference)
            temp_std_f = None
            if result_std is not None:
                temp_std_f = result_std[0] * 1.8
                
            p10_std_f = None
            p90_std_f = None
            if temp_f is not None and temp_std_f is not None:
                p10_std_f = temp_f - (1.28 * temp_std_f)
                p90_std_f = temp_f + (1.28 * temp_std_f)

            cycle_ts = pd.Timestamp(cycle)
            valid_ts = pd.Timestamp(valid)
            rows.append({
                "station": stn.icao,
                "city": stn.city,
                "model": self.SOURCE_NAME,
                "model_version": self.MODEL_VERSION,
                "model_run_time_utc": cycle_ts,
                "lead_time_minutes": forecast_minutes,
                "forecast_target_time_utc": valid_ts,
                "tmp_2m_f": temp_f,
                "tmp_2m_std_f": temp_std_f,
                "max_temp_p10_f_std": p10_std_f,
                "max_temp_p90_f_std": p90_std_f,
                "grid_lat": round(grid_lat, 4),
                "grid_lon": round(grid_lon, 4),
            })

        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows)
        return _add_time_columns(df, stations)

    def fetch_cycle(
        self,
        cycle: datetime,
        stations: list[NWPStation],
        fxx_range: range | None = None,
    ) -> pd.DataFrame:
        if fxx_range is None:
            fxx_range = range(0, self.max_forecast_hour + 1)
        frames = []
        for fxx in fxx_range:
            try:
                df = self.fetch_run(cycle, fxx, stations)
                if not df.empty:
                    frames.append(df)
            except Exception:
                logger.warning(
                    "%s: skipping fxx=%02d for cycle %s (not available)",
                    self.SOURCE_NAME, fxx, cycle.strftime("%Y-%m-%d %HZ"),
                )
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def fetch_latest(
        self,
        stations: list[NWPStation],
        fxx_range: range | None = None,
        lookback_hours: int = 6,
        save: bool = True,
    ) -> pd.DataFrame:
        import boto3

        s3 = boto3.client(
            "s3",
            region_name=REGION,
            config=Config(signature_version=UNSIGNED),
        )
        now = datetime.now(timezone.utc)
        best_cycle = None
        for day_offset in range(3):
            d = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(
                days=day_offset
            )
            for hour in range(23, -1, -1):
                hh = f"{hour:02d}00"
                prefix = f"{COG_VERSION}/conus/{d.year}/{d.month:02d}/{d.day:02d}/{hh}/{VARIABLE}/"
                r = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=3)
                if r.get("Contents"):
                    best_cycle = datetime(
                        d.year, d.month, d.day, hour, tzinfo=timezone.utc
                    )
                    break
            if best_cycle is not None:
                break
        if best_cycle is None:
            logger.warning("%s: no COG found in last 3 days", self.SOURCE_NAME)
            return pd.DataFrame()
        logger.info(
            "%s: latest available cycle: %s",
            self.SOURCE_NAME, best_cycle.strftime("%Y-%m-%d %HZ"),
        )
        df = self.fetch_cycle(best_cycle, stations, fxx_range)
        if save and not df.empty:
            self._save_by_station(df, best_cycle.date())
        return df

    def fetch_date_range(
        self,
        start_date: date,
        end_date: date,
        stations: list[NWPStation],
        cycles: list[int] | None = None,
        fxx_range: range | None = None,
        rolling_lead_minutes: int | None = None,
        save: bool = True,
    ) -> pd.DataFrame:
        if cycles is None:
            cycles = list(self.DEFAULT_CYCLES)
        if rolling_lead_minutes is not None:
            fxx = rolling_lead_minutes // 60
            fxx_range = range(fxx, fxx + 1)

        all_frames = []
        current = start_date
        while current <= end_date:
            for cycle_hour in cycles:
                cycle_dt = datetime(
                    current.year, current.month, current.day, cycle_hour,
                    tzinfo=timezone.utc,
                )
                logger.info(
                    "%s: fetching cycle %s",
                    self.SOURCE_NAME, cycle_dt.strftime("%Y-%m-%d %HZ"),
                )
                try:
                    df = self.fetch_cycle(cycle_dt, stations, fxx_range)
                except Exception:
                    logger.exception(
                        "%s: failed cycle %s",
                        self.SOURCE_NAME, cycle_dt.strftime("%Y-%m-%d %HZ"),
                    )
                    continue
                if not df.empty:
                    if rolling_lead_minutes is not None:
                        df = df[df["lead_time_minutes"] == rolling_lead_minutes]
                    if not df.empty and save:
                        self._save_by_station(df, current)
                    if not df.empty:
                        all_frames.append(df)
            current += timedelta(days=1)
        if not all_frames:
            return pd.DataFrame()
        return pd.concat(all_frames, ignore_index=True)

    def save_parquet(
        self, df: pd.DataFrame, station_icao: str, cycle_date: date
    ) -> Path:
        if df.empty:
            return self.data_dir
        path = self.data_dir / f"{station_icao}_{cycle_date.isoformat()}.parquet"
        if path.exists():
            existing = pd.read_parquet(path)
            combined = pd.concat([existing, df], ignore_index=True)
            dedup_cols = [c for c in ("station", "model_run_time_utc", "lead_time_minutes")
                         if c in combined.columns]
            if dedup_cols:
                combined = combined.drop_duplicates(subset=dedup_cols, keep="last")
        else:
            combined = df
        combined = combined.sort_values(
            ["model_run_time_utc", "lead_time_minutes"], ignore_index=True
        )
        combined.to_parquet(path, index=False)
        logger.info("Saved %d rows → %s", len(combined), path)
        return path

    def _save_by_station(self, df: pd.DataFrame, cycle_date: date) -> None:
        for icao in df["station"].unique():
            self.save_parquet(df[df["station"] == icao].copy(), icao, cycle_date)
