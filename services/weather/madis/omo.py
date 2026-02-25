"""MADIS One-Minute Observation (OMO) fetcher: 1-minute ASOS sensor data.

Downloads gzipped NetCDF files from s3://noaa-madis-pds/data/LDAD/OMO/netCDF/
and extracts temperature observations for configured stations.

MADIS OMO specifics:
  - Path: data/LDAD/OMO/netCDF/YYYYMMDD_HH00.gz
  - Format: Gzipped NetCDF
  - Frequency: New files every ~5 minutes
  - Temperature: Usually raw sensor values (Celsius or Kelvin)
  - 1-minute temporal resolution — snapshots from physical sensor
  - No T-group (raw binary sensor, not a METAR report)
  - Extremely precise but no manual QC

Caveat:
  OMO data is the raw sensor dump converted to NetCDF.  It's useful for
  high-frequency monitoring but can contain spikes and noise that a human
  METAR observer would filter out.

Data source: NOAA MADIS via AWS S3 (s3://noaa-madis-pds), us-east-1, public, no auth.
"""

from __future__ import annotations

import gzip
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from services.weather.station_registry import NWPStation

logger = logging.getLogger(__name__)

BUCKET = "noaa-madis-pds"
REGION = "us-east-1"


def _celsius_to_fahrenheit(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


def _kelvin_to_celsius(k: float) -> float:
    return k - 273.15


class MADISOMOFetcher:
    """Extracts station-level 1-minute ASOS observations from MADIS OMO NetCDF.

    Downloads the OMO file from S3, opens via netCDF4, and extracts
    observations for configured stations.
    """

    SOURCE_NAME = "madis_omo"

    def __init__(self, data_dir: Path | str | None = None):
        if data_dir is None:
            data_dir = Path(__file__).resolve().parent.parent.parent.parent / "data"
        self.data_dir = Path(data_dir) / self.SOURCE_NAME
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def fetch_from_s3(
        self,
        bucket: str,
        key: str,
        stations: list[NWPStation],
        notification_ts: datetime | None = None,
    ) -> pd.DataFrame:
        """Download a MADIS OMO NetCDF from S3 and extract station observations.

        Parameters
        ----------
        bucket : str
            S3 bucket name (noaa-madis-pds).
        key : str
            S3 object key (e.g. data/LDAD/OMO/netCDF/20260222_1800.gz).
        stations : list[NWPStation]
            Stations to extract.
        notification_ts : datetime, optional
            SNS notification timestamp for latency tracking.

        Returns
        -------
        pd.DataFrame with columns:
            station, city, source, obs_time_utc, temp_c, temp_f, temp_k,
            obs_time_local
        """
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config

        # NOAA MADIS bucket is public; anonymous access only (no credentials).
        s3 = boto3.client(
            "s3",
            region_name=REGION,
            config=Config(signature_version=UNSIGNED),
        )

        try:
            resp = s3.get_object(Bucket=bucket, Key=key)
            compressed = resp["Body"].read()
        except Exception as e:
            logger.warning("MADIS OMO download failed: %s/%s: %s", bucket, key, e)
            return pd.DataFrame()

        try:
            raw_nc = gzip.decompress(compressed)
        except Exception as e:
            logger.warning("MADIS OMO decompress failed: %s: %s", key, e)
            return pd.DataFrame()

        return self._parse_netcdf(raw_nc, stations, key)

    def _parse_netcdf(
        self,
        raw_nc: bytes,
        stations: list[NWPStation],
        key: str,
    ) -> pd.DataFrame:
        """Parse an OMO NetCDF and extract matching station observations."""
        from netCDF4 import Dataset

        station_set = {stn.icao: stn for stn in stations}
        # Also index without K prefix (OMO may use 3-char codes)
        for stn in stations:
            if stn.icao.startswith("K") and len(stn.icao) == 4:
                station_set[stn.icao[1:]] = stn

        with tempfile.NamedTemporaryFile(suffix=".nc") as tmp:
            tmp.write(raw_nc)
            tmp.flush()

            try:
                ds = Dataset(tmp.name, "r")
            except Exception as e:
                logger.warning("MADIS OMO NetCDF open failed: %s: %s", key, e)
                return pd.DataFrame()

            try:
                return self._extract_stations(ds, station_set, key)
            finally:
                ds.close()

    def _extract_stations(
        self,
        ds: Any,
        station_set: dict[str, NWPStation],
        key: str,
    ) -> pd.DataFrame:
        """Extract observations for matching stations from an open OMO NetCDF."""
        from netCDF4 import num2date

        # Station names
        name_var = None
        for vname in ("stationName", "stationId", "station_id"):
            if vname in ds.variables:
                name_var = ds.variables[vname]
                break

        if name_var is None:
            logger.warning("MADIS OMO: no station name variable in %s", key)
            return pd.DataFrame()

        raw_names = name_var[:]
        if raw_names.ndim == 2:
            names = [
                b"".join(raw_names[i]).decode("ascii", errors="ignore").strip()
                for i in range(raw_names.shape[0])
            ]
        else:
            names = [str(n).strip() for n in raw_names]

        # Find our stations
        target_indices: list[tuple[int, NWPStation]] = []
        for i, name in enumerate(names):
            icao = name.upper().strip()
            if icao in station_set:
                target_indices.append((i, station_set[icao]))

        if not target_indices:
            return pd.DataFrame()

        # Observation time
        time_var = None
        for tname in ("observationTime", "timeObs", "time_observation", "time"):
            if tname in ds.variables:
                time_var = ds.variables[tname]
                break

        if time_var is None:
            logger.warning("MADIS OMO: no time variable in %s", key)
            return pd.DataFrame()

        time_vals = time_var[:]
        try:
            times = num2date(time_vals, units=time_var.units,
                             calendar=getattr(time_var, "calendar", "standard"))
        except Exception:
            times = [datetime.fromtimestamp(float(t), tz=timezone.utc) for t in time_vals]

        # Temperature variable
        temp_var = None
        for tname in ("temperature", "temperatureD", "TD", "airTemperature"):
            if tname in ds.variables:
                temp_var = ds.variables[tname]
                break

        rows: list[dict] = []
        for idx, stn in target_indices:
            obs_time = times[idx]
            if hasattr(obs_time, "replace"):
                if obs_time.tzinfo is None:
                    obs_time = obs_time.replace(tzinfo=timezone.utc)
            else:
                obs_time = datetime.fromtimestamp(float(obs_time), tz=timezone.utc)

            obs_ts = pd.Timestamp(obs_time)

            row: dict = {
                "station": stn.icao,
                "city": stn.city,
                "source": self.SOURCE_NAME,
                "obs_time_utc": obs_ts,
            }

            if temp_var is not None:
                temp_val = float(temp_var[idx])
                if np.isnan(temp_val):
                    row["temp_c"] = np.nan
                    row["temp_f"] = np.nan
                    row["temp_k"] = np.nan
                elif temp_val > 100:
                    # Kelvin
                    temp_c = _kelvin_to_celsius(temp_val)
                    row["temp_c"] = temp_c
                    row["temp_f"] = _celsius_to_fahrenheit(temp_c)
                    row["temp_k"] = temp_val
                else:
                    # Celsius
                    row["temp_c"] = temp_val
                    row["temp_f"] = _celsius_to_fahrenheit(temp_val)
                    row["temp_k"] = temp_val + 273.15

            rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        # Add local time
        tz_map = {stn.icao: stn.tz for stn in [s for _, s in target_indices]}
        for icao in df["station"].unique():
            tz = tz_map.get(icao)
            if tz:
                mask = df["station"] == icao
                df.loc[mask, "obs_time_local"] = (
                    df.loc[mask, "obs_time_utc"].dt.tz_convert(tz).dt.tz_localize(None)
                )

        logger.info(
            "MADIS OMO: extracted %d obs for %d stations from %s",
            len(df), len(df["station"].unique()), key,
        )
        return df
