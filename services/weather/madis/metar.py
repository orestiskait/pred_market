"""MADIS decoded METAR fetcher: extract station observations from NetCDF.

Downloads gzipped NetCDF files from s3://noaa-madis-pds/data/observations/metar/decoded/
and extracts temperature observations for configured stations.

MADIS METAR specifics:
  - Path: data/observations/metar/decoded/YYYYMMDD_HH00.gz
  - Format: Gzipped NetCDF
  - Frequency: New files every 5 minutes (processed in hourly batches)
  - Temperature: Celsius (standard MADIS convention)
  - Contains rawMETAR string (for T-group parsing: e.g. T01540052 → 15.4°C)
  - Quality-controlled: MADIS applies QC flags to each observation

T-group precision:
  Standard METAR rounds temperature to whole degrees Celsius.  The T-group
  in the remarks section provides tenth-of-degree precision.  MADIS decoded
  files usually preserve the rawMETAR, letting us parse T-groups.

  Example: T01540052 → temp=+15.4°C, dewpoint=+5.2°C
           T10230118 → temp=-2.3°C,  dewpoint=-11.8°C
           (first digit: 0=positive, 1=negative; next 3 digits: tenths °C)

Data source: NOAA MADIS via AWS S3 (s3://noaa-madis-pds), us-east-1, public, no auth.
"""

from __future__ import annotations

import gzip
import io
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from services.weather.metar_parser import MetarParser
from services.weather.station_registry import NWPStation

logger = logging.getLogger(__name__)

BUCKET = "noaa-madis-pds"
REGION = "us-east-1"


def _celsius_to_fahrenheit(c: float) -> float:
    return c * 9.0 / 5.0 + 32.0


def _kelvin_to_celsius(k: float) -> float:
    return k - 273.15


class MADISMETARFetcher:
    """Extracts station-level METAR observations from MADIS NetCDF files.

    Downloads the decoded METAR file from S3, opens via NetCDF, and extracts
    observations for configured stations.  Parses T-group for high-precision
    temperature when available.
    """

    SOURCE_NAME = "madis_metar"

    def __init__(
        self,
        data_dir: Path | str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
    ):
        if data_dir is None:
            data_dir = Path(__file__).resolve().parent.parent.parent.parent / "data"
        self.data_dir = Path(data_dir) / self.SOURCE_NAME
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

    def fetch_from_s3(
        self,
        bucket: str,
        key: str,
        stations: list[NWPStation],
        notification_ts: datetime | None = None,
    ) -> pd.DataFrame:
        """Download a MADIS METAR NetCDF from S3 and extract station observations.

        Parameters
        ----------
        bucket : str
            S3 bucket name (noaa-madis-pds).
        key : str
            S3 object key (e.g. data/observations/metar/decoded/20260222_1800.gz).
        stations : list[NWPStation]
            Stations to extract.
        notification_ts : datetime, optional
            SNS notification timestamp for latency tracking.

        Returns
        -------
        pd.DataFrame with columns:
            station, city, source, obs_time_utc, temp_c, temp_f, temp_k,
            dewpoint_c, dewpoint_f, tgroup_temp_c, tgroup_dewpoint_c,
            raw_metar, qc_flag
        """
        import boto3
        from botocore import UNSIGNED
        from botocore.config import Config

        s3_kwargs = {"region_name": REGION}
        if self.aws_access_key_id and self.aws_secret_access_key:
            s3_kwargs["aws_access_key_id"] = self.aws_access_key_id
            s3_kwargs["aws_secret_access_key"] = self.aws_secret_access_key
        else:
            s3_kwargs["config"] = Config(signature_version=UNSIGNED)

        s3 = boto3.client("s3", **s3_kwargs)

        # Download the gzipped NetCDF
        try:
            resp = s3.get_object(Bucket=bucket, Key=key)
            compressed = resp["Body"].read()
        except Exception as e:
            logger.warning("MADIS METAR download failed: %s/%s: %s", bucket, key, e)
            return pd.DataFrame()

        # Decompress and read NetCDF
        try:
            raw_nc = gzip.decompress(compressed)
        except Exception as e:
            logger.warning("MADIS METAR decompress failed: %s: %s", key, e)
            return pd.DataFrame()

        return self._parse_netcdf(raw_nc, stations, key)

    def _parse_netcdf(
        self,
        raw_nc: bytes,
        stations: list[NWPStation],
        key: str,
    ) -> pd.DataFrame:
        """Parse a decoded METAR NetCDF and extract matching station observations."""
        from netCDF4 import Dataset, num2date

        # Build station lookup (ICAO → NWPStation)
        station_set = {stn.icao: stn for stn in stations}

        # Write to temp file and open (netCDF4 needs a file path)
        with tempfile.NamedTemporaryFile(suffix=".nc") as tmp:
            tmp.write(raw_nc)
            tmp.flush()

            try:
                ds = Dataset(tmp.name, "r")
            except Exception as e:
                logger.warning("MADIS METAR NetCDF open failed: %s: %s", key, e)
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
        """Extract observations for matching stations from an open NetCDF Dataset."""
        from netCDF4 import num2date

        # Read station names — char array or byte array
        if "stationName" in ds.variables:
            raw_names = ds.variables["stationName"][:]
        elif "stationId" in ds.variables:
            raw_names = ds.variables["stationId"][:]
        else:
            logger.warning("MADIS METAR: no station name variable in %s", key)
            return pd.DataFrame()

        # Decode station names to strings
        if hasattr(raw_names, "tobytes"):
            # Char array: shape (nobs, nchars)
            if raw_names.ndim == 2:
                names = [
                    b"".join(raw_names[i]).decode("ascii", errors="ignore").strip()
                    for i in range(raw_names.shape[0])
                ]
            else:
                names = [raw_names.tobytes().decode("ascii", errors="ignore").strip()]
        else:
            names = [str(n).strip() for n in raw_names]

        # Find indices of our stations
        target_indices: list[tuple[int, NWPStation]] = []
        for i, name in enumerate(names):
            # MADIS uses 4-char ICAO (KMDW) or 3-char (MDW)
            icao = name.upper()
            if icao in station_set:
                target_indices.append((i, station_set[icao]))
            elif f"K{icao}" in station_set:
                target_indices.append((i, station_set[f"K{icao}"]))

        if not target_indices:
            return pd.DataFrame()

        # Read observation times
        time_var = None
        for tname in ("timeObs", "observationTime", "time_observation"):
            if tname in ds.variables:
                time_var = ds.variables[tname]
                break

        if time_var is None:
            logger.warning("MADIS METAR: no time variable in %s", key)
            return pd.DataFrame()

        # Try to interpret time variable
        time_vals = time_var[:]
        try:
            times = num2date(time_vals, units=time_var.units,
                             calendar=getattr(time_var, "calendar", "standard"))
        except Exception:
            # Fall back: epoch seconds
            times = [datetime.fromtimestamp(float(t), tz=timezone.utc) for t in time_vals]

        # Read temperature (usually in Kelvin)
        temp_var = None
        for tname in ("temperature", "TD", "temperatureD"):
            if tname in ds.variables:
                temp_var = ds.variables[tname]
                break

        # Read dewpoint
        dew_var = None
        for dname in ("dewpoint", "dewPoint"):
            if dname in ds.variables:
                dew_var = ds.variables[dname]
                break

        # Read raw METAR (for T-group)
        metar_var = None
        for mname in ("rawMETAR", "rawOb", "rawReport"):
            if mname in ds.variables:
                metar_var = ds.variables[mname]
                break

        # Read QC flags for temperature
        qc_var = None
        for qname in ("temperatureQCR", "temperatureDD"):
            if qname in ds.variables:
                qc_var = ds.variables[qname]
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

            # Temperature
            if temp_var is not None:
                temp_val = float(temp_var[idx])
                if not np.isnan(temp_val) and temp_val > 100:
                    # Assume Kelvin
                    temp_c = _kelvin_to_celsius(temp_val)
                elif not np.isnan(temp_val):
                    temp_c = temp_val
                else:
                    temp_c = np.nan

                row["temp_c"] = float(temp_c) if not np.isnan(temp_c) else np.nan
                row["temp_f"] = _celsius_to_fahrenheit(temp_c) if not np.isnan(temp_c) else np.nan
                row["temp_k"] = temp_c + 273.15 if not np.isnan(temp_c) else np.nan

            # Dewpoint
            if dew_var is not None:
                dew_val = float(dew_var[idx])
                if not np.isnan(dew_val) and dew_val > 100:
                    dew_c = _kelvin_to_celsius(dew_val)
                elif not np.isnan(dew_val):
                    dew_c = dew_val
                else:
                    dew_c = np.nan

                row["dewpoint_c"] = float(dew_c) if not np.isnan(dew_c) else np.nan
                row["dewpoint_f"] = _celsius_to_fahrenheit(dew_c) if not np.isnan(dew_c) else np.nan

            # Raw METAR + T-group parsing
            if metar_var is not None:
                try:
                    raw = metar_var[idx]
                    if hasattr(raw, "tobytes"):
                        raw_str = raw.tobytes().decode("ascii", errors="ignore").strip()
                    elif hasattr(raw, "compressed"):
                        # Masked array — check if valid
                        if not raw.mask.all() if hasattr(raw, "mask") else True:
                            raw_str = str(raw).strip()
                        else:
                            raw_str = ""
                    else:
                        raw_str = str(raw).strip()

                    row["raw_metar"] = raw_str if raw_str else None

                    if raw_str:
                        tg_temp, tg_dew = MetarParser.parse_tgroup(raw_str)
                        row["tgroup_temp_c"] = tg_temp
                        row["tgroup_dewpoint_c"] = tg_dew
                        # If T-group available, use it as primary (higher precision)
                        if tg_temp is not None:
                            row["temp_c"] = tg_temp
                            row["temp_f"] = _celsius_to_fahrenheit(tg_temp)
                            row["temp_k"] = tg_temp + 273.15
                except Exception:
                    pass

            # QC flag
            if qc_var is not None:
                try:
                    row["qc_flag"] = str(qc_var[idx])
                except Exception:
                    pass

            rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        # Add valid_local for each station
        tz_map = {stn.icao: stn.tz for stn in [s for _, s in target_indices]}
        for icao in df["station"].unique():
            tz = tz_map.get(icao)
            if tz:
                mask = df["station"] == icao
                df.loc[mask, "obs_time_local"] = (
                    df.loc[mask, "obs_time_utc"].dt.tz_convert(tz).dt.tz_localize(None)
                )

        logger.info(
            "MADIS METAR: extracted %d obs for %d stations from %s",
            len(df), len(df["station"].unique()), key,
        )
        return df
