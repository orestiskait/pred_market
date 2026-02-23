"""Weather SNS Listener: real-time NWP + MADIS data ingest via AWS SNS → SQS.

Subscribes to NOAA AWS SNS topics for:
  - NWP models: HRRR, RRFS, NBM (gridded forecasts)
  - MADIS observations: decoded METAR, One-Minute ASOS/OMO (station obs)

When new S3 objects are published, this service:
  1. Receives the notification via SQS long-polling
  2. Parses the S3 key to identify the data type
  3. NWP: extracts point data at station coordinates from GRIB2/COG
     MADIS: extracts station observations from gzipped NetCDF
  4. Saves to Parquet with latency tracking metadata

Architecture:
  AWS SNS Topics  →  SQS Queue (auto-created)  →  This listener
       ↓
  Parse S3 event  →  Match file pattern  →  Fetch & extract  →  Save with latency

Usage:
    python -m services.weather.sns_listener
    python -m services.weather.sns_listener --config path/to/config.yaml

Requires: boto3, netCDF4, AWS credentials with SQS permissions (for queue
creation and SNS subscription). S3 buckets are public (no auth for reads).
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.core.config import (
    load_config,
    get_event_series,
    standard_argparser,
    configure_logging,
    get_aws_credentials,
)
from services.core.service import AsyncService
from services.weather.station_registry import nwp_stations_for_series, NWPStation
from services.weather.storage import NWPRealtimeStorage, MADISRealtimeStorage

logger = logging.getLogger(__name__)


# ======================================================================
# S3 key patterns for each model — extracts cycle hour and forecast hour
# ======================================================================

# HRRR sub-hourly: hrrr.YYYYMMDD/conus/hrrr.tCCz.wrfsubhfFF.grib2
# HRRR hourly:     hrrr.YYYYMMDD/conus/hrrr.tCCz.wrfsfcfFF.grib2
HRRR_PATTERN = re.compile(
    r"hrrr\.(\d{8})/conus/hrrr\.t(\d{2})z\.wrf(?:subhf|sfcf)(\d{2,3})\.grib2$"
)

# RRFS: rrfs_a/rrfs_a.YYYYMMDD/HH/control/conus/rrfs.tCCz.conus.f[FH].grib2
# RRFS sub-hourly:  ...rrfs.tCCz.conus.f[FH].subh.grib2
RRFS_PATTERN = re.compile(
    r"rrfs\.t(\d{2})z\.conus\.(?:prob\.)?f(\d{2,3})(?:\.subh)?\.grib2$"
)
RRFS_DATE_PATTERN = re.compile(r"rrfs_a\.(\d{8})")

# NBM COG: blendv4.3/conus/YYYY/MM/DD/HH00/temp/blendv4.3_conus_temp_RUN_VALID.tif
NBM_COG_PATTERN = re.compile(
    r"blendv4\.3/conus/(\d{4})/(\d{2})/(\d{2})/(\d{2})00/(?:temp|qmd|prob)/"
    r"blendv4\.3_conus_(?:temp|qmd|prob)_"
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})_(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})\.tif$"
)

# MADIS decoded METAR: data/observations/metar/decoded/YYYYMMDD_HH00.gz
MADIS_METAR_PATTERN = re.compile(
    r"data/observations/metar/decoded/(\d{8})_(\d{4})\.gz$"
)

# MADIS One-Minute ASOS (OMO): data/LDAD/OMO/netCDF/YYYYMMDD_HH00.gz
MADIS_OMO_PATTERN = re.compile(
    r"data/LDAD/OMO/netCDF/(\d{8})_(\d{4})\.gz$"
)


# ======================================================================
# Model config dataclass
# ======================================================================

class ModelSNSConfig:
    """Configuration for one model's SNS subscription."""

    def __init__(
        self,
        name: str,
        sns_topic_arn: str,
        s3_bucket: str,
        enabled: bool = True,
    ):
        self.name = name
        self.sns_topic_arn = sns_topic_arn
        self.s3_bucket = s3_bucket
        self.enabled = enabled


# ======================================================================
# SQS manager — creates queue, subscribes to SNS, polls for messages
# ======================================================================

class SQSManager:
    """Manages SQS queue creation, SNS subscriptions, and message polling."""

    def __init__(self, region: str, queue_name: str, aws_access_key_id: str, aws_secret_access_key: str):
        import boto3

        self.region = region
        self.queue_name = queue_name
        self.sqs = boto3.client(
            "sqs",
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self.sns = boto3.client(
            "sns",
            region_name=region,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
        )
        self.queue_url: str | None = None
        self.queue_arn: str | None = None

    def setup(self, topic_arns: list[str]) -> None:
        """Create SQS queue and subscribe it to the given SNS topics."""
        # Create or get existing queue
        resp = self.sqs.create_queue(
            QueueName=self.queue_name,
            Attributes={
                "ReceiveMessageWaitTimeSeconds": "20",  # Long polling
                "VisibilityTimeout": "300",  # 5 min to process
                "MessageRetentionPeriod": "86400",  # 1 day
            },
        )
        self.queue_url = resp["QueueUrl"]

        # Get queue ARN
        attrs = self.sqs.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=["QueueArn"],
        )
        self.queue_arn = attrs["Attributes"]["QueueArn"]
        logger.info("SQS queue ready: %s (%s)", self.queue_name, self.queue_arn)

        # Set policy to allow SNS topics to send to this queue
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": f"AllowSNS-{i}",
                    "Effect": "Allow",
                    "Principal": {"Service": "sns.amazonaws.com"},
                    "Action": "sqs:SendMessage",
                    "Resource": self.queue_arn,
                    "Condition": {
                        "ArnEquals": {"aws:SourceArn": arn}
                    },
                }
                for i, arn in enumerate(topic_arns)
            ],
        }
        self.sqs.set_queue_attributes(
            QueueUrl=self.queue_url,
            Attributes={"Policy": json.dumps(policy)},
        )

        # Subscribe to each SNS topic
        for arn in topic_arns:
            try:
                self.sns.subscribe(
                    TopicArn=arn,
                    Protocol="sqs",
                    Endpoint=self.queue_arn,
                )
                logger.info("Subscribed SQS to SNS topic: %s", arn)
            except Exception as e:
                logger.error("Failed to subscribe to %s: %s", arn, e)

    def receive_messages(self, max_messages: int = 10, wait_time: int = 20) -> list[dict]:
        """Long-poll for messages from the SQS queue."""
        if not self.queue_url:
            return []

        try:
            resp = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
                MessageAttributeNames=["All"],
            )
            return resp.get("Messages", [])
        except Exception as e:
            logger.error("SQS receive error: %s", e)
            return []

    def delete_message(self, receipt_handle: str) -> None:
        """Delete a processed message from the queue."""
        if self.queue_url:
            self.sqs.delete_message(
                QueueUrl=self.queue_url,
                ReceiptHandle=receipt_handle,
            )

    def cleanup(self) -> None:
        """Delete the SQS queue on shutdown."""
        if self.queue_url:
            try:
                self.sqs.delete_queue(QueueUrl=self.queue_url)
                logger.info("Deleted SQS queue: %s", self.queue_name)
            except Exception as e:
                logger.warning("Failed to delete SQS queue: %s", e)


# ======================================================================
# S3 event parser — extracts model/cycle/fxx from SNS notifications
# ======================================================================

# MADIS model name constants
MADIS_METAR_MODEL = "madis_metar"
MADIS_OMO_MODEL = "madis_omo"
MADIS_MODELS = {MADIS_METAR_MODEL, MADIS_OMO_MODEL}


class S3EventInfo:
    """Parsed S3 event from an SNS notification."""

    def __init__(
        self,
        model: str,
        bucket: str,
        key: str,
        cycle: datetime,
        fxx: int,
        notification_ts: datetime,
    ):
        self.model = model
        self.bucket = bucket
        self.key = key
        self.cycle = cycle
        self.fxx = fxx
        self.notification_ts = notification_ts

    @property
    def is_madis(self) -> bool:
        return self.model in MADIS_MODELS


def parse_sns_message(raw_body: str) -> list[S3EventInfo]:
    """Parse an SNS → SQS message and extract S3 event info.

    Returns a list because one SNS message can contain multiple S3 records.
    """
    try:
        sqs_msg = json.loads(raw_body)
    except json.JSONDecodeError:
        logger.warning("Could not parse SQS message body")
        return []

    # SNS wraps the actual message in a 'Message' field
    sns_message_str = sqs_msg.get("Message", raw_body)
    try:
        sns_payload = json.loads(sns_message_str)
    except (json.JSONDecodeError, TypeError):
        return []

    # Extract notification timestamp
    notification_ts_str = sqs_msg.get("Timestamp") or sns_payload.get("Timestamp")
    if notification_ts_str:
        try:
            notification_ts = datetime.fromisoformat(
                notification_ts_str.replace("Z", "+00:00")
            )
        except ValueError:
            notification_ts = datetime.now(timezone.utc)
    else:
        notification_ts = datetime.now(timezone.utc)

    # S3 event records
    records = sns_payload.get("Records", [])
    if not records and "s3" in sns_payload:
        records = [sns_payload]

    results: list[S3EventInfo] = []
    for record in records:
        s3_info = record.get("s3", {})
        bucket = s3_info.get("bucket", {}).get("name", "")
        key = s3_info.get("object", {}).get("key", "")

        if not key:
            continue

        event = _match_key(bucket, key, notification_ts)
        if event:
            results.append(event)

    return results


def _match_key(bucket: str, key: str, notification_ts: datetime) -> S3EventInfo | None:
    """Match an S3 key against known model patterns."""

    # HRRR
    m = HRRR_PATTERN.search(key)
    if m:
        date_str, cc, fh = m.groups()
        cycle = datetime.strptime(f"{date_str}{cc}", "%Y%m%d%H").replace(
            tzinfo=timezone.utc
        )
        return S3EventInfo(
            model="hrrr", bucket=bucket, key=key,
            cycle=cycle, fxx=int(fh), notification_ts=notification_ts,
        )

    # RRFS
    m = RRFS_PATTERN.search(key)
    if m:
        cc, fh = m.groups()
        # Extract date from path
        dm = RRFS_DATE_PATTERN.search(key)
        if dm:
            date_str = dm.group(1)
            cycle = datetime.strptime(f"{date_str}{cc}", "%Y%m%d%H").replace(
                tzinfo=timezone.utc
            )
            return S3EventInfo(
                model="rrfs", bucket=bucket, key=key,
                cycle=cycle, fxx=int(fh), notification_ts=notification_ts,
            )

    # NBM COG
    m = NBM_COG_PATTERN.search(key)
    if m:
        y, mo, d, hh, run_str, valid_str = m.groups()
        cycle = datetime.fromisoformat(run_str).replace(tzinfo=timezone.utc)
        valid = datetime.fromisoformat(valid_str).replace(tzinfo=timezone.utc)
        fxx = int((valid - cycle).total_seconds() // 3600)
        return S3EventInfo(
            model="nbm", bucket=bucket, key=key,
            cycle=cycle, fxx=fxx, notification_ts=notification_ts,
        )

    # MADIS decoded METAR
    m = MADIS_METAR_PATTERN.search(key)
    if m:
        date_str, hhmm = m.groups()
        cycle = datetime.strptime(f"{date_str}{hhmm[:2]}", "%Y%m%d%H").replace(
            tzinfo=timezone.utc
        )
        return S3EventInfo(
            model=MADIS_METAR_MODEL, bucket=bucket, key=key,
            cycle=cycle, fxx=0, notification_ts=notification_ts,
        )

    # MADIS One-Minute ASOS (OMO)
    m = MADIS_OMO_PATTERN.search(key)
    if m:
        date_str, hhmm = m.groups()
        cycle = datetime.strptime(f"{date_str}{hhmm[:2]}", "%Y%m%d%H").replace(
            tzinfo=timezone.utc
        )
        return S3EventInfo(
            model=MADIS_OMO_MODEL, bucket=bucket, key=key,
            cycle=cycle, fxx=0, notification_ts=notification_ts,
        )

    return None


# ======================================================================
# Main listener service
# ======================================================================

class NWPSNSListener(AsyncService):
    """Listens to NOAA AWS SNS topics for NWP + MADIS data and saves with latency tracking.

    Supports two data types:
      - NWP (HRRR, RRFS, NBM): gridded model output → extract at station coordinates
      - MADIS (METAR, OMO): station-based observations → extract by ICAO code

    Lifecycle:
      1. Creates SQS queue subscribed to configured SNS topics
      2. Long-polls SQS for new S3 object notifications
      3. For NWP: fetches point data at station coordinates
         For MADIS: downloads NetCDF and extracts station observations
      4. Saves to Parquet with latency metadata
      5. Deletes processed messages
      6. On shutdown, cleans up the SQS queue
    """

    def __init__(self, config: dict, config_dir: Path):
        self.config = config
        self.config_dir = config_dir

        # Stations from config
        series = get_event_series(config, "nwp_listener")
        self.stations = nwp_stations_for_series(series)
        logger.info(
            "NWP listener stations: %s",
            [f"{s.icao} ({s.city})" for s in self.stations],
        )

        # Storage — separate for NWP vs MADIS
        data_dir = (config_dir / config["storage"]["data_dir"]).resolve()
        self.nwp_storage = NWPRealtimeStorage(data_dir)
        self.madis_storage = MADISRealtimeStorage(data_dir)

        # NWP listener config
        nwp_cfg = config.get("nwp_listener", {})
        self.aws_region = nwp_cfg.get("aws_region", "us-east-1")
        self.aws_creds = get_aws_credentials(config)
        
        # Set environment variables so libraries like Herbie pick them up automatically
        if self.aws_creds:
            import os
            os.environ["AWS_ACCESS_KEY_ID"] = self.aws_creds[0]
            os.environ["AWS_SECRET_ACCESS_KEY"] = self.aws_creds[1]
            os.environ["AWS_DEFAULT_REGION"] = self.aws_region

        self.poll_interval = nwp_cfg.get("poll_interval_seconds", 20)
        self.delete_queue_on_shutdown = nwp_cfg.get("delete_queue_on_shutdown", True)

        # Parse model configs (NWP + MADIS share the same config structure)
        self.model_configs: dict[str, ModelSNSConfig] = {}
        for model_name, model_cfg in nwp_cfg.get("models", {}).items():
            mc = ModelSNSConfig(
                name=model_name,
                sns_topic_arn=model_cfg["sns_topic_arn"],
                s3_bucket=model_cfg["s3_bucket"],
                enabled=model_cfg.get("enabled", True),
            )
            if mc.enabled:
                self.model_configs[model_name] = mc
                logger.info("Model %s: SNS=%s", model_name, mc.sns_topic_arn)

        # Queue name (unique per deployment)
        queue_prefix = nwp_cfg.get("sqs_queue_prefix", "pred-market-nwp")
        self.queue_name = f"{queue_prefix}-{int(time.time())}"

        # SQS manager (initialized in run)
        self.sqs_manager: SQSManager | None = None

        # Fetchers (lazy-loaded) — NWP fetchers
        self._fetchers: dict[str, Any] = {}
        # MADIS fetchers (lazy-loaded)
        self._madis_fetchers: dict[str, Any] = {}

        # Stats
        self._events_processed = 0
        self._events_skipped = 0
        self._madis_events_processed = 0

    def _get_fetcher(self, model_name: str):
        """Lazy-load the appropriate NWP fetcher for a model."""
        if model_name not in self._fetchers:
            from services.weather.nwp import _load_models, MODEL_REGISTRY

            _load_models()
            if model_name in MODEL_REGISTRY:
                fetcher_cls = MODEL_REGISTRY[model_name]
                data_dir = (self.config_dir / self.config["storage"]["data_dir"]).resolve()
                model_cfg = self.config.get(model_name, {})
                max_fxx = model_cfg.get(
                    "max_forecast_hour", fetcher_cls.DEFAULT_MAX_FXX
                )
                self._fetchers[model_name] = fetcher_cls(
                    data_dir=data_dir,
                    max_forecast_hour=max_fxx,
                    aws_access_key_id=self.aws_creds[0],
                    aws_secret_access_key=self.aws_creds[1],
                )
            else:
                logger.warning("No fetcher registered for model: %s", model_name)
                self._fetchers[model_name] = None

        return self._fetchers.get(model_name)

    def _get_madis_fetcher(self, model_name: str):
        """Lazy-load the appropriate MADIS fetcher."""
        if model_name not in self._madis_fetchers:
            from services.weather.madis import _load_madis, MADIS_FETCHERS

            _load_madis()
            if model_name in MADIS_FETCHERS:
                fetcher_cls = MADIS_FETCHERS[model_name]
                data_dir = (self.config_dir / self.config["storage"]["data_dir"]).resolve()
                self._madis_fetchers[model_name] = fetcher_cls(
                    data_dir=data_dir,
                    aws_access_key_id=self.aws_creds[0],
                    aws_secret_access_key=self.aws_creds[1],
                )
            else:
                logger.warning("No MADIS fetcher for: %s", model_name)
                self._madis_fetchers[model_name] = None

        return self._madis_fetchers.get(model_name)

    async def _process_event(self, event: S3EventInfo) -> None:
        """Process a single S3 event: NWP or MADIS."""
        model = event.model
        if model not in self.model_configs:
            self._events_skipped += 1
            return

        if event.is_madis:
            await self._process_madis_event(event)
        else:
            await self._process_nwp_event(event)

    async def _process_nwp_event(self, event: S3EventInfo) -> None:
        """Process an NWP S3 event: fetch gridded data and save with latency."""
        model = event.model
        fetcher = self._get_fetcher(model)
        if fetcher is None:
            self._events_skipped += 1
            return

        logger.info(
            "Processing NWP %s: cycle=%s fxx=%02d key=%s",
            model,
            event.cycle.strftime("%Y-%m-%d %HZ"),
            event.fxx,
            event.key[-60:],
        )

        try:
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                lambda: fetcher.fetch_run(event.cycle, event.fxx, self.stations),
            )

            if not df.empty:
                self.nwp_storage.save(df, model, event.notification_ts)
                self._events_processed += 1
                logger.info(
                    "%s: saved %d rows for cycle=%s fxx=%02d "
                    "(notification_lag=%.0fs)",
                    model, len(df),
                    event.cycle.strftime("%Y-%m-%d %HZ"), event.fxx,
                    (event.notification_ts - event.cycle).total_seconds(),
                )
            else:
                logger.debug(
                    "%s: no data extracted for fxx=%02d", model, event.fxx
                )
        except Exception:
            logger.exception(
                "%s: failed to process cycle=%s fxx=%02d",
                model,
                event.cycle.strftime("%Y-%m-%d %HZ"),
                event.fxx,
            )

    async def _process_madis_event(self, event: S3EventInfo) -> None:
        """Process a MADIS S3 event: download NetCDF and extract station obs."""
        model = event.model
        fetcher = self._get_madis_fetcher(model)
        if fetcher is None:
            self._events_skipped += 1
            return

        logger.info(
            "Processing MADIS %s: key=%s",
            model, event.key[-60:],
        )

        try:
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                lambda: fetcher.fetch_from_s3(
                    event.bucket, event.key, self.stations, event.notification_ts,
                ),
            )

            if not df.empty:
                self.madis_storage.save(df, model, event.notification_ts)
                self._madis_events_processed += 1
                logger.info(
                    "%s: saved %d obs for %d stations "
                    "(notification_lag=%.0fs)",
                    model, len(df),
                    df["station"].nunique(),
                    (event.notification_ts - event.cycle).total_seconds(),
                )
            else:
                logger.debug("%s: no matching station obs in %s", model, event.key)
        except Exception:
            logger.exception(
                "%s: failed to process key=%s", model, event.key,
            )

    async def _poll_loop(self) -> None:
        """Main polling loop: receive SQS messages and process events."""
        logger.info("Starting SQS polling loop (interval=%ds)", self.poll_interval)

        while self._running:
            try:
                messages = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self.sqs_manager.receive_messages(
                        max_messages=10, wait_time=self.poll_interval
                    ),
                )

                for msg in messages:
                    if not self._running:
                        break

                    body = msg.get("Body", "")
                    events = parse_sns_message(body)

                    for event in events:
                        await self._process_event(event)

                    # Delete processed message
                    receipt = msg.get("ReceiptHandle")
                    if receipt:
                        await asyncio.get_event_loop().run_in_executor(
                            None,
                            lambda rh=receipt: self.sqs_manager.delete_message(rh),
                        )

            except Exception:
                if self._running:
                    logger.exception("Error in poll loop")
                    await asyncio.sleep(5)

    async def _stats_loop(self) -> None:
        """Periodically log processing stats."""
        while self._running:
            await asyncio.sleep(300)  # Every 5 minutes
            if self._running:
                logger.info(
                    "Listener stats: nwp_processed=%d madis_processed=%d "
                    "skipped=%d stations=%d",
                    self._events_processed,
                    self._madis_events_processed,
                    self._events_skipped,
                    len(self.stations),
                )

    # ------------------------------------------------------------------
    # AsyncService overrides
    # ------------------------------------------------------------------

    def _get_tasks(self) -> list:
        # Set up SQS queue + SNS subscriptions before polling
        self.sqs_manager = SQSManager(
            self.aws_region, self.queue_name, *self.aws_creds
        )
        topic_arns = sorted(list(set(
            mc.sns_topic_arn for mc in self.model_configs.values()
        )))
        self.sqs_manager.setup(topic_arns)

        return [self._poll_loop(), self._stats_loop()]

    def _on_shutdown(self) -> None:
        logger.info(
            "Shutting down listener. NWP=%d, MADIS=%d, Skipped=%d",
            self._events_processed,
            self._madis_events_processed,
            self._events_skipped,
        )
        if self.sqs_manager and self.delete_queue_on_shutdown:
            self.sqs_manager.cleanup()


# ======================================================================
# CLI
# ======================================================================

def main():
    parser = standard_argparser(
        "Weather SNS listener (real-time NWP + MADIS ingest via AWS SNS)"
    )
    args = parser.parse_args()

    configure_logging(args.log_level)

    config, config_path = load_config(args.config)
    svc = NWPSNSListener(config, config_dir=config_path.parent)
    asyncio.run(svc.run())


if __name__ == "__main__":
    main()
