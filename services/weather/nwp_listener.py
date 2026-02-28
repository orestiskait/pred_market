"""NWP listener: real-time weather model ingest via AWS SNS/SQS.

NWP (HRRR, RRFS, NBM): SQS long-poll receives SNS notifications for new S3
objects, extracts point data at station coordinates from GRIB2/COG files,
and saves to Parquet with latency tracking metadata.

Usage:
    python -m services.weather.nwp_listener
    python -m services.weather.nwp_listener --config path/to/config.yaml

Requires: boto3. AWS credentials (from config) are used ONLY for SQS/SNS
(queue creation, subscriptions, polling). S3 downloads use anonymous
access — NOAA buckets are public, no credentials for reads.
"""

from __future__ import annotations

import asyncio
import collections
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from services.core.config import (
    load_config,
    get_event_series,
    standard_argparser,
    configure_logging,
    get_aws_credentials,
)
from services.core.service import AsyncService
from services.weather.station_registry import nwp_stations_for_series, NWPStation
from services.weather.storage import NWPRealtimeStorage, SQSMessagesStorage

logger = logging.getLogger(__name__)


# ======================================================================
# S3 key patterns for each model — extracts cycle hour and forecast hour
# ======================================================================

# HRRR sub-hourly: hrrr.YYYYMMDD/conus/hrrr.tCCz.wrfsubhfFF.grib2
# HRRR hourly:     hrrr.YYYYMMDD/conus/hrrr.tCCz.wrfsfcfFF.grib2
HRRR_PATTERN = re.compile(
    r"hrrr\.(\d{8})/conus/hrrr\.t(\d{2})z\.wrf(?:subhf|sfcf)(\d{2,3})\.grib2$"
)

# RRFS: rrfs_a/rrfs.YYYYMMDD/HH/.../rrfs.tCCz.prslev.3km.f[FH].conus.grib2
RRFS_PATTERN = re.compile(
    r"rrfs\.t(\d{2})z\.prslev\.3km\.f(\d{2,3})\.conus\.grib2$"
)
RRFS_DATE_PATTERN = re.compile(r"rrfs\.(\d{8})")

# NBM COG: blendv4.3/conus/YYYY/MM/DD/HH00/temp/blendv4.3_conus_temp_RUN_VALID.tif
NBM_COG_PATTERN = re.compile(
    r"blendv4\.3/conus/(\d{4})/(\d{2})/(\d{2})/(\d{2})00/(?:temp|tempstddev|qmd|prob)/"
    r"blendv4\.3_conus_(?:temp|tempstddev|qmd|prob)_"
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})_(\d{4}-\d{2}-\d{2}T\d{2}:\d{2})\.tif$"
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

    def _delete_existing(self) -> bool:
        """DEPRECATED. Replaced by direct creation/reuse."""
        return False

    def _create_and_subscribe(self, topic_arns: list[str]) -> None:
        """Create queue and subscribe to SNS topics."""
        import botocore.exceptions
        try:
            resp = self.sqs.create_queue(
                QueueName=self.queue_name,
                Attributes={
                    "ReceiveMessageWaitTimeSeconds": "20",  # Long polling
                    "VisibilityTimeout": "300",  # 5 min to process
                    "MessageRetentionPeriod": "86400",  # 1 day
                },
            )
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "AWS.SimpleQueueService.QueueDeletedRecently":
                logger.warning("Queue deleted recently. Must wait 60s...")
                raise
            raise
        self.queue_url = resp["QueueUrl"]

        # Get queue ARN
        attrs = self.sqs.get_queue_attributes(
            QueueUrl=self.queue_url,
            AttributeNames=["QueueArn"],
        )
        self.queue_arn = attrs["Attributes"]["QueueArn"]
        logger.info("SQS queue ready: %s (%s)", self.queue_name, self.queue_arn)

        # Purge any existing messages (e.g. from while bot was offline)
        try:
            self.sqs.purge_queue(QueueUrl=self.queue_url)
            logger.info("Purged existing messages from queue: %s", self.queue_name)
        except Exception as e:
            # Purge can fail if called too recently (once per 60s)
            logger.debug("Skip purge: %s", e)

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

        # Subscribe to each SNS topic with a Filter Policy to reduce costs.
        # Only receive .grib2 and .tif (skip .idx, .gif, .json, etc.)
        # FilterPolicyScope="MessageBody" allows filtering on the S3 key in the JSON body.
        filter_policy = {
            "Records": {
                "s3": {
                    "object": {
                        "key": [{"suffix": ".grib2"}, {"suffix": ".tif"}]
                    }
                }
            }
        }
        
        for arn in topic_arns:
            try:
                self.sns.subscribe(
                    TopicArn=arn,
                    Protocol="sqs",
                    Endpoint=self.queue_arn,
                    Attributes={
                        "FilterPolicy": json.dumps(filter_policy),
                        "FilterPolicyScope": "MessageBody"
                    }
                )
                logger.info("Subscribed SQS to SNS topic with FilterPolicy: %s", arn)
            except Exception as e:
                # Fallback if topic doesn't support MessageBody filtering (unlikely for NOAA)
                logger.warning("FilterPolicy failed, trying basic subscription for %s: %s", arn, e)
                self.sns.subscribe(
                    TopicArn=arn,
                    Protocol="sqs",
                    Endpoint=self.queue_arn,
                )

    async def setup_async(self, topic_arns: list[str]) -> None:
        """Create/Reuse SQS queue and subscribe."""
        loop = asyncio.get_event_loop()
        # Direct call. If queue exists, it returns attributes. If just deleted, 
        # it might fail, but we will use a fresh name or just let it fail and 
        # have the user restart if they are in a 60s window.
        await loop.run_in_executor(None, lambda: self._create_and_subscribe(topic_arns))

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
        """Cleanup on shutdown. 
        Note: We NO LONGER delete the queue to avoid recreation delays and 
        high SQS billing from rapid churn. The FilterPolicy and startup Purge
        handle volume and stale data.
        """
        # if self.queue_url:
        #     try:
        #         self.sqs.delete_queue(QueueUrl=self.queue_url)
        #         logger.info("Deleted SQS queue: %s", self.queue_name)
        #     except Exception as e:
        #         logger.warning("Failed to delete SQS queue: %s", e)
        pass


# ======================================================================
# S3 event parser — extracts model/cycle/fxx from SNS notifications
# ======================================================================

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
        if key:
            import urllib.parse
            key = urllib.parse.unquote_plus(key)

        if not key:
            continue

        event = _match_key(bucket, key, notification_ts)
        if not event:
            # ONLY LOG if it looks like it might be one of our models but failed regex
            if any(m in key for m in ["hrrr", "rrfs", "nbm", "rtma"]):
                logger.debug("S3 key match skipped: bucket=%s key=%s", bucket, key)
            continue

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

    return None


# ======================================================================
# Main listener service
# ======================================================================

class NWPSNSListener(AsyncService):
    """Listens to NOAA AWS SNS topics for NWP data and saves with latency tracking.

    Supports NWP models (HRRR, RRFS, NBM): gridded model output → extract
    point data at station coordinates.

    Lifecycle:
      1. Creates SQS queue subscribed to configured SNS topics
      2. Long-polls SQS for new S3 object notifications
      3. Fetches point data at station coordinates from GRIB2/COG
      4. Saves to Parquet with latency metadata
      5. Deletes processed messages
      6. On shutdown, cleans up the SQS queue
    """

    def __init__(self, config: dict, config_dir: Path):
        self.config = config
        self.config_dir = config_dir

        # Stations from config
        series = get_event_series(config, "weather_bot")
        self.stations = nwp_stations_for_series(series)
        logger.info(
            "NWP listener stations: %s",
            [f"{s.icao} ({s.city})" for s in self.stations],
        )

        # Storage
        data_dir = (config_dir / config["storage"]["data_dir"]).resolve()
        self.nwp_storage = NWPRealtimeStorage(data_dir)

        # NWP config
        nwp_cfg = config.get("nwp", {})
        self.aws_region = nwp_cfg.get("aws_region", "us-east-1")
        # Credentials used ONLY for SQS/SNS (queue creation, subscriptions, polling).
        # S3 downloads (NOAA public buckets) use anonymous access — no credentials.
        self.aws_creds = get_aws_credentials(config)

        self.poll_interval = nwp_cfg.get("poll_interval_seconds", 20)
        self.flush_interval = config.get("storage", {}).get("flush_interval_seconds", 300)

        # Parse enabled model configs from nwp.models
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

        # Queue name — stable so restarts reuse the same queue (avoids orphan queues
        # that accumulate SNS deliveries and inflate SQS request costs)
        self.queue_name = nwp_cfg.get("sqs_queue_name") or nwp_cfg.get(
            "sqs_queue_prefix", "pred-market-nwp"
        )

        # SQS manager (initialized in run)
        self.sqs_manager: SQSManager | None = None

        # Fetchers (lazy-loaded)
        self._fetchers: dict[str, Any] = {}

        # Stats
        self._events_processed = 0
        self._events_skipped = 0

        # SQS stats
        self.sqs_storage = SQSMessagesStorage(data_dir)
        self._sqs_message_count = 0
        self._model_message_counts: dict[str, int] = collections.defaultdict(int)
        self._last_stats_date = datetime.now(timezone.utc).date()
        self._load_sqs_stats()

    def _get_fetcher(self, model_name: str):
        """Lazy-load the appropriate NWP fetcher for a model."""
        if model_name not in self._fetchers:
            from services.weather.nwp import _load_models, MODEL_REGISTRY

            _load_models()
            if model_name in MODEL_REGISTRY:
                fetcher_cls = MODEL_REGISTRY[model_name]
                data_dir = (self.config_dir / self.config["storage"]["data_dir"]).resolve()
                model_cfg = self.config.get("nwp", {}).get("models", {}).get(model_name, {})
                max_fxx = model_cfg.get(
                    "max_forecast_hour", fetcher_cls.DEFAULT_MAX_FXX
                )
                self._fetchers[model_name] = fetcher_cls(
                    data_dir=data_dir,
                    max_forecast_hour=max_fxx,
                )
            else:
                logger.warning("No fetcher registered for model: %s", model_name)
                self._fetchers[model_name] = None

        return self._fetchers.get(model_name)

    async def _process_event(self, event: S3EventInfo) -> None:
        """Process a single NWP S3 event."""
        if event.model not in self.model_configs:
            self._events_skipped += 1
            return
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
                await loop.run_in_executor(
                    None,
                    lambda: self.nwp_storage.save(df, model, event.notification_ts)
                )
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
                    self._sqs_message_count += 1
                    events = parse_sns_message(body)

                    for event in events:
                        self._model_message_counts[event.model] += 1
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
        """Periodically log and save processing stats."""
        while self._running:
            await asyncio.sleep(self.flush_interval)
            if self._running:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._save_sqs_stats)
                logger.info(
                    "Listener stats: processed=%d skipped=%d sqs_msgs=%d stations=%d",
                    self._events_processed,
                    self._events_skipped,
                    self._sqs_message_count,
                    len(self.stations),
                )

    def _save_sqs_stats(self) -> None:
        """Save daily SQS message counts to parquet."""
        now = datetime.now(timezone.utc)
        today = now.date()

        # Reset counts at midnight UTC
        if today != self._last_stats_date:
            logger.info("New day (%s). Resetting SQS message counters.", today)
            self._sqs_message_count = 0
            self._model_message_counts.clear()
            self._last_stats_date = today

        rows = []
        # Total messages received by the queue
        rows.append({
            "date": today,
            "queue_name": self.queue_name,
            "model": "TOTAL",
            "message_count": self._sqs_message_count
        })
        # Breakdown by model
        for model in self.model_configs:
            rows.append({
                "date": today,
                "queue_name": self.queue_name,
                "model": model,
                "message_count": self._model_message_counts.get(model, 0)
            })

        df = pd.DataFrame(rows)
        self.sqs_storage.save(df)

    def _load_sqs_stats(self) -> None:
        """Load today's starting SQS message counts from storage."""
        today = datetime.now(timezone.utc).date()
        try:
            df = self.sqs_storage.read(self.queue_name, start_date=today, end_date=today)
            if not df.empty:
                # We only keep the latest daily record now
                latest = df[df["date"] == today]

                # TOTAL count
                total_row = latest[latest["model"] == "TOTAL"]
                if not total_row.empty:
                    self._sqs_message_count = int(total_row["message_count"].iloc[0])

                # Model counts
                for _, row in latest[latest["model"] != "TOTAL"].iterrows():
                    self._model_message_counts[row["model"]] = int(row["message_count"])

                logger.info(
                    "Restored SQS daily counters for %s: total=%d, models=%s",
                    today, self._sqs_message_count, dict(self._model_message_counts)
                )
        except Exception as e:
            logger.debug("No existing SQS stats to restore for %s: %s", today, e)

    # ------------------------------------------------------------------
    # AsyncService overrides
    # ------------------------------------------------------------------

    async def _nwp_setup_and_poll(self) -> None:
        """Setup SQS (non-blocking 60s wait) then run poll loop."""
        topic_arns = sorted({mc.sns_topic_arn for mc in self.model_configs.values()})
        await self.sqs_manager.setup_async(topic_arns)
        await self._poll_loop()

    def _get_tasks(self) -> list:
        # Set up SQS manager; setup_async runs in _nwp_setup_and_poll (non-blocking)
        self.sqs_manager = SQSManager(
            self.aws_region, self.queue_name, *self.aws_creds
        )

        return [self._nwp_setup_and_poll(), self._stats_loop()]

    def _on_shutdown(self) -> None:
        logger.info(
            "Shutting down listener. Processed=%d, Skipped=%d",
            self._events_processed,
            self._events_skipped,
        )
        if self.sqs_manager:
            self.sqs_manager.cleanup()


# ======================================================================
# CLI
# ======================================================================

def main():
    parser = standard_argparser(
        "NWP listener (real-time weather model ingest)"
    )
    args = parser.parse_args()

    configure_logging(args.log_level)

    config, config_path = load_config(args.config)
    svc = NWPSNSListener(config, config_dir=config_path.parent)
    asyncio.run(svc.run())


if __name__ == "__main__":
    main()
