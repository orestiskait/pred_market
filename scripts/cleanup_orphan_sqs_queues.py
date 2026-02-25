#!/usr/bin/env python3
"""Delete orphan SQS queues created by the NWP listener before the stable-name fix.

Previously, the listener used queue names like pred-market-nwp-<timestamp>, creating
a new queue on every restart. Orphan queues (from crashes/restarts) kept receiving
SNS notifications, inflating SQS request costs.

Usage:
    pred_env/bin/python scripts/cleanup_orphan_sqs_queues.py
    pred_env/bin/python scripts/cleanup_orphan_sqs_queues.py --config services/config.yaml
    pred_env/bin/python scripts/cleanup_orphan_sqs_queues.py --dry-run  # list only, no delete

Requires: AWS credentials (same as NWP listener) in credentials dir.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

# Add services to path for config loading
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.core.config import load_config, get_aws_credentials


def main():
    parser = argparse.ArgumentParser(
        description="Delete orphan NWP SQS queues (pred-market-nwp-<timestamp>)"
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.yaml (default: auto-detect)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List queues only, do not delete",
    )
    args = parser.parse_args()

    config, _ = load_config(args.config)
    nwp_cfg = config.get("nwp", {})
    prefix = nwp_cfg.get("sqs_queue_prefix", "pred-market-nwp")
    region = nwp_cfg.get("aws_region", "us-east-1")
    stable_name = nwp_cfg.get("sqs_queue_name") or prefix

    # Orphan pattern: pred-market-nwp-<digits> (timestamp)
    orphan_pattern = re.compile(rf"^{re.escape(prefix)}-\d+$")

    import boto3

    aws_creds = get_aws_credentials(config)
    sqs = boto3.client(
        "sqs",
        region_name=region,
        aws_access_key_id=aws_creds[0],
        aws_secret_access_key=aws_creds[1],
    )

    paginator = sqs.get_paginator("list_queues")
    orphan_urls: list[tuple[str, str]] = []  # (name, url)

    for page in paginator.paginate():
        for url in page.get("QueueUrls", []):
            name = url.split("/")[-1]
            if orphan_pattern.match(name) and name != stable_name:
                orphan_urls.append((name, url))

    if not orphan_urls:
        print(f"No orphan queues found (prefix={prefix}-<timestamp>)")
        return

    print(f"Found {len(orphan_urls)} orphan queue(s):")
    for name, url in orphan_urls:
        print(f"  {name}")

    if args.dry_run:
        print("\n[DRY RUN] Run without --dry-run to delete.")
        return

    for name, url in orphan_urls:
        try:
            sqs.delete_queue(QueueUrl=url)
            print(f"Deleted: {name}")
        except Exception as e:
            print(f"Failed to delete {name}: {e}")

    print(f"\nDeleted {len(orphan_urls)} queue(s).")


if __name__ == "__main__":
    main()
