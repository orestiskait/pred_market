#!/bin/bash
# Probe the OCI collector VM: VM state, container status, recent logs, data freshness.
# Run from your local machine (requires OCI CLI + SSH access).
#
# Usage:
#   ./probe.sh
#
# Env vars (auto-detected if not set):
#   COMPARTMENT_ID  — OCI compartment (default: tenancy root)
#   DISPLAY_NAME   — instance name (default: kalshi-collector)
set -euo pipefail

DISPLAY_NAME="${DISPLAY_NAME:-kalshi-collector}"

# ── Auto-detect compartment ────────────────────────────────────────────────────
if [[ -z "${COMPARTMENT_ID:-}" ]]; then
  COMPARTMENT_ID=$(oci iam compartment list \
    --compartment-id-in-subtree true --all \
    --query 'data[0].id' --raw-output 2>/dev/null) || true
  [[ -z "$COMPARTMENT_ID" ]] && \
    COMPARTMENT_ID=$(grep -m1 '^tenancy=' ~/.oci/config 2>/dev/null | cut -d= -f2) || true
  [[ -z "$COMPARTMENT_ID" ]] && echo "ERROR: Set COMPARTMENT_ID." && exit 1
fi

# ── Resolve instance ─────────────────────────────────────────────────────────
echo "────────────────────────────────────────"
echo "[probe] $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "────────────────────────────────────────"

INSTANCE_ID=$(oci compute instance list -c "$COMPARTMENT_ID" \
  --display-name "$DISPLAY_NAME" \
  --query 'data[0].id' --raw-output 2>/dev/null) || true

if [[ -z "$INSTANCE_ID" || "$INSTANCE_ID" == "null" ]]; then
  echo "[probe] ERROR: No instance found with display-name '$DISPLAY_NAME'"
  exit 1
fi

STATE=$(oci compute instance get --instance-id "$INSTANCE_ID" \
  --query 'data."lifecycle-state"' --raw-output)
PUBLIC_IP=$(oci compute instance list-vnics --instance-id "$INSTANCE_ID" \
  --query 'data[0]."public-ip"' --raw-output)

echo "[probe] Instance: $INSTANCE_ID"
echo "[probe] State:    $STATE"
echo "[probe] IP:       ${PUBLIC_IP:-<none>}"
echo ""

if [[ "$STATE" != "RUNNING" ]]; then
  echo "[probe] VM is not RUNNING. Start it from OCI Console or:"
  echo "        oci compute instance action --instance-id $INSTANCE_ID --action START"
  exit 2
fi

if [[ -z "$PUBLIC_IP" || "$PUBLIC_IP" == "null" ]]; then
  echo "[probe] ERROR: No public IP assigned"
  exit 3
fi

# ── SSH probe ────────────────────────────────────────────────────────────────
echo "── Container status ──"
if ! ssh -o ConnectTimeout=10 -o BatchMode=yes ubuntu@"$PUBLIC_IP" \
  '~/pred_market/scripts/oci_collector/run_collector.sh status' 2>/dev/null; then
  echo "[probe] SSH failed (timeout, key, or VM unreachable)"
  echo "        Try: ssh ubuntu@$PUBLIC_IP"
  exit 4
fi

echo ""
echo "── Recent logs (last 15 lines) ──"
ssh -o ConnectTimeout=10 ubuntu@"$PUBLIC_IP" \
  'docker logs --tail 15 kalshi-collector' 2>/dev/null || echo "[probe] Could not fetch logs"

echo ""
echo "── Data freshness ──"
ssh -o ConnectTimeout=10 ubuntu@"$PUBLIC_IP" \
  'ls -la ~/collector-data/market_snapshots/ ~/collector-data/orderbook_snapshots/ 2>/dev/null || echo "No data dirs yet"' 2>/dev/null

echo ""
echo "────────────────────────────────────────"
echo "[probe] OK — VM running, collector reachable"
echo "────────────────────────────────────────"
