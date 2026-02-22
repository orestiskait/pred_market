#!/bin/bash
# Probe the OCI VM running Kalshi listener, Synoptic listener, and weather bot.
# Run from your local machine (requires OCI CLI + SSH access).
#
# Usage:
#   ./probe_vm_and_container_status.sh
#
# Env vars (auto-detected if not set):
#   COMPARTMENT_ID  — OCI compartment (default: tenancy root)
#   DISPLAY_NAME   — instance name (default: kalshi-collector)
set -euo pipefail

DISPLAY_NAME="${DISPLAY_NAME:-kalshi-collector}"
START_ALL_SCRIPT="/home/ubuntu/pred_market/scripts/oci_collector/manage_services/start_stop_all_services.sh"

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
echo "── Code version ──"
ssh -o ConnectTimeout=10 -o BatchMode=yes ubuntu@"$PUBLIC_IP" \
  'cd ~/pred_market && echo "Branch: $(git rev-parse --abbrev-ref HEAD)" && echo "Commit: $(git rev-parse --short HEAD)"' 2>/dev/null || echo "[probe] Could not get git info"

echo ""
echo "── Container status ──"
if ! ssh -o ConnectTimeout=10 -o BatchMode=yes ubuntu@"$PUBLIC_IP" \
  "$START_ALL_SCRIPT status" 2>/dev/null; then
  echo "[probe] SSH failed (timeout, key, or VM unreachable)"
  echo "        Try: ssh ubuntu@$PUBLIC_IP"
  exit 4
fi

echo ""
echo "── Recent logs (last 15 lines) ──"
ssh -o ConnectTimeout=10 ubuntu@"$PUBLIC_IP" \
  'echo "Kalshi Listener:" && docker logs --tail 15 kalshi-listener && echo "---" && echo "Synoptic Listener:" && docker logs --tail 15 synoptic-listener && echo "---" && echo "Weather Bot:" && docker logs --tail 15 weather-bot' 2>/dev/null || echo "[probe] Could not fetch logs"

echo ""
echo "── Data recency ──"
# Listeners flush every 5 min (flush_interval_seconds: 300). Flag stale if > 15 min.
ssh -o ConnectTimeout=10 ubuntu@"$PUBLIC_IP" 'bash -s' << 'RECENCY'
STALE_MIN=15
DATA_DIR=~/collector-data
NEWEST=$(find "$DATA_DIR/market_snapshots" "$DATA_DIR/orderbook_snapshots" "$DATA_DIR/synoptic_ws" -name "*.parquet" 2>/dev/null \
  | xargs -r stat -c "%Y %n" 2>/dev/null | sort -nr | head -1)
if [[ -z "$NEWEST" ]]; then
  echo "No parquet files yet"
else
  MTIME=${NEWEST%% *}
  FILE=${NEWEST#* }
  NOW=$(date +%s)
  AGE_SEC=$((NOW - MTIME))
  AGE_MIN=$((AGE_SEC / 60))
  SHORT=${FILE#${DATA_DIR}/}
  if [[ $AGE_MIN -gt $STALE_MIN ]]; then
    echo "Last write: ${AGE_MIN} min ago ($SHORT)"
    echo "Status: STALE (>${STALE_MIN} min — check Kalshi/Synoptic listeners)"
  else
    echo "Last write: ${AGE_MIN} min ago ($SHORT)"
    echo "Status: OK"
  fi
fi
RECENCY
2>/dev/null || echo "[probe] Could not get data recency"

echo ""
echo "── Data freshness ──"
ssh -o ConnectTimeout=10 ubuntu@"$PUBLIC_IP" \
  'ls -la ~/collector-data/market_snapshots/ ~/collector-data/orderbook_snapshots/ ~/collector-data/synoptic_ws/ 2>/dev/null || echo "No data dirs yet"' 2>/dev/null

echo ""
echo "────────────────────────────────────────"
echo "[probe] OK — VM running, Kalshi listener + Synoptic listener reachable"
echo "────────────────────────────────────────"
