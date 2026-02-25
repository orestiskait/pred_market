#!/bin/bash
# Probe the OCI VM running Kalshi listener and weather bot.
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
# Detection order:
#   1️⃣ Tenancy OCID from ~/.oci/config (most reliable)
#   2️⃣ First compartment returned by OCI CLI (fallback)
if [[ -z "${COMPARTMENT_ID:-}" ]]; then
  # 1) Try tenancy OCID from OCI config
  if [[ -f "${HOME}/.oci/config" ]]; then
    COMPARTMENT_ID=$(grep -m1 '^tenancy=' "${HOME}/.oci/config" | cut -d= -f2)
  fi

  # 2) If still empty, ensure OCI CLI is available before listing compartments
  if [[ -z "${COMPARTMENT_ID:-}" ]]; then
    if ! command -v oci >/dev/null 2>&1; then
      echo "ERROR: OCI CLI not installed. Install it or set COMPARTMENT_ID manually." >&2
      exit 1
    fi
    COMPARTMENT_ID=$(oci iam compartment list \
        --compartment-id-in-subtree true --all \
        --query 'data[0].id' --raw-output 2>/dev/null) || true
  fi

  # 3) If still empty, abort with clear message
  if [[ -z "${COMPARTMENT_ID:-}" ]]; then
    echo "ERROR: Set COMPARTMENT_ID (auto‑detect failed)." >&2
    exit 1
  fi
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
  'echo "Kalshi Listener:" && docker logs --tail 15 kalshi-listener && echo "---" && echo "Weather Bot:" && docker logs --tail 15 weather-bot' 2>/dev/null || echo "[probe] Could not fetch logs"

echo ""
echo "── Data recency ──"
# Listeners flush every 5 min (flush_interval_seconds: 300). Flag stale if > 15 min.
ssh -o ConnectTimeout=10 ubuntu@"$PUBLIC_IP" 'bash -s' << 'RECENCY'
STALE_MIN=15
DATA_DIR=~/collector-data
NEWEST=$(find "$DATA_DIR/kalshi_market_snapshots" "$DATA_DIR/kalshi_orderbook_snapshots" "$DATA_DIR/aviationweather_metar" "$DATA_DIR/wethr_push" "$DATA_DIR/nwp_realtime" "$DATA_DIR/madis_realtime" -name "*.parquet" 2>/dev/null \
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
    echo "Status: STALE (>${STALE_MIN} min — check Kalshi listener / weather bot)"
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
  'ls -la ~/collector-data/kalshi_market_snapshots/ ~/collector-data/kalshi_orderbook_snapshots/ ~/collector-data/aviationweather_metar/ ~/collector-data/wethr_push/ ~/collector-data/nwp_realtime/ ~/collector-data/madis_realtime/ 2>/dev/null || echo "No data dirs yet"' 2>/dev/null

echo ""
echo "────────────────────────────────────────"
echo "[probe] OK — VM running, Kalshi listener and weather bot reachable"
echo "────────────────────────────────────────"
