#!/bin/bash
# Sync collected parquet files from the OCI VM to this machine.
# Uses rsync — incremental, skips files already present, fast for daily runs.
#
# By default syncs to pred_market_src/collector/data/ (where the notebook reads from).
# Override with: LOCAL_DATA_DIR=/your/path ./fetch_data.sh
#
# Usage:
#   ./fetch_data.sh               # sync all data
#   ./fetch_data.sh --dry-run     # preview what would be transferred
#
# Env vars (auto-detected if not set):
#   COMPARTMENT_ID  — OCI compartment (default: tenancy root)
#   DISPLAY_NAME    — instance name (default: kalshi-collector)
#   LOCAL_DATA_DIR  — local destination (default: ../../pred_market_src/collector/data)
set -euo pipefail

DISPLAY_NAME="${DISPLAY_NAME:-kalshi-collector}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOCAL_DATA_DIR="${LOCAL_DATA_DIR:-$SCRIPT_DIR/../../pred_market_src/collector/data}"
LOCAL_DATA_DIR="$(realpath "$LOCAL_DATA_DIR")"

DRY_RUN=""
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN="--dry-run"

echo "────────────────────────────────────────"
echo "[fetch] $(date '+%Y-%m-%d %H:%M:%S %Z')"
[[ -n "$DRY_RUN" ]] && echo "[fetch] DRY RUN — no files will be written"
echo "────────────────────────────────────────"

# ── Resolve public IP ─────────────────────────────────────────────────────────
if [[ -z "${COMPARTMENT_ID:-}" ]]; then
  COMPARTMENT_ID=$(oci iam compartment list \
    --compartment-id-in-subtree true --all \
    --query 'data[0].id' --raw-output 2>/dev/null) || true
  [[ -z "$COMPARTMENT_ID" ]] && \
    COMPARTMENT_ID=$(grep -m1 '^tenancy=' ~/.oci/config 2>/dev/null | cut -d= -f2) || true
  [[ -z "$COMPARTMENT_ID" ]] && echo "ERROR: Set COMPARTMENT_ID." && exit 1
fi

INSTANCE_ID=$(oci compute instance list -c "$COMPARTMENT_ID" \
  --display-name "$DISPLAY_NAME" \
  --query 'data[0].id' --raw-output 2>/dev/null) || true
[[ -z "$INSTANCE_ID" || "$INSTANCE_ID" == "null" ]] && \
  echo "ERROR: No instance found with display-name '$DISPLAY_NAME'" && exit 1

STATE=$(oci compute instance get --instance-id "$INSTANCE_ID" \
  --query 'data."lifecycle-state"' --raw-output)
[[ "$STATE" != "RUNNING" ]] && echo "ERROR: VM is $STATE (not RUNNING)" && exit 2

PUBLIC_IP=$(oci compute instance list-vnics --instance-id "$INSTANCE_ID" \
  --query 'data[0]."public-ip"' --raw-output)
[[ -z "$PUBLIC_IP" || "$PUBLIC_IP" == "null" ]] && echo "ERROR: No public IP found" && exit 3

echo "[fetch] VM:    $PUBLIC_IP ($STATE)"
echo "[fetch] Dest:  $LOCAL_DATA_DIR"
echo ""

# ── Sync ──────────────────────────────────────────────────────────────────────
mkdir -p "$LOCAL_DATA_DIR"

# Subdirs to sync (mirrors ~/collector-data/ layout)
REMOTE_DIRS=(
  "market_snapshots"
  "orderbook_snapshots"
  "historical"
)

TOTAL_FILES=0
TOTAL_BYTES=0

for subdir in "${REMOTE_DIRS[@]}"; do
  REMOTE_PATH="ubuntu@${PUBLIC_IP}:/home/ubuntu/collector-data/${subdir}/"
  LOCAL_PATH="${LOCAL_DATA_DIR}/${subdir}/"

  # Check if the remote dir exists before trying to sync it
  if ! ssh -o ConnectTimeout=10 -o BatchMode=yes ubuntu@"$PUBLIC_IP" \
      "test -d /home/ubuntu/collector-data/${subdir}" 2>/dev/null; then
    echo "[fetch] $subdir/ — not present on VM, skipping"
    continue
  fi

  mkdir -p "$LOCAL_PATH"
  echo "[fetch] Syncing $subdir/ ..."

  rsync -avz --progress $DRY_RUN \
    -e "ssh -o ConnectTimeout=10 -o BatchMode=yes" \
    --include="*.parquet" \
    --exclude="*" \
    "$REMOTE_PATH" "$LOCAL_PATH" 2>&1 | tee /tmp/rsync_out.txt

  # Count what was transferred
  transferred=$(grep -c '^.*\.parquet$' /tmp/rsync_out.txt 2>/dev/null || true)
  TOTAL_FILES=$(( TOTAL_FILES + transferred ))
  echo ""
done

# ── Summary ───────────────────────────────────────────────────────────────────
echo "────────────────────────────────────────"
if [[ -n "$DRY_RUN" ]]; then
  echo "[fetch] Dry run complete. Run without --dry-run to transfer."
else
  echo "[fetch] Done."
  echo ""
  echo "Local data:"
  find "$LOCAL_DATA_DIR" -name "*.parquet" | sort | while read -r f; do
    SIZE=$(du -h "$f" | cut -f1)
    echo "  $SIZE  $(basename "$(dirname "$f")")/$(basename "$f")"
  done
fi
echo "────────────────────────────────────────"
