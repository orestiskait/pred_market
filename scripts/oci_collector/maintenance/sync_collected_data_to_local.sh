#!/bin/bash
# Sync collected parquet files from the OCI VM to this machine.
# Optimized version: single rsync pass, nested directory support, fast summaries.
#
# By default syncs to data/ at project root.
# Override with: LOCAL_DATA_DIR=/your/path ./sync_collected_data_to_local.sh

set -euo pipefail

DISPLAY_NAME="${DISPLAY_NAME:-kalshi-collector}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOCAL_DATA_DIR="${LOCAL_DATA_DIR:-$SCRIPT_DIR/../../../data}"
LOCAL_DATA_DIR="$(realpath "$LOCAL_DATA_DIR")"

DRY_RUN=""
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN="--dry-run"

echo "────────────────────────────────────────"
echo "[fetch] $(date '+%Y-%m-%d %H:%M:%S %Z')"
[[ -n "$DRY_RUN" ]] && echo "[fetch] DRY RUN — no files will be written"
echo "────────────────────────────────────────"

# ── Resolve public IP ─────────────────────────────────────────────────────────
if [[ -z "${COMPARTMENT_ID:-}" ]]; then
  # Try to get from OCI CLI, fallback to config
  COMPARTMENT_ID=$(oci iam compartment list --compartment-id-in-subtree true --all --query 'data[0].id' --raw-output 2>/dev/null) || true
  [[ -z "$COMPARTMENT_ID" ]] && COMPARTMENT_ID=$(grep -m1 '^tenancy=' ~/.oci/config 2>/dev/null | cut -d= -f2) || true
  [[ -z "$COMPARTMENT_ID" ]] && { echo "ERROR: Set COMPARTMENT_ID or verify ~/.oci/config"; exit 1; }
fi

INSTANCE_ID=$(oci compute instance list -c "$COMPARTMENT_ID" --display-name "$DISPLAY_NAME" --query 'data[0].id' --raw-output 2>/dev/null) || true
[[ -z "$INSTANCE_ID" || "$INSTANCE_ID" == "null" ]] && { echo "ERROR: No instance found with display-name '$DISPLAY_NAME'"; exit 1; }

# Fast check: get state and IP in one OCI metadata call if possible, but list-vnics is more reliable
PUBLIC_IP=$(oci compute instance list-vnics --instance-id "$INSTANCE_ID" --query 'data[0]."public-ip"' --raw-output)
[[ -z "$PUBLIC_IP" || "$PUBLIC_IP" == "null" ]] && { echo "ERROR: No public IP found"; exit 3; }

echo "[fetch] VM Host: $PUBLIC_IP"
echo "[fetch] Local:   $LOCAL_DATA_DIR"
echo "────────────────────────────────────────"

# ── Sync ──────────────────────────────────────────────────────────────────────
mkdir -p "$LOCAL_DATA_DIR"

echo "[fetch] Starting incremental sync..."
# Filter Rules: Include directories, include parquets, exclude everything else
FILTER_FILE=$(mktemp)
cat <<EOF > "$FILTER_FILE"
+ */
+ **/*.parquet
- *
EOF

rsync -avz $DRY_RUN \
  --ignore-existing \
  --include-from="$FILTER_FILE" \
  -e "ssh -o ConnectTimeout=10 -o BatchMode=yes" \
  "ubuntu@${PUBLIC_IP}:/home/ubuntu/collector-data/" \
  "$LOCAL_DATA_DIR/" \
  | tee /tmp/rsync_out.txt

rm -f "$FILTER_FILE"

# ── Summary ───────────────────────────────────────────────────────────────────
echo "────────────────────────────────────────"
if [[ -n "$DRY_RUN" ]]; then
  echo "[fetch] Dry run complete."
else
  # Fast calculation of transfer stats
  TRANSFERRED=$(grep -c '\.parquet$' /tmp/rsync_out.txt || true)
  
  # Get total size of data dir quickly
  TOTAL_SIZE=$(du -sh "$LOCAL_DATA_DIR" | cut -f1)
  
  echo "[fetch] Sync complete."
  echo "  Files transferred: $TRANSFERRED"
  echo "  Total local storage: $TOTAL_SIZE"
  echo ""
  
  # Show most recent activity (top 3 subdirs by latest file)
  echo "Recent updates (local):"
  # Look for files modified in the last 60 mins (likely the ones we just synced)
  find "$LOCAL_DATA_DIR" -name "*.parquet" -mmin -60 | rev | cut -d/ -f2 | rev | sort | uniq -c | sort -nr | head -n 3 || true
fi
echo "────────────────────────────────────────"
