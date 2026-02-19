#!/bin/bash
# Pull latest code from GitHub, rebuild the Docker image, and restart the collector.
# Safe to re-run at any time — the collector is only down for the rebuild+restart window.
#
# Usage (on the VM):
#   cd ~/pred_market/scripts/oci_collector && ./update.sh
#
# Usage (from local machine):
#   ssh ubuntu@<PUBLIC_IP> '~/pred_market/scripts/oci_collector/update.sh'
#
# Cron (auto-update every 6 hours):
#   0 */6 * * * /home/ubuntu/pred_market/scripts/oci_collector/update.sh >> /home/ubuntu/collector-data/update.log 2>&1
set -euo pipefail

REPO_DIR="/home/ubuntu/pred_market"
SCRIPT_DIR="$REPO_DIR/scripts/oci_collector"
IMAGE="kalshi-collector:latest"
CONTAINER="kalshi-collector"

DOCKER="docker"
$DOCKER info &>/dev/null 2>&1 || DOCKER="sudo docker"

echo "────────────────────────────────────────"
echo "[update] $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "────────────────────────────────────────"

# ── Pull latest code ─────────────────────────────────────────────────────────
cd "$REPO_DIR"
BEFORE=$(git rev-parse HEAD)
git fetch origin main
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

if [[ "$LOCAL" == "$REMOTE" ]]; then
  echo "[update] Already up to date ($(git rev-parse --short HEAD)). Nothing to do."
  exit 0
fi

echo "[update] Updating $(git rev-parse --short HEAD) → $(git rev-parse --short origin/main)..."
git pull --ff-only origin main
AFTER=$(git rev-parse HEAD)

echo "[update] Changes:"
git --no-pager log --oneline "$BEFORE..$AFTER"
echo ""

# ── Rebuild Docker image ─────────────────────────────────────────────────────
echo "[update] Rebuilding Docker image..."
$DOCKER build -t "$IMAGE" "$REPO_DIR/pred_market_src/collector/"

# ── Restart collector ────────────────────────────────────────────────────────
echo "[update] Restarting collector..."
"$SCRIPT_DIR/run_collector.sh" start

echo ""
echo "[update] Done. Now running $(git -C "$REPO_DIR" rev-parse --short HEAD)."
