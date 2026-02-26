#!/bin/bash
# Pull latest code from GitHub, rebuild the Docker image, and restart all services:
# Kalshi listener and weather bot.
# Safe to re-run at any time — services are only down for the rebuild+restart window.
#
# Usage (on the VM):
#   cd ~/pred_market/scripts/oci_collector/maintenance && ./update_code_and_restart_services.sh
#
# Usage (from local machine):
#   ssh ubuntu@<PUBLIC_IP> '~/pred_market/scripts/oci_collector/maintenance/update_code_and_restart_services.sh'
#
# Cron (auto-update every 6 hours):
#   0 */6 * * * /home/ubuntu/pred_market/scripts/oci_collector/maintenance/update_code_and_restart_services.sh >> /home/ubuntu/collector-data/update.log 2>&1
set -euo pipefail

# Derive repo root from this script's location (works on VM and local)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
OCI_ROOT="$REPO_DIR/scripts/oci_collector"
START_ALL_SCRIPT="$OCI_ROOT/manage_services/start_stop_all_services.sh"
IMAGE="kalshi-services:latest"  # Shared image for Kalshi listener and weather bot

DOCKER="docker"
$DOCKER info &>/dev/null 2>&1 || DOCKER="sudo docker"

echo "────────────────────────────────────────"
echo "[update] $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "────────────────────────────────────────"

# ── Update code ─────────────────────────────────────────────────────────────
cd "$REPO_DIR"
BEFORE=$(git rev-parse HEAD)

echo "[update] Fetching and resetting to origin/main..."
git fetch origin main
# Hard reset discards ALL local changes and makes the VM match GitHub exactly
git reset --hard origin/main
AFTER=$(git rev-parse HEAD)

if [[ "$BEFORE" == "$AFTER" ]]; then
  echo "[update] Already up to date ($(git rev-parse --short HEAD))."
else
  echo "[update] Updated $(git rev-parse --short "$BEFORE") → $(git rev-parse --short "$AFTER")"
  echo "[update] Changes:"
  git --no-pager log --oneline "$BEFORE..$AFTER"
fi
echo ""

# ── Rebuild Docker image ─────────────────────────────────────────────────────
echo "[update] Rebuilding Docker image..."
$DOCKER build -f "$REPO_DIR/services/Dockerfile" -t "$IMAGE" "$REPO_DIR"

# ── Restart all services (Kalshi listener, weather bot) ─
echo "[update] Restarting all services..."
"$START_ALL_SCRIPT" start

echo ""
echo "[update] Done. Now running $(git -C "$REPO_DIR" rev-parse --short HEAD)."
