#!/bin/bash
# Run on the OCI VM to set up or update Kalshi listener, Synoptic listener, and weather bot.
# Safe to re-run — pulls latest code and rebuilds the image each time.
#
# Usage:
#   ./first_time_vm_setup.sh                          # interactive credential prompt on first run
#   GITHUB_TOKEN=xxx ./first_time_vm_setup.sh         # private repo
#   KALSHI_API_KEY_ID=xxx \
#     KALSHI_PRIVATE_KEY_FILE=~/.kalshi/kalshi_api_key.txt \
#     SYNOPTIC_API_TOKEN=xxx \
#     ./first_time_vm_setup.sh                        # non-interactive credential setup
set -euo pipefail

REPO_URL="https://github.com/orestiskait/pred_market.git"
REPO_DIR="/home/ubuntu/pred_market"
CREDS_DIR="/home/ubuntu/.kalshi"
KEY_ID_FILE="$CREDS_DIR/kalshi_api_key_id"
SYNOPTIC_FILE="$CREDS_DIR/synoptic_token"
DATA_DIR="/home/ubuntu/collector-data"
IMAGE="kalshi-services:latest"
CONTAINER="kalshi-listener"

OCI_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
START_ALL_SCRIPT="$OCI_ROOT/manage_services/start_stop_all_services.sh"

# ── Docker ────────────────────────────────────────────────────────────────────
if ! command -v docker &>/dev/null; then
  echo "[setup] Installing Docker..."
  curl -fsSL https://get.docker.com | sudo sh
  sudo usermod -aG docker ubuntu
  echo "[setup] Docker installed. If docker commands fail, log out/in to refresh group."
  DOCKER="sudo docker"
else
  DOCKER="docker"
  # Use sudo if current user is not yet in the docker group
  if ! $DOCKER info &>/dev/null 2>&1; then
    DOCKER="sudo docker"
  fi
fi

# ── Code ──────────────────────────────────────────────────────────────────────
if [[ -d "$REPO_DIR/.git" ]]; then
  echo "[setup] Pulling latest code..."
  if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    git -C "$REPO_DIR" remote set-url origin \
      "https://${GITHUB_TOKEN}@github.com/orestiskait/pred_market.git"
  fi
  git -C "$REPO_DIR" pull --ff-only
else
  echo "[setup] Cloning repo..."
  if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    git clone "https://${GITHUB_TOKEN}@github.com/orestiskait/pred_market.git" "$REPO_DIR"
  else
    git clone "$REPO_URL" "$REPO_DIR"
  fi
fi
unset GITHUB_TOKEN

# ── Docker image ──────────────────────────────────────────────────────────────
echo "[setup] Building Docker image..."
$DOCKER build -f "$REPO_DIR/services/Dockerfile" -t "$IMAGE" "$REPO_DIR"
echo "[setup] Image built: $IMAGE"

# ── Credentials ───────────────────────────────────────────────────────────────
mkdir -p "$CREDS_DIR"
chmod 700 "$CREDS_DIR"

# Kalshi private key: use KALSHI_PRIVATE_KEY_FILE or default ~/.kalshi/kalshi_api_key.txt
KALSHI_KEY_SRC="${KALSHI_PRIVATE_KEY_FILE:-$CREDS_DIR/kalshi_api_key.txt}"
KALSHI_KEY_DST="$CREDS_DIR/kalshi_api_key.txt"
if [[ ! -f "$KALSHI_KEY_SRC" ]]; then
  echo "ERROR: Kalshi private key not found at $KALSHI_KEY_SRC"
  echo "       Copy it first: scp ~/.kalshi/kalshi_api_key.txt ubuntu@<IP>:~/.kalshi/"
  exit 1
fi
if [[ "$KALSHI_KEY_SRC" != "$KALSHI_KEY_DST" ]]; then
  cp "$KALSHI_KEY_SRC" "$KALSHI_KEY_DST"
  chmod 600 "$KALSHI_KEY_DST"
fi

if [[ -f "$KEY_ID_FILE" ]] && [[ -f "$SYNOPTIC_FILE" ]]; then
  echo "[setup] Credentials already exist in $CREDS_DIR — skipping."
  echo "        Delete kalshi_api_key_id and synoptic_token to reset."
else
  echo "[setup] Configuring credentials (files in $CREDS_DIR)..."

  if [[ -z "${KALSHI_API_KEY_ID:-}" ]]; then
    read -rp "  KALSHI_API_KEY_ID: " KALSHI_API_KEY_ID
  fi
  printf '%s' "$KALSHI_API_KEY_ID" > "$KEY_ID_FILE"
  chmod 600 "$KEY_ID_FILE"

  if [[ -z "${SYNOPTIC_API_TOKEN:-}" ]]; then
    read -rp "  SYNOPTIC_API_TOKEN: " SYNOPTIC_API_TOKEN
  fi
  printf '%s' "$SYNOPTIC_API_TOKEN" > "$SYNOPTIC_FILE"
  chmod 600 "$SYNOPTIC_FILE"

  echo "[setup] Credentials saved to $CREDS_DIR"
fi

# ── Data directory ────────────────────────────────────────────────────────────
mkdir -p "$DATA_DIR"

# ── Daily restarts (optional; superseded by in-process periodic re-discovery) ─
# Services now re-discover event tickers every rediscover_interval_seconds (default 5 min)
# via config event_rollover. Cron is no longer required. To keep legacy cron:
#   SKIP_CRON=0 ./first_time_vm_setup.sh
if [[ "${SKIP_CRON:-1}" != "0" ]]; then
  echo "[setup] Skipping daily restart crons (event_rollover.rediscover_interval_seconds handles rollover)."
  ( crontab -l 2>/dev/null | grep -vF "${START_ALL_SCRIPT} start" ) | crontab - 2>/dev/null || true
else
  CRON_CMD_NY="1 0 * * * ${START_ALL_SCRIPT} start >> ${DATA_DIR}/daily-restart.log 2>&1"
  CRON_CMD_CHI="1 1 * * * ${START_ALL_SCRIPT} start >> ${DATA_DIR}/daily-restart.log 2>&1"
  ( crontab -l 2>/dev/null | grep -vF "0 2 * * * ${START_ALL_SCRIPT} start" ) | crontab -
  if crontab -l 2>/dev/null | grep -qF "1 0 * * * ${START_ALL_SCRIPT} start"; then
    echo "[setup] Daily restart crons already installed — skipping."
  else
    ( crontab -l 2>/dev/null; echo "$CRON_CMD_NY"; echo "$CRON_CMD_CHI" ) | crontab -
    echo "[setup] Installed daily restart crons (12:01 AM and 1:01 AM America/New_York)."
  fi
fi

echo ""
echo "[setup] Done. Run Kalshi listener, Synoptic listener, and weather bot with:"
echo "  cd $OCI_ROOT/manage_services && ./start_stop_all_services.sh"
echo "  Or individually: ./start_stop_kalshi_listener.sh ./start_stop_synoptic_listener.sh ./start_stop_weather_bot.sh"
