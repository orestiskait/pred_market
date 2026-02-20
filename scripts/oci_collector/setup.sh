#!/bin/bash
# Run on the OCI VM to set up or update the collector.
# Safe to re-run — pulls latest code and rebuilds the image each time.
#
# Usage:
#   ./setup.sh                          # interactive credential prompt on first run
#   GITHUB_TOKEN=xxx ./setup.sh         # private repo
#   KALSHI_API_KEY_ID=xxx \
#     KALSHI_PRIVATE_KEY_FILE=/path/to/key.pem \
#     ./setup.sh                        # non-interactive credential setup
set -euo pipefail

REPO_URL="https://github.com/orestiskait/pred_market.git"
REPO_DIR="/home/ubuntu/pred_market"
CREDS_DIR="/home/ubuntu/.kalshi"
ENV_FILE="$CREDS_DIR/collector.env"
DATA_DIR="/home/ubuntu/collector-data"
IMAGE="kalshi-collector:latest"
CONTAINER="kalshi-collector"

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
$DOCKER build -t "$IMAGE" "$REPO_DIR/pred_market_src/collector/"
echo "[setup] Image built: $IMAGE"

# ── Credentials ───────────────────────────────────────────────────────────────
mkdir -p "$CREDS_DIR"
chmod 700 "$CREDS_DIR"

if [[ -f "$ENV_FILE" ]]; then
  echo "[setup] Credentials already exist at $ENV_FILE — skipping."
  echo "        Delete the file and re-run to reset credentials."
else
  echo "[setup] Configuring credentials..."

  # Prefer env vars; fall back to interactive prompts
  if [[ -z "${KALSHI_API_KEY_ID:-}" ]]; then
    read -rp "  KALSHI_API_KEY_ID: " KALSHI_API_KEY_ID
  fi

  if [[ -z "${KALSHI_PRIVATE_KEY_FILE:-}" ]]; then
    read -rp "  Path to Kalshi private key PEM file: " KALSHI_PRIVATE_KEY_FILE
  fi

  if [[ -z "${SYNOPTIC_API_TOKEN:-}" ]]; then
    read -rp "  SYNOPTIC_API_TOKEN: " SYNOPTIC_API_TOKEN
  fi

  [[ ! -f "$KALSHI_PRIVATE_KEY_FILE" ]] && \
    echo "ERROR: key file not found: $KALSHI_PRIVATE_KEY_FILE" && exit 1

  KALSHI_PRIVATE_KEY_B64=$(base64 -w0 "$KALSHI_PRIVATE_KEY_FILE" 2>/dev/null \
    || base64 "$KALSHI_PRIVATE_KEY_FILE" | tr -d '\n')

  printf 'KALSHI_API_KEY_ID=%s\nKALSHI_PRIVATE_KEY_B64=%s\nSYNOPTIC_API_TOKEN=%s\n' \
    "$KALSHI_API_KEY_ID" "$KALSHI_PRIVATE_KEY_B64" "$SYNOPTIC_API_TOKEN" > "$ENV_FILE"
  chmod 600 "$ENV_FILE"
  echo "[setup] Credentials saved to $ENV_FILE"
  unset KALSHI_PRIVATE_KEY_B64
fi

# ── Data directory ────────────────────────────────────────────────────────────
mkdir -p "$DATA_DIR"

# ── Daily restarts (re-resolves event series for new NY/CHI dates) ──────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CRON_CMD_NY="1 0 * * * ${SCRIPT_DIR}/run_collector.sh start >> ${DATA_DIR}/daily-restart.log 2>&1"
CRON_CMD_CHI="1 1 * * * ${SCRIPT_DIR}/run_collector.sh start >> ${DATA_DIR}/daily-restart.log 2>&1"

# Remove the old 2 AM cron job if it exists
( crontab -l 2>/dev/null | grep -vF "0 2 * * * ${SCRIPT_DIR}/run_collector.sh start" ) | crontab -

if crontab -l 2>/dev/null | grep -qF "1 0 * * * ${SCRIPT_DIR}/run_collector.sh start"; then
  echo "[setup] Daily restart crons already installed — skipping."
else
  ( crontab -l 2>/dev/null; echo "$CRON_CMD_NY"; echo "$CRON_CMD_CHI" ) | crontab -
  echo "[setup] Installed daily restart crons (12:01 AM and 1:01 AM America/New_York)."
  echo "        Log: $DATA_DIR/daily-restart.log"
fi

echo ""
echo "[setup] Done. Run the collector with:"
echo "  ./run_collector.sh"
