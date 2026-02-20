#!/bin/sh
# Decode Kalshi private key from base64 env var and start collector.
# Required env vars:
#   KALSHI_API_KEY_ID        - Kalshi API key ID
#   KALSHI_PRIVATE_KEY_B64   - base64-encoded PEM private key
# Optional:
#   COLLECTOR_CONFIG         - path to config.yaml (default: /app/config.yaml)
set -e

KEY_PATH="/tmp/kalshi_key.pem"
CONFIG="${COLLECTOR_CONFIG:-/app/config.yaml}"

if [ -z "$KALSHI_API_KEY_ID" ] || [ -z "$KALSHI_PRIVATE_KEY_B64" ]; then
  echo "ERROR: KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_B64 must be set" >&2
  exit 1
fi

printf '%s' "$KALSHI_PRIVATE_KEY_B64" | base64 -d > "$KEY_PATH"
chmod 600 "$KEY_PATH"

export KALSHI_PRIVATE_KEY_PATH="$KEY_PATH"

unset KALSHI_PRIVATE_KEY_B64

if [ $# -gt 0 ]; then
  exec "$@"
else
  exec python /app/collector.py --config "$CONFIG"
fi
