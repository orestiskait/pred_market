#!/bin/sh
# Entrypoint for Kalshi listener, Synoptic listener, weather bot.
#
# Credentials are read from files. Mount ~/.kalshi to /app/credentials and set
# CREDENTIALS_DIR=/app/credentials (done by manage_services scripts).
#
# Optional: KALSHI_LISTENER_CONFIG or SERVICES_CONFIG for config path.
set -e

CONFIG="${KALSHI_LISTENER_CONFIG:-${SERVICES_CONFIG:-/app/services/config.yaml}}"

if [ $# -gt 0 ]; then
  exec "$@"
else
  exec python -m services.kalshi.listener --config "$CONFIG"
fi
