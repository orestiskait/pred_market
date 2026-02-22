#!/bin/bash
# Backward-compatible wrapper: delegates to run_all.sh.
# Prefer: ./run_all.sh or ./run_kalshi_listener.sh ./run_synoptic_listener.sh ./run_weather_bot.sh
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "$SCRIPT_DIR/run_all.sh" "$@"
