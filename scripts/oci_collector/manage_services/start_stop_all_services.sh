#!/bin/bash
# Start / stop / logs / status for both containers.
# Assumes setup_collector/first_time_vm_setup.sh has already been run.
#
# Config: services/config.yaml
#   oci.kalshi_listener — if false, kalshi-listener container is skipped
#   oci.weather_bot     — if false, weather-bot container is skipped
#
# Usage:
#   ./start_stop_all_services.sh          # start (or restart) all containers
#   ./start_stop_all_services.sh stop     # stop all
#   ./start_stop_all_services.sh logs     # tail logs
#   ./start_stop_all_services.sh status   # show container status
#
# To run individual containers:
#   ./start_stop_kalshi_listener.sh start
#   ./start_stop_weather_bot.sh start
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CONFIG="${REPO_DIR}/services/config.yaml"

KALSHI_LISTENER=true
WEATHER_BOT=true
if [[ -f "$CONFIG" ]]; then
  eval "$(python3 -c "
import yaml
try:
    with open('$CONFIG') as f:
        cfg = yaml.safe_load(f)
    oci = cfg.get('oci', {})
    print('KALSHI_LISTENER=' + str(oci.get('kalshi_listener', True)).lower())
    print('WEATHER_BOT=' + str(oci.get('weather_bot', True)).lower())
except Exception:
    print('KALSHI_LISTENER=true')
    print('WEATHER_BOT=true')
" 2>/dev/null || echo "KALSHI_LISTENER=true; WEATHER_BOT=true")"
fi

cmd="${1:-start}"

case "$cmd" in
  stop)
    "$SCRIPT_DIR/start_stop_kalshi_listener.sh" stop
    "$SCRIPT_DIR/start_stop_weather_bot.sh" stop
    ;;

  logs)
    echo "Kalshi Listener logs:"
    "$SCRIPT_DIR/start_stop_kalshi_listener.sh" logs &
    echo "Weather Bot logs:"
    "$SCRIPT_DIR/start_stop_weather_bot.sh" logs &
    wait
    ;;

  status)
    "$SCRIPT_DIR/start_stop_kalshi_listener.sh" status
    "$SCRIPT_DIR/start_stop_weather_bot.sh" status
    ;;

  start)
    if [[ "$KALSHI_LISTENER" == "true" ]]; then
      "$SCRIPT_DIR/start_stop_kalshi_listener.sh" start
    else
      echo "[start_all] Kalshi listener disabled (oci.kalshi_listener: false)"
      "$SCRIPT_DIR/start_stop_kalshi_listener.sh" stop 2>/dev/null || true
    fi
    echo "---"
    if [[ "$WEATHER_BOT" == "true" ]]; then
      "$SCRIPT_DIR/start_stop_weather_bot.sh" start
    else
      echo "[start_all] Weather bot disabled (oci.weather_bot: false)"
      "$SCRIPT_DIR/start_stop_weather_bot.sh" stop 2>/dev/null || true
    fi
    echo ""
    echo "  Tail logs:  ./start_stop_all_services.sh logs"
    echo "  Stop:       ./start_stop_all_services.sh stop"
    ;;

  *)
    echo "Usage: $0 [start|stop|logs|status]"
    exit 1
    ;;
esac
