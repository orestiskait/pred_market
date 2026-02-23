#!/bin/bash
# Start Kalshi listener, Synoptic listener, NWP listener, and weather bot containers.
# Assumes setup_collector/first_time_vm_setup.sh has already been run.
#
# Config: services/config.yaml oci.synoptic_enabled — if false, Synoptic listener is skipped.
#
# Usage:
#   ./start_stop_all_services.sh          # start (or restart) all services
#   ./start_stop_all_services.sh stop     # stop all
#   ./start_stop_all_services.sh logs     # tail logs
#   ./start_stop_all_services.sh status   # show container status
#
# To run individual services:
#   ./start_stop_kalshi_listener.sh start
#   ./start_stop_synoptic_listener.sh start
#   ./start_stop_nwp_listener.sh start
#   ./start_stop_weather_bot.sh start
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CONFIG="${REPO_DIR}/services/config.yaml"

# Read oci.synoptic_enabled from config (default: true)
SYNOPTIC_ENABLED=true
if [[ -f "$CONFIG" ]]; then
  val=$(python3 -c "
import yaml
try:
    with open('$CONFIG') as f:
        cfg = yaml.safe_load(f)
    print(str(cfg.get('oci', {}).get('synoptic_enabled', True)).lower())
except Exception:
    print('true')
" 2>/dev/null || echo "true")
  [[ "$val" == "false" ]] && SYNOPTIC_ENABLED=false
fi

cmd="${1:-start}"

case "$cmd" in
  stop)
    "$SCRIPT_DIR/start_stop_kalshi_listener.sh" stop
    "$SCRIPT_DIR/start_stop_synoptic_listener.sh" stop
    "$SCRIPT_DIR/start_stop_nwp_listener.sh" stop
    "$SCRIPT_DIR/start_stop_weather_bot.sh" stop
    ;;

  logs)
    echo "Kalshi Listener logs:"
    "$SCRIPT_DIR/start_stop_kalshi_listener.sh" logs &
    [[ "$SYNOPTIC_ENABLED" == "true" ]] && { echo "Synoptic Listener logs:"; "$SCRIPT_DIR/start_stop_synoptic_listener.sh" logs & }
    echo "NWP Listener logs:"
    "$SCRIPT_DIR/start_stop_nwp_listener.sh" logs &
    echo "Weather Bot logs:"
    "$SCRIPT_DIR/start_stop_weather_bot.sh" logs &
    wait
    ;;

  status)
    "$SCRIPT_DIR/start_stop_kalshi_listener.sh" status
    [[ "$SYNOPTIC_ENABLED" == "true" ]] && "$SCRIPT_DIR/start_stop_synoptic_listener.sh" status
    "$SCRIPT_DIR/start_stop_nwp_listener.sh" status
    "$SCRIPT_DIR/start_stop_weather_bot.sh" status
    ;;

  start)
    "$SCRIPT_DIR/start_stop_kalshi_listener.sh" start
    echo "---"
    if [[ "$SYNOPTIC_ENABLED" == "true" ]]; then
      "$SCRIPT_DIR/start_stop_synoptic_listener.sh" start
    else
      echo "[start_all] Synoptic listener disabled (oci.synoptic_enabled: false)"
      "$SCRIPT_DIR/start_stop_synoptic_listener.sh" stop 2>/dev/null || true
    fi
    echo "---"
    "$SCRIPT_DIR/start_stop_nwp_listener.sh" start
    echo "---"
    "$SCRIPT_DIR/start_stop_weather_bot.sh" start
    echo ""
    echo "  Tail logs:  ./start_stop_all_services.sh logs"
    echo "  Stop:       ./start_stop_all_services.sh stop"
    ;;

  *)
    echo "Usage: $0 [start|stop|logs|status]"
    exit 1
    ;;
esac
