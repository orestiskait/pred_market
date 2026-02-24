#!/bin/bash
# Start Kalshi listener, Synoptic listener, Wethr listener, NWP listener, and weather bot containers.
# Assumes setup_collector/first_time_vm_setup.sh has already been run.
#
# Config: services/config.yaml
#   oci.synoptic_container — if false, synoptic-listener container is skipped.
#   oci.wethr_container    — if false, wethr-listener container is skipped.
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
#   ./start_stop_wethr_listener.sh start
#   ./start_stop_nwp_listener.sh start
#   ./start_stop_weather_bot.sh start
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
CONFIG="${REPO_DIR}/services/config.yaml"

# Read oci toggles from config (default: true)
SYNOPTIC_CONTAINER=true
WETHR_CONTAINER=true
if [[ -f "$CONFIG" ]]; then
  eval "$(python3 -c "
import yaml
try:
    with open('$CONFIG') as f:
        cfg = yaml.safe_load(f)
    oci = cfg.get('oci', {})
    print('SYNOPTIC_CONTAINER=' + str(oci.get('synoptic_container', True)).lower())
    print('WETHR_CONTAINER=' + str(oci.get('wethr_container', True)).lower())
except Exception:
    print('SYNOPTIC_CONTAINER=true')
    print('WETHR_CONTAINER=true')
" 2>/dev/null || echo "SYNOPTIC_CONTAINER=true; WETHR_CONTAINER=true")"
fi

cmd="${1:-start}"

case "$cmd" in
  stop)
    "$SCRIPT_DIR/start_stop_kalshi_listener.sh" stop
    "$SCRIPT_DIR/start_stop_synoptic_listener.sh" stop
    "$SCRIPT_DIR/start_stop_wethr_listener.sh" stop
    "$SCRIPT_DIR/start_stop_nwp_listener.sh" stop
    "$SCRIPT_DIR/start_stop_weather_bot.sh" stop
    ;;

  logs)
    echo "Kalshi Listener logs:"
    "$SCRIPT_DIR/start_stop_kalshi_listener.sh" logs &
    [[ "$SYNOPTIC_CONTAINER" == "true" ]] && { echo "Synoptic Listener logs:"; "$SCRIPT_DIR/start_stop_synoptic_listener.sh" logs & }
    [[ "$WETHR_CONTAINER" == "true" ]] && { echo "Wethr Listener logs:"; "$SCRIPT_DIR/start_stop_wethr_listener.sh" logs & }
    echo "NWP Listener logs:"
    "$SCRIPT_DIR/start_stop_nwp_listener.sh" logs &
    echo "Weather Bot logs:"
    "$SCRIPT_DIR/start_stop_weather_bot.sh" logs &
    wait
    ;;

  status)
    "$SCRIPT_DIR/start_stop_kalshi_listener.sh" status
    [[ "$SYNOPTIC_CONTAINER" == "true" ]] && "$SCRIPT_DIR/start_stop_synoptic_listener.sh" status
    [[ "$WETHR_CONTAINER" == "true" ]] && "$SCRIPT_DIR/start_stop_wethr_listener.sh" status
    "$SCRIPT_DIR/start_stop_nwp_listener.sh" status
    "$SCRIPT_DIR/start_stop_weather_bot.sh" status
    ;;

  start)
    "$SCRIPT_DIR/start_stop_kalshi_listener.sh" start
    echo "---"
    if [[ "$SYNOPTIC_CONTAINER" == "true" ]]; then
      "$SCRIPT_DIR/start_stop_synoptic_listener.sh" start
    else
      echo "[start_all] Synoptic listener container disabled (oci.synoptic_container: false)"
      "$SCRIPT_DIR/start_stop_synoptic_listener.sh" stop 2>/dev/null || true
    fi
    echo "---"
    if [[ "$WETHR_CONTAINER" == "true" ]]; then
      "$SCRIPT_DIR/start_stop_wethr_listener.sh" start
    else
      echo "[start_all] Wethr listener container disabled (oci.wethr_container: false)"
      "$SCRIPT_DIR/start_stop_wethr_listener.sh" stop 2>/dev/null || true
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
