#!/bin/bash
# Start Kalshi listener, Synoptic listener, and weather bot containers.
# Assumes setup_collector/first_time_vm_setup.sh has already been run.
#
# Usage:
#   ./start_stop_all_services.sh          # start (or restart) all three services
#   ./start_stop_all_services.sh stop     # stop Kalshi listener, Synoptic listener, and bot
#   ./start_stop_all_services.sh logs     # tail logs from all three
#   ./start_stop_all_services.sh status   # show container status
#
# To run individual services:
#   ./start_stop_kalshi_listener.sh start
#   ./start_stop_synoptic_listener.sh start
#   ./start_stop_weather_bot.sh start
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

cmd="${1:-start}"

case "$cmd" in
  stop)
    "$SCRIPT_DIR/start_stop_kalshi_listener.sh" stop
    "$SCRIPT_DIR/start_stop_synoptic_listener.sh" stop
    "$SCRIPT_DIR/start_stop_weather_bot.sh" stop
    ;;

  logs)
    echo "Kalshi Listener logs:"
    "$SCRIPT_DIR/start_stop_kalshi_listener.sh" logs &
    echo "Synoptic Listener logs:"
    "$SCRIPT_DIR/start_stop_synoptic_listener.sh" logs &
    echo "Weather Bot logs:"
    "$SCRIPT_DIR/start_stop_weather_bot.sh" logs &
    wait
    ;;

  status)
    "$SCRIPT_DIR/start_stop_kalshi_listener.sh" status
    "$SCRIPT_DIR/start_stop_synoptic_listener.sh" status
    "$SCRIPT_DIR/start_stop_weather_bot.sh" status
    ;;

  start)
    "$SCRIPT_DIR/start_stop_kalshi_listener.sh" start
    echo "---"
    "$SCRIPT_DIR/start_stop_synoptic_listener.sh" start
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
