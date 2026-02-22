#!/bin/bash
# Start Kalshi listener, Synoptic listener, and weather bot containers.
# Assumes setup.sh has already been run.
#
# Usage:
#   ./run_all.sh          # start (or restart) all three services
#   ./run_all.sh stop     # stop Kalshi listener, Synoptic listener, and bot
#   ./run_all.sh logs     # tail logs from all three
#   ./run_all.sh status   # show container status
#
# To run individual services:
#   ./run_kalshi_listener.sh start
#   ./run_synoptic_listener.sh start
#   ./run_weather_bot.sh start
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

cmd="${1:-start}"

case "$cmd" in
  stop)
    "$SCRIPT_DIR/run_kalshi_listener.sh" stop
    "$SCRIPT_DIR/run_synoptic_listener.sh" stop
    "$SCRIPT_DIR/run_weather_bot.sh" stop
    ;;

  logs)
    echo "Kalshi Listener logs:"
    "$SCRIPT_DIR/run_kalshi_listener.sh" logs &
    echo "Synoptic Listener logs:"
    "$SCRIPT_DIR/run_synoptic_listener.sh" logs &
    echo "Weather Bot logs:"
    "$SCRIPT_DIR/run_weather_bot.sh" logs &
    wait
    ;;

  status)
    "$SCRIPT_DIR/run_kalshi_listener.sh" status
    "$SCRIPT_DIR/run_synoptic_listener.sh" status
    "$SCRIPT_DIR/run_weather_bot.sh" status
    ;;

  start)
    "$SCRIPT_DIR/run_kalshi_listener.sh" start
    echo "---"
    "$SCRIPT_DIR/run_synoptic_listener.sh" start
    echo "---"
    "$SCRIPT_DIR/run_weather_bot.sh" start
    echo ""
    echo "  Tail logs:  ./run_all.sh logs"
    echo "  Stop:       ./run_all.sh stop"
    ;;

  *)
    echo "Usage: $0 [start|stop|logs|status]"
    exit 1
    ;;
esac
