#!/bin/bash
# Kalshi listener: start / stop / logs / status for the Kalshi market data container.
#
# Usage:
#   ./start_stop_kalshi_listener.sh start   # start Kalshi listener
#   ./start_stop_kalshi_listener.sh stop    # stop Kalshi listener
#   ./start_stop_kalshi_listener.sh logs    # tail Kalshi listener logs
#   ./start_stop_kalshi_listener.sh status  # show container status
set -euo pipefail

ENV_FILE="/home/ubuntu/.kalshi/collector.env"
DATA_DIR="/home/ubuntu/collector-data"
IMAGE="kalshi-collector:latest"
CONTAINER="kalshi-listener"

DOCKER="docker"
$DOCKER info &>/dev/null 2>&1 || DOCKER="sudo docker"

cmd="${1:-start}"

case "$cmd" in
  stop)
    echo "[start_stop_kalshi_listener] Stopping Kalshi listener..."
    $DOCKER stop "$CONTAINER" 2>/dev/null || echo "(kalshi-listener not running)"
    ;;

  logs)
    $DOCKER logs -f "$CONTAINER"
    ;;

  status)
    $DOCKER ps -a --filter "name=$CONTAINER"
    ;;

  start)
    [[ ! -f "$ENV_FILE" ]] && \
      echo "ERROR: $ENV_FILE not found. Run setup_collector/first_time_vm_setup.sh first." && exit 1

    $DOCKER rm -f "$CONTAINER" 2>/dev/null || true

    echo "[start_stop_kalshi_listener] Starting Kalshi listener..."
    $DOCKER run -d \
      --name "$CONTAINER" \
      --env-file "$ENV_FILE" \
      -v "$DATA_DIR:/app/data" \
      --restart unless-stopped \
      "$IMAGE"

    echo "[start_stop_kalshi_listener] Kalshi listener running."
    sleep 2
    $DOCKER logs "$CONTAINER" --tail 10
    ;;

  *)
    echo "Usage: $0 [start|stop|logs|status]"
    exit 1
    ;;
esac
