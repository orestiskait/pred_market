#!/bin/bash
# Start the Kalshi collector container.
# Assumes setup.sh has already been run.
#
# Usage:
#   ./run_collector.sh          # start (or restart) the collector
#   ./run_collector.sh stop     # stop the collector
#   ./run_collector.sh logs     # tail collector logs
#   ./run_collector.sh status   # show container status
set -euo pipefail

ENV_FILE="/home/ubuntu/.kalshi/collector.env"
DATA_DIR="/home/ubuntu/collector-data"
IMAGE="kalshi-collector:latest"
CONTAINER="kalshi-collector"

DOCKER="docker"
$DOCKER info &>/dev/null 2>&1 || DOCKER="sudo docker"

cmd="${1:-start}"

case "$cmd" in
  stop)
    echo "[collector] Stopping..."
    $DOCKER stop "$CONTAINER" 2>/dev/null || echo "(not running)"
    ;;

  logs)
    $DOCKER logs -f "$CONTAINER"
    ;;

  status)
    $DOCKER ps -a --filter "name=$CONTAINER"
    ;;

  start)
    [[ ! -f "$ENV_FILE" ]] && \
      echo "ERROR: $ENV_FILE not found. Run setup.sh first." && exit 1

    # Remove any stopped container with the same name
    $DOCKER rm -f "$CONTAINER" 2>/dev/null || true

    echo "[collector] Starting $IMAGE..."
    $DOCKER run -d \
      --name "$CONTAINER" \
      --env-file "$ENV_FILE" \
      -v "$DATA_DIR:/app/data" \
      --restart unless-stopped \
      "$IMAGE"

    echo "[collector] Running. Logs:"
    sleep 2
    $DOCKER logs "$CONTAINER"
    echo ""
    echo "  Tail logs:  ./run_collector.sh logs"
    echo "  Stop:       ./run_collector.sh stop"
    echo "  Data:       $DATA_DIR"
    ;;

  *)
    echo "Usage: $0 [start|stop|logs|status]"
    exit 1
    ;;
esac
