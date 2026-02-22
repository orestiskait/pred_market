#!/bin/bash
# Synoptic listener: start / stop / logs / status for the Synoptic weather data container.
#
# Usage:
#   ./run_synoptic_listener.sh start   # start Synoptic listener
#   ./run_synoptic_listener.sh stop    # stop Synoptic listener
#   ./run_synoptic_listener.sh logs    # tail Synoptic listener logs
#   ./run_synoptic_listener.sh status  # show container status
set -euo pipefail

ENV_FILE="/home/ubuntu/.kalshi/collector.env"
DATA_DIR="/home/ubuntu/collector-data"
IMAGE="kalshi-collector:latest"
CONTAINER="synoptic-listener"

DOCKER="docker"
$DOCKER info &>/dev/null 2>&1 || DOCKER="sudo docker"

cmd="${1:-start}"

case "$cmd" in
  stop)
    echo "[run_synoptic_listener] Stopping Synoptic listener..."
    $DOCKER stop "$CONTAINER" 2>/dev/null || echo "(synoptic-listener not running)"
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

    $DOCKER rm -f "$CONTAINER" 2>/dev/null || true

    echo "[run_synoptic_listener] Starting Synoptic listener..."
    $DOCKER run -d \
      --name "$CONTAINER" \
      --env-file "$ENV_FILE" \
      -v "$DATA_DIR:/app/data" \
      --restart unless-stopped \
      "$IMAGE" python -m pred_market_src.collector.synoptic.listener

    echo "[run_synoptic_listener] Synoptic listener running."
    sleep 2
    $DOCKER logs "$CONTAINER" --tail 10
    ;;

  *)
    echo "Usage: $0 [start|stop|logs|status]"
    exit 1
    ;;
esac
