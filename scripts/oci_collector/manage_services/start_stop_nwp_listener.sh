#!/bin/bash
# NWP SNS listener: start / stop / logs / status for the real-time model ingest container.
#
# Usage:
#   ./start_stop_nwp_listener.sh start   # start NWP listener
#   ./start_stop_nwp_listener.sh stop    # stop NWP listener
#   ./start_stop_nwp_listener.sh logs    # tail NWP listener logs
#   ./start_stop_nwp_listener.sh status  # show container status
set -euo pipefail

CREDS_DIR="${HOME:-/home/ubuntu}/.kalshi"
DATA_DIR="${HOME:-/home/ubuntu}/collector-data"
IMAGE="kalshi-services:latest"
CONTAINER="nwp-listener"

DOCKER="docker"
$DOCKER info &>/dev/null 2>&1 || DOCKER="sudo docker"

cmd="${1:-start}"

case "$cmd" in
  stop)
    echo "[start_stop_nwp_listener] Stopping NWP listener..."
    $DOCKER stop "$CONTAINER" 2>/dev/null || echo "(nwp-listener not running)"
    ;;

  logs)
    $DOCKER logs -f "$CONTAINER"
    ;;

  status)
    $DOCKER ps -a --filter "name=$CONTAINER"
    ;;

  start)
    [[ ! -d "$CREDS_DIR" ]] && \
      echo "ERROR: $CREDS_DIR not found. Run setup_collector/first_time_vm_setup.sh first." && exit 1

    $DOCKER rm -f "$CONTAINER" 2>/dev/null || true

    echo "[start_stop_nwp_listener] Starting NWP listener..."
    $DOCKER run -d \
      --name "$CONTAINER" \
      -e CREDENTIALS_DIR=/app/credentials \
      -v "$CREDS_DIR:/app/credentials:ro" \
      -v "$DATA_DIR:/app/data" \
      --restart unless-stopped \
      "$IMAGE" python -m services.weather.sns_listener

    echo "[start_stop_nwp_listener] NWP listener running."
    sleep 2
    $DOCKER logs "$CONTAINER" --tail 10
    ;;

  *)
    echo "Usage: $0 [start|stop|logs|status]"
    exit 1
    ;;
esac
