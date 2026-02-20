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
SYN_CONTAINER="synoptic-listener"
BOT_CONTAINER="weather-bot"

DOCKER="docker"
$DOCKER info &>/dev/null 2>&1 || DOCKER="sudo docker"

cmd="${1:-start}"

case "$cmd" in
  stop)
    echo "[collector] Stopping..."
    $DOCKER stop "$CONTAINER" 2>/dev/null || echo "(kalshi-collector not running)"
    $DOCKER stop "$SYN_CONTAINER" 2>/dev/null || echo "(synoptic-listener not running)"
    $DOCKER stop "$BOT_CONTAINER" 2>/dev/null || echo "(weather-bot not running)"
    ;;

  logs)
    echo "Kalshi Collector logs:"
    $DOCKER logs -f "$CONTAINER" &
    echo "Synoptic Listener logs:"
    $DOCKER logs -f "$SYN_CONTAINER" &
    echo "Trading Bot logs:"
    $DOCKER logs -f "$BOT_CONTAINER" &
    wait
    ;;

  status)
    $DOCKER ps -a --filter "name=$CONTAINER" --filter "name=$SYN_CONTAINER" --filter "name=$BOT_CONTAINER"
    ;;

  start)
    [[ ! -f "$ENV_FILE" ]] && \
      echo "ERROR: $ENV_FILE not found. Run setup.sh first." && exit 1

    # Remove any stopped container with the same name
    $DOCKER rm -f "$CONTAINER" 2>/dev/null || true
    $DOCKER rm -f "$SYN_CONTAINER" 2>/dev/null || true
    $DOCKER rm -f "$BOT_CONTAINER" 2>/dev/null || true

    echo "[collector] Starting Kalshi websocket: $IMAGE..."
    $DOCKER run -d \
      --name "$CONTAINER" \
      --env-file "$ENV_FILE" \
      -v "$DATA_DIR:/app/data" \
      --restart unless-stopped \
      "$IMAGE"

    echo "[collector] Starting Synoptic listener: $IMAGE..."
    $DOCKER run -d \
      --name "$SYN_CONTAINER" \
      --env-file "$ENV_FILE" \
      -v "$DATA_DIR:/app/data" \
      --restart unless-stopped \
      "$IMAGE" python /app/synoptic_listener.py

    echo "[collector] Starting Trading Bot: $IMAGE..."
    $DOCKER run -d \
      --name "$BOT_CONTAINER" \
      --env-file "$ENV_FILE" \
      -v "$DATA_DIR:/app/data" \
      --restart unless-stopped \
      "$IMAGE" python /app/weather_bot.py

    echo "[collector] Running. Logs:"
    sleep 2
    $DOCKER logs "$CONTAINER" --tail 10
    echo "---"
    $DOCKER logs "$SYN_CONTAINER" --tail 10
    echo "---"
    $DOCKER logs "$BOT_CONTAINER" --tail 10
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
