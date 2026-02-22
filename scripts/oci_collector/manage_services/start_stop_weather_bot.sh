#!/bin/bash
# Weather bot: start / stop / logs / status for the weather arbitrage bot container.
#
# Usage:
#   ./start_stop_weather_bot.sh start   # start weather bot
#   ./start_stop_weather_bot.sh stop    # stop weather bot
#   ./start_stop_weather_bot.sh logs    # tail weather bot logs
#   ./start_stop_weather_bot.sh status  # show container status
set -euo pipefail

CREDS_DIR="${HOME:-/home/ubuntu}/.kalshi"
DATA_DIR="${HOME:-/home/ubuntu}/collector-data"
IMAGE="kalshi-services:latest"
CONTAINER="weather-bot"

DOCKER="docker"
$DOCKER info &>/dev/null 2>&1 || DOCKER="sudo docker"

cmd="${1:-start}"

case "$cmd" in
  stop)
    echo "[start_stop_weather_bot] Stopping weather bot..."
    $DOCKER stop "$CONTAINER" 2>/dev/null || echo "(weather-bot not running)"
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

    echo "[start_stop_weather_bot] Starting weather bot..."
    $DOCKER run -d \
      --name "$CONTAINER" \
      -e CREDENTIALS_DIR=/app/credentials \
      -v "$CREDS_DIR:/app/credentials:ro" \
      -v "$DATA_DIR:/app/data" \
      --restart unless-stopped \
      "$IMAGE" python -m services.bot.weather_bot

    echo "[start_stop_weather_bot] Weather bot running."
    sleep 2
    $DOCKER logs "$CONTAINER" --tail 10
    ;;

  *)
    echo "Usage: $0 [start|stop|logs|status]"
    exit 1
    ;;
esac
