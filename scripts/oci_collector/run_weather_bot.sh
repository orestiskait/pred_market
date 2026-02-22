#!/bin/bash
# Weather bot: start / stop / logs / status for the weather arbitrage bot container.
#
# Usage:
#   ./run_weather_bot.sh start   # start weather bot
#   ./run_weather_bot.sh stop    # stop weather bot
#   ./run_weather_bot.sh logs    # tail weather bot logs
#   ./run_weather_bot.sh status  # show container status
set -euo pipefail

ENV_FILE="/home/ubuntu/.kalshi/collector.env"
DATA_DIR="/home/ubuntu/collector-data"
IMAGE="kalshi-collector:latest"
CONTAINER="weather-bot"

DOCKER="docker"
$DOCKER info &>/dev/null 2>&1 || DOCKER="sudo docker"

cmd="${1:-start}"

case "$cmd" in
  stop)
    echo "[run_weather_bot] Stopping weather bot..."
    $DOCKER stop "$CONTAINER" 2>/dev/null || echo "(weather-bot not running)"
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

    echo "[run_weather_bot] Starting weather bot..."
    $DOCKER run -d \
      --name "$CONTAINER" \
      --env-file "$ENV_FILE" \
      -v "$DATA_DIR:/app/data" \
      --restart unless-stopped \
      "$IMAGE" python -m pred_market_src.collector.bot.weather_bot

    echo "[run_weather_bot] Weather bot running."
    sleep 2
    $DOCKER logs "$CONTAINER" --tail 10
    ;;

  *)
    echo "Usage: $0 [start|stop|logs|status]"
    exit 1
    ;;
esac
