#!/bin/bash
# =============================================================================
# LDM Setup — run on the OCI VM after setup.sh
# =============================================================================
# Sets up the Unidata LDM container for real-time surface observation ingest.
#
# Prerequisites:
#   - setup.sh has been run (Docker is installed, repo is cloned)
#   - Port 388 is open in the OCI security list (for LDM upstream connections)
#   - You have emailed support-idd@unidata.ucar.edu with your VM's hostname/IP
#     to request an ALLOW entry for IDS|DDPLUS data
#
# Usage:
#   ./setup_ldm.sh                                         # interactive
#   LDM_HOSTNAME=my.host.com ./setup_ldm.sh               # non-interactive
# =============================================================================
set -euo pipefail

REPO_DIR="/home/ubuntu/pred_market"
COLLECTOR_DIR="$REPO_DIR/pred_market_src/collector"
LDM_CONF_DIR="$COLLECTOR_DIR/ldm"
DATA_DIR="/home/ubuntu/collector-data"

DOCKER="docker"
$DOCKER info &>/dev/null 2>&1 || DOCKER="sudo docker"

echo "============================================"
echo " LDM Weather Ingest — Setup"
echo "============================================"
echo ""

# ── Hostname configuration ────────────────────────────────────────────────
# The LDM requires a hostname that matches what the upstream sees.
# For OCI VMs, this is typically the public IP or a DNS name pointing to it.
if [[ -z "${LDM_HOSTNAME:-}" ]]; then
  # Try to auto-detect the public IP
  PUBLIC_IP=$(curl -s http://checkip.amazonaws.com 2>/dev/null || echo "")
  if [[ -n "$PUBLIC_IP" ]]; then
    echo "[ldm-setup] Detected public IP: $PUBLIC_IP"
    read -rp "  Use this as LDM hostname? [Y/n]: " USE_IP
    if [[ "${USE_IP,,}" != "n" ]]; then
      LDM_HOSTNAME="$PUBLIC_IP"
    fi
  fi

  if [[ -z "${LDM_HOSTNAME:-}" ]]; then
    read -rp "  LDM hostname (FQDN or IP visible to upstream): " LDM_HOSTNAME
  fi
fi

echo "[ldm-setup] LDM hostname: $LDM_HOSTNAME"

# ── Update registry.xml with hostname ────────────────────────────────────
echo "[ldm-setup] Updating registry.xml with hostname..."
sed -i "s|<hostname>.*</hostname>|<hostname>${LDM_HOSTNAME}</hostname>|" \
  "$LDM_CONF_DIR/registry.xml"

# ── Update pqact.conf Python paths ───────────────────────────────────────
# The Dockerfile puts Python at /app/venv/bin/python and weather code at /app/weather/
echo "[ldm-setup] pqact.conf Python paths are configured for Docker."

# ── Create data directories ──────────────────────────────────────────────
echo "[ldm-setup] Creating data directories..."
mkdir -p "$DATA_DIR/weather_obs/ldm_surface"
mkdir -p "$DATA_DIR/ldm_logs"
mkdir -p "$DATA_DIR/ldm_raw"

# ── OCI Security list reminder ───────────────────────────────────────────
echo ""
echo "┌────────────────────────────────────────────────────────────────┐"
echo "│  IMPORTANT: OCI Networking                                      │"
echo "│                                                                  │"
echo "│  The LDM requires port 388 (TCP) to be open for inbound and    │"
echo "│  outbound traffic. Add an ingress rule to your subnet's         │"
echo "│  security list:                                                  │"
echo "│                                                                  │"
echo "│    Source: 0.0.0.0/0   Protocol: TCP   Port: 388               │"
echo "│                                                                  │"
echo "│  Also ensure outbound TCP 388 is allowed (default is all).     │"
echo "└────────────────────────────────────────────────────────────────┘"
echo ""

# ── Upstream data feed reminder ──────────────────────────────────────────
echo "┌────────────────────────────────────────────────────────────────┐"
echo "│  IMPORTANT: Upstream Data Access                                │"
echo "│                                                                  │"
echo "│  Before the LDM will receive data, you must email:             │"
echo "│                                                                  │"
echo "│    support-idd@unidata.ucar.edu                                │"
echo "│                                                                  │"
echo "│  Include:                                                        │"
echo "│    - Hostname/IP: $LDM_HOSTNAME"
echo "│    - Your name and email                                         │"
echo "│    - Feedtypes: IDS|DDPLUS                                      │"
echo "│    - Purpose: Surface obs for research/trading                  │"
echo "│                                                                  │"
echo "│  Ask them to add an ALLOW entry for your LDM.                  │"
echo "└────────────────────────────────────────────────────────────────┘"
echo ""

# ── Build LDM Docker image ──────────────────────────────────────────────
echo "[ldm-setup] Building LDM Docker image..."
$DOCKER build -t pred-market-ldm:latest -f "$LDM_CONF_DIR/Dockerfile" "$COLLECTOR_DIR/"
echo "[ldm-setup] Image built: pred-market-ldm:latest"

echo ""
echo "[ldm-setup] Done. Start the LDM with:"
echo "  cd $COLLECTOR_DIR && docker compose up -d ldm"
echo ""
echo "  Or start both services:"
echo "  cd $COLLECTOR_DIR && docker compose up -d"
echo ""
echo "  Check logs:"
echo "  docker logs -f ldm-weather"
