#!/bin/bash
# Launch OCI compute instance for Kalshi collector (first-time setup)
#
# Prerequisites:
#   - OCI CLI installed and configured: oci setup config
#   - Fill in COMPARTMENT_ID, SUBNET_ID, AD below (or discover via commands below)
#
# To discover IDs (run these and copy the values):
#   oci iam compartment list --all
#   oci network subnet list -c <COMPARTMENT_ID>
#   oci iam availability-domain list -c <COMPARTMENT_ID>
#   oci compute image list -c <COMPARTMENT_ID> --operating-system "Canonical Ubuntu" --operating-system-version "22.04" --limit 1
#
# Usage:
#   ./launch.sh                    # uses env vars or prompts
#   COMPARTMENT_ID=xxx AD=yyy SUBNET_ID=zzz ./launch.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CLOUD_INIT="${SCRIPT_DIR}/cloud-init.yaml"

# --- Required: set these or pass as env vars ---
COMPARTMENT_ID="${COMPARTMENT_ID:-}"
SUBNET_ID="${SUBNET_ID:-}"
AD="${AD:-}"  # Availability domain, e.g. "xxx:PHX-AD-1"

# Optional
DISPLAY_NAME="${DISPLAY_NAME:-kalshi-collector}"
SHAPE="${SHAPE:-VM.Standard.E2.1.Micro}"   # Free tier shape
ASSIGN_PUBLIC_IP="${ASSIGN_PUBLIC_IP:-true}"
IMAGE_ID="${IMAGE_ID:-}"  # Auto-detected if empty
SSH_KEY_FILE="${SSH_KEY_FILE:-}"  # Default: ~/.ssh/id_rsa.pub or ~/.ssh/id_ed25519.pub

# Credentials for private repo + Kalshi (all setup happens on VM via metadata)
GITHUB_TOKEN="${GITHUB_TOKEN:-}"           # Required: GitHub PAT for private repo
KALSHI_API_KEY_ID="${KALSHI_API_KEY_ID:-}" # Optional: for auto-start
KALSHI_PRIVATE_KEY_FILE="${KALSHI_PRIVATE_KEY_FILE:-}"  # Optional: path to PEM file

# Load Kalshi creds from collector .env/config if not set
ENV_FILE="${SCRIPT_DIR}/../../pred_market_src/collector/.env"
CONFIG_FILE="${SCRIPT_DIR}/../../pred_market_src/collector/config.yaml"
if [[ -z "$KALSHI_API_KEY_ID" && -f "$ENV_FILE" ]]; then
  KALSHI_API_KEY_ID=$(grep -E "^KALSHI_API_KEY_ID=" "$ENV_FILE" 2>/dev/null | cut -d= -f2- | tr -d '"'\'')
fi
if [[ -z "$KALSHI_PRIVATE_KEY_FILE" ]]; then
  if [[ -f "$ENV_FILE" ]]; then
    KALSHI_PRIVATE_KEY_FILE=$(grep -E "^KALSHI_PRIVATE_KEY_PATH=" "$ENV_FILE" 2>/dev/null | cut -d= -f2- | tr -d '"'\'' | xargs)
  fi
  if [[ -z "$KALSHI_PRIVATE_KEY_FILE" && -f "$CONFIG_FILE" ]]; then
    KALSHI_PRIVATE_KEY_FILE=$(grep "private_key_path:" "$CONFIG_FILE" 2>/dev/null | sed -n 's/.*"\([^"]*\)".*/\1/p' | head -1)
  fi
fi

# --- Helpers ---
die() { echo "Error: $1" >&2; exit 1; }

discover_compartment() {
  # Uses tenancy (root) from ~/.oci/config if -c not set
  oci iam compartment list --compartment-id-in-subtree true --all --query 'data[0].id' --raw-output 2>/dev/null | tr -d '"' || \
  oci iam compartment list --query 'data[0].id' --raw-output 2>/dev/null | tr -d '"'
}

discover_ad() {
  local cid="${1:-$COMPARTMENT_ID}"
  oci iam availability-domain list -c "$cid" --query 'data[0].name' --raw-output 2>/dev/null | tr -d '"'
}

discover_subnet() {
  local cid="${1:-$COMPARTMENT_ID}"
  oci network subnet list -c "$cid" --query 'data[0].id' --raw-output 2>/dev/null | tr -d '"'
}

discover_image() {
  local cid="${1:-$COMPARTMENT_ID}"
  # Try shape-specific image first; fallback to any Ubuntu 22.04
  oci compute image list -c "$cid" \
    --operating-system "Canonical Ubuntu" \
    --operating-system-version "22.04" \
    --shape "${SHAPE}" \
    --limit 1 \
    --query 'data[0].id' --raw-output 2>/dev/null | tr -d '"' || \
  oci compute image list -c "$cid" \
    --operating-system "Canonical Ubuntu" \
    --operating-system-version "22.04" \
    --limit 1 \
    --query 'data[0].id' --raw-output 2>/dev/null | tr -d '"'
}

# --- Resolve IDs ---
[[ -z "$COMPARTMENT_ID" ]] && COMPARTMENT_ID=$(discover_compartment) || true
[[ -z "$AD" ]] && AD=$(discover_ad) || true
[[ -z "$SUBNET_ID" ]] && SUBNET_ID=$(discover_subnet) || true
[[ -z "$IMAGE_ID" ]] && IMAGE_ID=$(discover_image) || true

# Validate
[[ -z "$COMPARTMENT_ID" ]] && die "COMPARTMENT_ID required. Run: oci iam compartment list --all"
[[ -z "$AD" ]] && die "AD (availability domain) required. Run: oci iam availability-domain list -c <COMPARTMENT_ID>"
[[ -z "$SUBNET_ID" ]] && die "SUBNET_ID required. Run: oci network subnet list -c <COMPARTMENT_ID>"
[[ -z "$IMAGE_ID" ]] && die "IMAGE_ID required. Try a different SHAPE or: oci compute image list -c <COMPARTMENT_ID> --operating-system \"Canonical Ubuntu\""
[[ ! -f "$CLOUD_INIT" ]] && die "cloud-init.yaml not found at $CLOUD_INIT"

# SSH key for instance access (required for first login)
if [[ -z "$SSH_KEY_FILE" ]]; then
  for f in ~/.ssh/id_ed25519.pub ~/.ssh/id_rsa.pub; do
    [[ -f "$f" ]] && SSH_KEY_FILE="$f" && break
  done
fi
[[ -z "$SSH_KEY_FILE" || ! -f "$SSH_KEY_FILE" ]] && die "SSH public key not found. Create one with ssh-keygen or set SSH_KEY_FILE"
[[ -z "$GITHUB_TOKEN" ]] && die "GITHUB_TOKEN required for private repo. Create a PAT at: https://github.com/settings/tokens"
SSH_KEY=$(cat "$SSH_KEY_FILE")

# Encode Kalshi key if provided
KALSHI_PRIVATE_KEY_B64=""
if [[ -n "$KALSHI_API_KEY_ID" && -n "$KALSHI_PRIVATE_KEY_FILE" && -f "$KALSHI_PRIVATE_KEY_FILE" ]]; then
  KALSHI_PRIVATE_KEY_B64=$(base64 -w0 "$KALSHI_PRIVATE_KEY_FILE" 2>/dev/null || base64 "$KALSHI_PRIVATE_KEY_FILE" | tr -d '\n')
fi

echo "Launching instance:"
echo "  compartment: $COMPARTMENT_ID"
echo "  availability-domain: $AD"
echo "  subnet: $SUBNET_ID"
echo "  shape: $SHAPE"
echo "  image: $IMAGE_ID"
echo "  cloud-init: $CLOUD_INIT"
echo ""

# Build metadata JSON (escape for JSON)
escape_json() { printf '%s' "$1" | python3 -c "import json,sys; print(json.dumps(sys.stdin.read()))"; }
METADATA_OBJ="\"ssh_authorized_keys\": $(escape_json "$SSH_KEY")"
METADATA_OBJ="$METADATA_OBJ, \"GITHUB_TOKEN\": $(escape_json "$GITHUB_TOKEN")"
[[ -n "$KALSHI_API_KEY_ID" ]] && METADATA_OBJ="$METADATA_OBJ, \"KALSHI_API_KEY_ID\": $(escape_json "$KALSHI_API_KEY_ID")"
[[ -n "$KALSHI_PRIVATE_KEY_B64" ]] && METADATA_OBJ="$METADATA_OBJ, \"KALSHI_PRIVATE_KEY_B64\": $(escape_json "$KALSHI_PRIVATE_KEY_B64")"
METADATA_JSON="{${METADATA_OBJ}}"

oci compute instance launch \
  --compartment-id "$COMPARTMENT_ID" \
  --availability-domain "$AD" \
  --display-name "$DISPLAY_NAME" \
  --shape "$SHAPE" \
  --subnet-id "$SUBNET_ID" \
  --image-id "$IMAGE_ID" \
  --assign-public-ip "$ASSIGN_PUBLIC_IP" \
  --user-data-file "$CLOUD_INIT" \
  --metadata "$METADATA_JSON"

echo ""
echo "Instance launched. Cloud-init will clone repo, install deps, and start the collector."
echo "Wait ~3–4 min, then:"
echo "  Get public IP: oci compute instance list -c $COMPARTMENT_ID --display-name $DISPLAY_NAME --query 'data[0].id' -r"
echo "  oci compute instance list-vnics --instance-id <INSTANCE_ID>"
echo "  SSH: ssh ubuntu@<PUBLIC_IP>"
echo "  Check: sudo systemctl status kalshi-collector"
echo "  Data: /home/ubuntu/pred_market/pred_market_src/collector/data/"
