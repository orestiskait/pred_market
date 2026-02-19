#!/bin/bash
# Launch a Kalshi collector VM on OCI using the A2→A1 shape swap trick.
#
# A1.Flex capacity is almost always exhausted. The workaround:
#   1. Create an A2.Flex instance (usually available).
#   2. Stop the instance.
#   3. Update the shape to A1.Flex.
#   4. Start the instance.
#
# Specs: 4 OCPUs · 24 GB RAM · 150 GB boot volume · SSH enabled.
#
# After launch, SSH in and run setup.sh to install the collector.
#
# Env vars (auto-detected if not set):
#   COMPARTMENT_ID, AD, SUBNET_ID, IMAGE_ID
#
# Optional:
#   SSH_PUBLIC_KEY_FILE  — defaults to ~/.ssh/id_ed25519.pub or ~/.ssh/id_rsa.pub
#   DISPLAY_NAME         — instance name (default: kalshi-collector)
#
# Usage:
#   ./launch.sh
#   COMPARTMENT_ID=ocid1... AD=xxx:PHX-AD-1 SUBNET_ID=ocid1... ./launch.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DISPLAY_NAME="${DISPLAY_NAME:-kalshi-collector}"
BOOT_VOLUME_GB=150
TARGET_OCPUS=4
TARGET_MEMORY_GB=24

# ── SSH public key ────────────────────────────────────────────────────────────
if [[ -z "${SSH_PUBLIC_KEY_FILE:-}" ]]; then
  for candidate in ~/.ssh/id_ed25519.pub ~/.ssh/id_rsa.pub; do
    [[ -f "$candidate" ]] && SSH_PUBLIC_KEY_FILE="$candidate" && break
  done
fi
[[ -z "${SSH_PUBLIC_KEY_FILE:-}" ]] && echo "ERROR: No SSH public key found. Set SSH_PUBLIC_KEY_FILE." && exit 1
SSH_PUBLIC_KEY="$(cat "$SSH_PUBLIC_KEY_FILE")"
echo "[launch] SSH key: $SSH_PUBLIC_KEY_FILE"

# ── Auto-detect OCI IDs ──────────────────────────────────────────────────────
if [[ -z "${COMPARTMENT_ID:-}" ]]; then
  echo "[launch] Detecting compartment..."
  COMPARTMENT_ID=$(oci iam compartment list \
    --compartment-id-in-subtree true --all \
    --query 'data[0].id' --raw-output 2>/dev/null) || true
  # Fall back to tenancy OCID from ~/.oci/config (root compartment)
  [[ -z "$COMPARTMENT_ID" ]] && \
    COMPARTMENT_ID=$(grep -m1 '^tenancy=' ~/.oci/config 2>/dev/null | cut -d= -f2) || true
  [[ -z "$COMPARTMENT_ID" ]] && echo "ERROR: Set COMPARTMENT_ID." && exit 1
  echo "  → $COMPARTMENT_ID"
fi

if [[ -z "${AD:-}" ]]; then
  echo "[launch] Detecting availability domain..."
  AD=$(oci iam availability-domain list -c "$COMPARTMENT_ID" \
    --query 'data[0].name' --raw-output)
  [[ -z "$AD" ]] && echo "ERROR: Set AD." && exit 1
  echo "  → $AD"
fi

if [[ -z "${SUBNET_ID:-}" ]]; then
  echo "[launch] Detecting public subnet..."
  SUBNET_ID=$(oci network subnet list -c "$COMPARTMENT_ID" \
    --query 'data[?("prohibit-public-ip-on-vnic"==`false`)] | [0].id' \
    --raw-output 2>/dev/null)
  [[ -z "$SUBNET_ID" || "$SUBNET_ID" == "null" ]] && echo "ERROR: Set SUBNET_ID." && exit 1
  echo "  → $SUBNET_ID"
fi

if [[ -z "${IMAGE_ID:-}" ]]; then
  echo "[launch] Finding Ubuntu 22.04 aarch64 image..."
  IMAGE_ID=$(oci compute image list -c "$COMPARTMENT_ID" \
    --operating-system "Canonical Ubuntu" --operating-system-version "22.04" \
    --shape "VM.Standard.A2.Flex" \
    --sort-by TIMECREATED --sort-order DESC \
    --query 'data[0].id' --raw-output 2>/dev/null)
  if [[ -z "$IMAGE_ID" || "$IMAGE_ID" == "null" ]]; then
    IMAGE_ID=$(oci compute image list -c "$COMPARTMENT_ID" \
      --operating-system "Canonical Ubuntu" --operating-system-version "22.04" \
      --sort-by TIMECREATED --sort-order DESC \
      --query 'data[0].id' --raw-output)
  fi
  [[ -z "$IMAGE_ID" || "$IMAGE_ID" == "null" ]] && echo "ERROR: Could not find image." && exit 1
  echo "  → $IMAGE_ID"
fi

# ── Cloud-init user-data ─────────────────────────────────────────────────────
CLOUD_INIT="$SCRIPT_DIR/cloud-init.yaml"
[[ ! -f "$CLOUD_INIT" ]] && echo "ERROR: $CLOUD_INIT not found." && exit 1
USER_DATA=$(base64 -w0 "$CLOUD_INIT" 2>/dev/null || base64 "$CLOUD_INIT" | tr -d '\n')

# ── Step 1: Create as A2.Flex ────────────────────────────────────────────────
echo ""
echo "══ Step 1/4: Creating VM.Standard.A2.Flex ══"

INSTANCE_ID=$(oci compute instance launch \
  -c "$COMPARTMENT_ID" \
  --availability-domain "$AD" \
  --display-name "$DISPLAY_NAME" \
  --shape "VM.Standard.A2.Flex" \
  --shape-config "{\"ocpus\": $TARGET_OCPUS, \"memoryInGBs\": $TARGET_MEMORY_GB}" \
  --image-id "$IMAGE_ID" \
  --boot-volume-size-in-gbs "$BOOT_VOLUME_GB" \
  --subnet-id "$SUBNET_ID" \
  --assign-public-ip true \
  --metadata "{\"ssh_authorized_keys\": \"$SSH_PUBLIC_KEY\", \"user_data\": \"$USER_DATA\"}" \
  --query 'data.id' --raw-output)

echo "[launch] Instance: $INSTANCE_ID"
wait_for_state() {
  local target="$1"
  echo "[launch] Waiting for $target..."
  while true; do
    local state
    state=$(oci compute instance get --instance-id "$INSTANCE_ID" \
      --query 'data."lifecycle-state"' --raw-output)
    [[ "$state" == "$target" ]] && break
    sleep 10
  done
}
wait_for_state RUNNING

# ── Step 2: Stop ─────────────────────────────────────────────────────────────
echo ""
echo "══ Step 2/4: Stopping instance ══"
oci compute instance action --instance-id "$INSTANCE_ID" --action STOP >/dev/null 2>&1
wait_for_state STOPPED

# ── Step 3: Swap shape A2 → A1 ──────────────────────────────────────────────
echo ""
echo "══ Step 3/4: Updating shape → VM.Standard.A1.Flex ══"
oci compute instance update --instance-id "$INSTANCE_ID" \
  --shape "VM.Standard.A1.Flex" \
  --shape-config "{\"ocpus\": $TARGET_OCPUS, \"memoryInGBs\": $TARGET_MEMORY_GB}" \
  --force >/dev/null 2>&1

echo "[launch] Waiting for shape change to propagate..."
while true; do
  SHAPE=$(oci compute instance get --instance-id "$INSTANCE_ID" \
    --query 'data.shape' --raw-output)
  [[ "$SHAPE" == "VM.Standard.A1.Flex" ]] && break
  sleep 10
done

# ── Step 4: Start on A1 ─────────────────────────────────────────────────────
echo ""
echo "══ Step 4/4: Starting on A1.Flex ══"
oci compute instance action --instance-id "$INSTANCE_ID" --action START >/dev/null 2>&1
wait_for_state RUNNING

# ── Done ─────────────────────────────────────────────────────────────────────
sleep 5
PUBLIC_IP=$(oci compute instance list-vnics --instance-id "$INSTANCE_ID" \
  --query 'data[0]."public-ip"' --raw-output)

echo ""
echo "══ Launch complete ══"
echo "  Instance:  $INSTANCE_ID"
echo "  Shape:     VM.Standard.A1.Flex ($TARGET_OCPUS OCPUs, ${TARGET_MEMORY_GB}GB)"
echo "  Boot vol:  ${BOOT_VOLUME_GB}GB"
echo "  Public IP: $PUBLIC_IP"
echo ""
echo "  Wait ~2 min for cloud-init, then:"
echo "    ssh ubuntu@$PUBLIC_IP"
echo "    cd pred_market/scripts/oci_collector && ./setup.sh && ./run_collector.sh"
echo ""
