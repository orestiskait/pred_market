# OCI Deployment: Kalshi Collector

Run the Kalshi market data collector on Oracle Cloud Infrastructure (OCI) so it collects data 24/7. All setup runs **on the VM** via cloud-init: clone from GitHub, install deps, configure credentials, start the collector.

## Prerequisites

1. **OCI CLI** installed and configured (`oci setup config`)
2. **SSH public key** at `~/.ssh/id_rsa.pub` or `~/.ssh/id_ed25519.pub`
3. **GitHub PAT** with `repo` scope (private repo): https://github.com/settings/tokens
4. **VCN and subnet** in OCI (public subnet for SSH)

## First-Time Setup

### 1. Discover your OCI IDs (optional; script can auto-detect)

```bash
# Compartment
oci iam compartment list --compartment-id-in-subtree true --all

# Availability domain
oci iam availability-domain list -c <COMPARTMENT_ID>

# Subnet (use a *public* subnet so the instance gets a public IP)
oci network subnet list -c <COMPARTMENT_ID>

# Ubuntu image (optional; launch script auto-detects)
oci compute image list -c <COMPARTMENT_ID> --operating-system "Canonical Ubuntu" --operating-system-version "22.04" --limit 1
```

### 2. Launch the instance (one command does everything)

From the project root. **GITHUB_TOKEN is required** for the private repo. Kalshi creds are optional; if you pass them, the collector starts automatically.

```bash
cd scripts/oci_collector
chmod +x launch.sh

# Required: GITHUB_TOKEN
GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx ./launch.sh

# With Kalshi credentials (auto-starts collector; reads from .env + config.yaml if not passed)
GITHUB_TOKEN=ghp_xxx KALSHI_API_KEY_ID=your-key-id KALSHI_PRIVATE_KEY_FILE=/path/to/kalshi_key.pem ./launch.sh

# Pass OCI IDs explicitly if auto-detect fails
COMPARTMENT_ID=ocid1... AD=xxx:PHX-AD-1 SUBNET_ID=ocid1... GITHUB_TOKEN=ghp_xxx ./launch.sh
```

### 3. Wait ~3–4 minutes for cloud-init

Cloud-init runs everything on the VM:
- Clone the private repo (using GitHub token)
- Install Python venv and collector deps
- If Kalshi creds were passed: write `.env`, `config.yaml`, key file, and **start the collector**

### 4. Verify

```bash
INSTANCE_ID=$(oci compute instance list -c <COMPARTMENT_ID> --display-name kalshi-collector --query 'data[0].id' -r | tr -d '"')
oci compute instance list-vnics --instance-id $INSTANCE_ID --query 'data[0]."public-ip"' -r

ssh ubuntu@<PUBLIC_IP>
sudo systemctl status kalshi-collector
ls -la ~/pred_market/pred_market_src/collector/data/
```

## Updating code from GitHub

```bash
ssh ubuntu@$IP 'cd pred_market && git pull'
sudo systemctl restart kalshi-collector
```

## Data location

Collected data lives on the instance at:

```
/home/ubuntu/pred_market/pred_market_src/collector/data/
├── market_snapshots/
├── orderbook_snapshots/
└── historical/
```

To pull data locally:

```bash
scp -r ubuntu@$IP:pred_market/pred_market_src/collector/data ./collector_data_backup/
```

## Optional: OCI Object Storage for persistence

To persist data beyond the instance lifecycle, add a cron job or script to sync `data/` to OCI Object Storage. Example:

```bash
oci os object bulk-upload -ns <NAMESPACE> -bn kalshi-collector-data --src-dir /home/ubuntu/pred_market/pred_market_src/collector/data
```

## Security group (ingress)

Ensure the instance's subnet/security list allows:
- **SSH (22)** from your IP (or 0.0.0.0/0 for convenience; restrict in production)
- Outbound HTTPS for Kalshi API and GitHub
