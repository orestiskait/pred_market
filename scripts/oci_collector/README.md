# OCI Deployment: Kalshi Collector

Run the Kalshi market data collector on Oracle Cloud Infrastructure (OCI) so it collects data 24/7.

## Existing VM (Docker)

If you already have an OCI VM running:

### 1. Deploy from your machine (one command)

```bash
cd scripts/oci_collector
chmod +x deploy_to_vm.sh

# With interactive credential prompts:
./deploy_to_vm.sh <PUBLIC_IP>

# Non-interactive (key must already be on VM at ~/.kalshi/kalshi_key.pem):
./deploy_to_vm.sh <PUBLIC_IP> your-api-key-id /home/ubuntu/.kalshi/kalshi_key.pem
```

Or SSH in and run manually:

### 1b. SSH in (manual)

```bash
ssh ubuntu@<PUBLIC_IP>
```

### 2. Clone repo (if needed)

```bash
cd /home/ubuntu
git clone https://github.com/orestiskait/pred_market.git pred_market
cd pred_market/scripts/oci_collector
chmod +x setup.sh run_collector.sh
```

For a private repo: `GITHUB_TOKEN=ghp_xxx git clone "https://${GITHUB_TOKEN}@github.com/orestiskait/pred_market.git" pred_market`

If the repo exists: `cd /home/ubuntu/pred_market && git pull`, then `cd scripts/oci_collector`.

### 3. Run setup

```bash
./setup.sh
```

On first run you’ll be prompted for:
- **KALSHI_API_KEY_ID**
- **Path to Kalshi private key PEM file** (e.g. `/home/ubuntu/.kalshi/kalshi_key.pem`)

If the key isn’t on the VM yet, copy it first from your machine:

```bash
scp /path/to/kalshi_key.pem ubuntu@<PUBLIC_IP>:/home/ubuntu/.kalshi/
```

Non-interactive: `KALSHI_API_KEY_ID=xxx KALSHI_PRIVATE_KEY_FILE=/path/to/key.pem ./setup.sh`

### 4. Start the collector

```bash
./run_collector.sh
```

### 5. Verify

```bash
./run_collector.sh status
./run_collector.sh logs
ls -la /home/ubuntu/collector-data/market_snapshots/
```

### VM layout (unchanged by setup)

| Path | Purpose |
|------|---------|
| `/home/ubuntu/pred_market` | Repo |
| `/home/ubuntu/.kalshi/collector.env` | Credentials (env vars for Docker) |
| `/home/ubuntu/collector-data` | Parquet output (market_snapshots, orderbook_snapshots) |
| `/home/ubuntu/collector-cron.log` | Daily restart cron output |

### Commands

| Command | Action |
|---------|--------|
| `./run_collector.sh` or `./run_collector.sh start` | Start / restart |
| `./run_collector.sh stop` | Stop |
| `./run_collector.sh logs` | Tail logs |
| `./run_collector.sh status` | Container status |

---

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

The launch script uses the **A2→A1 shape swap trick** to get around A1.Flex capacity limits:

1. Creates a `VM.Standard.A2.Flex` instance (usually available)
2. Stops the instance once it's running
3. Updates the shape to `VM.Standard.A1.Flex`
4. Starts the instance on the new shape

Final specs: **4 OCPUs · 24 GB RAM · 150 GB boot volume**.

```bash
cd scripts/oci_collector
chmod +x launch.sh cloud-init.sh

# Auto-detects OCI IDs:
./launch.sh

# Pass OCI IDs explicitly if auto-detect fails:
COMPARTMENT_ID=ocid1... AD=xxx:PHX-AD-1 SUBNET_ID=ocid1... ./launch.sh

# Use a custom SSH key:
SSH_PUBLIC_KEY_FILE=~/.ssh/my_key.pub ./launch.sh
```

### 3. SSH in and run setup

Cloud-init pre-installs Docker and basic packages (~2 min). Then SSH in and use the existing `setup.sh` and `run_collector.sh`:

```bash
ssh ubuntu@<PUBLIC_IP>
cd ~/pred_market/scripts/oci_collector
./setup.sh            # clone repo, build image, configure credentials
./run_collector.sh    # start the collector
```

## Updating code from GitHub

```bash
ssh ubuntu@$IP 'cd /home/ubuntu/pred_market && git pull && cd scripts/oci_collector && ./setup.sh && ./run_collector.sh'
```

## Data location

Collected data lives at `/home/ubuntu/collector-data/` (Docker volume):

```
/home/ubuntu/collector-data/
├── market_snapshots/
├── orderbook_snapshots/
└── historical/
```

To pull data locally:

```bash
scp -r ubuntu@$IP:collector-data ./collector_data_backup/
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
