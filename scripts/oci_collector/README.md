# OCI Deployment: Kalshi Collector

Run the Kalshi market data collector 24/7 on an OCI ARM instance (Always Free tier eligible).

## Overview

| Script | What it does |
|--------|-------------|
| `launch.sh` | Creates the OCI VM (A2→A1 shape swap trick), prints the public IP |
| `cloud-init.yaml` | Runs on first boot: installs Docker, git, fail2ban, sets up passwordless sudo |
| `setup.sh` | Run on the VM: clones repo, builds Docker image, configures Kalshi credentials |
| `run_collector.sh` | Start / stop / logs / status for the collector container |
| `update.sh` | Pull latest code, rebuild image, restart collector (skip if already up to date) |
| `probe.sh` | Probe from local: VM state, container status, recent logs, data freshness |

## Prerequisites

1. **OCI CLI** installed and configured (`oci setup config`)
2. **SSH key pair** — public key at `~/.ssh/id_ed25519.pub` or `~/.ssh/id_rsa.pub`
3. **VCN and public subnet** in OCI (the subnet must allow public IPs)
4. **Kalshi credentials** — API key ID and private key PEM file

## Quick Start

### 1. Launch the VM

```bash
cd scripts/oci_collector
chmod +x launch.sh setup.sh run_collector.sh
./launch.sh
```

The launch script uses the **A2→A1 shape swap trick** to work around A1.Flex capacity limits:

1. Creates a `VM.Standard.A2.Flex` instance (usually has availability)
2. Stops the instance
3. Updates the shape to `VM.Standard.A1.Flex`
4. Starts the instance on the A1 shape

Final specs: **4 OCPUs · 24 GB RAM · 150 GB boot volume** (aarch64).

The script auto-detects compartment, availability domain, subnet, and Ubuntu image. To override:

```bash
COMPARTMENT_ID=ocid1... AD=lqls:US-ASHBURN-AD-1 SUBNET_ID=ocid1... ./launch.sh

# Custom SSH key:
SSH_PUBLIC_KEY_FILE=~/.ssh/my_key.pub ./launch.sh
```

At the end it prints the public IP.

### 2. SSH into the VM

Wait ~2 minutes for cloud-init to install Docker, then:

```bash
ssh ubuntu@<PUBLIC_IP>
```

To verify cloud-init finished:

```bash
ssh ubuntu@<PUBLIC_IP> 'tail -5 /var/log/cloud-init-output.log'
```

You should see `[cloud-init] Done`.

### 3. Copy the Kalshi private key to the VM

From your local machine:

```bash
scp ~/.kalshi/kalshi_api_key.txt ubuntu@<PUBLIC_IP>:~/.kalshi/
```

### 4. Clone the repo and run setup

On the VM:

```bash
git clone https://github.com/orestiskait/pred_market.git ~/pred_market
cd ~/pred_market/scripts/oci_collector
./setup.sh
```

Setup will prompt for:
- **KALSHI_API_KEY_ID** — your Kalshi API key ID
- **Path to Kalshi private key** — e.g. `/home/ubuntu/.kalshi/kalshi_api_key.txt`

Or pass them non-interactively:

```bash
KALSHI_API_KEY_ID=your-key-id \
  KALSHI_PRIVATE_KEY_FILE=/home/ubuntu/.kalshi/kalshi_api_key.txt \
  ./setup.sh
```

### 5. Start the collector

```bash
./run_collector.sh
```

### 6. Verify

```bash
./run_collector.sh status    # container running?
./run_collector.sh logs      # tail live logs
ls -la ~/collector-data/market_snapshots/
```

## SSH Reference

**Connect:**

```bash
ssh ubuntu@<PUBLIC_IP>
```

**Find the public IP** (if you lost it):

```bash
# From your local machine with OCI CLI configured:
oci compute instance list -c <COMPARTMENT_ID> \
  --display-name kalshi-collector \
  --query 'data[0].id' --raw-output \
  | xargs -I{} oci compute instance list-vnics --instance-id {} \
    --query 'data[0]."public-ip"' --raw-output
```

**Copy files to/from the VM:**

```bash
# Upload:
scp local_file.txt ubuntu@<PUBLIC_IP>:~/

# Download collected data:
scp -r ubuntu@<PUBLIC_IP>:~/collector-data ./collector_data_backup/
```

## Collector Commands

| Command | Action |
|---------|--------|
| `./run_collector.sh` or `./run_collector.sh start` | Start / restart |
| `./run_collector.sh stop` | Stop |
| `./run_collector.sh logs` | Tail logs |
| `./run_collector.sh status` | Container status |

## Probing from Local Machine

Check if the VM is running and actively collecting:

```bash
./probe.sh
```

Reports: VM lifecycle state, public IP, container status, last 15 log lines, and data directory freshness. Requires OCI CLI and SSH access.

## Updating Code

`update.sh` pulls the latest code from GitHub, rebuilds the Docker image, and restarts the collector. It skips everything if the VM is already on the latest commit.

### One-off update (from your local machine)

```bash
ssh ubuntu@<PUBLIC_IP> '~/pred_market/scripts/oci_collector/update.sh'
```

Or on the VM directly:

```bash
cd ~/pred_market/scripts/oci_collector && ./update.sh
```

### Automatic updates (cron)

To auto-pull every 6 hours, add a cron entry on the VM:

```bash
ssh ubuntu@<PUBLIC_IP>
crontab -e
```

Add this line:

```
0 */6 * * * /home/ubuntu/pred_market/scripts/oci_collector/update.sh >> /home/ubuntu/collector-data/update.log 2>&1
```

The update log is written to `~/collector-data/update.log`. Check it anytime:

```bash
tail -50 ~/collector-data/update.log
```

To change the frequency, adjust the cron schedule (e.g. `*/30 * * * *` for every 30 minutes, `0 * * * *` for hourly).

## VM Layout

| Path | Purpose |
|------|---------|
| `/home/ubuntu/pred_market` | Repo clone |
| `/home/ubuntu/.kalshi/collector.env` | Credentials (env vars for Docker) |
| `/home/ubuntu/.kalshi/kalshi_api_key.txt` | Kalshi private key |
| `/home/ubuntu/collector-data/` | Parquet output (mounted into Docker container) |

```
/home/ubuntu/collector-data/
├── market_snapshots/       # price/volume snapshots (one parquet per day)
├── orderbook_snapshots/    # orderbook depth data
└── historical/             # backfill data
```

## Security

- Cloud-init installs **fail2ban** for SSH brute-force protection.
- Ensure the subnet's security list allows **inbound SSH (port 22)** from your IP.
- Outbound HTTPS is needed for Kalshi API and GitHub.
