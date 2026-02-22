# OCI Deployment: Kalshi listener, Synoptic listener, weather bot

Run Kalshi listener, Synoptic listener, and weather bot 24/7 on an OCI ARM instance (Always Free tier eligible).

## Overview

| Script | What it does |
|--------|-------------|
| `launch.sh` | Creates the OCI VM (A2→A1 shape swap trick), assigns reserved public IP |
| `cloud-init.yaml` | Runs on first boot: installs Docker, git, fail2ban |
| `setup.sh` | Run on the VM: clones repo, builds Docker image, configures credentials |
| `run_all.sh` | Start / stop / logs / status for all three services |
| `run_kalshi_listener.sh` | Kalshi listener only |
| `run_synoptic_listener.sh` | Synoptic listener only |
| `run_weather_bot.sh` | Weather bot only |
| `update.sh` | Pull latest code, rebuild image, restart all three (skip if already up to date) |
| `probe.sh` | Probe from local: VM state, container status, recent logs, data freshness |

## Architecture

The VM runs Docker containers for data collection:

```
┌─────────────────────────────────────────────────────────────────┐
│  OCI VM (A1.Flex, 4 OCPU, 24 GB, ARM64)                        │
│                                                                  │
│  ┌──────────────────────┐    ┌──────────────────────────────┐   │
│  │  kalshi-listener      │    │  synoptic-listener            │   │
│  │  (WebSocket → parquet)│    │  (WebSocket → parquet)        │   │
│  └───────┬───────────────┘    └──────┬───────────────────────┘   │
│          │                           │                            │
│          ▼                           ▼                            │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  ~/collector-data/  (shared volume)                       │   │
│  │  ├── market_snapshots/     ← Kalshi prices                │   │
│  │  ├── orderbook_snapshots/  ← Kalshi orderbooks            │   │
│  │  ├── synoptic_ws/          ← Synoptic Websocket API       │   │
│  │  └── weather_obs/                                          │   │
│  │      ├── asos_1min/        ← ASOS 1-min (IEM, ~24h lag)   │   │
│  │      ├── metar/            ← METAR (AWC API)              │   │
│  │      └── daily_climate/    ← Official CLI high/low        │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

## Prerequisites

1. **OCI CLI** installed and configured (`oci setup config`)
2. **SSH key pair** — public key at `~/.ssh/id_ed25519.pub` or `~/.ssh/id_rsa.pub`
3. **VCN and public subnet** in OCI (the subnet must allow public IPs)
4. **Kalshi credentials** — API key ID and private key PEM file

## Quick Start

### 1. Launch the VM

```bash
cd scripts/oci_collector
chmod +x launch.sh setup.sh run_all.sh run_kalshi_listener.sh run_synoptic_listener.sh run_weather_bot.sh
./launch.sh
```

The launch script uses the **A2→A1 shape swap trick** to work around A1.Flex capacity limits:

1. Creates a `VM.Standard.A2.Flex` instance (no ephemeral IP)
2. Stops the instance
3. Updates the shape to `VM.Standard.A1.Flex`
4. Starts the instance on the A1 shape
5. Assigns a **reserved public IP** (creates one named `kalshi-collector-ip` if it doesn't exist yet, otherwise re-uses it)

Final specs: **4 OCPUs · 24 GB RAM · 150 GB boot volume** (aarch64).

The reserved IP (`129.158.203.11`) persists across VM recreations — if you terminate and re-launch the VM, the same IP is automatically re-attached in step 5.

The script auto-detects compartment, availability domain, subnet, and Ubuntu image. To override:

```bash
COMPARTMENT_ID=ocid1... AD=lqls:US-ASHBURN-AD-1 SUBNET_ID=ocid1... ./launch.sh

# Custom SSH key:
SSH_PUBLIC_KEY_FILE=~/.ssh/my_key.pub ./launch.sh

# Custom reserved IP name:
RESERVED_IP_NAME=my-ip-name ./launch.sh
```

At the end it prints the (permanent) public IP.

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
- **SYNOPTIC_API_TOKEN** — your Synoptic Push API Token

Or pass them non-interactively:

```bash
KALSHI_API_KEY_ID=your-key-id \
  KALSHI_PRIVATE_KEY_FILE=/home/ubuntu/.kalshi/kalshi_api_key.txt \
  SYNOPTIC_API_TOKEN=your-synoptic-token \
  ./setup.sh
```

### 5. Start Kalshi listener, Synoptic listener, and weather bot

```bash
./run_all.sh
```

Or run individually:
```bash
./run_kalshi_listener.sh start
./run_synoptic_listener.sh start
./run_weather_bot.sh start
```

### 6. Verify services

```bash
./run_all.sh status    # containers running?
./run_all.sh logs      # tail live logs
ls -la ~/collector-data/market_snapshots/
```

## SSH Reference

**Connect:**

```bash
ssh ubuntu@<PUBLIC_IP>
```

**Find the public IP** (if you lost it):

```bash
# Option 1 — look up the reserved IP directly by name (always works, even if VM is stopped):
oci network public-ip list \
  -c $(grep -m1 '^tenancy=' ~/.oci/config | cut -d= -f2) \
  --scope REGION \
  --query 'data[?"display-name"==`kalshi-collector-ip`]."ip-address" | [0]' \
  --raw-output

# Option 2 — via instance VNIC (VM must be RUNNING):
oci compute instance list -c <COMPARTMENT_ID> \
  --display-name kalshi-collector \
  --query 'data[0].id' --raw-output \
  | xargs -I{} oci compute instance list-vnics --instance-id {} \
    --query 'data[0]."public-ip"' --raw-output
```

The reserved IP is **`129.158.203.11`** (OCID: `ocid1.publicip.oc1.iad.amaaaaaazqlyc2yaslssz2pswhwg7pbrklpifsjyw6wze2jsbhp7blkijjia`).

**Copy files to/from the VM:**

```bash
# Upload:
scp local_file.txt ubuntu@<PUBLIC_IP>:~/

# Download collected data:
scp -r ubuntu@<PUBLIC_IP>:~/collector-data ./collector_data_backup/
```

## Fetching Data Locally

`fetch_data.sh` syncs the collected parquet files from the VM to `pred_market_src/collector/data/` (the path the analysis notebook reads from). It uses `rsync` — only new/changed files are transferred.

```bash
# Sync all data
./fetch_data.sh

# Preview what would be transferred without downloading
./fetch_data.sh --dry-run

# Sync to a custom local path
LOCAL_DATA_DIR=~/my-data ./fetch_data.sh
```

The script auto-detects the VM's public IP via OCI CLI. On success it prints a summary of all local parquet files and their sizes.

## Daily Restart (Event Series Roll)

The Kalshi listener resolves event series prefixes (e.g. `KXHIGHCHI`) to dated tickers (e.g. `KXHIGHCHI-26FEB19`) once at startup. To pick up the next day's events, the services restart automatically at **12:01 AM** and **1:01 AM New York time** via cron jobs installed by `setup.sh`.

Two restarts are necessary because NY contracts typically roll over around midnight, and Chicago contracts roll over around 1:00 AM ET (midnight CT).

The VM timezone is set to `America/New_York` by cloud-init, so the cron schedule adjusts for daylight saving time automatically.

Restart log: `~/collector-data/daily-restart.log`

To verify the cron is installed:

```bash
crontab -l | grep run_all
```

To change the schedule, edit the crontab on the VM:

```bash
crontab -e
```

## Service Commands

### Kalshi listener, Synoptic listener, weather bot

| Command | Action |
|---------|--------|
| `./run_all.sh` or `./run_all.sh start` | Start / restart all three |
| `./run_all.sh stop` | Stop all |
| `./run_all.sh logs` | Tail logs from all |
| `./run_all.sh status` | Container status |
| `./run_kalshi_listener.sh start` | Kalshi listener only |
| `./run_synoptic_listener.sh start` | Synoptic listener only |
| `./run_weather_bot.sh start` | Weather bot only |

## Probing from Local Machine

Check if the VM is running and actively collecting:

```bash
./probe.sh
```

Reports: VM lifecycle state, public IP, container status, last 15 log lines, and data directory freshness. Requires OCI CLI and SSH access.

## Updating Code

`update.sh` pulls the latest code from GitHub, rebuilds the Docker image, and restarts Kalshi listener, Synoptic listener, and weather bot. It skips everything if the VM is already on the latest commit.

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
| `/home/ubuntu/collector-data/` | All data output (mounted into both containers) |

```
/home/ubuntu/collector-data/
├── market_snapshots/       # Kalshi price/volume snapshots (one parquet per day)
├── orderbook_snapshots/    # Kalshi orderbook depth data
├── historical/             # Kalshi backfill data
└── weather_obs/
    ├── asos_1min/          # ASOS 1-min archive (IEM, ~24h delay)
    ├── metar/              # METAR via AWC API
    └── daily_climate/      # Official NWS daily climate reports
```

## Security

- Cloud-init installs **fail2ban** for SSH brute-force protection.
- Ensure the subnet's security list allows:
  - **Inbound SSH (port 22)** from your IP
- Outbound HTTPS is needed for Kalshi API and GitHub.
