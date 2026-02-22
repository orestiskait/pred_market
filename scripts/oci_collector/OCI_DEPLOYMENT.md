# OCI Deployment: Kalshi listener, Synoptic listener, weather bot

Run Kalshi listener, Synoptic listener, and weather bot 24/7 on an OCI ARM instance (Always Free tier eligible).

**→ See [OCI_SETUP_GUIDE.md](OCI_SETUP_GUIDE.md) for when to use each script, one-off vs regular, and credentials.**

## Directory Layout

```
scripts/oci_collector/
├── provision_vm/                              # VM creation
│   ├── create_oci_vm_instance.sh              # Creates OCI VM (A2→A1 shape swap), assigns reserved public IP
│   └── cloud_init_first_boot.yaml             # First-boot config: Docker, git, fail2ban
├── setup_collector/                           # One-time VM setup
│   └── first_time_vm_setup.sh                # Clone repo, build Docker image, configure credentials
├── manage_services/                           # Start/stop/status containers
│   ├── start_stop_all_services.sh            # Start/stop/logs/status for all three services
│   ├── start_stop_kalshi_listener.sh         # Kalshi market data listener only
│   ├── start_stop_synoptic_listener.sh       # Synoptic weather listener only
│   └── start_stop_weather_bot.sh             # Weather arbitrage bot only
├── maintenance/                               # Updates, monitoring, data sync
│   ├── update_code_and_restart_services.sh    # Pull latest code, rebuild image, restart all services
│   ├── probe_vm_and_container_status.sh      # Check VM state, container status, logs, data freshness
│   └── sync_collected_data_to_local.sh       # Rsync parquet files from VM to local machine
├── OCI_DEPLOYMENT.md                          # This file
└── OCI_SETUP_GUIDE.md          # When to use each script, one-off vs regular, credentials
```

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
│  │  ├── kalshi_market_snapshots/     ← Kalshi prices         │   │
│  │  ├── kalshi_orderbook_snapshots/  ← Kalshi orderbooks     │   │
│  │  ├── synoptic_weather_observations/ ← Synoptic WebSocket │   │
│  │  ├── iem_asos_1min/       ← IEM ASOS 1-min (~24h lag)     │   │
│  │  ├── awc_metar/           ← AWC METAR                     │   │
│  │  ├── iem_daily_climate/   ← IEM NWS Daily Climate (CLI)   │   │
│  │  ├── kalshi_historical/  ← Kalshi backfill (candlesticks, trades) │
│  │  └── weather_bot_paper_trades/ ← Paper trade logs         │   │
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
cd scripts/oci_collector/provision_vm
chmod +x create_oci_vm_instance.sh
./create_oci_vm_instance.sh
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
COMPARTMENT_ID=ocid1... AD=lqls:US-ASHBURN-AD-1 SUBNET_ID=ocid1... ./create_oci_vm_instance.sh

# Custom SSH key:
SSH_PUBLIC_KEY_FILE=~/.ssh/my_key.pub ./create_oci_vm_instance.sh

# Custom reserved IP name:
RESERVED_IP_NAME=my-ip-name ./create_oci_vm_instance.sh
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
cd ~/pred_market/scripts/oci_collector/setup_collector
./first_time_vm_setup.sh
```

Setup will prompt for:
- **KALSHI_API_KEY_ID** — your Kalshi API key ID
- **SYNOPTIC_API_TOKEN** — your Synoptic Push API Token

Credentials are written to `~/.kalshi/` (kalshi_api_key_id, synoptic_token). The Kalshi private key must already be at `~/.kalshi/kalshi_api_key.txt` (copy via scp first).

Or pass non-interactively:

```bash
KALSHI_API_KEY_ID=your-key-id \
  SYNOPTIC_API_TOKEN=your-synoptic-token \
  ./first_time_vm_setup.sh
```

### 5. Start Kalshi listener, Synoptic listener, and weather bot

```bash
cd ~/pred_market/scripts/oci_collector/manage_services
./start_stop_all_services.sh
```

Or run individually:
```bash
./start_stop_kalshi_listener.sh start
./start_stop_synoptic_listener.sh start
./start_stop_weather_bot.sh start
```

### 6. Verify services

```bash
./start_stop_all_services.sh status    # containers running?
./start_stop_all_services.sh logs      # tail live logs
ls -la ~/collector-data/kalshi_market_snapshots/
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

`maintenance/sync_collected_data_to_local.sh` syncs the collected parquet files from the VM to `data/` (the path the analysis notebook reads from). It uses `rsync` — only new/changed files are transferred.

```bash
cd scripts/oci_collector/maintenance
./sync_collected_data_to_local.sh
```

```bash
# Preview what would be transferred without downloading
./sync_collected_data_to_local.sh --dry-run

# Sync to a custom local path
LOCAL_DATA_DIR=~/my-data ./sync_collected_data_to_local.sh
```

The script auto-detects the VM's public IP via OCI CLI. On success it prints a summary of all local parquet files and their sizes.

## Event Series Roll (No Cron Required)

The Kalshi listener and weather bot **periodically re-discover** event tickers in-process (every `rediscover_interval_seconds`, default 5 min). When markets roll over at local midnight (NWS timezone per city), new tickers are picked up automatically and the WebSocket reconnects with updated subscriptions. **Cron jobs are no longer required.**

Config (`config.yaml` → `event_rollover`):

- `event_selection`: `"active"` (earliest close_time) or `"next"` (earliest strike_date ≥ today; picks tomorrow when today has closed)
- `rediscover_interval_seconds`: 300 (5 min). Set to 0 to disable in-process rollover.

Legacy cron (optional): `first_time_vm_setup.sh` skips cron by default. To restore the old 12:01/1:01 AM restarts, run `SKIP_CRON=0 ./first_time_vm_setup.sh`.

## Service Commands

### Kalshi listener, Synoptic listener, weather bot

| Command | Action |
|---------|--------|
| `./start_stop_all_services.sh` or `./start_stop_all_services.sh start` | Start / restart all three |
| `./start_stop_all_services.sh stop` | Stop all |
| `./start_stop_all_services.sh logs` | Tail logs from all |
| `./start_stop_all_services.sh status` | Container status |
| `./start_stop_kalshi_listener.sh start` | Kalshi listener only |
| `./start_stop_synoptic_listener.sh start` | Synoptic listener only |
| `./start_stop_weather_bot.sh start` | Weather bot only |

## Probing from Local Machine

Check if the VM is running and actively collecting:

```bash
cd scripts/oci_collector/maintenance
./probe_vm_and_container_status.sh
```

Reports: VM lifecycle state, public IP, container status, last 15 log lines, and data directory freshness. Requires OCI CLI and SSH access.

## Updating Code

`maintenance/update_code_and_restart_services.sh` pulls the latest code from GitHub, rebuilds the Docker image, and restarts Kalshi listener, Synoptic listener, and weather bot. It skips everything if the VM is already on the latest commit.

### One-off update (from your local machine)

```bash
ssh ubuntu@<PUBLIC_IP> '~/pred_market/scripts/oci_collector/maintenance/update_code_and_restart_services.sh'
```

Or on the VM directly:

```bash
cd ~/pred_market/scripts/oci_collector/maintenance && ./update_code_and_restart_services.sh
```

### Automatic updates (cron)

To auto-pull every 6 hours, add a cron entry on the VM:

```bash
ssh ubuntu@<PUBLIC_IP>
crontab -e
```

Add this line:

```
0 */6 * * * /home/ubuntu/pred_market/scripts/oci_collector/maintenance/update_code_and_restart_services.sh >> /home/ubuntu/collector-data/update.log 2>&1
```

The update log is written to `~/collector-data/update.log`. Check it anytime:

```bash
tail -50 ~/collector-data/update.log
```

To change the frequency, adjust the cron schedule (e.g. `*/30 * * * *` for every 30 minutes, `0 * * * *` for hourly).

## VM Layout

| Path | Purpose |
|------|---------|
| `~/pred_market` | Repo clone |
| `~/.kalshi/kalshi_api_key.txt` | Kalshi private key (copy via scp) |
| `~/.kalshi/kalshi_api_key_id` | Kalshi API key ID (created by setup) |
| `~/.kalshi/synoptic_token` | Synoptic token (created by setup) |
| `~/collector-data/` | All data output (mounted into containers) |

```
~/collector-data/
├── kalshi_market_snapshots/       # Kalshi price/volume snapshots (one parquet per day)
├── kalshi_orderbook_snapshots/   # Kalshi orderbook depth data
├── synoptic_weather_observations/ # Synoptic real-time weather
├── kalshi_historical/             # Kalshi backfill (candlesticks, trades)
├── weather_bot_paper_trades/     # Paper trade logs
├── iem_asos_1min/                # ASOS 1-min archive (IEM, ~24h delay)
├── awc_metar/                    # METAR via AWC API
└── iem_daily_climate/            # Official NWS daily climate reports
```

## Security

- Cloud-init installs **fail2ban** for SSH brute-force protection.
- Ensure the subnet's security list allows:
  - **Inbound SSH (port 22)** from your IP
- Outbound HTTPS is needed for Kalshi API and GitHub.
