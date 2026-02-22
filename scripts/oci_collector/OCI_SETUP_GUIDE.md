# OCI Collector — Setup Guide & Script Reference

Complete guide for deploying and operating the Kalshi/Synoptic collector on Oracle Cloud. Use this doc to understand **when** to run each script and **how often**.

---

## One-off vs regular scripts

| Frequency | Scripts | When you run them |
|-----------|---------|-------------------|
| **One-off** (once per VM lifetime) | `provision_vm/create_oci_vm_instance.sh`, `setup_collector/first_time_vm_setup.sh` | When creating a new VM or re-provisioning from scratch |
| **Regular** (ongoing use) | `manage_services/*`, `maintenance/*` | Daily or weekly, depending on need |

---

## Script reference: when to use each

### One-off scripts (run once when setting up)

#### `provision_vm/create_oci_vm_instance.sh`

**When:** You need a new OCI VM to run the collector (first time, or after terminating the old VM).

**Where:** Run from your **local machine** (requires OCI CLI).

**What it does:** Creates an OCI instance (A2→A1 shape swap), assigns a reserved public IP, runs cloud-init (Docker, git, fail2ban).

**After it finishes:** Wait ~2 min for cloud-init, then SSH in and run `first_time_vm_setup.sh`.

```bash
cd scripts/oci_collector/provision_vm
./create_oci_vm_instance.sh
```

---

#### `setup_collector/first_time_vm_setup.sh`

**When:** First time you SSH into the VM, or when you want to reset credentials / reinstall.

**Where:** Run **on the VM** (after `ssh ubuntu@<PUBLIC_IP>`).

**What it does:** Clones the repo (if needed), builds the Docker image, prompts for Kalshi + Synoptic credentials, creates `~/.kalshi/collector.env`. Cron jobs are skipped by default (in-process periodic re-discovery handles event rollover).

**Prerequisite:** Copy your Kalshi private key to the VM first:  
`scp ~/.kalshi/kalshi_api_key.txt ubuntu@<PUBLIC_IP>:~/.kalshi/`

```bash
cd ~/pred_market/scripts/oci_collector/setup_collector
./first_time_vm_setup.sh
```

---

### Regular scripts (run often)

#### `manage_services/start_stop_all_services.sh`

**When:** You want to start, stop, check status, or tail logs for **all three** services (Kalshi listener, Synoptic listener, weather bot).

**Where:** Run **on the VM**.

**Typical use:**
- `./start_stop_all_services.sh` or `./start_stop_all_services.sh start` — start everything (e.g. right after setup, or after a reboot)
- `./start_stop_all_services.sh stop` — stop everything
- `./start_stop_all_services.sh status` — see if containers are running
- `./start_stop_all_services.sh logs` — tail logs from all three

**Note:** Event rollover is handled in-process (periodic re-discovery). Cron is skipped by default; run manually when needed (e.g. after VM reboot). You usually don’t need to run it manually unless you restarted the VM or changed something.

```bash
cd ~/pred_market/scripts/oci_collector/manage_services
./start_stop_all_services.sh start
```

---

#### `manage_services/start_stop_kalshi_listener.sh`  
#### `manage_services/start_stop_synoptic_listener.sh`  
#### `manage_services/start_stop_weather_bot.sh`

**When:** You want to control **one** service (start, stop, logs, status).

**Where:** Run **on the VM**.

**Typical use:** Debugging a single service, or running only the listener without the bot.

```bash
./start_stop_kalshi_listener.sh start
./start_stop_synoptic_listener.sh logs
```

---

#### `maintenance/sync_collected_data_to_local.sh`

**When:** You want to **download** the collected parquet files from the VM to your local machine (e.g. for the analysis notebook).

**Where:** Run from your **local machine** (requires OCI CLI + SSH).

**Typical use:** Run periodically (daily, weekly) to pull new data. Uses rsync — only new/changed files are transferred.

```bash
cd scripts/oci_collector/maintenance
./sync_collected_data_to_local.sh

# Preview without downloading:
./sync_collected_data_to_local.sh --dry-run
```

---

#### `maintenance/probe_vm_and_container_status.sh`

**When:** You want to **check** if the VM is running, containers are up, and data is fresh — without SSH-ing in.

**Where:** Run from your **local machine** (requires OCI CLI + SSH).

**Typical use:** Quick health check. Reports VM state, public IP, container status, last 15 log lines, and data recency (flags stale if >15 min since last write).

```bash
cd scripts/oci_collector/maintenance
./probe_vm_and_container_status.sh
```

---

#### `maintenance/update_code_and_restart_services.sh`

**When:** You want to **update** the collector to the latest code from GitHub and restart all services.

**Where:** Run **on the VM** (or via SSH from local).

**Typical use:** After pushing changes to the repo. Skips if already up to date. Can be run manually or via cron (e.g. every 6 hours).

```bash
# On the VM:
cd ~/pred_market/scripts/oci_collector/maintenance
./update_code_and_restart_services.sh

# From local:
ssh ubuntu@<PUBLIC_IP> '~/pred_market/scripts/oci_collector/maintenance/update_code_and_restart_services.sh'
```

---

## Typical workflow

### First-time setup

1. **Local:** `provision_vm/create_oci_vm_instance.sh` → creates VM
2. **Local:** `scp ~/.kalshi/kalshi_api_key.txt ubuntu@<IP>:~/.kalshi/`
3. **Local:** `ssh ubuntu@<IP>`
4. **VM:** `git clone ... ~/pred_market` (or clone before SSH)
5. **VM:** `setup_collector/first_time_vm_setup.sh` → credentials, Docker image
6. **VM:** `manage_services/start_stop_all_services.sh start` → start collecting

### Ongoing

- **Daily:** In-process re-discovery picks up new event tickers every 5 min (no cron, no action needed)
- **When you want local data:** `maintenance/sync_collected_data_to_local.sh`
- **When you want a health check:** `maintenance/probe_vm_and_container_status.sh`
- **When you push code changes:** `maintenance/update_code_and_restart_services.sh` (on VM or via SSH)

---

## Public IP & SSH

| Resource | Value |
|----------|-------|
| **Reserved public IP** | `129.158.203.11` |
| **Reserved IP name** | `kalshi-collector-ip` |

SSH: `ssh ubuntu@129.158.203.11`

---

## Files required (not in GitHub)

These are created locally, copied to the VM, or generated during setup.

### On the OCI VM (`~` = home directory)

| Path | Purpose |
|------|---------|
| `~/.kalshi/kalshi_api_key.txt` | Kalshi API private key (PEM). Copy from local before running `first_time_vm_setup.sh`. |
| `~/.kalshi/collector.env` | Generated by `first_time_vm_setup.sh`. Contains credentials. Do not edit manually. |
| `~/collector-data/` | Parquet output. Created by setup. Mounted into containers. |

### On your local machine

| Path | Purpose |
|------|---------|
| `~/.oci/config` | OCI CLI config. Created by `oci setup config`. |
| `~/.oci/<key_file>` | OCI API signing key. Referenced in config. |
| `~/.ssh/id_ed25519` or `~/.ssh/id_rsa` | SSH key pair. Used by `create_oci_vm_instance.sh` and to connect to the VM. |

---

## Credential flow (no secrets in this doc)

1. **Kalshi API:** Get API key ID + private key PEM from Kalshi. Copy PEM to `~/.kalshi/kalshi_api_key.txt` on the VM. Run `first_time_vm_setup.sh` (prompts for key ID, creates `collector.env`).
2. **OCI:** `oci setup config` creates `~/.oci/config`. Need OCI account + API key from Console.
3. **SSH:** Public key injected at launch by `create_oci_vm_instance.sh`. Private key used to connect.
