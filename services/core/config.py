"""Configuration loading, client factories, and CLI helpers.

Centralized so that every service in the project uses the same config
resolution. Credentials are read from files; paths are in config.yaml
(credentials section). No .env needed.
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Optional

import yaml

logger = logging.getLogger(__name__)


def _credentials_dir(config: dict) -> Path:
    """Resolve credentials base directory. CREDENTIALS_DIR env overrides config."""
    creds_cfg = config.get("credentials", {})
    dir_path = os.environ.get("CREDENTIALS_DIR") or creds_cfg.get("dir", "~/.kalshi")
    return Path(os.path.expanduser(dir_path))


def _read_credential(config: dict, key: str) -> str:
    """Read a credential from a file. Returns stripped content."""
    creds_dir = _credentials_dir(config)
    creds_cfg = config.get("credentials", {})
    filename = creds_cfg.get(key, key)
    path = creds_dir / filename
    if not path.exists():
        raise FileNotFoundError(
            f"Credential file not found: {path}. "
            f"Create it or set CREDENTIALS_DIR if using Docker."
        )
    return path.read_text().strip()


# ======================================================================
# Config loading
# ======================================================================

def load_config(config_path: Optional[str] = None) -> tuple[dict, Path]:
    """Load and return the YAML config dictionary.

    Falls back to ``config.yaml`` next to the services package root
    when no path is given.
    """
    if config_path:
        path = Path(config_path)
    else:
        path = Path(__file__).resolve().parent.parent / "config.yaml"
    with open(path) as f:
        return yaml.safe_load(f), path


# ======================================================================
# Client factories
# ======================================================================

def make_kalshi_clients(config: dict):
    """Build ``(KalshiAuth, KalshiRestClient)`` from config.

    Credentials are read from files under config credentials.dir.
    """
    from ..kalshi.client import KalshiAuth, KalshiRestClient

    creds_dir = _credentials_dir(config)
    creds_cfg = config.get("credentials", {})
    pk_path = creds_dir / creds_cfg.get("kalshi_private_key", "kalshi_api_key.txt")
    pk_path = pk_path.resolve()
    if not pk_path.exists():
        raise FileNotFoundError(
            f"Kalshi private key not found: {pk_path}. "
            f"Create it or set CREDENTIALS_DIR if using Docker."
        )

    api_key_id = _read_credential(config, "kalshi_api_key_id")
    kcfg = config.get("kalshi", {})
    base_url = kcfg.get("base_url", "https://api.elections.kalshi.com/trade-api/v2")

    auth = KalshiAuth(api_key_id, str(pk_path))
    rest = KalshiRestClient(base_url, auth)
    return auth, rest


def get_synoptic_token(config: dict) -> str:
    """Return the Synoptic API token from the credentials file."""
    return _read_credential(config, "synoptic_token")


def build_synoptic_ws_url(
    token: str,
    stations: list[str],
    variables: list[str],
) -> str:
    """Construct the Synoptic push WebSocket URL."""
    stid_str = ",".join(stations)
    vars_str = ",".join(variables)
    return (
        f"wss://push.synopticdata.com/feed/{token}/"
        f"?units=english&stid={stid_str}&vars={vars_str}"
    )


# ======================================================================
# CLI helpers
# ======================================================================

def standard_argparser(description: str) -> argparse.ArgumentParser:
    """Return an ``ArgumentParser`` with common flags."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument(
        "--config",
        default=None,
        help="Path to config.yaml (default: auto-detect)",
    )
    parser.add_argument("--log-level", default="INFO")
    return parser


def configure_logging(level_name: str = "INFO") -> None:
    """Set up root logging with a consistent format."""
    logging.basicConfig(
        level=getattr(logging, level_name.upper()),
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    )
