"""Configuration loading, client factories, and CLI helpers.

Centralized so that every service in the project uses the same config
resolution, .env loading, and logging setup.
"""

from __future__ import annotations

import argparse
import logging
import os
from pathlib import Path
from typing import Optional

import yaml
from dotenv import load_dotenv

# Resolve .env once at import time (idempotent).
_ENV_PATH = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(_ENV_PATH)

logger = logging.getLogger(__name__)


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
    """Build ``(KalshiAuth, KalshiRestClient)`` from config + env.

    Credential resolution order: env var → config key.
    """
    from ..kalshi.client import KalshiAuth, KalshiRestClient

    kcfg = config.get("kalshi", {})
    api_key_id = os.environ.get("KALSHI_API_KEY_ID") or kcfg.get("api_key_id", "")
    pk_path = os.environ.get("KALSHI_PRIVATE_KEY_PATH") or kcfg.get("private_key_path", "")
    if pk_path:
        pk_path = os.path.expanduser(pk_path)
    base_url = kcfg.get("base_url", "https://api.elections.kalshi.com/trade-api/v2")

    auth = KalshiAuth(api_key_id, pk_path)
    rest = KalshiRestClient(base_url, auth)
    return auth, rest


def get_synoptic_token() -> str:
    """Return the Synoptic API token from the environment."""
    token = os.environ.get("SYNOPTIC_API_TOKEN")
    if not token:
        raise ValueError("SYNOPTIC_API_TOKEN must be set in .env")
    return token


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
