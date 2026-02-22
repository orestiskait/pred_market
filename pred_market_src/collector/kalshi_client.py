"""Kalshi API client — REST helpers and WebSocket authentication."""

import base64
import logging
import time

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

logger = logging.getLogger(__name__)


# ──────────────────────────────────────────────────────────────
# Authentication
# ──────────────────────────────────────────────────────────────

class KalshiAuth:
    """RSA-PSS request signing for Kalshi API."""

    def __init__(self, api_key_id: str, private_key_path: str):
        self.api_key_id = api_key_id
        with open(private_key_path, "rb") as f:
            self.private_key = serialization.load_pem_private_key(f.read(), password=None)

    def _sign(self, timestamp_ms: str, method: str, path: str) -> str:
        msg = f"{timestamp_ms}{method}{path}".encode("utf-8")
        sig = self.private_key.sign(
            msg,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode("utf-8")

    def rest_headers(self, method: str, path: str) -> dict:
        """Auth headers for a REST request.  *path* is the full path after the host
        (e.g. ``/trade-api/v2/markets``)."""
        ts = str(int(time.time() * 1000))
        return {
            "Content-Type": "application/json",
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": self._sign(ts, method.upper(), path),
        }

    def ws_headers(self) -> dict:
        """Auth headers for opening the WebSocket connection."""
        ts = str(int(time.time() * 1000))
        return {
            "KALSHI-ACCESS-KEY": self.api_key_id,
            "KALSHI-ACCESS-TIMESTAMP": ts,
            "KALSHI-ACCESS-SIGNATURE": self._sign(ts, "GET", "/trade-api/ws/v2"),
        }


# ──────────────────────────────────────────────────────────────
# REST client
# ──────────────────────────────────────────────────────────────

class KalshiRestClient:
    """Thin synchronous wrapper around the Kalshi v2 REST API."""

    def __init__(self, base_url: str, auth: KalshiAuth):
        self.base_url = base_url.rstrip("/")
        self.auth = auth
        self.session = requests.Session()

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self.base_url}{path}"
        headers = self.auth.rest_headers("GET", f"/trade-api/v2{path}")
        resp = self.session.get(url, headers=headers, params=params)
        resp.raise_for_status()
        return resp.json()

    # -- Markets & events ------------------------------------------------

    def get_event(self, event_ticker: str) -> dict:
        return self._get(f"/events/{event_ticker}")

    def get_events_for_series(self, series_ticker: str, status: str | None = None) -> list:
        params: dict = {"series_ticker": series_ticker}
        if status:
            params["status"] = status
        return self._get("/events", params=params).get("events", [])

    def get_markets_for_event(self, event_ticker: str) -> list:
        return self._get("/markets", params={"event_ticker": event_ticker}).get("markets", [])

    def get_market(self, ticker: str) -> dict:
        resp = self._get(f"/markets/{ticker}")
        return resp.get("market", resp)

    def get_orderbook(self, ticker: str, depth: int = 0) -> dict:
        return self._get(f"/markets/{ticker}/orderbook", params={"depth": depth})

    # -- Historical -------------------------------------------------------

    def get_candlesticks(self, series_ticker: str, market_ticker: str,
                         start_ts: int, end_ts: int,
                         period_interval: int = 60) -> list:
        """Fetch OHLC candlesticks.  *period_interval*: 1 | 60 | 1440 (minutes)."""
        path = f"/series/{series_ticker}/markets/{market_ticker}/candlesticks"
        return self._get(path, params={
            "start_ts": start_ts,
            "end_ts": end_ts,
            "period_interval": period_interval,
        }).get("candlesticks", [])

    def get_trades(self, ticker: str | None = None, min_ts: int | None = None,
                   max_ts: int | None = None, limit: int = 1000,
                   cursor: str | None = None) -> dict:
        """Paginated trade history.  Returns ``{"trades": [...], "cursor": "..."}``."""
        params: dict = {"limit": limit}
        if ticker:
            params["ticker"] = ticker
        if min_ts is not None:
            params["min_ts"] = min_ts
        if max_ts is not None:
            params["max_ts"] = max_ts
        if cursor:
            params["cursor"] = cursor
        return self._get("/markets/trades", params=params)
