"""Coinbase Exchange REST client (historical trades) and message helpers."""

import json
import urllib.request

PAGE_SIZE = 1000  # maximum the trades endpoint returns per request


def fetch_trades_before(rest_url: str, product: str, before_id: int, timeout: float) -> list[dict]:
    """Up to PAGE_SIZE trades with trade_id < before_id, newest first. Blocking HTTP call."""
    url = f"{rest_url}/products/{product}/trades?limit={PAGE_SIZE}&after={before_id}"
    req = urllib.request.Request(url, headers={"User-Agent": "crypto-etl-producer"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.load(resp)


def to_backfill_record(product: str, trade: dict) -> dict:
    """REST trades lack these fields; add them so they parse like live matches (see docs/CONTRACTS.md)."""
    return {"type": "match", "product_id": product, "source": "rest_backfill", **trade}


def subscribe_message(products: tuple[str, ...]) -> dict:
    # heartbeat: one message per product per second, used as a liveness signal
    return {"type": "subscribe", "product_ids": list(products), "channels": ["matches", "heartbeat"]}
