"""The live WebSocket loop: forward trades to Kafka, start backfills for gaps, reconnect on failure."""

import asyncio
import json
import logging
import random
from collections.abc import Callable

import websockets

from .coinbase import subscribe_message
from .config import Settings
from .gaps import BackfillJob, observe_trade

log = logging.getLogger(__name__)

MAX_BACKOFF = 60.0


def reconnect_delay(backoff: float, rand: Callable[[], float] = random.random) -> float:
    """Backoff plus up to 1s of jitter, so many clients don't reconnect in lockstep."""
    return backoff + rand()


def next_backoff(backoff: float) -> float:
    return min(backoff * 2, MAX_BACKOFF)


class TradeStream:
    """Streams Coinbase matches into Kafka until cancelled.

    `emit_raw(product, raw)` produces a live message; `start_backfill(job)` handles a gap.
    """

    def __init__(
        self,
        settings: Settings,
        last_trade_id: dict[str, int],
        emit_raw: Callable[[str, str], None],
        start_backfill: Callable[[BackfillJob], None],
        on_alive: Callable[[], None] = lambda: None,
    ):
        self.settings = settings
        self.last_trade_id = last_trade_id
        self.emit_raw = emit_raw
        self.start_backfill = start_backfill
        self.on_alive = on_alive

    def handle(self, raw: str):
        msg = json.loads(raw)
        kind = msg.get("type")
        if kind == "match":
            product = msg["product_id"]
            if job := observe_trade(self.last_trade_id, product, msg["trade_id"]):
                self.start_backfill(job)
            self.emit_raw(product, raw)
            self.on_alive()
        elif kind == "heartbeat":
            self.on_alive()
        elif kind == "error":
            log.error("coinbase error: %s", msg)

    async def run(self):
        subscribe = json.dumps(subscribe_message(self.settings.products))
        backoff = 1.0
        while True:
            try:
                # close_timeout: on shutdown, don't spend the stop budget waiting for the server's close ack
                async with websockets.connect(
                    self.settings.ws_url, ping_interval=20, open_timeout=10, close_timeout=1
                ) as ws:
                    # Coinbase drops connections that don't subscribe within ~5s
                    await ws.send(subscribe)
                    log.info("connected, subscribed to %s", list(self.settings.products))
                    backoff = 1.0
                    async for raw in ws:
                        self.handle(raw)
                log.warning("connection closed by server, reconnecting")
            except Exception as e:
                delay = reconnect_delay(backoff)
                if isinstance(e, websockets.WebSocketException | OSError):
                    log.warning("connection lost (%s: %s), reconnecting in %.1fs", type(e).__name__, e, delay)
                else:
                    log.exception("unexpected error, reconnecting in %.1fs", delay)
                await asyncio.sleep(delay)
                backoff = next_backoff(backoff)
