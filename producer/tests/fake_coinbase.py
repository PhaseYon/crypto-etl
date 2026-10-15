"""In-process fake of the Coinbase Exchange WebSocket feed and REST trades endpoint.

Runs both servers on random local ports in a background thread, so tests (and the
producer under test, as a subprocess) can talk to them over real sockets, offline.
"""

import asyncio
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import websockets


def match(product: str, trade_id: int, price: str = "100.00") -> dict:
    """A live `match` message as Coinbase sends it."""
    return {
        "type": "match",
        "trade_id": trade_id,
        "maker_order_id": f"m-{trade_id}",
        "taker_order_id": f"t-{trade_id}",
        "side": "buy",
        "size": "0.01",
        "price": price,
        "product_id": product,
        "sequence": trade_id * 10,
        "time": "2026-10-15T12:00:00.000000Z",
    }


def rest_trade(trade_id: int) -> dict:
    return {
        "trade_id": trade_id,
        "side": "sell",
        "size": "0.02",
        "price": "101.00",
        "time": "2026-10-15T11:59:00.000000Z",
    }


class FakeCoinbase:
    """`ws_script`: messages sent to each new WebSocket connection after it subscribes.

    The REST endpoint serves every trade_id below `rest_max_id` (newest first, pages of
    `limit`), sleeping `rest_page_delay` seconds per page to simulate a slow API.
    """

    def __init__(self, ws_script: list[dict] | None = None, rest_max_id: int = 10**9, rest_page_delay: float = 0.0):
        self.ws_script = ws_script or []
        self.rest_max_id = rest_max_id
        self.rest_page_delay = rest_page_delay
        self.rest_requests: list[str] = []
        self.subscriptions: list[dict] = []
        self._loop = asyncio.new_event_loop()
        self._ready = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._http = ThreadingHTTPServer(("127.0.0.1", 0), self._handler())
        self._http_thread = threading.Thread(target=self._http.serve_forever, daemon=True)
        self.ws_port = 0

    @property
    def ws_url(self) -> str:
        return f"ws://127.0.0.1:{self.ws_port}"

    @property
    def rest_url(self) -> str:
        return f"http://127.0.0.1:{self._http.server_port}"

    def __enter__(self):
        self._http_thread.start()
        self._thread.start()
        self._ready.wait(5)
        return self

    def __exit__(self, *exc):
        self._loop.call_soon_threadsafe(self._stop.set)
        self._thread.join(5)
        self._http.shutdown()

    def _run(self):
        asyncio.set_event_loop(self._loop)
        self._loop.run_until_complete(self._serve())

    async def _serve(self):
        self._stop = asyncio.Event()
        async with websockets.serve(self._ws_handler, "127.0.0.1", 0) as server:
            self.ws_port = next(iter(server.sockets)).getsockname()[1]
            self._ready.set()
            await self._stop.wait()

    async def _ws_handler(self, ws):
        self.subscriptions.append(json.loads(await ws.recv()))
        for msg in self.ws_script:
            await ws.send(json.dumps(msg))
        await asyncio.Future()  # keep the connection open

    def _handler(self):
        fake = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                fake.rest_requests.append(self.path)
                url = urlparse(self.path)
                qs = parse_qs(url.query)
                limit = int(qs.get("limit", ["1000"])[0])
                after = int(qs.get("after", [str(fake.rest_max_id + 1)])[0])
                time.sleep(fake.rest_page_delay)
                start = min(after, fake.rest_max_id + 1) - 1
                body = json.dumps([rest_trade(i) for i in range(start, max(start - limit, 0), -1)]).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        return Handler
