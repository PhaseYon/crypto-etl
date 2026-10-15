"""Test doubles for unit tests (no network, no Kafka)."""

import time


class FakeProducer:
    """Records produced messages. Optionally raises BufferError for the first `full_for` calls."""

    def __init__(self, full_for: int = 0):
        self.messages: list[tuple[str, str, str]] = []
        self.full_for = full_for
        self.polls = 0

    def produce(self, topic, key, value, on_delivery=None):
        if self.full_for > 0:
            self.full_for -= 1
            raise BufferError("queue full")
        self.messages.append((topic, key, value))

    def poll(self, timeout=0):
        self.polls += 1
        return 0

    def flush(self, timeout=None):
        return 0

    def __len__(self):
        return 0


def make_fetch(available_ids: range, page_size: int = 1000, delay: float = 0.0, fail_times: int = 0):
    """A fake REST fetch over `available_ids`: trades with id < before_id, newest first.

    Raises ConnectionError on its first `fail_times` calls. Records each before_id in `.calls`.
    """
    state = {"fails": fail_times}

    def fetch(product: str, before_id: int) -> list[dict]:
        fetch.calls.append(before_id)
        if delay:
            time.sleep(delay)
        if state["fails"] > 0:
            state["fails"] -= 1
            raise ConnectionError("boom")
        ids = [i for i in range(before_id - 1, available_ids.start - 1, -1) if i in available_ids][:page_size]
        return [{"trade_id": i, "side": "buy", "size": "1", "price": "10", "time": "t"} for i in ids]

    fetch.calls = []
    return fetch
