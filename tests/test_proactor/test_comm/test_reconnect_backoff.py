"""A link that cannot reach its broker retries at most 30 s apart, on both
retry paths: the client thread's outer loop, which owns a link that has never
connected, and paho's own reconnect inside loop_forever, which owns a link
whose connection dropped."""

import socket
import threading
from typing import Any

import pytest

from gwproactor import config
from gwproactor.config.mqtt import TLSInfo
from gwproactor.links import mqtt
from gwproactor.links.mqtt import MQTTClientWrapper
from gwproactor.sync_thread import AsyncQueueWriter

MAX_RETRY_WAIT_S = 30
RECORDED_WAITS = 12
"""Enough outer-loop waits to pass the 30 s cap by doubling from 1 s."""


class DiscardingQueueWriter(AsyncQueueWriter):
    """Accepts the thread's problem messages without an event loop."""

    def put(self, item: Any) -> None:
        pass


def closed_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def wrapper_for_closed_port() -> MQTTClientWrapper:
    return MQTTClientWrapper(
        client_name="nowhere",
        topic_dst="nowhere",
        client_config=config.MQTTClient(
            host="127.0.0.1",
            port=closed_port(),
            tls=TLSInfo(use_tls=False),
        ),
        receive_queue=DiscardingQueueWriter(),
        client_id="test-reconnect-backoff",
    )


def test_never_connected_link_waits_at_most_30s_between_attempts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    waits: list[float] = []
    enough = threading.Event()

    def record_wait(obj: Any, seconds: float, **kwargs: Any) -> None:
        waits.append(seconds)
        if len(waits) >= RECORDED_WAITS:
            enough.set()
            obj.stop()

    monkeypatch.setattr(mqtt, "responsive_sleep", record_wait)
    wrapper = wrapper_for_closed_port()
    wrapper.start()
    assert enough.wait(timeout=30), f"only {len(waits)} connect attempts"
    assert len(waits) >= RECORDED_WAITS
    assert max(waits) <= MAX_RETRY_WAIT_S, waits


def test_dropped_link_paho_reconnect_capped_at_30s() -> None:
    # paho exposes the reconnect delay only through reconnect_delay_set, so the
    # configured cap is read back from the attribute that setter writes.
    wrapper = wrapper_for_closed_port()
    assert wrapper._client._reconnect_max_delay <= MAX_RETRY_WAIT_S  # noqa: SLF001
