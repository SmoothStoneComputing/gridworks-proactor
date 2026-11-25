"""A link that cannot reach its broker retries on one proactor-owned schedule,
whether it never connected or its connection dropped: full-jitter waits, at
most 30 s apart, reset by an accepting CONNACK. The keepalive the link
connects with is the configured one."""

# ruff: noqa: S311 -- seeded generators make the jitter reproducible here
import random
import socket
import threading
from typing import Any

import pytest

from gwproactor import config
from gwproactor.config.mqtt import TLSInfo
from gwproactor.links import StateName, mqtt
from gwproactor.links.mqtt import MQTTClientWrapper
from gwproactor.links.reconnect_backoff import MAX_RECONNECT_WAIT_S, ReconnectBackoff
from gwproactor.sync_thread import AsyncQueueWriter
from gwproactor_test.live_test_helper import LiveTest
from gwproactor_test.wait import await_for

RECORDED_WAITS = 12
"""Enough outer-loop waits to pass the 30 s cap by doubling from 1 s."""
CONNECTS_AFTER_RECONNECT = 2


class DiscardingQueueWriter(AsyncQueueWriter):
    """Accepts the thread's problem messages without an event loop."""

    def put(self, item: Any) -> None:
        pass


def closed_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def wrapper_for_closed_port(keepalive: int = 30) -> MQTTClientWrapper:
    return MQTTClientWrapper(
        client_name="nowhere",
        topic_dst="nowhere",
        client_config=config.MQTTClient(
            host="127.0.0.1",
            port=closed_port(),
            keepalive=keepalive,
            tls=TLSInfo(use_tls=False),
        ),
        receive_queue=DiscardingQueueWriter(),
        client_id="test-reconnect-backoff",
    )


def test_schedule_doubles_to_the_cap_with_full_jitter() -> None:
    backoff = ReconnectBackoff(MAX_RECONNECT_WAIT_S, rng=random.Random(1))
    step = 1.0
    for _ in range(RECORDED_WAITS):
        wait = backoff.next_wait()
        assert 0 <= wait <= step
        step = min(step * 2, MAX_RECONNECT_WAIT_S)
    assert backoff.step_s == MAX_RECONNECT_WAIT_S
    backoff.reset()
    assert backoff.next_wait() <= 1.0


def test_schedule_differs_between_links() -> None:
    waits = [
        [
            ReconnectBackoff(MAX_RECONNECT_WAIT_S, rng=random.Random(seed)).next_wait()
            for _ in range(4)
        ]
        for seed in range(2)
    ]
    assert waits[0] != waits[1]


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
    assert max(waits) <= MAX_RECONNECT_WAIT_S, waits


def test_link_connects_with_the_configured_keepalive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    keepalive = 17
    seen: list[int] = []
    connected = threading.Event()
    wrapper = wrapper_for_closed_port(keepalive=keepalive)

    def record_connect(*args: Any, **kwargs: Any) -> None:
        seen.append(kwargs["keepalive"])
        connected.set()
        wrapper.stop()
        raise ConnectionRefusedError

    monkeypatch.setattr(wrapper._client, "connect", record_connect)  # noqa: SLF001
    wrapper.start()
    assert connected.wait(timeout=5)
    assert seen == [keepalive]


@pytest.mark.asyncio
async def test_dropped_link_reconnects_on_the_proactor_schedule(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    waits: list[float] = []

    def record_wait(obj: Any, seconds: float, **kwargs: Any) -> None:
        waits.append(seconds)

    monkeypatch.setattr(mqtt, "responsive_sleep", record_wait)
    async with LiveTest(add_child=True, add_parent=True, request=request) as h:
        child = h.child
        link = child.links.link(child.upstream_client)
        h.start_child()
        await await_for(
            lambda: link.state not in (StateName.not_started, StateName.connecting),
            3,
            "ERROR waiting for first connect",
            err_str_f=h.summary_str,
        )
        assert waits == []
        child.force_mqtt_disconnect(child.upstream_client)
        await await_for(
            lambda: child.stats.link(child.upstream_client).comm_event_counts[
                "gridworks.event.comm.mqtt.connect"
            ]
            == CONNECTS_AFTER_RECONNECT,
            3,
            "ERROR waiting for reconnect",
            err_str_f=h.summary_str,
        )
        # the drop went through the client thread's schedule, once, from step 1
        assert len(waits) == 1
        assert 0 <= waits[0] <= 1.0
