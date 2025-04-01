# ruff: noqa: PLR2004, ERA001
import logging
from typing import Any

import pytest
from gwproto import MQTTTopic

from gwproactor.links import StateName
from gwproactor_test import CommTestHelper
from gwproactor_test.certs import uses_tls
from gwproactor_test.wait import await_for


@pytest.mark.asyncio
async def test_no_parent(request: Any) -> None:
    async with CommTestHelper(add_child=True, request=request) as h:
        child = h.child
        stats = child.stats.link(child.upstream_client)
        comm_event_counts = stats.comm_event_counts
        link = child.links.link(child.upstream_client)

        # unstarted child
        assert stats.num_received == 0
        assert link.state == StateName.not_started
        child.logger.info(child.settings.model_dump_json(indent=2))

        # start child
        h.start_child()
        await await_for(
            link.active_for_send,
            1,
            "ERROR waiting link active_for_send",
            err_str_f=child.summary_str,
        )
        assert not link.active_for_recv()
        assert not link.active()
        assert StateName(link.state) == StateName.awaiting_peer
        assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 1
        assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 1
        assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 0
        assert len(stats.comm_events) == 2
        for comm_event in stats.comm_events:
            assert comm_event.MessageId in child.event_persister

        # Tell client we lost comm.
        child.force_mqtt_disconnect("parent")

        # Wait for reconnect
        await await_for(
            lambda: stats.comm_event_counts[
                "gridworks.event.comm.mqtt.fully.subscribed"
            ]
            > 1,
            3,
            "ERROR waiting link to resubscribe after comm loss",
            err_str_f=child.summary_str,
        )
        assert link.active_for_send()
        assert not link.active_for_recv()
        assert not link.active()
        assert StateName(link.state) == StateName.awaiting_peer
        assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 2
        assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 2
        assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 1
        assert len(stats.comm_events) == 5
        for comm_event in stats.comm_events:
            assert comm_event.MessageId in child.event_persister


@pytest.mark.asyncio
async def test_basic_comm_child_first(request: Any) -> None:
    async with CommTestHelper(add_child=True, add_parent=True, request=request) as h:
        child = h.child
        child_stats = child.stats.link(child.upstream_client)
        child_comm_event_counts = child_stats.comm_event_counts
        child_link = child.links.link(child.upstream_client)

        # unstarted child, parent
        assert child_stats.num_received == 0
        assert child_link.state == StateName.not_started

        # start child
        h.start_child()
        await await_for(
            child_link.active_for_send,
            1,
            "ERROR waiting link active_for_send",
            err_str_f=child.summary_str,
        )
        assert not child_link.active_for_recv()
        assert not child_link.active()
        assert StateName(child_link.state) == StateName.awaiting_peer
        assert child_comm_event_counts["gridworks.event.comm.mqtt.connect"] == 1
        assert (
            child_comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 1
        )
        assert child_comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 0
        assert child_comm_event_counts["gridworks.event.comm.peer.active"] == 0
        assert len(child_stats.comm_events) == 2
        for comm_event in child_stats.comm_events:
            assert comm_event.MessageId in child.event_persister

        # start parent
        h.start_parent()

        # wait for link to go active
        await await_for(
            child_link.active,
            10,
            "ERROR waiting link active",
            err_str_f=child.summary_str,
        )
        assert child_link.active_for_recv()
        assert child_link.active()
        assert StateName(child_link.state) == StateName.active
        assert child_comm_event_counts["gridworks.event.comm.mqtt.connect"] == 1
        assert (
            child_comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 1
        )
        assert child_comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 0
        assert child_comm_event_counts["gridworks.event.comm.peer.active"] == 1
        assert len(child_stats.comm_events) == 3

        # wait for all events to be acked
        await await_for(
            lambda: child.event_persister.num_pending == 0,
            1,
            "ERROR waiting for events to be acked",
            err_str_f=child.summary_str,
        )

        # Tell client we lost comm.
        child.force_mqtt_disconnect("parent")

        # Wait for reconnect
        await await_for(
            lambda: child_stats.comm_event_counts["gridworks.event.comm.peer.active"]
            > 1,
            3,
            "ERROR waiting link to resubscribe after comm loss",
            err_str_f=child.summary_str,
        )
        assert child_link.active_for_send()
        assert child_link.active_for_recv()
        assert child_link.active()
        assert StateName(child_link.state) == StateName.active
        assert child_comm_event_counts["gridworks.event.comm.mqtt.connect"] == 2
        assert (
            child_comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 2
        )
        assert child_comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 1
        assert child_comm_event_counts["gridworks.event.comm.peer.active"] == 2
        assert len(child_stats.comm_events) == 7

        # wait for all events to be acked
        await await_for(
            lambda: child.event_persister.num_pending == 0,
            1,
            "ERROR waiting for events to be acked",
            err_str_f=child.summary_str,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("suppress_tls", [False, True])
async def test_basic_comm_parent_first(request: Any, suppress_tls: bool) -> None:
    async with CommTestHelper(request=request) as h:
        child_settings = h.child_app.config.settings
        parent_settings = h.parent_app.config.settings
        base_logger = logging.getLogger(child_settings.logging.base_log_name)
        base_logger.warning(f"{request.node.name}  suppress_tls: {suppress_tls}")
        if suppress_tls:
            if not uses_tls(child_settings) and not uses_tls(
                parent_settings,
            ):
                base_logger.warning(
                    "Skipping test <%s> since TLS has already been suppressed by environment variables",
                    request.node.name,
                )
            else:
                h.set_use_tls(False)
        h.add_child()
        h.add_parent()
        child = h.child
        child_stats = child.stats.link(child.upstream_client)
        child_comm_event_counts = child_stats.comm_event_counts
        child_link = child.links.link(child.upstream_client)
        parent = h.parent
        parent_link = parent.links.link(parent.downstream_client)

        # unstarted parent
        assert parent_link.state == StateName.not_started

        # start parent
        h.start_parent()
        await await_for(
            parent_link.active_for_send,
            1,
            "ERROR waiting link active_for_send",
            err_str_f=parent.summary_str,
        )

        # unstarted child
        assert child_stats.num_received == 0
        assert child_link.state == StateName.not_started

        # start child
        h.start_child()
        await await_for(
            child_link.active,
            1,
            "ERROR waiting link active",
            err_str_f=parent.summary_str,
        )

        assert child_link.active_for_recv()
        assert child_link.active()
        assert StateName(child_link.state) == StateName.active
        assert child_comm_event_counts["gridworks.event.comm.mqtt.connect"] == 1

        assert (
            child_comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 1
        )
        assert child_comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 0
        assert child_comm_event_counts["gridworks.event.comm.peer.active"] == 1
        assert len(child_stats.comm_events) == 3

        # wait for all events to be acked
        await await_for(
            lambda: child.event_persister.num_pending == 0,
            1,
            "ERROR waiting for events to be acked",
            err_str_f=child.summary_str,
        )


@pytest.mark.asyncio
async def test_basic_parent_comm_loss(request: Any) -> None:
    async with CommTestHelper(add_child=True, add_parent=True, request=request) as h:
        child = h.child
        child_stats = child.stats.link(child.upstream_client)
        child_comm_event_counts = child_stats.comm_event_counts
        child_link = child.links.link(child.upstream_client)
        parent = h.parent
        parent_link = parent.links.link(parent.downstream_client)

        # unstarted child, parent
        assert parent_link.state == StateName.not_started
        assert child_stats.num_received == 0
        assert child_link.state == StateName.not_started

        # start child, parent
        h.start_child()
        h.start_parent()
        await await_for(
            child_link.active,
            1,
            "ERROR waiting link active",
            err_str_f=child.summary_str,
        )
        assert child_link.active_for_recv()
        assert child_link.active()
        assert StateName(child_link.state) == StateName.active
        assert child_comm_event_counts["gridworks.event.comm.mqtt.connect"] == 1
        assert (
            child_comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 1
        )
        assert child_comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 0
        assert child_comm_event_counts["gridworks.event.comm.peer.active"] == 1
        assert len(child_stats.comm_events) == 3

        # wait for all events to be acked
        await await_for(
            lambda: child.event_persister.num_pending == 0,
            1,
            "ERROR waiting for events to be acked",
            err_str_f=child.summary_str,
        )

        # Tell *child* client we lost comm.
        child.force_mqtt_disconnect(child.upstream_client)

        # Wait for reconnect
        await await_for(
            lambda: child_stats.comm_event_counts["gridworks.event.comm.peer.active"]
            > 1,
            3,
            "ERROR waiting link to resubscribe after comm loss",
            err_str_f=child.summary_str,
        )
        assert child_link.active_for_send()
        assert child_link.active_for_recv()
        assert child_link.active()
        assert StateName(child_link.state) == StateName.active
        assert child_comm_event_counts["gridworks.event.comm.mqtt.connect"] == 2
        assert (
            child_comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 2
        )
        assert child_comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 1
        assert child_comm_event_counts["gridworks.event.comm.peer.active"] == 2
        assert len(child_stats.comm_events) == 7

        # wait for all events to be acked
        await await_for(
            lambda: child.event_persister.num_pending == 0,
            1,
            "ERROR waiting for events to be acked",
            err_str_f=child.summary_str,
        )

        # Tell *parent* client we lost comm.
        parent.force_mqtt_disconnect(parent.downstream_client)
        # wait for child to get ping from parent when parent reconnects to mqtt
        # noinspection PyTypeChecker
        parent_ping_topic = MQTTTopic.encode(
            envelope_type="gw",
            src=parent.publication_name,
            dst=child.subscription_name,
            message_type="gridworks-ping",
        )
        num_parent_pings = child_stats.num_received_by_topic[parent_ping_topic]
        await await_for(
            lambda: child_stats.num_received_by_topic[parent_ping_topic]
            == num_parent_pings + 1,
            3,
            f"ERROR waiting for parent ping {parent_ping_topic}",
            err_str_f=child.summary_str,
        )
        # verify no child comm state change has occurred.
        err_str = f"\n{child.summary_str()}\n" f"{parent.summary_str()}\n"
        assert child_link.active_for_send()
        assert child_link.active_for_recv()
        assert child_link.active()
        assert StateName(child_link.state) == StateName.active, err_str
        assert (
            child_comm_event_counts["gridworks.event.comm.mqtt.connect"] == 2
        ), err_str
        assert (
            child_comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 2
        ), err_str
        assert (
            child_comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 1
        ), err_str
        assert child_comm_event_counts["gridworks.event.comm.peer.active"] == 2, err_str
        assert len(child_stats.comm_events) == 7, err_str
        assert child.event_persister.num_pending == 0, err_str

        # Tell *both* clients we lost comm.
        parent.force_mqtt_disconnect(parent.downstream_client)
        child.force_mqtt_disconnect(child.upstream_client)

        # Wait for reconnect
        await await_for(
            lambda: child_stats.comm_event_counts["gridworks.event.comm.peer.active"]
            > 2,
            3,
            "ERROR waiting link to resubscribe after comm loss",
            err_str_f=child.summary_str,
        )
        assert child_link.active_for_send()
        assert child_link.active_for_recv()
        assert child_link.active()
        assert StateName(child_link.state) == StateName.active
        assert child_comm_event_counts["gridworks.event.comm.mqtt.connect"] == 3
        assert (
            child_comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 3
        )
        assert child_comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 2
        assert child_comm_event_counts["gridworks.event.comm.peer.active"] == 3
        assert len(child_stats.comm_events) == 11

        # wait for all events to be acked
        await await_for(
            lambda: child.event_persister.num_pending == 0,
            1,
            "ERROR waiting for events to be acked",
            err_str_f=child.summary_str,
        )
