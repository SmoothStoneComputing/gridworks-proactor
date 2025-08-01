# ruff: noqa: PLR2004, ERA001
import logging

import pytest
from gwproto import MQTTTopic

from gwproactor.links import StateName
from gwproactor_test import LiveTest
from gwproactor_test.certs import uses_tls
from gwproactor_test.wait import await_for


@pytest.mark.asyncio
async def test_basic_connection(request: pytest.FixtureRequest) -> None:
    async with LiveTest(start_child=True, start_parent=True, request=request) as h:
        await h.await_quiescent_connections()


@pytest.mark.asyncio
async def test_no_parent(request: pytest.FixtureRequest) -> None:
    async with LiveTest(add_child=True, request=request) as h:
        child = h.child
        link_stats = h.child_to_parent_stats
        comm_event_counts = link_stats.comm_event_counts
        link = h.child_to_parent_link

        # unstarted child
        assert link_stats.num_received == 0
        assert link.state == StateName.not_started
        child.logger.info(child.settings.model_dump_json(indent=2))

        # start child
        h.start_child()
        await h.await_for(
            link.active_for_send,
            "ERROR waiting link active_for_send",
        )
        assert not link.active_for_recv()
        assert not link.active()
        assert StateName(link.state) == StateName.awaiting_peer
        assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 1
        assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 1
        assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 0
        child.assert_event_counts(
            num_pending=3,  # 2 comm events + 1 startup event
            num_persists=3,
            num_in_flight=0,
        )
        assert len(link_stats.comm_events) == 2
        for comm_event in link_stats.comm_events:
            assert comm_event.MessageId in child.event_persister

        # Tell client we lost comm.
        child.force_mqtt_disconnect("parent")

        # Wait for reconnect
        await h.await_for(
            lambda: comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] > 1,
            "ERROR waiting link to resubscribe after comm loss",
        )
        assert link.active_for_send()
        assert not link.active_for_recv()
        assert not link.active()
        assert StateName(link.state) == StateName.awaiting_peer
        assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 2
        assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 2
        assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 1
        child.assert_event_counts(
            num_pending=6,  # 5 comm events + 1 startup event
            num_persists=6,
            num_in_flight=0,
        )
        for comm_event in link_stats.comm_events:
            assert comm_event.MessageId in child.event_persister


@pytest.mark.asyncio
async def test_basic_comm_child_first(request: pytest.FixtureRequest) -> None:
    async with LiveTest(add_child=True, add_parent=True, request=request) as h:
        child = h.child
        child_stats = h.child_to_parent_stats
        child_comm_event_counts = child_stats.comm_event_counts
        child_link = h.child_to_parent_link

        # unstarted child, parent
        assert child_stats.num_received == 0
        assert child_link.state == StateName.not_started

        # start child
        h.start_child()
        await h.await_for(
            child_link.active_for_send,
            "ERROR waiting link active_for_send",
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
        h.child.assert_event_counts(
            num_pending=3,  # 2 comm events + 1 startup event
            num_persists=3,
            num_in_flight=0,
        )
        assert len(child_stats.comm_events) == 2
        for comm_event in child_stats.comm_events:
            assert comm_event.MessageId in child.event_persister

        # start parent
        h.start_parent()

        # wait for link to go active
        await h.child_to_parent_active()
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
        assert 0 <= child.links.num_in_flight <= 4

        # wait for all events to be acked
        await h.await_quiescent_connections(exp_child_persists=3)

        activation_count = child_stats.comm_event_counts[
            "gridworks.event.comm.peer.active"
        ]
        # Tell client we lost comm
        child.force_mqtt_disconnect("parent")

        # Wait for reconnect
        await h.await_for(
            lambda: child_stats.comm_event_counts["gridworks.event.comm.peer.active"]
            > activation_count,
            "ERROR waiting link to resubscribe after comm loss",
        )
        err_str = h.summary_str()
        assert child_link.active_for_send(), err_str
        assert child_link.active_for_recv(), err_str
        assert child_link.active(), err_str
        assert StateName(child_link.state) == StateName.active, err_str
        assert (
            child_comm_event_counts["gridworks.event.comm.mqtt.connect"] >= 2
        ), err_str
        assert (
            child_comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] >= 2
        ), err_str
        assert (
            child_comm_event_counts["gridworks.event.comm.mqtt.disconnect"] >= 1
        ), err_str
        assert child_comm_event_counts["gridworks.event.comm.peer.active"] >= 2, err_str
        assert len(child_stats.comm_events) >= 7, err_str
        assert 0 <= child.links.num_in_flight <= 4, err_str

        # wait for all events to be acked
        await h.await_quiescent_connections(
            # peer active event should never be persisted
            exp_child_persists=6,
            # parent pending:
            #   parent-startup, parent-connect, parent-subscribe +
            #   events persisted and then reuploaded by child +
            #   1 peer active event for parent and *2* for child.
            exp_parent_pending=(3 + 6 + 3),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("suppress_tls", [False, True])
async def test_basic_comm_parent_first(
    request: pytest.FixtureRequest, suppress_tls: bool
) -> None:
    async with LiveTest(request=request) as h:
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
        child_stats = h.child_to_parent_stats
        child_comm_event_counts = child_stats.comm_event_counts
        child_link = h.child_to_parent_link
        parent_link = h.parent_to_child_link

        # unstarted parent
        assert parent_link.state == StateName.not_started

        # start parent
        h.start_parent()
        await h.await_for(
            parent_link.active_for_send,
            "ERROR waiting link active_for_send",
        )

        # unstarted child
        assert child_stats.num_received == 0
        assert child_link.state == StateName.not_started

        # start child
        h.start_child()
        await h.await_for(
            child_link.active,
            "ERROR waiting link active",
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
        await h.await_quiescent_connections()


@pytest.mark.asyncio
async def test_basic_parent_comm_loss(request: pytest.FixtureRequest) -> None:
    async with LiveTest(add_child=True, add_parent=True, request=request) as h:
        child = h.child
        child_stats = h.child_to_parent_stats
        child_comm_event_counts = child_stats.comm_event_counts
        child_link = h.child_to_parent_link
        parent = h.parent
        parent_link = h.parent_to_child_link

        # unstarted child, parent
        assert parent_link.state == StateName.not_started
        assert child_stats.num_received == 0
        assert child_link.state == StateName.not_started

        # start child, parent
        h.start_child()
        h.start_parent()
        await h.await_for(
            child_link.active,
            "ERROR waiting link active",
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
        await h.await_quiescent_connections()

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
        await h.await_quiescent_connections(
            exp_child_persists=6,
            exp_parent_pending=sum(
                [
                    1,  # parent startup
                    3,  # parent connect, subscribe, peer active
                    1,  # child startup
                    3,  # child connect, subscribe, peer active
                    4,  # child disconnect, connect, subscribe, peer active
                ]
            ),
        )

        # get ping topic and current number of pings
        parent_ping_topic = MQTTTopic.encode(
            envelope_type="gw",
            src=parent.publication_name,
            dst=child.subscription_name,
            message_type="gridworks-ping",
        )
        num_parent_pings = child_stats.num_received_by_topic[parent_ping_topic]

        # Tell *parent* client we lost comm.
        parent.force_mqtt_disconnect(parent.downstream_client)
        # wait for child to get ping from parent when parent reconnects to mqtt
        await h.await_for(
            lambda: child_stats.num_received_by_topic[parent_ping_topic]
            > num_parent_pings,
            f"ERROR waiting for parent ping {parent_ping_topic}",
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
        h.child.assert_event_counts(
            num_persists=6,
            all_clear=True,
        )

        await h.parent_to_child_active()

        # parent should have persisted:
        exp_events = sum(
            [
                1,  # parent startup
                3,  # parent connect, subscribe, peer active
                1,  # child startup
                3,  # child connect, subscribe, peer active
                4,  # child disconnect, connect, subscribe, peer active
                4,  # parent disconnect, connect, subscribe, peer active
            ]
        )
        # wait for parent to finish persisting
        await h.await_for(
            lambda: h.parent.event_persister.num_persists == exp_events,
            f"ERROR waiting for parent to finish persisting {exp_events} events",
        )

        # Tell *both* clients we lost comm.
        parent.force_mqtt_disconnect(parent.downstream_client)
        child.force_mqtt_disconnect(child.upstream_client)

        # Wait for reconnect
        await h.await_for(
            lambda: child_stats.comm_event_counts["gridworks.event.comm.peer.active"]
            > 2
            and child_link.active(),
            "ERROR waiting link to resubscribe after comm loss",
        )
        assert child_link.active_for_send()
        assert child_link.active_for_recv()
        assert StateName(child_link.state) == StateName.active
        assert child_comm_event_counts["gridworks.event.comm.mqtt.connect"] == 3
        assert (
            child_comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 3
        )
        assert child_comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 2
        assert child_comm_event_counts["gridworks.event.comm.peer.active"] == 3
        assert len(child_stats.comm_events) == 11

        # wait for all events to be acked
        await h.await_quiescent_connections(
            exp_child_persists=9,
            exp_parent_pending=sum(
                [
                    1,  # parent startup
                    3,  # parent connect, subscribe, peer active
                    1,  # child startup
                    3,  # child connect, subscribe, peer active
                    4,  # child disconnect, connect, subscribe, peer active
                    4,  # parent disconnect, connect, subscribe, peer active
                    4,  # child disconnect, connect, subscribe, peer active
                    4,  # parent disconnect, connect, subscribe, peer active
                ]
            ),
        )
