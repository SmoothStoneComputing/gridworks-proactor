# ruff: noqa: PLR2004, ERA001

import asyncio

import pytest
from gwproto import MQTTTopic

from gwproactor.links import StateName
from gwproactor_test.live_test_helper import (
    LiveTest,
)
from gwproactor_test.wait import await_for


@pytest.mark.asyncio
async def test_response_timeout(request: pytest.FixtureRequest) -> None:
    """
    Test:
        (awaiting_peer -> response_timeout -> awaiting_peer)
        (active -> response_timeout -> awaiting_peer)
    """

    async with LiveTest(
        add_child=True,
        add_parent=True,
        request=request,
    ) as h:
        child = h.child
        link = child.links.link(child.upstream_client)
        stats = child.stats.link(child.upstream_client)
        parent = h.parent
        parent_link = parent.links.link(parent.downstream_client)

        # Timeout while awaiting setup
        # (awaiting_peer -> response_timeout -> awaiting_peer)

        # start parent
        parent.pause_acks()
        h.start_parent()
        await await_for(
            lambda: parent_link.in_state(StateName.awaiting_peer),
            3,
            "ERROR waiting for parent to connect to broker",
            err_str_f=h.summary_str,
        )

        # start child
        child.set_ack_timeout_seconds(1)
        assert stats.timeouts == 0
        h.start_child()
        await h.await_for(
            lambda: link.in_state(StateName.awaiting_peer),
            "ERROR waiting for child to connect to broker",
        )
        # (awaiting_peer -> response_timeout -> awaiting_peer)
        await h.await_for(
            lambda: stats.timeouts > 0,
            "ERROR waiting for child to timeout",
        )
        assert link.state == StateName.awaiting_peer
        assert child.event_persister.num_pending == 3
        assert child.links.num_in_flight == 0
        assert child.event_persister.num_persists == 3
        assert child.event_persister.num_retrieves == 0
        assert child.event_persister.num_clears == 0

        # release the hounds
        # (awaiting_peer -> message_from_peer -> active)
        parent.release_acks()
        await await_for(
            lambda: link.in_state(StateName.active),
            1,
            "ERROR waiting for parent to restore link #1",
            err_str_f=h.summary_str,
        )
        # wait for all events to be acked
        await await_for(
            lambda: child.event_persister.num_pending == 0
            and child.links.num_in_flight == 0,
            1,
            "ERROR waiting for events to be acked",
            err_str_f=h.summary_str,
        )
        assert child.event_persister.num_pending == 0
        assert child.links.num_in_flight == 0
        assert child.event_persister.num_persists == 3
        assert child.event_persister.num_retrieves == 3
        assert child.event_persister.num_clears == 3

        # Timeout while active
        # (active -> response_timeout -> awaiting_peer)
        parent.pause_acks()
        child.force_ping(child.upstream_client)
        child_acks_map = child.links.ack_manager._acks  # noqa: SLF001
        child_acks = child_acks_map[child.upstream_client]
        exp_timeouts = int(stats.timeouts) + len(child_acks)
        await await_for(
            lambda: stats.timeouts == exp_timeouts,
            10,
            f"ERROR waiting for child to timeout, exp_timeouts: {exp_timeouts}",
            err_str_f=h.summary_str,
        )
        assert link.state == StateName.awaiting_peer
        assert child.event_persister.num_pending == 1
        assert child.links.num_in_flight == 0
        assert child.event_persister.num_persists == 4
        assert child.event_persister.num_retrieves == 3
        assert child.event_persister.num_clears == 3
        await await_for(
            lambda: len(parent.needs_ack) >= 1,
            1,
            "ERROR waiting for parent to have messages to be send",
            err_str_f=h.summary_str,
        )

        # (awaiting_peer -> message_from_peer -> active)
        parent.release_acks()
        await await_for(
            lambda: link.in_state(StateName.active)
            and child.links.num_in_flight == 0
            and child.links.num_pending == 0,
            1,
            "ERROR waiting for parent to restore link #1",
            err_str_f=h.summary_str,
        )
        assert child.event_persister.num_persists == 4
        assert child.event_persister.num_retrieves == 4
        assert child.event_persister.num_clears == 4

        # parent should have persisted:
        exp_events = sum(
            [
                1,  # parent startup
                3,  # parent connect, subscribe, peer active
                1,  # child startup
                3,  # child connect, subscribe, peer active
                2,  # child timeout, peer active
            ]
        )
        # wait for parent to finish persisting
        await await_for(
            lambda: h.parent.event_persister.num_persists == exp_events,
            3,
            f"ERROR waiting for parent to finish persisting {exp_events} events",
            err_str_f=h.summary_str,
        )


@pytest.mark.asyncio
async def test_ping(request: pytest.FixtureRequest) -> None:
    """
    Test:
        ping sent peridoically if no messages sent
        ping not sent if messages are sent
        ping restores comm
    """
    from gwproactor import ProactorSettings
    from gwproactor_test.dummies.pair.child import DummyChildSettings

    async with LiveTest(
        add_child=True,
        add_parent=True,
        child_app_settings=DummyChildSettings(
            proactor=ProactorSettings(mqtt_link_poll_seconds=0.1),
        ),
        request=request,
    ) as h:
        parent = h.parent
        parent_stats = parent.stats.link(parent.downstream_client)
        child = h.child
        # noinspection PyTypeChecker
        pings_from_parent_topic = MQTTTopic.encode(
            envelope_type="gw",
            src=parent.publication_name,
            dst=parent.links.topic_dst(parent.downstream_client),
            message_type="gridworks-ping",
        )
        child.disable_derived_events()
        child.set_ack_timeout_seconds(1)
        link = child.links.link(child.upstream_client)
        stats = child.stats.link(child.upstream_client)
        ping_from_child_topic = MQTTTopic.encode(
            envelope_type="gw",
            src=child.publication_name,
            dst=child.links.topic_dst(child.downstream_client),
            message_type="gridworks-ping",
        )
        # start parent and child
        h.start_parent()
        h.start_child()
        await await_for(
            lambda: link.in_state(StateName.active)
            and child.links.num_in_flight == 0
            and child.links.num_pending == 0,
            3,
            "ERROR waiting for child active",
            err_str_f=h.summary_str,
        )
        assert child.event_persister.num_persists == 3
        assert child.event_persister.num_retrieves == 3
        assert child.event_persister.num_clears == 3

        # parent should have persisted:
        exp_events = sum(
            [
                1,  # parent startup
                3,  # parent connect, subscribe, peer active
                1,  # child startup
                3,  # child connect, subscribe, peer active
            ]
        )
        # wait for parent to finish persisting
        await await_for(
            lambda: h.parent.event_persister.num_persists == exp_events,
            3,
            f"ERROR waiting for parent to finish persisting {exp_events} events",
            err_str_f=h.summary_str,
        )

        # Test that ping sent peridoically if no messages sent
        start_pings_from_parent = stats.num_received_by_topic[pings_from_parent_topic]
        start_pings_from_child = parent_stats.num_received_by_topic[
            ping_from_child_topic
        ]
        start_messages_from_parent = stats.num_received
        start_messages_from_child = parent_stats.num_received
        wait_seconds = 0.5
        await asyncio.sleep(wait_seconds)
        pings_from_parent = (
            stats.num_received_by_topic[pings_from_parent_topic]
            - start_pings_from_parent
        )
        pings_from_child = (
            parent_stats.num_received_by_topic[ping_from_child_topic]
            - start_pings_from_child
        )
        messages_from_parent = stats.num_received - start_messages_from_parent
        messages_from_child = parent_stats.num_received - start_messages_from_child
        exp_pings_nominal = (
            wait_seconds / parent.settings.proactor.mqtt_link_poll_seconds
        ) - 1
        err_str = (
            f"\npings_from_parent: {pings_from_parent}  ({stats.num_received_by_topic[pings_from_parent_topic]} - {start_pings_from_parent})  on <{pings_from_parent_topic}>\n"
            f"messages_from_parent: {messages_from_parent}\n"
            f"pings_from_child: {pings_from_child}  ({parent_stats.num_received_by_topic[ping_from_child_topic]} - {start_pings_from_child})  on {ping_from_child_topic}\n"
            f"messages_from_child: {messages_from_child}\n"
            f"exp_pings_nominal: {exp_pings_nominal}\n"
            f"\n{h.summary_str()}\n"
        )
        assert (pings_from_child + pings_from_parent) >= exp_pings_nominal, err_str
        assert messages_from_child >= exp_pings_nominal, err_str
        assert messages_from_parent >= exp_pings_nominal, err_str

        # Test that ping not sent peridoically if messages are sent
        start_pings_from_parent = stats.num_received_by_topic[pings_from_parent_topic]
        start_pings_from_child = parent_stats.num_received_by_topic[
            ping_from_child_topic
        ]
        start_messages_from_parent = stats.num_received
        start_messages_from_child = parent_stats.num_received
        reps = 50
        for _ in range(reps):
            parent.send_dbg(parent.downstream_client)
            await asyncio.sleep(0.01)
        pings_from_parent = (
            stats.num_received_by_topic[pings_from_parent_topic]
            - start_pings_from_parent
        )
        pings_from_child = (
            parent_stats.num_received_by_topic[ping_from_child_topic]
            - start_pings_from_child
        )
        messages_from_parent = stats.num_received - start_messages_from_parent
        messages_from_child = parent_stats.num_received - start_messages_from_child
        exp_pings_nominal = 2
        err_str = (
            f"\npings_from_parent: {pings_from_parent}  ({stats.num_received_by_topic[pings_from_parent_topic]} - {start_pings_from_parent})  on <{pings_from_parent_topic}>\n"
            f"messages_from_parent: {messages_from_parent}\n"
            f"pings_from_child: {pings_from_child}  ({parent_stats.num_received_by_topic[ping_from_child_topic]} - {start_pings_from_child})  on {ping_from_child_topic}\n"
            f"messages_from_child: {messages_from_child}\n"
            f"exp_pings_nominal: {exp_pings_nominal}\n"
            f"reps: {reps}, {reps * 0.5}\n"
            f"\n{h.summary_str()}\n"
        )
        assert pings_from_parent <= exp_pings_nominal, err_str
        assert pings_from_child <= exp_pings_nominal, err_str
        # Allow wide variance in number of messages exchanged - we are really testing pings, which
        # Should be should be close to 0 when a lot of messages are being exchanged.
        assert messages_from_parent >= reps * 0.5, err_str
        assert messages_from_child >= reps * 0.5, err_str

        # wait for all in-flight events to be acked
        # parent should have persisted:
        exp_events = sum(
            [
                1,  # parent startup
                3,  # parent connect, subscribe, peer active
                1,  # child startup
                3,  # child connect, subscribe, peer active
                reps,  # dbg events
            ]
        )
        await h.await_quiescent_connections(
            exp_child_persists=3,
            exp_parent_pending=exp_events,
        )
        last_parent_persists = parent.event_persister.num_persists

        parent.pause_acks()
        await await_for(
            lambda: link.in_state(StateName.awaiting_peer),
            child.links.ack_manager.default_delay_seconds + 1,
            "ERROR waiting for for parent to be slow",
            err_str_f=h.summary_str,
        )
        parent.release_acks(clear=True)
        await await_for(
            lambda: link.in_state(StateName.active)
            and child.links.num_pending == 0
            and child.links.num_in_flight == 0,
            1,
            "ERROR waiting for parent to respond",
            err_str_f=h.summary_str,
        )
        assert child.event_persister.num_persists >= 4
        assert child.event_persister.num_retrieves == child.event_persister.num_persists
        assert child.event_persister.num_clears == child.event_persister.num_persists

        # wait for parent to finish persisting
        exp_parent_persists = last_parent_persists + 2  # (child timeout, peer active)
        await h.await_for(
            lambda: h.parent.event_persister.num_persists >= exp_parent_persists,
            f"ERROR waiting for parent to finish persisting {exp_parent_persists} events",
        )
