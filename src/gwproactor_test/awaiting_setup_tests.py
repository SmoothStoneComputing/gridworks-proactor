# ruff: noqa: PLR2004, ERA001
# mypy: disable-error-code="union-attr"
import typing
import warnings
from typing import Any, Type

import pytest
from gwproto import MQTTTopic
from paho.mqtt.client import MQTT_ERR_CONN_LOST

from gwproactor.links import StateName
from gwproactor.message import DBGPayload
from gwproactor_test.comm_test_helper import (
    ChildSettingsT,
    ChildT,
    CommTestHelper,
    ParentSettingsT,
    ParentT,
)
from gwproactor_test.wait import await_for


@pytest.mark.asyncio
class ProactorCommAwaitingSetupTests(
    typing.Generic[ParentT, ChildT, ParentSettingsT, ChildSettingsT]
):
    CTH: Type[CommTestHelper[ParentT, ChildT, ParentSettingsT, ChildSettingsT]]

    @pytest.mark.asyncio
    async def test_awaiting_setup_and_peer(self) -> None:
        """
        Test:
         (connecting -> connected -> awaiting_setup_and_peer)
         (awaiting_setup_and_peer -> mqtt_suback -> awaiting_peer)
         (awaiting_setup_and_peer -> disconnected -> connecting)
        """
        async with self.CTH(add_child=True) as h:
            child = h.child
            stats = child.stats.link(child.upstream_client)
            comm_event_counts = stats.comm_event_counts
            link = child.links.link(child.upstream_client)

            # unstarted child
            assert stats.num_received == 0
            assert link.state == StateName.not_started

            # start child
            child.pause_upstream_subacks()
            h.start_child()
            await await_for(
                lambda: child.num_upstream_subacks_available() == 1,
                1,
                "ERROR waiting suback pending",
                err_str_f=h.summary_str,
            )
            assert not link.active_for_send()
            assert not link.active_for_recv()
            assert not link.active()
            assert link.state == StateName.awaiting_setup_and_peer
            assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 1
            assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 0
            assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 0
            assert len(stats.comm_events) == 1
            for comm_event in stats.comm_events:
                assert comm_event.MessageId in child.event_persister

            # Allow suback to arrive
            child.release_upstream_subacks()
            await await_for(
                lambda: link.in_state(StateName.awaiting_peer),
                1,
                "ERROR waiting mqtt_suback",
                err_str_f=h.summary_str,
            )
            assert link.active_for_send()
            assert not link.active_for_recv()
            assert not link.active()
            assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 1
            assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 1
            assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 0
            assert len(stats.comm_events) == 2
            for comm_event in stats.comm_events:
                assert comm_event.MessageId in child.event_persister

            # Tell client we lost comm
            child.pause_upstream_subacks()
            child.mqtt_client_wrapper(  # noqa: SLF001
                child.upstream_client
            ).mqtt_client._loop_rc_handle(MQTT_ERR_CONN_LOST)
            await await_for(
                lambda: child.num_upstream_subacks_available() == 1,
                3,
                "ERROR waiting suback pending",
                err_str_f=h.summary_str,
            )
            assert not link.active_for_send()
            assert not link.active_for_recv()
            assert not link.active()
            assert link.state == StateName.awaiting_setup_and_peer
            assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 2
            assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 1
            assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 1
            assert len(stats.comm_events) == 4
            for comm_event in stats.comm_events:
                assert comm_event.MessageId in child.event_persister

            # Tell client we lost comm
            child.clear_upstream_subacks()
            child.mqtt_client_wrapper(  # noqa: SLF001
                child.upstream_client
            ).mqtt_client._loop_rc_handle(MQTT_ERR_CONN_LOST)
            await await_for(
                lambda: len(stats.comm_events) > 4,
                1,
                "ERROR waiting comm fail",
                err_str_f=h.summary_str,
            )
            await await_for(
                lambda: link.in_state(StateName.awaiting_setup_and_peer),
                3,
                "ERROR waiting comm restore",
                err_str_f=h.summary_str,
            )
            assert not link.active_for_send()
            assert not link.active_for_recv()
            assert not link.active()
            assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 3
            assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 1
            assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 2
            assert len(stats.comm_events) == 6
            for comm_event in stats.comm_events:
                assert comm_event.MessageId in child.event_persister

            # Allow suback to arrive
            child.release_upstream_subacks()
            await await_for(
                lambda: link.in_state(StateName.awaiting_peer),
                1,
                "ERROR waiting mqtt_suback",
                err_str_f=h.summary_str,
            )
            assert link.active_for_send()
            assert not link.active_for_recv()
            assert not link.active()
            assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 3
            assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 2
            assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 2
            assert len(stats.comm_events) == 7
            for comm_event in stats.comm_events:
                assert comm_event.MessageId in child.event_persister

    @pytest.mark.asyncio
    async def test_awaiting_setup_and_peer_corner_cases(self, request: Any) -> None:
        """
        Test corner cases:
         (connecting -> connected -> awaiting_setup_and_peer)
         (awaiting_setup_and_peer -> mqtt_suback -> awaiting_setup_and_peer)
         (awaiting_setup_and_peer -> mqtt_suback -> awaiting_peer)
         (awaiting_setup_and_peer -> message_from_peer -> awaiting_setup)
        Force 1 suback per subscription. By default MQTTClientWrapper packs as many subscriptions as possible into a
        single subscribe message, so by default child only receives a single suback for all subscriptions.
        So that we can test (awaiting_setup_and_peer -> mqtt_suback -> awaiting_setup_and_peer) self-loop transition,
        which might occur if we have too many subscriptions for that to be possible, we force the suback response to
        be split into multiple messages.

        In practice these might be corner cases that rarely or never occur, since by default all subacks will come and
        one message and we should not receive any messages before subscribing.
        """
        async with self.CTH(add_child=True) as h:
            child = h.child
            child_subscriptions = child.mqtt_subscriptions(child.upstream_client)
            if len(child_subscriptions) < 2:
                if h.warn_if_multi_subscription_tests_skipped:
                    warnings.warn(
                        (
                            f"Skipping <{request.node.name}> because configured child proactor <{child.name}> "
                            f"has < 2 subscriptions. Subscriptions: {child_subscriptions}"
                        ),
                        stacklevel=2,
                    )
                return
            stats = child.stats.link(child.upstream_client)
            comm_event_counts = stats.comm_event_counts
            link = child.links.link(child.upstream_client)

            # unstarted child
            assert stats.num_received == 0
            assert link.state == StateName.not_started

            # start child
            child.split_client_subacks(child.upstream_client)
            child.pause_upstream_subacks()
            h.start_child()
            await await_for(
                lambda: child.num_upstream_subacks_available() == 3,
                3,
                "ERROR waiting link reconnect",
                err_str_f=h.summary_str,
            )
            assert link.state == StateName.awaiting_setup_and_peer
            assert not link.active_for_recv()
            assert not link.active()
            assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 1
            assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 0
            assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 0
            assert len(stats.comm_events) == 1
            for comm_event in stats.comm_events:
                assert comm_event.MessageId in child.event_persister

            # Allow one suback at a time to arrive
            # suback 1/3
            # (mqtt_suback -> awaiting_setup_and_peer)
            num_subacks = child.stats.num_received_by_type["mqtt_suback"]
            child.release_upstream_subacks(1)
            exp_subacks = num_subacks + 1
            await await_for(
                lambda: child.stats.num_received_by_type["mqtt_suback"] == exp_subacks,
                1,
                f"ERROR waiting mqtt_suback {exp_subacks} (1/3)",
                err_str_f=child.summary_str,
            )
            assert link.state == StateName.awaiting_setup_and_peer

            # suback 2/3
            # (mqtt_suback -> awaiting_setup_and_peer)
            child.release_upstream_subacks(1)
            exp_subacks += 1
            await await_for(
                lambda: child.stats.num_received_by_type["mqtt_suback"] == exp_subacks,
                1,
                f"ERROR waiting mqtt_suback {exp_subacks} (2/3)",
                err_str_f=child.summary_str,
            )
            assert link.state == StateName.awaiting_setup_and_peer

            # suback 3/3
            # (mqtt_suback -> awaiting_peer)
            child.release_upstream_subacks(1)
            exp_subacks += 1
            await await_for(
                lambda: child.stats.num_received_by_type["mqtt_suback"] == exp_subacks,
                1,
                f"ERROR waiting mqtt_suback {exp_subacks} (3/3)",
                err_str_f=child.summary_str,
            )
            assert link.state == StateName.awaiting_peer
            assert not link.active_for_recv()
            assert not link.active()
            assert link.active_for_send()
            assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 1
            assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 1
            assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 0
            assert len(stats.comm_events) == 2
            for comm_event in stats.comm_events:
                assert comm_event.MessageId in child.event_persister

            # (message_from_peer -> awaiting_setup)
            # Tell client we lost comm
            child.pause_upstream_subacks()
            child.mqtt_client_wrapper(  # noqa: SLF001
                child.upstream_client
            ).mqtt_client._loop_rc_handle(MQTT_ERR_CONN_LOST)
            await await_for(
                lambda: child.num_upstream_subacks_available() == 3,
                3,
                "ERROR waiting suback pending",
                err_str_f=h.summary_str,
            )
            assert not link.active_for_send()
            assert not link.active_for_recv()
            assert not link.active()
            assert link.state == StateName.awaiting_setup_and_peer
            assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 2
            assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 1
            assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 1
            assert len(stats.comm_events) == 4
            for comm_event in stats.comm_events:
                assert comm_event.MessageId in child.event_persister

            # Allow one suback at a time to arrive
            # (Not strictly necessary, since message receiving code does not check if the source topic suback
            #  has arrived).
            num_subacks = child.stats.num_received_by_type["mqtt_suback"]
            child.release_upstream_subacks(1)
            exp_subacks = num_subacks + 1
            await await_for(
                lambda: child.stats.num_received_by_type["mqtt_suback"] == exp_subacks,
                1,
                f"ERROR waiting mqtt_suback {exp_subacks} (1/3)",
                err_str_f=h.summary_str,
            )
            assert link.state == StateName.awaiting_setup_and_peer

            # Start the parent, wait for it to send us a message, which will
            # transition us into awaiting_setup
            h.add_parent()
            h.start_parent()
            await await_for(
                lambda: link.in_state(StateName.awaiting_setup),
                3,
                "ERROR waiting suback pending",
                err_str_f=child.summary_str,
            )
            assert not link.active_for_send()
            assert not link.active_for_recv()
            assert not link.active()
            assert link.state == StateName.awaiting_setup
            assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 2
            assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 1
            assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 1
            assert len(stats.comm_events) == 4

    @pytest.mark.asyncio
    async def test_awaiting_setup2__(self, request: Any) -> None:
        """
        Test awaiting_setup (corner state):
         (awaiting_setup_and_peer -> message_from_peer -> awaiting_setup)
         (awaiting_setup -> mqtt_suback -> awaiting_setup)
         (awaiting_setup -> mqtt_suback -> active)
         (awaiting_setup -> message_from_peer -> awaiting_setup)
         (awaiting_setup -> disconnected -> connecting)
        Force 1 suback per subscription. By default MQTTClientWrapper packs as many subscriptions as possible into a
        single subscribe message, so by default child only receives a single suback for all subscriptions.
        So that we can test (awaiting_setup_and_peer -> mqtt_suback -> awaiting_setup_and_peer) self-loop transition,
        which might occur if we have too many subscriptions for that to be possible, we force the suback response to
        be split into multiple messages.

        In practice these might be corner cases that rarely or never occur, since by default all subacks will come and
        one message and we should not receive any messages before subscribing.
        """
        async with self.CTH(add_child=True, add_parent=True) as h:
            child = h.child
            child_subscriptions = child.mqtt_subscriptions(child.upstream_client)
            if len(child_subscriptions) < 2:
                if h.warn_if_multi_subscription_tests_skipped:
                    warnings.warn(
                        (
                            f"Skipping <{request.node.name}> because configured child proactor <{child.name}> "
                            f"has < 2 subscriptions. Subscriptions: {child_subscriptions}"
                        ),
                        stacklevel=2,
                    )
                return
            stats = child.stats.link(child.upstream_client)
            comm_event_counts = stats.comm_event_counts
            link = child.links.link(child.upstream_client)

            parent = h.parent

            # unstarted child
            assert stats.num_received == 0
            assert link.state == StateName.not_started

            # start child
            # (not_started -> started -> connecting)
            # (connecting -> connected -> awaiting_setup_and_peer)
            child.split_client_subacks(child.upstream_client)
            child.pause_upstream_subacks()
            h.start_child()
            await await_for(
                lambda: child.num_upstream_subacks_available() == 3,
                3,
                "ERROR waiting link reconnect",
                err_str_f=h.summary_str,
            )
            assert link.state == StateName.awaiting_setup_and_peer
            assert not link.active_for_recv()
            assert not link.active()
            assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 1
            assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 0
            assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 0
            assert len(stats.comm_events) == 1
            for comm_event in stats.comm_events:
                assert comm_event.MessageId in child.event_persister

            # Allow one suback at a time to arrive
            # (Not strictly necessary, since message receiving code does not check if the source topic suback
            #  has arrived).
            num_subacks = child.stats.num_received_by_type["mqtt_suback"]
            child.release_upstream_subacks(1)
            exp_subacks = num_subacks + 1
            await await_for(
                lambda: child.stats.num_received_by_type["mqtt_suback"] == exp_subacks,
                1,
                f"ERROR waiting mqtt_suback {exp_subacks} (1/3)",
                err_str_f=h.summary_str,
            )
            assert link.state == StateName.awaiting_setup_and_peer

            # (awaiting_setup_and_peer -> message_from_peer -> awaiting_setup)
            # Start the parent, wait for it to send us a message, which will
            # transition us into awaiting_setup
            h.start_parent()

            await await_for(
                lambda: link.in_state(StateName.awaiting_setup),
                3,
                "ERROR waiting suback pending",
                err_str_f=h.summary_str,
            )
            assert not link.active_for_send()
            assert not link.active_for_recv()
            assert not link.active()
            assert link.state == StateName.awaiting_setup
            assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 1
            assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 0
            assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 0
            assert len(stats.comm_events) == 1

            # (awaiting_setup -> mqtt_suback -> awaiting_setup)
            # Allow another suback to arrive, remaining in awaiting_setup
            child.release_upstream_subacks(1)
            exp_subacks = num_subacks + 1
            await await_for(
                lambda: child.stats.num_received_by_type["mqtt_suback"] == exp_subacks,
                1,
                f"ERROR waiting mqtt_suback {exp_subacks} (2/3)",
                err_str_f=h.summary_str,
            )
            assert link.state == StateName.awaiting_setup

            # (awaiting_setup -> message_from_peer -> awaiting_setup)
            # Receive another message from peer, remaining in awaiting_setup

            # noinspection PyTypeChecker
            dbg_topic = MQTTTopic.encode(
                "gw",
                parent.publication_name,
                parent.links.topic_dst(parent.downstream_client),
                DBGPayload.model_fields["TypeName"].default,
            )
            assert stats.num_received_by_topic[dbg_topic] == 0
            parent.send_dbg(parent.downstream_client)
            await await_for(
                lambda: stats.num_received_by_topic[dbg_topic] == 1,
                1,
                "ERROR waiting for dbg message",
                err_str_f=h.summary_str,
            )
            assert link.state == StateName.awaiting_setup

            # (awaiting_setup -> disconnected -> connecting)
            # Tell client we lost comm
            child.clear_upstream_subacks()
            child.pause_upstream_subacks()
            child.mqtt_client_wrapper(  # noqa: SLF001
                child.upstream_client
            ).mqtt_client._loop_rc_handle(MQTT_ERR_CONN_LOST)
            await await_for(
                lambda: child.num_upstream_subacks_available() == 3,
                3,
                "ERROR waiting suback pending",
                err_str_f=h.summary_str,
            )
            assert link.state == StateName.awaiting_setup_and_peer
            assert comm_event_counts["gridworks.event.comm.mqtt.connect"] == 2
            assert comm_event_counts["gridworks.event.comm.mqtt.fully.subscribed"] == 0
            assert comm_event_counts["gridworks.event.comm.mqtt.disconnect"] == 1
            assert len(stats.comm_events) == 3
            for comm_event in stats.comm_events:
                assert comm_event.MessageId in child.event_persister

            # Allow one suback at a time to arrive
            # (Not strictly necessary, since message receiving code does not check if the source topic suback
            #  has arrived).
            num_subacks = child.stats.num_received_by_type["mqtt_suback"]
            child.release_upstream_subacks(1)
            exp_subacks = num_subacks + 1
            await await_for(
                lambda: child.stats.num_received_by_type["mqtt_suback"] == exp_subacks,
                1,
                f"ERROR waiting mqtt_suback {exp_subacks} (1/3)",
                err_str_f=h.summary_str,
            )
            assert link.state == StateName.awaiting_setup_and_peer

            # (awaiting_setup_and_peer -> message_from_peer -> awaiting_setup)
            # Force parent to restore comm, delivering a message, sending us to awaiting_setup
            parent.mqtt_client_wrapper(  # noqa: SLF001
                parent.downstream_client
            ).mqtt_client._loop_rc_handle(MQTT_ERR_CONN_LOST)
            await await_for(
                lambda: link.in_state(StateName.awaiting_setup),
                3,
                "ERROR waiting for message from peer",
                err_str_f=h.summary_str,
            )

            # (awaiting_setup -> mqtt_suback -> active)
            # Release all subacks, allowing child to go active
            child.release_upstream_subacks()
            await await_for(
                lambda: link.in_state(StateName.active),
                1,
                "ERROR waiting for active",
                err_str_f=h.summary_str,
            )
