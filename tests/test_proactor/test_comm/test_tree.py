# ruff: noqa: PLR2004, ERA001
from typing import Any

import pytest

from gwproactor.links import StateName
from gwproactor_test.dummies.tree.messages import RelayInfoReported
from gwproactor_test.tree_live_test_helper import TreeLiveTest
from gwproactor_test.wait import await_for


@pytest.mark.asyncio
async def test_tree_no_parent(request: Any) -> None:
    async with TreeLiveTest(request=request) as h:
        # add child 1
        h.add_child()
        child1 = h.child1  # noqa
        stats1 = child1.stats
        stats1to2 = stats1.link(child1.downstream_client)
        stats1toAtn = stats1.link(child1.upstream_client)
        counts1to2 = stats1to2.comm_event_counts
        counts1toAtn = stats1toAtn.comm_event_counts
        link1to2 = child1.links.link(child1.downstream_client)
        link1toAtn = child1.links.link(child1.upstream_client)
        assert stats1.num_received == 0
        assert link1to2.state == StateName.not_started
        assert link1toAtn.state == StateName.not_started

        # start child 1
        h.start_child1()
        await await_for(
            lambda: link1to2.active_for_send()
            and link1toAtn.active_for_send()
            and child1.links.num_pending == 7,
            1,
            "ERROR waiting child1 links to be active_for_send",
            err_str_f=h.summary_str,
        )
        assert StateName(link1to2.state) == StateName.awaiting_peer, h.summary_str()
        assert StateName(link1toAtn.state) == StateName.awaiting_peer
        assert counts1to2["gridworks.event.comm.mqtt.connect"] == 1
        assert counts1to2["gridworks.event.comm.mqtt.fully.subscribed"] == 1
        assert len(stats1to2.comm_events) == 2
        assert counts1toAtn["gridworks.event.comm.mqtt.connect"] == 1
        assert counts1toAtn["gridworks.event.comm.mqtt.fully.subscribed"] == 1
        assert len(stats1toAtn.comm_events) == 2
        # 1 startup event + (atn, admin, scada2) x (connect, subscribed)
        assert child1.links.num_pending == 7
        assert child1.links.num_in_flight == 0
        assert child1.event_persister.num_persists == 7
        assert child1.event_persister.num_retrieves == 0
        assert child1.event_persister.num_clears == 0

        # add child 2
        h.add_child2()
        child2 = h.child2
        stats2 = child2.stats
        stats2to1 = stats2.link(child2.upstream_client)
        counts2to1 = stats2to1.comm_event_counts
        link2to1 = child2.links.link(child2.upstream_client)
        assert stats2.num_received == 0
        assert link2to1.state == StateName.not_started

        # start child 2
        h.start_child2()
        await await_for(
            lambda: link1to2.active()
            and link2to1.active()
            and child2.links.num_in_flight == 0
            and child2.links.num_in_flight == 0
            # 7 child1 events +
            # 1 child1 peer active +
            # 6 child2 startup, (admin, scada1) x (connect, subscribe), peer active
            and child1.links.num_pending == 14,
            1,
            "ERROR waiting child2 links to be active",
            err_str_f=h.summary_str,
        )
        assert StateName(link1to2.state) == StateName.active
        assert StateName(link2to1.state) == StateName.active
        assert counts1to2["gridworks.event.comm.peer.active"] == 1
        assert len(stats1to2.comm_events) == 3
        assert counts2to1["gridworks.event.comm.peer.active"] == 1
        assert counts2to1["gridworks.event.comm.mqtt.connect"] == 1
        assert counts2to1["gridworks.event.comm.mqtt.fully.subscribed"] == 1
        assert len(stats2to1.comm_events) == 3

        assert child1.links.num_pending == 14
        assert child1.links.num_in_flight == 0
        assert child1.event_persister.num_persists == 14
        assert child1.event_persister.num_retrieves == 0
        assert child1.event_persister.num_clears == 0

        assert child2.links.num_pending == 0
        assert child2.links.num_in_flight == 0
        # child2 never persists its own peer active, and whether it persists
        # admin events depends on when child1 link goes active
        persister2 = child2.event_persister
        assert 3 <= persister2.num_persists <= 5
        assert persister2.num_retrieves == persister2.num_persists
        assert persister2.num_clears == persister2.num_persists


@pytest.mark.asyncio
async def test_tree_message_exchange(request: Any) -> None:
    async with TreeLiveTest(
        start_child1=True,
        start_child2=True,
        request=request,
    ) as h:
        child1 = h.child
        stats1 = child1.stats.link(child1.downstream_client)
        link1to2 = child1.links.link(child1.downstream_client)
        link1toAtn = child1.links.link(child1.upstream_client)

        child2 = h.child2
        stats2 = child2.stats.link(child2.upstream_client)
        link2to1 = child2.links.link(child2.upstream_client)

        # Wait for children to connect
        await await_for(
            lambda: link1to2.active()
            and link2to1.active()
            and link1toAtn.active_for_send(),
            1,
            "ERROR waiting children to connect",
            err_str_f=h.summary_str,
        )

        # exchange messages
        assert stats2.num_received_by_type["gridworks.dummy.set.relay"] == 0
        relay_name = "scada2.relay1"
        h.child.logger.info("SETTING RELAY")
        h.child_app.prime_actor.set_relay(relay_name, True)

        # wait for response to be received
        await await_for(
            lambda: stats1.num_received_by_type["gridworks.event.relay.report"] == 1,
            1,
            "ERROR waiting child1 to receive relay report from child2",
            err_str_f=h.summary_str,
        )
        assert stats2.num_received_by_type["gridworks.dummy.set.relay"] == 1
        assert h.child_app.prime_actor.relays.Relays == {
            relay_name: RelayInfoReported(Closed=True)
        }
        assert h.child2_app.prime_actor.relays == {relay_name: True}
        assert h.child_app.prime_actor.relays.TotalChangeMismatches == 0

        # wait for all events to be at rest
        exp_child1_events = sum(
            [
                1,  # child1 startup
                6,  # child1 (parent, child2, admin) x (connect, subscribe)
                1,  # child1 peer active
                2,  # relay set, relay report
                1,  # child2 startup
                4,  # child2 (child1, admin) x (connect, substribe)
                1,  # child2 peer active
            ]
        )
        await await_for(
            lambda: child1.links.num_pending == exp_child1_events
            and child1.links.num_in_flight == 0
            and child2.links.num_pending == 0
            and child2.links.num_in_flight == 0,
            1,
            f"ERROR waiting for child1 to receive {exp_child1_events} events",
            err_str_f=h.summary_str,
        )
        assert child1.links.num_pending == exp_child1_events
        assert child1.links.num_in_flight == 0
        assert child1.event_persister.num_persists == exp_child1_events
        assert child1.event_persister.num_retrieves == 0
        assert child1.event_persister.num_clears == 0
        assert child2.links.num_pending == 0
        assert child2.links.num_in_flight == 0
        # child2 never persists its own peer active, and whether it persists
        # admin events depends on when child1 link goes active
        persister2 = child2.event_persister
        assert 3 <= persister2.num_persists <= 5
        assert persister2.num_retrieves == persister2.num_persists
        assert persister2.num_clears == persister2.num_persists


@pytest.mark.asyncio
async def test_tree_parent_comm(request: Any) -> None:
    async with TreeLiveTest(add_child=True, request=request) as h:
        h.start_child1()
        await await_for(
            h.child1.mqtt_quiescent,
            3,
            "child1.mqtt_quiescent",
            err_str_f=h.summary_str,
        )
        h.add_child2()
        h.start_child2()
        h.add_parent()
        h.start_parent()

        # wait for all events to be at rest
        exp_child2_events = sum(
            [
                1,  # child2 startup
                4,  # child2 (child1, admin) x (connect, substribe)
                1,  # child2 peer active
            ]
        )
        exp_child1_events = sum(
            [
                1,  # child1 startup
                6,  # child1 (parent, child2, admin) x (connect, subscribe)
                2,  # child1 (parent, child2) x peer active
                exp_child2_events,
            ]
        )
        exp_parent_events = sum(
            [
                1,  # parent startup
                2,  # parent connect, subscribe
                1,  # child1 peer active
                exp_child1_events,
            ]
        )
        await h.await_quiescent_connections(exp_parent_pending=exp_parent_events)


@pytest.mark.asyncio
async def test_tree_event_forward(request: Any) -> None:
    async with TreeLiveTest(
        start_child=True,
        start_child2=True,
        start_parent=True,
        request=request,
    ) as h:
        link1to2 = h.child1.links.link(h.child1.downstream_client)
        link2to1 = h.child2.links.link(h.child2.upstream_client)
        link1toAtn = h.child1.links.link(h.child1.upstream_client)
        linkAtnto1 = h.parent.links.link(h.parent.downstream_client)
        await await_for(
            lambda: (
                link1toAtn.active()
                and linkAtnto1.active()
                and link1to2.active()
                and link2to1.active()
            ),
            3,
            "link1toAtn.active() and linkAtnto1.active()",
            err_str_f=h.summary_str,
        )
        relay_name = "scada2.relay1"
        h.child_app.prime_actor.set_relay(relay_name, True)

        statsAtnTo1 = h.parent.stats.link(h.parent.downstream_client)

        def _atn_heard_reports() -> bool:
            es1 = statsAtnTo1.event_counts.get(str(h.child1.publication_name))
            es2 = statsAtnTo1.event_counts.get(str(h.child2.publication_name))
            return bool(
                es1
                and es1["gridworks.event.relay.report.received"] == 1
                and es2
                and es2["gridworks.event.relay.report"] == 1
            )

        await await_for(
            _atn_heard_reports,
            1,
            "ERROR waiting for atn to hear reports",
            err_str_f=h.summary_str,
        )

        # wait for all events to be at rest
        exp_child2_events = sum(
            [
                1,  # child2 startup
                4,  # child2 (child1, admin) x (connect, substribe)
                1,  # child2 peer active
                2,  # relay set, relay report
            ]
        )
        exp_child1_events = sum(
            [
                1,  # child1 startup
                6,  # child1 (parent, child2, admin) x (connect, subscribe)
                2,  # child1 (parent, child2) x peer active
                exp_child2_events,
            ]
        )
        exp_parent_events = sum(
            [
                1,  # parent startup
                2,  # parent connect, subscribe
                1,  # child1 peer active
                exp_child1_events,
            ]
        )
        await await_for(
            lambda: h.child1.links.num_pending == 0
            and h.child1.links.num_in_flight == 0
            and h.child2.links.num_pending == 0
            and h.child2.links.num_in_flight == 0
            and h.parent.links.num_pending == exp_parent_events,
            1,
            f"ERROR waiting for parent to persist {exp_parent_events} events",
            err_str_f=h.summary_str,
        )
        persister0 = h.parent.event_persister
        persister1 = h.child1.event_persister
        persister2 = h.child2.event_persister
        assert h.parent.links.num_pending == exp_parent_events
        assert h.parent.links.num_in_flight == 0
        assert persister0.num_persists == exp_parent_events
        assert persister0.num_retrieves == 0
        assert persister0.num_clears == 0
        assert h.child1.links.num_pending == 0
        if h.child1.links.num_in_flight != 0:
            import rich

            rich.print(h.child1.links.in_flight_events)
        assert h.child1.links.num_in_flight == 0
        # child 1 will persist between 3 and (exp_child1_events - 1) events, depending
        # on when parent link went active
        assert 3 <= persister1.num_persists <= (exp_child1_events - 1), h.summary_str()
        assert persister1.num_retrieves == persister1.num_persists
        assert persister1.num_clears == persister1.num_persists

        assert h.child2.links.num_pending == 0
        assert h.child2.links.num_in_flight == 0
        # child2 never persists its own peer active, and whether it persists
        # admin events depends on when child1 link goes active
        assert 3 <= persister2.num_persists <= 5
        assert persister2.num_retrieves == persister2.num_persists
        assert persister2.num_clears == persister2.num_persists
