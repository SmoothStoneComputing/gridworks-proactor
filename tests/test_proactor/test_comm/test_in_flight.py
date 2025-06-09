from math import floor
from typing import Any

import pytest

from gwproactor.message import DBGEvent, DBGPayload
from gwproactor_test import LiveTest, await_for


@pytest.mark.asyncio
async def test_in_flight_happy_path(request: Any) -> None:
    """Generate a bunch of events. While they are being acked generate a bunch
    more. Verify the child has not persisted any of them."""
    async with LiveTest(start_child=True, start_parent=True, request=request) as h:
        child = h.child
        child_link = child.links.link(child.upstream_client)
        parent = h.parent
        parent_link = parent.links.link(parent.downstream_client)

        # wait for all events to be at rest
        exp_child_events = sum(
            [
                1,  # child startup
                2,  # child connect, substribe
                1,  # child2 peer active
            ]
        )
        exp_parent_events = sum(
            [
                exp_child_events,
                1,  # parent startup
                2,  # parent connect, subscribe
                1,  # child1 peer active
            ]
        )
        await await_for(
            lambda: child_link.active() and child.events_at_rest(),
            1,
            "ERROR waiting for child events upload",
            err_str_f=h.summary_str,
        )
        await await_for(
            lambda: parent_link.active()
            and parent.events_at_rest(num_pending=exp_parent_events),
            1,
            f"ERROR waiting for parent to persist {exp_parent_events} events",
            err_str_f=h.summary_str,
        )
        parent.assert_events_at_rest(
            num_pending=exp_parent_events,
            num_persists=exp_parent_events,
            all_pending=True,
            tag="parent",
            err_str=h.summary_str(),
        )
        child.assert_events_at_rest(
            # child will not persist peer active event
            num_persists=exp_child_events - 1,
            all_clear=True,
            tag="child",
            err_str=h.summary_str(),
        )

        # generate a "bunch" of events, but not more than are allowed in-flight
        a_bunch = floor(child.settings.proactor.num_inflight_events * 0.8)
        exp_parent_events = parent.links.num_pending + 2 * a_bunch
        child_startup_persists = child.event_persister.num_persists

        h.child.delimit(f"Generating {a_bunch} events")
        for i in range(a_bunch):
            child.generate_event(
                DBGEvent(Command=DBGPayload(), Msg=f"event {i+1} / {a_bunch}")
            )
        last_in_flight = child.links.num_in_flight

        def _child_got_more_acks() -> bool:
            return child.links.num_in_flight < last_in_flight

        # now generate a "bunch" more, but each one after an ack
        h.child.delimit(f"Generating {a_bunch} more events")
        for i in range(a_bunch):
            child.generate_event(
                DBGEvent(
                    Command=DBGPayload(), Msg=f"event {a_bunch + i+1} / {a_bunch * 2}"
                )
            )
            last_in_flight = child.links.num_in_flight
            assert last_in_flight > 0
            await await_for(
                lambda: _child_got_more_acks(),
                1,
                "ERROR waiting for child to receive some acks",
                retry_duration=0.001,
                err_str_f=h.summary_str,
            )
            last_in_flight = child.links.num_in_flight
            assert child.event_persister.num_persists == exp_child_events
        # now wait for all events to rest
        await await_for(
            lambda: child.events_at_rest()
            and parent.events_at_rest(num_pending=exp_parent_events),
            1,
            "ERROR waiting for child events upload",
            err_str_f=h.summary_str,
        )
        parent.assert_events_at_rest(
            num_pending=exp_parent_events,
            num_persists=exp_parent_events,
            all_pending=True,
            tag="parent",
            err_str=h.summary_str(),
        )
        # child should have persisted no new events
        child.assert_events_at_rest(
            num_persists=child_startup_persists,
            all_clear=True,
            tag="child",
            err_str=h.summary_str(),
        )


@pytest.mark.asyncio
async def test_in_flight_overflow(request: Any) -> None:
    """Generate more events than fit "in-fight". Verify persisted as expected
    and reach their destination without timeouts.
    """

    # Start parent and child and wait for them to be at rest
    async with LiveTest(start_child=True, start_parent=True, request=request) as h:
        child = h.child
        child_link = child.links.link(child.upstream_client)
        parent = h.parent
        parent_link = parent.links.link(parent.downstream_client)

        # wait for all events to be at rest
        exp_child_startup_events = sum(
            [
                1,  # child startup
                2,  # child connect, substribe
                1,  # child2 peer active
            ]
        )
        exp_parent_startup_events = sum(
            [
                exp_child_startup_events,
                1,  # parent startup
                2,  # parent connect, subscribe
                1,  # child1 peer active
            ]
        )
        await await_for(
            lambda: child_link.active() and child.events_at_rest(),
            1,
            "ERROR waiting for child events upload",
            err_str_f=h.summary_str,
        )
        await await_for(
            lambda: parent_link.active()
            and parent.events_at_rest(num_pending=exp_parent_startup_events),
            1,
            f"ERROR waiting for parent to persist {exp_parent_startup_events} events",
            err_str_f=h.summary_str,
        )
        parent.assert_events_at_rest(
            num_pending=exp_parent_startup_events,
            num_persists=exp_parent_startup_events,
            all_pending=True,
            tag="parent",
            err_str=h.summary_str(),
        )
        child.assert_events_at_rest(
            # child will not persist peer active event
            num_persists=exp_child_startup_events - 1,
            all_clear=True,
            tag="child",
            err_str=h.summary_str(),
        )

        # generate more events than fit in the pipe
        a_bunch = child.settings.proactor.num_inflight_events * 2
        exp_parent_events = parent.links.num_pending + a_bunch
        exp_child_persists = (
            child.event_persister.num_persists
            + a_bunch
            - child.settings.proactor.num_inflight_events
        )

        h.child.delimit(f"Generating {a_bunch} events")
        for i in range(a_bunch):
            child.generate_event(
                DBGEvent(Command=DBGPayload(), Msg=f"event {i+1} / {a_bunch}")
            )
        last_in_flight = child.links.num_in_flight

        def _child_got_more_acks() -> bool:
            return child.links.num_in_flight < last_in_flight

        # now wait for all events to rest
        await await_for(
            lambda: child.events_at_rest()
            and parent.events_at_rest(num_pending=exp_parent_events),
            1,
            "ERROR waiting for child events upload",
            err_str_f=h.summary_str,
        )
        parent.assert_events_at_rest(
            num_pending=exp_parent_events,
            num_persists=exp_parent_events,
            all_pending=True,
            tag="parent",
            err_str=h.summary_str(),
        )
        # child should have persisted no new events
        child.assert_events_at_rest(
            num_persists=exp_child_persists,
            # overflow events generated without loss of comm do not need to be
            # retrieved
            num_retrieves=exp_child_startup_events - 1,
            all_clear=True,
            tag="child",
            err_str=h.summary_str(),
        )
