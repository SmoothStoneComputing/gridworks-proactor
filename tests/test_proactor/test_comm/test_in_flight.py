import logging
from math import floor
from typing import Any, Optional

import pytest
from gwproto.messages import Ack, AnyEvent
from result import Err, Ok

from gwproactor import Problems
from gwproactor.links import StateName
from gwproactor.message import DBGEvent, DBGPayload
from gwproactor.persister import FileEmptyWarning
from gwproactor_test import LiveTest, await_for


def assert_acks_consistent(
    h: LiveTest,
    *,
    print_summary: bool = False,
    print_all: bool = False,
    log_level: int = logging.ERROR,
) -> None:
    full_s = f"\n\nParent paused acks: {len(h.parent.needs_ack)}\n"
    summary_s = full_s
    needs_ack_set: set[str] = set()
    for i, needs_ack in enumerate(h.parent.needs_ack):
        ack = needs_ack.message.Payload
        if not isinstance(ack, Ack):
            raise ValueError(f"WHOOPS: {type(ack)}")  # noqa: TRY004
        if ack.AckMessageID in h.child.links.in_flight_events:
            loc_s = "in-flight"
        elif ack.AckMessageID in h.child.event_persister.pending_ids():
            loc_s = "pending"
        else:
            loc_s = "*UKNONWN*"
        full_s += f"  {i+1:3d} / {len(h.parent.needs_ack):3d}  {ack.AckMessageID[:8]}  {loc_s}\n"
        needs_ack_set.add(ack.AckMessageID)
    s = f"Child in-flight events: {h.child.links.num_in_flight}\n"
    full_s += s
    summary_s += s
    for i, (event_id, event) in enumerate(h.child.links.in_flight_events.items()):
        if event_id != event.MessageId:
            raise ValueError(f"WHOOPS 1 {event_id} != {event.MessageId}")
        full_s += f"  {i+1:3d} / {h.child.links.num_in_flight:3d}  {event_id[:8]}\n"
    s = f"Child pending events: {h.child.links.num_pending}\n"
    full_s += s
    summary_s += s
    for i, event_id in enumerate(h.child.event_persister.pending_ids()):
        full_s += f"  {i+1:3d} / {h.child.links.num_pending:3d}  {event_id[:8]}\n"
    in_flight_set: set[str] = set(h.child.links.in_flight_events.keys())
    pending_set: set[str] = set(h.child.event_persister.pending_ids())
    error_s = ""
    error_summary_s = ""
    if not in_flight_set.issubset(needs_ack_set):
        in_flight_not_in_needs_ack_set: set[str] = in_flight_set.difference(
            needs_ack_set
        )
        error_summary_line = f"Child in-flight events not in needs ack: {len(in_flight_not_in_needs_ack_set)}\n"
        error_summary_s += error_summary_line
        error_s += error_summary_line
        for i, event_id in enumerate(
            sorted(
                in_flight_not_in_needs_ack_set,
                key=lambda x: h.child.links.in_flight_events[x].TimeCreatedMs,
            )
        ):
            event = h.child.links.in_flight_events[event_id]
            error_s += (
                f"  {i+1:3d} / {len(in_flight_not_in_needs_ack_set):3d}  "
                f"{event_id[:8]}  "
                f"{event.TimeCreatedMs:15d}  "
                f"{event.TypeName}\n"
            )
    if not pending_set.issubset(needs_ack_set):
        pending_not_in_needs_ack_set: set[str] = pending_set.difference(needs_ack_set)
        pending_not_in_needs_ack_events: list[AnyEvent] = []
        deserialize_problems: list[Problems] = []
        for event_id in pending_not_in_needs_ack_set:
            match h.child.event_persister.retrieve(event_id):
                case Ok(content):
                    if content is not None:
                        pending_not_in_needs_ack_events.append(
                            AnyEvent.model_validate_json(content)
                        )
                    else:
                        deserialize_problems.append(
                            Problems(warnings=[FileEmptyWarning(uid=event_id)])
                        )
                case Err(one_retrieve_problems):
                    deserialize_problems.append(one_retrieve_problems)
        pending_not_in_needs_ack_events = sorted(
            pending_not_in_needs_ack_events, key=lambda x: x.TimeCreatedMs
        )
        error_summary_line = f"Child pending events not in needs ack: {len(pending_not_in_needs_ack_events)}\n"
        error_summary_s += error_summary_line
        error_s += error_summary_line
        for i, event in enumerate(pending_not_in_needs_ack_events):
            error_s += (
                f"  {i+1:3d} / {len(pending_not_in_needs_ack_set):3d}  "
                f"{event.MessageId[:8]}  "
                f"{event.TimeCreatedMs:15d}  "
                f"{event.TypeName}\n"
            )
        if deserialize_problems:
            error_summary_line = f"Problems deserializing child pending events for display: {len(deserialize_problems)}"
            error_summary_s += error_summary_line
            error_s += error_summary_line
            for i, problem in enumerate(deserialize_problems):
                error_s += f"  {i+1:3d} / {len(deserialize_problems):3d}  {problem}\n"

    needs_ack_not_in_events = needs_ack_set - (in_flight_set | pending_set)
    if needs_ack_not_in_events:
        error_summary_line = (
            f"Parent needs_ack not in child events: {len(needs_ack_not_in_events)}\n"
        )
        error_summary_s += error_summary_line
        error_s += error_summary_line
        for i, event_id in enumerate(needs_ack_not_in_events):
            error_s += (
                f"  {i+1:3d} / {len(needs_ack_not_in_events):3d}  {event_id[:8]}\n"
            )
    if error_s:
        full_s += error_s + summary_s + error_summary_s
        raise ValueError(
            f"ERROR. Acks inconsistent\n{full_s}\n{error_s}\n{h.summary_str()}\n"
        )
    consistent_s = "Acks CONSISTENT\n"
    full_s += summary_s
    full_s += consistent_s
    summary_s += consistent_s

    if print_all:
        h.child.logger.log(log_level, full_s)
    elif print_summary:
        h.child.logger.log(log_level, summary_s)


async def await_quiescent_connections(
    h: LiveTest,
    exp_child_persists: Optional[int] = None,
    exp_parent_pending: Optional[int] = None,
    exp_parent_persists: Optional[int] = None,
) -> None:
    child = h.child
    child_link = child.links.link(child.upstream_client)
    parent = h.parent
    parent_link = parent.links.link(parent.downstream_client)

    # wait for all events to be at rest
    exp_child_persists = (
        exp_child_persists
        if exp_child_persists is not None
        else sum(
            [
                1,  # child startup
                2,  # child connect, substribe
            ]
        )
    )

    exp_parent_pending = (
        exp_parent_pending
        if exp_parent_pending is not None
        else (
            sum(
                [
                    exp_child_persists,
                    1,  # child peer active
                    1,  # parent startup
                    3,  # parent connect, subscribe, peer active
                ]
            )
        )
    )
    exp_parent_persists = (
        exp_parent_persists if exp_parent_persists is not None else exp_parent_pending
    )
    await await_for(
        lambda: child_link.active() and child.events_at_rest(),
        1,
        "ERROR waiting for child events upload",
        err_str_f=h.summary_str,
    )
    await await_for(
        lambda: parent_link.active()
        and parent.events_at_rest(num_pending=exp_parent_pending),
        1,
        f"ERROR waiting for parent to persist {exp_parent_pending} events",
        err_str_f=h.summary_str,
    )
    parent.assert_event_counts(
        num_pending=exp_parent_pending,
        num_persists=exp_parent_persists,
        num_clears=0,
        num_retrieves=0,
        tag="parent",
        err_str=h.summary_str(),
    )
    child.assert_event_counts(
        # child will not persist peer active event
        num_persists=exp_child_persists,
        all_clear=True,
        tag="child",
        err_str=h.summary_str(),
    )


@pytest.mark.asyncio
async def test_in_flight_happy_path(request: Any) -> None:
    """Generate a bunch of events. While they are being acked generate a bunch
    more. Verify the child has not persisted any of them."""
    async with LiveTest(start_child=True, start_parent=True, request=request) as h:
        await await_quiescent_connections(h)

        child = h.child
        parent = h.parent

        # generate a "bunch" of events, but not more than are allowed in-flight
        a_bunch = floor(child.settings.proactor.num_inflight_events * 0.8)
        exp_parent_events = parent.links.num_pending + 2 * a_bunch
        child_startup_persists = child.event_persister.num_persists

        for i in range(a_bunch):
            child.generate_event(
                DBGEvent(Command=DBGPayload(), Msg=f"event {i+1} / {a_bunch}")
            )
        last_in_flight = child.links.num_in_flight

        def _child_got_more_acks() -> bool:
            return child.links.num_in_flight < last_in_flight

        # now generate a "bunch" more, but each one after an ack
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
        # now wait for all events to rest
        await await_for(
            lambda: child.events_at_rest()
            and parent.events_at_rest(num_pending=exp_parent_events),
            1,
            "ERROR waiting for child events upload",
            err_str_f=h.summary_str,
        )
        parent.assert_event_counts(
            num_pending=exp_parent_events,
            num_persists=exp_parent_events,
            all_pending=True,
            tag="parent",
            err_str=h.summary_str(),
        )
        # child should have persisted no new events
        child.assert_event_counts(
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
        await await_quiescent_connections(h)

        child = h.child
        parent = h.parent

        # generate more events than fit in the pipe
        initial_child_retrieves = child.event_persister.num_retrieves
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
        parent.assert_event_counts(
            num_pending=exp_parent_events,
            num_persists=exp_parent_events,
            all_pending=True,
            tag="parent",
            err_str=h.summary_str(),
        )
        # child should have persisted no new events
        child.assert_event_counts(
            num_persists=exp_child_persists,
            # overflow events generated without loss of comm do not need to be
            # retrieved
            num_retrieves=initial_child_retrieves,
            all_clear=True,
            tag="child",
            err_str=h.summary_str(),
        )


@pytest.mark.asyncio
async def test_in_flight_flowcontrol(request: Any) -> None:
    """Test in-flight with carefully controlled acks. This is probably
    duplication of the above less controlled tests, but seems worth having.
    """

    async with LiveTest(start_child=True, start_parent=True, request=request) as h:
        await await_quiescent_connections(h)
        child = h.child
        parent = h.parent
        parent.pause_acks()
        child.set_ack_timeout_seconds(100)  # please no timeouts while we are busy

        # Walk through generating in-flight events.

        # generate a "bunch" of events, but not more than are allowed in-flight
        in_flight_buffer_size = child.settings.proactor.num_inflight_events
        child_startup_persists = child.event_persister.num_persists
        for i in range(in_flight_buffer_size):
            child.generate_event(
                DBGEvent(
                    Command=DBGPayload(), Msg=f"event {i+1} / {in_flight_buffer_size}"
                )
            )
            child.assert_event_counts(
                num_pending=0,
                num_in_flight=i + 1,
                num_persists=child_startup_persists,
                all_clear=True,
            )
        child.assert_event_counts(
            num_pending=0,
            num_in_flight=in_flight_buffer_size,
            num_persists=child_startup_persists,
            all_clear=True,
        )
        # paused acks: [50 in-flight]
        await await_for(
            lambda: len(parent.links.needs_ack) == in_flight_buffer_size,
            1,
            f"ERROR waiting for parent to have {in_flight_buffer_size} paused acks",
        )
        exp_parent_pending = 8 + in_flight_buffer_size
        parent.assert_event_counts(
            num_pending=exp_parent_pending,
            all_pending=True,
            tag="parent after events generated",
        )
        assert_acks_consistent(h)

        # Walk through generating more in-flight while in-flight buffer
        # has room

        # generate more events, one after each ack. None should be persisted.
        for i in range(in_flight_buffer_size):
            parent.release_acks(num_to_release=1)
            exp_in_flight = in_flight_buffer_size - 1
            await await_for(
                lambda: child.links.num_in_flight == exp_in_flight,  # noqa: B023
                3,
                f"ERROR waiting for child to have {exp_in_flight} in flight, i:{i}",
                err_str_f=h.summary_str,
            )
            child.assert_event_counts(
                num_pending=0,
                num_in_flight=exp_in_flight,
                num_persists=child_startup_persists,
                all_clear=True,
            )
            child.generate_event(
                DBGEvent(
                    Command=DBGPayload(),
                    Msg=f"event {in_flight_buffer_size + i + 1} / {in_flight_buffer_size * 2}",
                )
            )
            child.assert_event_counts(
                num_pending=0,
                num_in_flight=in_flight_buffer_size,
                num_persists=child_startup_persists,
                all_clear=True,
            )
        # paused acks: [50 in-flight]
        await await_for(
            lambda: len(parent.links.needs_ack) == in_flight_buffer_size,
            1,
            f"ERROR waiting for parent to have {in_flight_buffer_size} paused acks",
        )
        exp_parent_pending += in_flight_buffer_size
        parent.assert_event_counts(
            num_pending=exp_parent_pending,
            all_pending=True,
            tag="parent after events generated",
        )
        assert_acks_consistent(h)

        # Walk through overflowing the buffer

        # Now overflow the in-flight buffer. Verify new events are persisted.
        overflow_size = 10
        for i in range(overflow_size):
            child.generate_event(
                DBGEvent(
                    Command=DBGPayload(),
                    Msg=f"overflow event {i + 1} / {overflow_size}",
                )
            )
            child.assert_event_counts(
                num_pending=i + 1,
                num_in_flight=in_flight_buffer_size,
                num_persists=child_startup_persists + i + 1,
                num_retrieves=child_startup_persists,
                num_clears=child_startup_persists,
            )
        # paused acks: [50 in-flight][10 persisted]
        exp_needs_ack = in_flight_buffer_size + 10
        await await_for(
            lambda: len(parent.links.needs_ack) == exp_needs_ack,
            1,
            f"ERROR waiting for parent to have {exp_needs_ack} paused acks",
            err_str_f=h.summary_str,
        )
        exp_parent_pending += 10
        parent.assert_event_counts(
            num_pending=exp_parent_pending,
            all_pending=True,
            tag="parent after overflow events generated",
        )
        assert_acks_consistent(h)
        # Walk through mixing overflow and in flight.

        # first with a small mix, release an 3 acks so we have some room in the
        # in-flight buffer
        acks_released = 3
        parent.release_acks(num_to_release=acks_released)
        await await_for(
            lambda: child.links.num_in_flight == in_flight_buffer_size - acks_released,
            1,
            f"ERROR waiting for child to receive an {acks_released} acks",
            err_str_f=h.summary_str,
        )
        child.assert_event_counts(
            num_pending=overflow_size,
            num_in_flight=in_flight_buffer_size - acks_released,
            num_persists=child_startup_persists + overflow_size,
            # acks should have been for in-flight events, not those persisted
            num_retrieves=child_startup_persists,
            num_clears=child_startup_persists,
        )
        # paused acks: [47 in-flight][10 persisted]
        exp_needs_ack = in_flight_buffer_size + 10 - acks_released
        await await_for(
            lambda: len(parent.links.needs_ack) == exp_needs_ack,
            1,
            f"ERROR waiting for parent to have {exp_needs_ack} paused acks",
            err_str_f=h.summary_str,
        )
        parent.assert_event_counts(
            num_pending=exp_parent_pending,
            all_pending=True,
            tag="parent after small mix",
        )
        assert_acks_consistent(h)

        # re-fill the in-flight buffer.
        for i in range(acks_released):
            child.generate_event(
                DBGEvent(
                    Command=DBGPayload(),
                    Msg=f"refill event event {i}",
                )
            )
            child.assert_event_counts(
                num_pending=overflow_size,
                num_in_flight=in_flight_buffer_size - acks_released + i + 1,
                num_persists=child_startup_persists + overflow_size,
                num_retrieves=child_startup_persists,
                num_clears=child_startup_persists,
            )
        # paused acks: [47 in-flight][10 persisted][3 in-flight]
        exp_needs_ack = in_flight_buffer_size + 10
        await await_for(
            lambda: len(parent.links.needs_ack) == exp_needs_ack,
            1,
            f"ERROR waiting for parent to have {exp_needs_ack} paused acks",
            err_str_f=h.summary_str,
        )
        exp_parent_pending += 3
        parent.assert_event_counts(
            num_pending=exp_parent_pending,
            all_pending=True,
            tag="parent after in-flight refilled",
        )
        assert_acks_consistent(h)

        # next event should be persisted
        child.generate_event(
            DBGEvent(
                Command=DBGPayload(),
                Msg="overflow event",
            )
        )
        child.assert_event_counts(
            num_pending=overflow_size + 1,
            num_in_flight=in_flight_buffer_size,
            num_persists=child_startup_persists + overflow_size + 1,
            num_retrieves=child_startup_persists,
            num_clears=child_startup_persists,
        )
        # paused acks: [47 in-flight][10 persisted][3 in-flight][1 persisted]
        exp_needs_ack = in_flight_buffer_size + 11
        await await_for(
            lambda: len(parent.links.needs_ack) == exp_needs_ack,
            1,
            f"ERROR waiting for parent to have {exp_needs_ack} paused acks",
            err_str_f=h.summary_str,
        )
        exp_parent_pending += 1
        parent.assert_event_counts(
            num_pending=exp_parent_pending,
            all_pending=True,
            tag="parent after overflow",
        )
        assert_acks_consistent(h)
        # now generate a bigger mix
        # release 30
        acks_released = 30
        exp_in_flight = in_flight_buffer_size - acks_released
        parent.release_acks(num_to_release=acks_released)
        await await_for(
            lambda: child.links.num_in_flight == exp_in_flight,
            1,
            f"ERROR waiting for child to receive an {acks_released} acks",
            err_str_f=h.summary_str,
        )

        # releases should not have changed persistence
        child.assert_event_counts(
            num_pending=overflow_size + 1,
            num_in_flight=in_flight_buffer_size - acks_released,
            num_persists=child_startup_persists + overflow_size + 1,
            num_retrieves=child_startup_persists,
            num_clears=child_startup_persists,
        )
        # paused acks: [17 in-flight][10 persisted][3 in-flight][1 persisted]
        exp_needs_ack = in_flight_buffer_size + 11 - 30
        await await_for(
            lambda: len(parent.links.needs_ack) == exp_needs_ack,
            1,
            f"ERROR waiting for parent to have {exp_needs_ack} paused acks",
            err_str_f=h.summary_str,
        )
        parent.assert_event_counts(
            num_pending=exp_parent_pending,
            all_pending=True,
            tag="parent after overflow",
        )
        assert_acks_consistent(h)

        # generate 60 events. 30 Should end up in-flight, and 30 persisted
        exp_pending = 11
        exp_persists = child_startup_persists + exp_pending
        num_to_generate = 60
        for i in range(num_to_generate):
            child.generate_event(
                DBGEvent(
                    Command=DBGPayload(),
                    Msg=f"refill event {i}",
                )
            )
            if exp_in_flight < in_flight_buffer_size:
                exp_in_flight += 1
                # paused acks:
                # [17 in-flight][10 persisted][3 in-flight][1 persisted][n in-flight]
            else:
                exp_pending += 1
                exp_persists += 1
                # paused acks:
                # [17 in-flight][10 persisted][3 in-flight][1 persisted][30 in-flight][n persisted]
            child.assert_event_counts(
                num_pending=exp_pending,
                num_in_flight=exp_in_flight,
                num_persists=exp_persists,
                num_retrieves=child_startup_persists,
                num_clears=child_startup_persists,
                tag=f"generated event {i+1} / {num_to_generate}  ",
                err_str=h.summary_str(),
            )

        # paused acks:
        # [17 in-flight][10 persisted][3 in-flight][1 persisted][30 in-flight][30 persisted]
        child.assert_event_counts(
            num_pending=41,
            num_in_flight=50,
            num_persists=44,
            num_retrieves=3,
            num_clears=3,
            tag=f"After generating {num_to_generate} events",
            err_str=h.summary_str(),
        )
        exp_needs_ack = in_flight_buffer_size + 11 + 30
        assert (child.links.num_pending + child.links.num_in_flight) == exp_needs_ack
        await await_for(
            lambda: len(parent.links.needs_ack) == exp_needs_ack,
            1,
            f"ERROR waiting for parent to have {exp_needs_ack} paused acks",
            err_str_f=h.summary_str,
        )
        exp_parent_pending += 60
        parent.assert_event_counts(
            num_pending=exp_parent_pending,
            all_pending=True,
            tag="parent after add 60 more",
        )
        assert_acks_consistent(h)

        # release all the acks, group by group
        exp_in_flight = in_flight_buffer_size
        exp_pending = 41
        exp_persists = child_startup_persists + exp_pending
        exp_clears = child_startup_persists
        for group_idx, (group_size, group_in_flight) in enumerate(
            [
                (17, True),
                (10, False),
                (3, True),
                (1, False),
                (30, True),
                (30, False),
            ]
        ):
            parent.release_acks(num_to_release=group_size)
            if group_in_flight:
                exp_in_flight -= group_size
                await await_for(
                    lambda: child.links.num_in_flight == exp_in_flight,  # noqa: B023
                    1,
                    f"ERROR waiting for in-flight to be {exp_in_flight} in flight acks",
                    err_str_f=h.summary_str,
                )
            else:
                exp_pending -= group_size
                exp_clears += group_size
                await await_for(
                    lambda: child.links.num_pending == exp_pending,  # noqa: B023
                    1,
                    f"ERROR waiting for child to receive an {acks_released} acks",
                )
            child.assert_event_counts(
                num_pending=exp_pending,
                num_in_flight=exp_in_flight,
                num_persists=exp_persists,
                num_retrieves=child_startup_persists,
                num_clears=exp_clears,
                tag=f"group {group_idx}  group_size: {group_size}  group_in_flight: {group_in_flight}  ",
                err_str=h.summary_str(),
            )
            exp_needs_ack -= group_size
            await await_for(
                lambda: len(parent.links.needs_ack) == exp_needs_ack,  # noqa: B023
                1,
                f"ERROR waiting for parent to have {exp_needs_ack} paused acks",
            )
            parent.assert_event_counts(
                num_pending=exp_parent_pending,
                all_pending=True,
                tag=f"parent after add group {group_idx}  group_size: {group_size}  ",
            )
            assert_acks_consistent(h)

        # all events should have been passed along.
        child.assert_event_counts(
            num_pending=0,
            num_in_flight=0,
            num_persists=exp_persists,
            num_retrieves=child_startup_persists,
            num_clears=exp_clears,
            tag="Child events are clear",
            err_str=h.summary_str(),
        )
        exp_parent_persists = sum(
            [
                4,  # parent startup, connect, subscribe, peer active
                4,  # child startup, connect, subscribe, peer active
                in_flight_buffer_size,  # first events
                in_flight_buffer_size,  # flowing events whiles acks arrive
                overflow_size,  # overflow events while acks arrive
                4,  # refill buffer and overflow again
                60,  # 60 more
            ]
        )
        await await_for(
            lambda: parent.event_persister.num_persists == exp_parent_persists,
            1,
            f"ERROR waiting for parent to persist {exp_parent_persists} events",
        )
        assert_acks_consistent(h)


@pytest.mark.asyncio
async def test_in_flight_comm_loss(request: Any) -> None:
    """Verify that events are persisted if we lose comm while events are
    in-flight"""

    async with LiveTest(start_child=True, start_parent=True, request=request) as h:
        await await_quiescent_connections(h)
        child = h.child
        upstream_link = child.links.link(child.upstream_client)
        parent = h.parent
        parent.pause_acks()

        startup_persists = 3
        exp_persists = startup_persists  # startup, connect, subscribed
        child.assert_event_counts(num_persists=exp_persists, all_clear=True)
        exp_parent_pending = 8
        parent.assert_event_counts(
            num_pending=exp_parent_pending,
            all_pending=True,
            tag="parent after add group quiescent  ",
        )

        # generate some in-flight events
        num_to_generate = 21
        exp_in_flight = num_to_generate - 1
        for i in range(exp_in_flight):
            child.generate_event(DBGEvent(Command=DBGPayload(), Msg=f"event {i+1}"))
        await await_for(
            lambda: len(parent.needs_ack) == exp_in_flight,
            1,
            f"ERROR waiting for parent to have {exp_in_flight} paused acks",
        )
        child.assert_event_counts(
            num_in_flight=exp_in_flight, num_persists=exp_persists, all_clear=True
        )
        exp_parent_pending += exp_in_flight
        parent.assert_event_counts(
            num_pending=exp_parent_pending,
            all_pending=True,
            tag=f"parent after add group generating {exp_in_flight} events  ",
        )
        # generate one more event and time it out
        child.set_ack_timeout_seconds(0.001)
        child.generate_event(DBGEvent(Command=DBGPayload(), Msg=f"event {i+2}"))
        exp_in_flight += 1
        exp_pending = num_to_generate + 1  # generated + timeout
        exp_persists += exp_pending
        await await_for(
            lambda: upstream_link.in_state(StateName.awaiting_peer),
            1,
            "ERROR waiting for child to see timeout",
            err_str_f=h.summary_str,
        )
        exp_in_flight = 0
        child.assert_event_counts(
            num_pending=exp_pending,
            num_in_flight=exp_in_flight,
            num_persists=exp_persists,
            num_clears=startup_persists,
            num_retrieves=startup_persists,
        )
        exp_parent_pending += 1
        await await_for(
            lambda: parent.links.num_pending == exp_parent_pending,
            1,
            f"ERROR waiting for parent to have {exp_in_flight} paused acks",
        )
        parent.assert_event_counts(
            num_pending=exp_parent_pending,
            all_pending=True,
            tag=f"parent after add group generating {exp_in_flight} events  ",
        )

        # release acks, wait for events be at rest.
        child.restore_ack_timeout_seconds()
        parent.release_acks(num_to_release=-1)
        exp_parent_pending += 2
        # The parent will receive the generated events twice since they
        # all time out and are then re-sent.
        exp_parent_persists = exp_parent_pending + num_to_generate
        await await_quiescent_connections(
            h,
            exp_child_persists=exp_persists,
            exp_parent_pending=exp_parent_pending,
            exp_parent_persists=exp_parent_persists,
        )
        exp_in_flight = 0
        exp_pending = 0
        child.assert_event_counts(
            num_pending=0,
            num_in_flight=0,
            num_persists=exp_persists,
            num_clears=exp_persists,
            num_retrieves=exp_persists,
        )


@pytest.mark.asyncio
async def test_in_flight_overflow_comm_loss(request: Any) -> None:
    async with LiveTest(start_child=True, start_parent=True, request=request) as h:
        await await_quiescent_connections(h)
        child = h.child
        upstream_link = child.links.link(child.upstream_client)
        parent = h.parent
        parent.pause_acks()

        startup_persists = child.event_persister.num_persists
        exp_child_persists = startup_persists
        exp_parent_pending = parent.links.num_pending

        # generate events, filling up the in-flight buffer and overflowing
        num_to_generate = child.settings.proactor.num_inflight_events * 2
        for i in range(num_to_generate - 1):
            child.generate_event(DBGEvent(Command=DBGPayload(), Msg=f"event {i+1}"))
        # generate one more and time it out
        child.set_ack_timeout_seconds(0.001)
        child.generate_event(DBGEvent(Command=DBGPayload(), Msg=f"event {i+2}"))
        await await_for(
            lambda: upstream_link.in_state(StateName.awaiting_peer),
            1,
            "ERROR waiting for child to see timeout",
            err_str_f=h.summary_str,
        )
        exp_child_pending = num_to_generate + 1
        exp_child_persists += exp_child_pending
        child.assert_event_counts(
            num_pending=exp_child_pending,
            num_in_flight=0,
            num_persists=exp_child_persists,
            num_clears=startup_persists,
            num_retrieves=startup_persists,
        )

        await await_for(
            lambda: len(parent.needs_ack) == num_to_generate,
            1,
            f"ERROR waiting for parent to have {num_to_generate} paused acks",
        )
        exp_parent_pending += num_to_generate
        parent.assert_event_counts(
            num_pending=exp_parent_pending,
            all_pending=True,
            tag=f"parent after generating {num_to_generate} events  ",
        )
        # release acks, wait for events be at rest.
        child.restore_ack_timeout_seconds()
        parent.release_acks(num_to_release=-1)
        exp_parent_pending += 2  # timeout and peer active
        # The parent will receive the generated events twice since they
        # all time out and are then re-sent.
        exp_parent_persists = exp_parent_pending + num_to_generate
        await await_quiescent_connections(
            h,
            exp_child_persists=exp_child_persists,
            exp_parent_pending=exp_parent_pending,
            exp_parent_persists=exp_parent_persists,
        )
        child.assert_event_counts(
            num_pending=0,
            num_in_flight=0,
            num_persists=exp_child_persists,
            all_clear=True,
        )
