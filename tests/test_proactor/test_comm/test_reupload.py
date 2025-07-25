# ruff: noqa: ERA001,PLR2004
import time
from dataclasses import dataclass
from pathlib import Path

import pytest
from gwproto import MQTTTopic
from result import Err

from gwproactor import Proactor
from gwproactor.links import StateName
from gwproactor.message import DBGEvent, DBGPayload
from gwproactor.persister import TimedRollingFilePersister
from gwproactor_test.live_test_helper import (
    LiveTest,
)
from gwproactor_test.wait import await_for


@pytest.mark.asyncio
async def test_reupload_basic(request: pytest.FixtureRequest) -> None:
    """
    Test:
        reupload not requiring flow control
    """
    async with LiveTest(
        start_child=True,
        add_parent=True,
        request=request,
    ) as h:
        child = h.child
        child.disable_derived_events()
        upstream_link = h.child.links.link(child.upstream_client)
        reupload_counts = h.child.stats.link(child.upstream_client).reupload_counts
        await await_for(
            lambda: child.mqtt_quiescent(),
            1,
            "ERROR waiting for child to connect to mqtt",
            err_str_f=h.summary_str,
        )
        # Some events should have been generated, and they should have all been sent
        assert child.links.num_pending > 0
        assert child.links.num_reupload_pending == 0
        assert child.links.num_reuploaded_unacked == 0
        assert not child.links.reuploading()
        assert reupload_counts.started == 0
        assert reupload_counts.completed == 0

        # Start parent, wait for reconnect.
        h.start_parent()
        await await_for(
            lambda: upstream_link.active(),
            1,
            "ERROR waiting for parent",
            err_str_f=h.summary_str,
        )

        # Wait for reuploading to complete
        await h.await_for(
            lambda: reupload_counts.completed > 0
            and child.links.num_pending == 0
            and child.links.num_in_flight == 0,
            "ERROR waiting for re-upload to complete",
        )

        # All events should have been reuploaded.
        assert child.links.num_reupload_pending == 0
        assert child.links.num_reuploaded_unacked == 0
        assert not child.links.reuploading()

        assert child.event_persister.num_persists >= 3
        assert child.event_persister.num_retrieves >= child.event_persister.num_persists
        assert child.event_persister.num_clears == child.event_persister.num_persists

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
        await h.await_for(
            lambda: h.parent.event_persister.num_persists > exp_events,
            f"ERROR waiting for parent to finish persisting {exp_events} events",
        )


@pytest.mark.asyncio
async def test_reupload_flow_control_simple(request: pytest.FixtureRequest) -> None:
    """
    Test:
        reupload requiring flow control
    """

    from gwproactor import ProactorSettings
    from gwproactor_test.dummies.pair.child import DummyChildSettings

    async with LiveTest(
        start_child=True,
        add_parent=True,
        child_app_settings=DummyChildSettings(
            proactor=ProactorSettings(num_initial_event_reuploads=5)
        ),
        request=request,
    ) as h:
        child = h.child
        child.disable_derived_events()
        upstream_link = h.child.links.link(child.upstream_client)
        reupload_counts = h.child.stats.link(child.upstream_client).reupload_counts
        await h.await_for(
            lambda: child.mqtt_quiescent(),
            "ERROR waiting for child to connect to mqtt",
        )
        # Some events should have been generated, and they should have all been sent
        assert child.links.num_pending == 3
        assert child.links.num_reupload_pending == 0
        assert child.links.num_reuploaded_unacked == 0
        assert not child.links.reuploading()

        # Generate more events than fit in pipe.
        events_to_generate = child.settings.proactor.num_initial_event_reuploads * 2
        for i in range(events_to_generate):
            child.generate_event(
                DBGEvent(
                    Command=DBGPayload(),
                    Msg=f"event {i + 1} / {events_to_generate}",
                )
            )
            assert child.links.num_pending == 3 + i + 1
        child.logger.info(
            f"Generated {events_to_generate} events. Total pending events: {child.links.num_pending}"
        )
        assert child.links.num_pending == (3 + events_to_generate)
        assert child.links.num_in_flight == 0
        assert child.event_persister.num_persists == (3 + events_to_generate)
        assert child.event_persister.num_retrieves == 0
        assert child.event_persister.num_clears == 0

        # Start parent, wait for connect.
        h.start_parent()
        await await_for(
            lambda: upstream_link.active(),
            1,
            "ERROR waiting for parent",
            err_str_f=h.summary_str,
        )

        # Wait for reupload to complete
        await h.await_for(
            lambda: reupload_counts.completed > 0
            and child.links.num_pending == 0
            and child.links.num_in_flight == 0,
            "ERROR waiting for reupload to complete",
        )
        assert child.event_persister.num_persists >= (3 + events_to_generate)
        assert child.event_persister.num_retrieves >= child.event_persister.num_persists
        assert child.event_persister.num_clears == child.event_persister.num_persists
        # parent should have persisted:
        exp_events = sum(
            [
                1,  # parent startup
                3,  # parent connect, subscribe, peer active
                1,  # child startup
                3,  # child connect, subscribe, peer active
                events_to_generate,  # generated events
            ]
        )
        # wait for parent to finish persisting
        await h.await_for(
            lambda: h.parent.event_persister.num_persists >= exp_events,
            f"ERROR waiting for parent to finish persisting {exp_events} events",
        )


@pytest.mark.asyncio
async def test_reupload_flow_control_detail(request: pytest.FixtureRequest) -> None:
    """
    Test:
        reupload requiring flow control
    """
    from gwproactor import ProactorSettings
    from gwproactor_test.dummies.pair.child import DummyChildSettings

    async with LiveTest(
        start_child=True,
        add_parent=True,
        child_app_settings=DummyChildSettings(
            proactor=ProactorSettings(num_initial_event_reuploads=5)
        ),
        request=request,
    ) as h:
        child = h.child
        child.disable_derived_events()
        child_links = h.child.links
        upstream_link = child_links.link(child.upstream_client)
        await await_for(
            lambda: child.mqtt_quiescent(),
            1,
            "ERROR waiting for child to connect to mqtt",
            err_str_f=h.summary_str,
        )
        # Some events should happened already, through the startup and mqtt connect process, and they should have
        # all been sent.
        # These events include: There are at least 3 non-generated events: startup, (mqtt connect, mqtt subscribed)/mqtt client.
        assert child_links.num_pending == 3
        assert child_links.num_reupload_pending == 0
        assert child_links.num_reuploaded_unacked == 0
        assert not child_links.reuploading()

        # Generate more events than fit in reupload pipe.
        events_to_generate = child.settings.proactor.num_initial_event_reuploads * 2
        for i in range(events_to_generate):
            child.generate_event(
                DBGEvent(
                    Command=DBGPayload(),
                    Msg=f"event {i + 1} / {events_to_generate}",
                )
            )
        child.logger.info(
            f"Generated {events_to_generate} events. Total pending events: {child_links.num_pending}"
        )
        await await_for(
            lambda: child_links.link_state(child.upstream_client)
            == StateName.awaiting_peer,
            1,
            "ERROR wait for child to flush all events to database",
            err_str_f=h.summary_str,
        )

        assert child.links.num_pending == 3 + events_to_generate
        assert child.links.num_in_flight == 0
        assert child.event_persister.num_persists == (3 + events_to_generate)
        assert child.event_persister.num_retrieves == 0
        assert child.event_persister.num_clears == 0
        assert child_links.num_reupload_pending == 0
        assert child_links.num_reuploaded_unacked == 0
        assert not child_links.reuploading()

        # Restore child to normal ack timeout
        child.restore_ack_timeout_seconds()

        # pause parent acks so that we watch flow control
        h.parent.pause_acks()

        # Start parent, wait for parent to be subscribed.
        h.start_parent()
        await await_for(
            lambda: h.parent.links.link_state(h.parent.downstream_client)
            == StateName.awaiting_peer,
            1,
            "ERROR waiting for parent awaiting_peer",
            err_str_f=h.summary_str,
        )

        # Wait for parent to have ping waiting to be sent
        await await_for(
            lambda: len(h.parent.links.needs_ack) > 0,
            1,
            "ERROR waiting for parent awaiting_peer",
            err_str_f=h.summary_str,
        )

        # release the ping
        h.parent.release_acks(num_to_release=1)

        # wait for child to receive ping
        await await_for(
            lambda: upstream_link.active(),
            1,
            "ERROR waiting for child peer_active",
            err_str_f=h.summary_str,
        )
        # There are 3 non-generated events: startup, mqtt connect, mqtt subscribed.
        # A "PeerActive" event is also pending but that is _not_ part of re-upload because it is
        # generated _after_ the peer is active (and therefore has its own ack timeout running, so does not need to
        # be managed by reupload).
        last_num_to_reupload = events_to_generate + 3
        last_num_reuploaded_unacked = (
            child.settings.proactor.num_initial_event_reuploads
        )
        last_num_repuload_pending = (
            last_num_to_reupload - child_links.num_reuploaded_unacked
        )
        err_s = (
            f"child_links.num_reuploaded_unacked: {child_links.num_reuploaded_unacked}\n"
            f"last_num_reuploaded_unacked:        {last_num_reuploaded_unacked}\n"
            f"child_links.num_reupload_pending:   {child_links.num_reupload_pending}\n"
            f"last_num_repuload_pending:          {last_num_repuload_pending}\n"
            f"{child.summary_str()}"
        )
        assert child_links.num_reuploaded_unacked == last_num_reuploaded_unacked, err_s
        assert child_links.num_reupload_pending == last_num_repuload_pending, err_s
        assert child_links.reuploading()
        assert child_links.num_pending == last_num_to_reupload
        assert child.links.num_in_flight == 1
        assert child.event_persister.num_persists == last_num_to_reupload
        assert (
            child.event_persister.num_retrieves
            == child.settings.proactor.num_initial_event_reuploads
        )
        assert child.event_persister.num_clears == 0

        parent_ack_topic = MQTTTopic.encode(
            "gw",
            h.parent.publication_name,
            h.child.subscription_name,
            "gridworks-ack",
        )

        # Release acks one by one.
        #
        #   Bound this loop by time, not by total number of acks since at least one non-reupload ack should arrive
        #   (for the PeerActive event) and others could arrive if, for example, a duplicate MQTT message appeared.
        #
        end_time = time.time() + 5
        loop_count_dbg = 0
        loop_path_dbg = 0
        acks_released = 0
        last_pending = child_links.num_pending
        last_in_flight = child.links.num_in_flight
        assert last_in_flight == 1
        last_persists = child.event_persister.num_persists
        initial_persists = child.event_persister.num_persists
        last_retrieves = child.event_persister.num_retrieves
        last_clears = child.event_persister.num_clears
        curr_num_reuploaded_unacked = child_links.num_reuploaded_unacked
        curr_num_repuload_pending = child_links.num_reupload_pending
        curr_num_to_reuplad = curr_num_reuploaded_unacked + curr_num_repuload_pending

        def _loop_dbg(tag: str = "") -> str:
            return (
                f"ack loop: {loop_count_dbg} ({acks_released}): "
                f"reupload: ({last_num_reuploaded_unacked}, {last_num_repuload_pending}) -> "
                f"({curr_num_reuploaded_unacked}, {curr_num_repuload_pending})  "
                f"in-flight/pending: ({last_in_flight}, {last_pending}) -> "
                f"({child.links.num_in_flight}, {child_links.num_pending})  "
                f"persister: ({last_persists}, {last_retrieves}, {last_clears}) -> "
                f"({child.event_persister.num_persists}, "
                f"{child.event_persister.num_retrieves}, "
                f"{child.event_persister.num_clears})  "
                f"loop_path_dbg: 0x{loop_path_dbg:08X}{f'  [{tag}]' if tag else ''}"
            )

        assert child_links.num_pending == last_num_to_reupload
        child.logger.info(_loop_dbg("pre-loop"))
        while child_links.reuploading() and time.time() < end_time:
            loop_path_dbg = 0
            loop_count_dbg += 1

            # release one ack
            assert child_links.num_pending == last_pending
            acks_released += h.parent.release_acks(num_to_release=1)

            # Wait for child to receive an ack
            last_events = last_in_flight + last_pending
            await h.await_for(
                lambda: child.links.num_pending + child.links.num_in_flight
                == last_events - 1,  # noqa: B023
                tag=(
                    "ERROR waiting for child to receive ack "
                    f"(acks_released: {acks_released}) "
                    f"on topic <{parent_ack_topic}>"
                ),
            )
            curr_num_reuploaded_unacked = child_links.num_reuploaded_unacked
            curr_num_repuload_pending = child_links.num_reupload_pending
            curr_num_to_reuplad = (
                curr_num_reuploaded_unacked + curr_num_repuload_pending
            )

            # There should be no persisting during reupload.
            assert child.event_persister.num_persists == initial_persists

            # The peer active event is not part of re-upload, and might get
            # acked during the re-upload. There should be no other in-flight
            # events.
            if child.links.num_in_flight not in (1, 0):
                raise ValueError(
                    f"ERROR got unexpected num_in_flight "
                    f"({child.links.num_in_flight}). Expected 1 or 0"
                )
            # ack of a re-upload event
            if child.links.num_in_flight == 1:
                assert child_links.num_pending == last_pending - 1
                assert child.event_persister.num_retrieves == last_retrieves + 1
                assert child.event_persister.num_clears == last_clears + 1
            # ack of the peer-active, in-flight event
            elif last_in_flight == 1 and child.links.num_in_flight == 0:
                assert child_links.num_pending == last_pending
                assert child.event_persister.num_retrieves == last_retrieves
                assert child.event_persister.num_clears == last_clears

            # ack of the peer active event
            if curr_num_to_reuplad == last_num_to_reupload:
                loop_path_dbg |= 0x00000001
                assert curr_num_reuploaded_unacked == last_num_reuploaded_unacked
                assert curr_num_repuload_pending == last_num_repuload_pending
            # ack of a re-upload event
            elif curr_num_to_reuplad == last_num_to_reupload - 1:
                loop_path_dbg |= 0x00000002
                if curr_num_reuploaded_unacked == last_num_reuploaded_unacked:
                    assert curr_num_repuload_pending == last_num_repuload_pending - 1
                else:
                    assert (
                        curr_num_reuploaded_unacked == last_num_reuploaded_unacked - 1
                    )
                    assert curr_num_repuload_pending == last_num_repuload_pending
                assert child_links.reuploading() == bool(
                    curr_num_reuploaded_unacked > 0
                )
            else:
                raise ValueError(
                    "Unexpected change in reupload counts: "
                    f"({last_num_reuploaded_unacked}, {last_num_repuload_pending}) -> "
                    f"({curr_num_reuploaded_unacked}, {curr_num_repuload_pending})"
                )

            child.logger.info(_loop_dbg("iteration complete"))
            last_num_to_reupload = curr_num_to_reuplad
            last_num_reuploaded_unacked = curr_num_reuploaded_unacked
            last_num_repuload_pending = curr_num_repuload_pending
            last_pending = child_links.num_pending
            last_in_flight = child.links.num_in_flight
            last_persists = child.event_persister.num_persists
            last_retrieves = child.event_persister.num_retrieves
            last_clears = child.event_persister.num_clears

        assert not child_links.reuploading()


@dataclass
class _EventEntry:
    uid: str
    path: Path


class _EventGen:
    ok: list[_EventEntry]
    corrupt: list[_EventEntry]
    empty: list[_EventEntry]
    missing: list[_EventEntry]

    persister: TimedRollingFilePersister

    def __len__(self) -> int:
        return len(self.ok) + len(self.corrupt) + len(self.empty)

    def __init__(self, proactor: Proactor) -> None:
        self.ok = []
        self.corrupt = []
        self.empty = []
        self.missing = []
        persister = proactor.event_persister
        assert isinstance(persister, TimedRollingFilePersister)
        self.persister = persister

    def _generate_event(self, member_name: str) -> _EventEntry:
        event = DBGEvent(Command=DBGPayload(), Msg=f"event {len(self)} {member_name}")
        match self.persister.persist(
            event.MessageId, event.model_dump_json(indent=2).encode()
        ):
            case Err(exception):
                raise exception
        entry = _EventEntry(
            event.MessageId,
            self.persister.get_path(event.MessageId),  # type: ignore[arg-type]
        )
        getattr(self, member_name).append(entry)
        return entry

    def _generate_ok(self) -> _EventEntry:
        return self._generate_event("ok")

    def _generate_corrupt(self) -> _EventEntry:
        entry = self._generate_event("corrupt")
        with entry.path.open() as f:
            contents = f.read()
        with entry.path.open("w") as f:
            f.write(contents[:-6])
        return entry

    def _generate_empty(self) -> _EventEntry:
        entry = self._generate_event("empty")
        with entry.path.open("w") as f:
            f.write("")
        return entry

    def _generate_missing(self) -> _EventEntry:
        entry = self._generate_event("missing")
        entry.path.unlink()
        return entry

    def generate(
        self,
        num_ok: int = 0,
        num_corrupt: int = 0,
        num_empty: int = 0,
        num_missing: int = 0,
    ) -> None:
        for _ in range(num_ok):
            self._generate_ok()
        for _ in range(num_corrupt):
            self._generate_corrupt()
        for _ in range(num_empty):
            self._generate_empty()
        for _ in range(num_missing):
            self._generate_missing()


@pytest.mark.asyncio
async def test_reupload_errors(request: pytest.FixtureRequest) -> None:
    """Verify that errors occurring *during* re-upload, relating to retrieving
    persisted events, are detected and uploaded as expected.
    """
    async with LiveTest(
        start_child=True,
        add_parent=True,
        request=request,
    ) as h:
        child = h.child
        child.disable_derived_events()
        child_links = h.child.links

        await h.await_for(
            lambda: child.mqtt_quiescent(),
            "ERROR waiting for child to connect to mqtt",
        )
        child.assert_event_counts(
            num_pending=(3, None),
            num_retrieves=0,
            num_clears=0,
        )
        assert child_links.num_reupload_pending == 0
        assert child_links.num_reuploaded_unacked == 0
        assert not child_links.reuploading()

        # Generate a bunch of 'bad' events which will cause errors during
        # reupload.
        generator = _EventGen(child)
        generator.generate(num_corrupt=10)
        generator.generate(num_ok=10)
        generator.generate(num_empty=10)
        generator.generate(num_ok=10)
        generator.generate(num_missing=10)
        generator.generate(num_ok=10)
        assert child.links.num_pending >= 63
        assert child.links.num_in_flight == 0
        assert child.event_persister.num_persists >= 63
        assert child.event_persister.num_retrieves == 0
        assert child.event_persister.num_clears == 0

        h.start_parent()
        await h.await_quiescent_connections(
            exp_child_persists=63,
            exp_parent_pending=64,
        )
