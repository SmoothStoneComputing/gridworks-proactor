# ruff: noqa: ERA001
# mypy: disable-error-code="union-attr"


import time
from dataclasses import dataclass
from pathlib import Path
from typing import Generic, Type

import pytest
from gwproto import MQTTTopic
from result import Err

from gwproactor import Proactor
from gwproactor.links import StateName
from gwproactor.message import DBGEvent, DBGPayload
from gwproactor.persister import TimedRollingFilePersister
from gwproactor_test.comm_test_helper import (
    ChildSettingsT,
    ChildT,
    CommTestHelper,
    ParentSettingsT,
    ParentT,
)
from gwproactor_test.wait import await_for


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
class ProactorReuploadTests(Generic[ParentT, ChildT, ParentSettingsT, ChildSettingsT]):
    CTH: Type[CommTestHelper[ParentT, ChildT, ParentSettingsT, ChildSettingsT]]

    @pytest.mark.asyncio
    async def test_reupload_basic(self) -> None:
        """
        Test:
            reupload not requiring flow control
        """
        async with self.CTH(
            start_child=True,
            add_parent=True,
            verbose=False,
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
            await await_for(
                lambda: reupload_counts.completed > 0,
                1,
                "ERROR waiting for re-upload to complete",
                err_str_f=h.summary_str,
            )

            # All events should have been reuploaded.
            assert child.links.num_reupload_pending == 0
            assert child.links.num_reuploaded_unacked == 0
            assert not child.links.reuploading()

    @pytest.mark.asyncio
    async def test_reupload_flow_control_simple(self) -> None:
        """
        Test:
            reupload requiring flow control
        """
        async with self.CTH(
            start_child=True,
            add_parent=True,
            child_settings=self.CTH.child_settings_t(num_initial_event_reuploads=5),
            verbose=False,
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
            base_num_pending = child.links.num_pending
            assert base_num_pending > 0
            assert child.links.num_reupload_pending == 0
            assert child.links.num_reuploaded_unacked == 0
            assert not child.links.reuploading()

            # Generate more events than fit in pipe.
            events_to_generate = child.settings.num_initial_event_reuploads * 2
            for i in range(events_to_generate):
                child.generate_event(
                    DBGEvent(
                        Command=DBGPayload(),
                        Msg=f"event {i + 1} / {events_to_generate}",
                    )
                )
            child.logger.info(
                f"Generated {events_to_generate} events. Total pending events: {child.links.num_pending}"
            )

            # Start parent, wait for connect.
            h.start_parent()
            await await_for(
                lambda: upstream_link.active(),
                1,
                "ERROR waiting for parent",
                err_str_f=h.summary_str,
            )

            # Wait for reupload to complete
            await await_for(
                lambda: reupload_counts.completed > 0,
                1,
                "ERROR waiting for reupload to complete",
                err_str_f=h.summary_str,
            )

    @pytest.mark.asyncio
    async def test_reupload_flow_control_detail(self) -> None:
        """
        Test:
            reupload requiring flow control
        """
        async with self.CTH(
            start_child=True,
            add_parent=True,
            child_settings=self.CTH.child_settings_t(num_initial_event_reuploads=5),
            child_verbose=False,
            verbose=False,
            # parent_on_screen=True,
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
            base_num_pending = child_links.num_pending
            assert base_num_pending > 0
            assert child_links.num_reupload_pending == 0
            assert child_links.num_reuploaded_unacked == 0
            assert not child_links.reuploading()

            # Generate more events than fit in pipe.
            events_to_generate = child.settings.num_initial_event_reuploads * 2
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
            assert child_links.num_reupload_pending == 0
            assert child_links.num_reuploaded_unacked == 0
            assert not child_links.reuploading()

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
            last_num_to_reupload = events_to_generate + base_num_pending
            last_num_reuploaded_unacked = child.settings.num_initial_event_reuploads
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
            assert (
                child_links.num_reuploaded_unacked == last_num_reuploaded_unacked
            ), err_s
            assert child_links.num_reupload_pending == last_num_repuload_pending, err_s
            assert child_links.num_pending == last_num_to_reupload + 1
            assert child_links.reuploading()

            # noinspection PyTypeChecker
            parent_ack_topic = MQTTTopic.encode(
                "gw",
                h.parent.publication_name,
                h.child.subscription_name,
                "gridworks-ack",
            )
            acks_received_by_child = child.stats.num_received_by_topic[parent_ack_topic]

            # Release acks one by one.
            #
            #   Bound this loop by time, not by total number of acks since at least one non-reupload ack should arrive
            #   (for the PeerActive event) and others could arrive if, for example, a duplicate MQTT message appeared.
            #
            end_time = time.time() + 5
            # loop_count_dbg = 0
            acks_released = 0
            while child_links.reuploading() and time.time() < end_time:
                # loop_path_dbg = 0
                # loop_count_dbg += 1

                # release one ack
                acks_released += h.parent.release_acks(num_to_release=1)

                # Wait for child to receive an ack
                await await_for(
                    lambda: child.stats.num_received_by_topic[parent_ack_topic]
                    == acks_received_by_child + acks_released,  # noqa: B023
                    timeout=1,
                    tag=(
                        "ERROR waiting for child to receive ack "
                        f"(acks_released: {acks_released}) "
                        f"on topic <{parent_ack_topic}>"
                    ),
                    err_str_f=h.summary_str,
                )
                curr_num_reuploaded_unacked = child_links.num_reuploaded_unacked
                curr_num_repuload_pending = child_links.num_reupload_pending
                curr_num_to_reuplad = (
                    curr_num_reuploaded_unacked + curr_num_repuload_pending
                )
                if curr_num_to_reuplad == last_num_to_reupload:
                    # loop_path_dbg |= 0x00000001
                    assert curr_num_reuploaded_unacked == last_num_reuploaded_unacked
                    assert curr_num_repuload_pending == last_num_repuload_pending
                elif curr_num_to_reuplad == last_num_to_reupload - 1:
                    # loop_path_dbg |= 0x00000002
                    if curr_num_reuploaded_unacked == last_num_reuploaded_unacked:
                        assert (
                            curr_num_repuload_pending == last_num_repuload_pending - 1
                        )
                    else:
                        assert (
                            curr_num_reuploaded_unacked
                            == last_num_reuploaded_unacked - 1
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

                # child.logger.info(
                #     f"ack loop: {loop_count_dbg} / {acks_released}:"
                #     f"({last_num_reuploaded_unacked}, {last_num_repuload_pending}) -> "
                #     f"({curr_num_reuploaded_unacked}, {curr_num_repuload_pending})"
                #     f" loop_path_dbg: 0x{loop_path_dbg:08X}")

                last_num_to_reupload = curr_num_to_reuplad
                last_num_reuploaded_unacked = curr_num_reuploaded_unacked
                last_num_repuload_pending = curr_num_repuload_pending

            assert not child_links.reuploading()

    @pytest.mark.skip(
        reason="Test seems to gotten flakier; unclear if this is because test is too sensitive or because it is broken"
    )
    @pytest.mark.asyncio
    async def test_reupload_errors(self) -> None:
        async with self.CTH(
            start_child=True,
            add_parent=True,
            child_verbose=False,
        ) as h:
            child = h.child
            child.disable_derived_events()
            reupload_counts = h.child.stats.link(child.upstream_client).reupload_counts
            child_links = h.child.links
            upstream_link = child_links.link(child.upstream_client)
            parent = h.parent

            def _err_str() -> str:
                return (
                    f"\nCHILD\n{child.summary_str()}\n"
                    f"\nPARENT\n{parent.summary_str()}\n"
                )

            await await_for(
                lambda: child.mqtt_quiescent(),
                1,
                "ERROR waiting for child to connect to mqtt",
                err_str_f=_err_str,
            )
            base_num_pending = child_links.num_pending
            assert base_num_pending > 0
            assert child_links.num_reupload_pending == 0
            assert child_links.num_reuploaded_unacked == 0
            assert not child_links.reuploading()

            generator = _EventGen(child)  # type: ignore[arg-type]
            generator.generate(num_corrupt=10)
            generator.generate(num_ok=10)
            generator.generate(num_empty=10)
            generator.generate(num_ok=10)
            generator.generate(num_missing=10)
            generator.generate(num_ok=10)

            h.start_parent()
            await await_for(
                lambda: upstream_link.active(),
                1,
                "ERROR waiting for active",
                err_str_f=_err_str,
            )

            # Wait for reupload to complete
            await await_for(
                lambda: reupload_counts.completed > 0,
                3,
                "ERROR waiting for reupload to complete",
                err_str_f=_err_str,
            )
            assert reupload_counts.started == reupload_counts.completed
            assert parent.stats.num_events_received >= base_num_pending + 60
