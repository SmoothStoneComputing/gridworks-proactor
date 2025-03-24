# ruff: noqa: PLR2004, ERA001
# mypy: disable-error-code="union-attr,attr-defined"


import typing
from typing import Type

import pytest

from gwproactor.links import StateName
from gwproactor_test.comm_test_helper import (
    ChildSettingsT,
    ChildT,
    ParentSettingsT,
    ParentT,
)
from gwproactor_test.dummies.tree.messages import RelayInfoReported
from gwproactor_test.recorder import RecorderLinkStats
from gwproactor_test.tree_comm_test_helper import TreeCommTestHelper
from gwproactor_test.wait import await_for


@pytest.mark.asyncio
class ProactorTreeCommBasicTests(
    typing.Generic[ParentT, ChildT, ParentSettingsT, ChildSettingsT]
):
    CTH: Type[TreeCommTestHelper[ParentT, ChildT, ParentSettingsT, ChildSettingsT]]

    async def test_tree_no_parent(self) -> None:
        async with self.CTH() as h:
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
                lambda: link1to2.active_for_send() and link1toAtn.active_for_send(),
                1,
                "ERROR waiting child1 links to be active_for_send",
                err_str_f=h.summary_str,
            )
            assert link1to2.state == StateName.awaiting_peer
            assert link1toAtn.state == StateName.awaiting_peer
            assert counts1to2["gridworks.event.comm.mqtt.connect"] == 1
            assert counts1to2["gridworks.event.comm.mqtt.fully.subscribed"] == 1
            assert len(stats1to2.comm_events) == 2
            assert counts1toAtn["gridworks.event.comm.mqtt.connect"] == 1
            assert counts1toAtn["gridworks.event.comm.mqtt.fully.subscribed"] == 1
            assert len(stats1toAtn.comm_events) == 2

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
                lambda: link1to2.active() and link2to1.active(),
                1,
                "ERROR waiting child2 links to be active",
                err_str_f=h.summary_str,
            )
            assert link1to2.state == StateName.active
            assert link2to1.state == StateName.active
            assert counts1to2["gridworks.event.comm.peer.active"] == 1
            assert len(stats1to2.comm_events) == 3
            assert counts2to1["gridworks.event.comm.peer.active"] == 1
            assert counts2to1["gridworks.event.comm.mqtt.connect"] == 1
            assert counts2to1["gridworks.event.comm.mqtt.fully.subscribed"] == 1
            assert len(stats2to1.comm_events) == 3

    async def test_tree_message_exchange(self) -> None:
        async with self.CTH(
            start_child1=True,
            start_child2=True,
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
            child1.set_relay(relay_name, True)

            # wait for response to be received
            await await_for(
                lambda: stats1.num_received_by_type["gridworks.event.relay.report"]
                == 1,
                1,
                "ERROR waiting child1 to receive relay report from child2",
                err_str_f=h.summary_str,
            )
            assert stats2.num_received_by_type["gridworks.dummy.set.relay"] == 1
            assert child1.relays.Relays == {relay_name: RelayInfoReported(Closed=True)}
            assert child2.relays == {relay_name: True}
            assert child1.relays.TotalChangeMismatches == 0

    @pytest.mark.asyncio
    async def test_tree_parent_comm(self) -> None:
        async with self.CTH(add_child=True) as h:
            h.start_child1()
            await await_for(
                h.child1.mqtt_quiescent,
                3,
                "child1.mqtt_quiescent",
                err_str_f=h.summary_str,
            )
            h.add_child2()
            h.start_child2()
            link1to2 = h.child1.links.link(h.child1.downstream_client)
            link2to1 = h.child2.links.link(h.child2.upstream_client)
            await await_for(
                lambda: link1to2.active() and link2to1.active(),
                3,
                "link1to2.active() and link2to1.active()",
                err_str_f=h.summary_str,
            )
            h.add_parent()
            h.start_parent()
            link1toAtn = h.child1.links.link(h.child1.upstream_client)
            linkAtnto1 = h.parent.links.link(h.parent.downstream_client)
            await await_for(
                lambda: link1toAtn.active() and linkAtnto1.active(),
                3,
                "link1toAtn.active() and linkAtnto1.active()",
                err_str_f=h.summary_str,
            )

    @pytest.mark.asyncio
    async def test_tree_event_forward(self) -> None:
        # child1_settings = DummyScada1Settings(
        #     logging=LoggingSettings(
        #         levels=LoggerLevels(
        #             message_summary=logging.INFO,
        #         )
        #     ),
        #     mqtt_link_poll_seconds=5,
        # )
        async with self.CTH(
            # child_settings=child1_settings,
            start_child=True,
            start_child2=True,
            start_parent=True,
            # verbose=True,
            # child1_verbose=True,
            # child2_verbose=True,
            child2_on_screen=False,
            parent_on_screen=False,
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
            h.child1.set_relay(relay_name, True)

            statsAtnTo1 = typing.cast(
                RecorderLinkStats, h.parent.stats.link(h.parent.downstream_client)
            )

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
