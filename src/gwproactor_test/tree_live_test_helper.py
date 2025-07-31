import functools
import logging
import textwrap
import typing
from typing import Any, Optional, Self

from gwproto import HardwareLayout

from gwproactor import AppSettings, setup_logging
from gwproactor.app import App
from gwproactor.config import MQTTClient
from gwproactor.links import LinkState
from gwproactor_test.dummies.tree.atn import DummyAtnApp
from gwproactor_test.dummies.tree.scada1 import DummyScada1App
from gwproactor_test.dummies.tree.scada2 import DummyScada2App
from gwproactor_test.event_consistency_checks import EventAckCounts
from gwproactor_test.instrumented_proactor import (
    InstrumentedProactor,
    MinRangeTuple,
    RangeTuple,
    as_range_tuple,
    caller_str,
)
from gwproactor_test.instrumented_stats import RecorderLinkStats
from gwproactor_test.live_test_helper import (
    LiveTest,
    get_option_value,
)
from gwproactor_test.logger_guard import LoggerGuards


class TreeLiveTest(LiveTest):
    _child2_app: App
    child2_verbose: bool = False
    child2_message_summary: bool = False
    child2_on_screen: bool = False
    child2_logger_guards: LoggerGuards

    def __init__(
        self,
        *,
        add_child1: bool = False,
        start_child1: bool = False,
        child1_verbose: Optional[bool] = None,
        child1_message_summary: Optional[bool] = None,
        child1_layout: Optional[HardwareLayout] = None,
        child2_app_settings: Optional[AppSettings] = None,
        child2_layout: Optional[HardwareLayout] = None,
        child2_verbose: Optional[bool] = None,
        child2_message_summary: Optional[bool] = None,
        add_child2: bool = False,
        start_child2: bool = False,
        child2_on_screen: Optional[bool] = None,
        **kwargs: Any,
    ) -> None:
        kwargs["add_child"] = add_child1 or kwargs.get("add_child", False)
        kwargs["start_child"] = start_child1 or kwargs.get("start_child", False)
        if child1_verbose is None:
            kwargs["child_verbose"] = get_option_value(
                parameter_value=child1_verbose,
                option_name="--child1-verbose",
                request=kwargs.get("request"),
            )
        if child1_message_summary is None:
            kwargs["child_message_summary"] = get_option_value(
                parameter_value=child1_verbose,
                option_name="--child1-message-summary",
                request=kwargs.get("request"),
            )
        kwargs["child_layout"] = child1_layout or kwargs.get("child_layout")
        kwargs["request"] = kwargs.get("request")
        super().__init__(**kwargs)
        self.child2_verbose = get_option_value(
            parameter_value=child2_verbose,
            option_name="--child2-verbose",
            request=kwargs.get("request"),
        )
        self.child2_message_summary = get_option_value(
            parameter_value=child2_message_summary,
            option_name="--child2-message-summary",
            request=kwargs.get("request"),
        )
        self.child2_on_screen = get_option_value(
            parameter_value=child2_on_screen,
            option_name="--child2-on-screen",
            request=kwargs.get("request"),
        )
        self._child2_app = self._make_app(
            self.child2_app_type(),
            child2_app_settings,
            app_verbose=self.child2_verbose,
            app_message_summary=self.child2_message_summary,
            layout=child2_layout if child2_layout is not None else kwargs.get("layout"),
        )
        self.setup_child2_logging()
        add_child2 = (
            add_child2
            or start_child2
            or kwargs.get("add_all", False)
            or kwargs.get("start_all", False)
        )
        start_child2 = start_child2 or kwargs.get("start_all", False)
        if add_child2 or start_child2:
            self.add_child2()
            if start_child2:
                self.start_child2()

    @classmethod
    def child_app_type(cls) -> type[App]:
        return DummyScada1App

    @property
    def child_app(self) -> DummyScada1App:
        return typing.cast(DummyScada1App, self._child_app)

    @property
    def child1_app(self) -> DummyScada1App:
        return self.child_app

    @classmethod
    def child2_app_type(cls) -> type[App]:
        return DummyScada2App

    @property
    def child2_app(self) -> DummyScada2App:
        return typing.cast(DummyScada2App, self._child2_app)

    @classmethod
    def parent_app_type(cls) -> type[App]:
        return DummyAtnApp

    @property
    def parent_app(self) -> DummyAtnApp:
        return typing.cast(DummyAtnApp, self._parent_app)

    @property
    def child1(self) -> InstrumentedProactor:
        return self.child

    def add_child1(self) -> Self:
        return self.add_child()

    def start_child1(
        self,
    ) -> Self:
        return self.start_child()

    def remove_child1(
        self,
    ) -> Self:
        return self.remove_child()

    @property
    def child2(self) -> InstrumentedProactor:
        if self.child2_app.proactor is None:
            raise RuntimeError(
                "ERROR. CommTestHelper.child accessed before creating child."
                "pass add_child=True to CommTestHelper constructor or call "
                "CommTestHelper.add_child()"
            )
        return typing.cast(InstrumentedProactor, self.child2_app.proactor)

    def add_child2(
        self,
    ) -> Self:
        self.child2_app.instantiate()
        return self

    def start_child2(
        self,
    ) -> Self:
        if self.child2_app.raw_proactor is None:
            self.add_child2()
        return self.start_proactor(self.child2)

    def remove_child2(
        self,
    ) -> Self:
        self.child2_app.raw_proactor = None
        return self

    @property
    def child1_to_parent_link(self) -> LinkState:
        return self.child_to_parent_link

    @property
    def child1_to_child2_link(self) -> LinkState:
        return self.child1.downstream_link

    @property
    def child2_to_child1_link(self) -> LinkState:
        return self.child2.upstream_link

    @property
    def parent_to_child1_link(self) -> LinkState:
        return self.parent_to_child_link

    @property
    def child1_to_parent_stats(self) -> RecorderLinkStats:
        return self.child_to_parent_stats

    @property
    def child1_to_child2_stats(self) -> RecorderLinkStats:
        return self.child1.downstream_stats

    @property
    def child2_to_child1_stats(self) -> RecorderLinkStats:
        return self.child2.upstream_stats

    @property
    def parent_to_child1_stats(self) -> RecorderLinkStats:
        return self.parent_to_child_stats

    def _get_child2_clients_supporting_tls(self) -> list[MQTTClient]:
        return self._get_clients_supporting_tls(self.child2_app.config.settings)

    def set_use_tls(self, use_tls: bool) -> None:
        super().set_use_tls(use_tls)
        self._set_settings_use_tls(use_tls, self._get_child2_clients_supporting_tls())

    def setup_child2_logging(self) -> None:
        self.child2_app.config.settings.paths.mkdirs(parents=True)
        errors: list[Exception] = []
        self.logger_guards.add_loggers(
            list(
                self.child2_app.config.settings.logging.qualified_logger_names().values()
            )
        )
        setup_logging(
            self.child2_app.config.settings,
            errors=errors,
            add_screen_handler=self.child2_on_screen,
            root_gets_handlers=False,
        )
        assert not errors

    def get_proactors(self) -> list[InstrumentedProactor]:
        proactors = super().get_proactors()
        if self.child2_app.raw_proactor is not None:
            proactors.append(self.child2)
        return proactors

    def get_log_path_str(self, exc: BaseException) -> str:
        return (
            f"CommTestHelper caught error {exc}.\n"
            "Working log dirs:"
            f"\n\t[{self.child_app.config.settings.paths.log_dir}]"
            f"\n\t[{self.parent_app.config.settings.paths.log_dir}]"
        )

    def summary_str(self) -> str:
        s = ""
        if self.child_app.raw_proactor is None:
            s += "SCADA1: None\n"
        else:
            s += "SCADA1:\n"
            s += (
                textwrap.indent(
                    self.child1.summary_str(ack_tracking=self.ack_tracking), "    "
                )
                + "\n"
            )
        if self.child2_app.raw_proactor is None:
            s += "SCADA2: None\n"
        else:
            s += "SCADA2:\n"
            s += (
                textwrap.indent(
                    self.child2.summary_str(ack_tracking=self.ack_tracking), "    "
                )
                + "\n"
            )
        if self.parent_app.raw_proactor is None:
            s += "ATN: None\n"
        else:
            s += "ATN:\n"
            s += (
                textwrap.indent(
                    self.parent.summary_str(ack_tracking=self.ack_tracking), "    "
                )
                + "\n"
            )
        return s

    def assert_child1_events_at_rest(
        self, *args: typing.Any, **kwargs: typing.Any
    ) -> None:
        self.child.assert_event_counts(*args, **kwargs)

    def assert_acks_consistent(
        self,
        *,
        print_summary: bool = False,
        verbose: bool = False,
        log_level: int = logging.ERROR,
        raise_errors: bool = True,
        child1_to_parent: bool = True,
        child2_to_child1: bool = True,
    ) -> None:
        called_from_str = (
            f"\nassert_acks_consistent() called from {caller_str(depth=2)}"
        )
        counts_ok = True
        summary_str = ""
        report_str = ""
        for check_pair, parent, child, prefix in [
            (child1_to_parent, self.parent, self.child, "Child1 / Parent"),
            (child2_to_child1, self.child, self.child2, "Child2 / Child1"),
        ]:
            if check_pair:
                counts = EventAckCounts(parent=parent, child=child, verbose=verbose)
                counts_ok = counts_ok and counts.ok()
                summary_str += f"{prefix} summary\n{counts.summary}\n"
                report_str += f"{prefix} report\n{counts.report}\n"
        if not counts_ok and raise_errors:
            raise AssertionError(f"ERROR {called_from_str}\n{report_str}")
        if verbose or not counts_ok:
            self.child.logger.log(log_level, f"{called_from_str}\n{report_str}")
        elif print_summary:
            self.child.logger.log(log_level, f"{called_from_str}\n{summary_str}")

    async def await_child2_at_rest(
        self,
        *,
        exp_child_persists: Optional[int] = None,  # noqa: ARG002
        exact: bool = False,  # noqa: ARG002
        caller_depth: int = 4,
    ) -> None:
        # Multiple waits for clarity when something goes wrong, rather than
        # one long wait with many possible failures.
        await self.await_for(
            lambda: self.child2.links.link(self.child2.upstream_client).active(),
            (
                "ERROR in await_quiescent_connections: waiting for child2 to "
                "child1 link to be active"
            ),
            caller_depth=caller_depth,
        )
        await self.await_for(
            lambda: self.child2.events_at_rest(),
            (
                "ERROR in await_quiescent_connections: waiting for child2 "
                "events to upload to child1"
            ),
            caller_depth=caller_depth,
        )

    async def await_child_at_rest(
        self,
        *,
        exp_child_persists: Optional[int | RangeTuple] = None,
        exact: bool = False,
        caller_depth: int = 5,
    ) -> None:
        await self.await_child1_at_rest(
            exp_child_persists=exp_child_persists,
            exact=exact,
            caller_depth=caller_depth,
        )

    async def await_child1_at_rest(
        self,
        *,
        exp_child_persists: Optional[int | RangeTuple] = None,  # noqa: ARG002
        exact: bool = False,  # noqa: ARG002
        caller_depth: int = 4,
    ) -> None:
        await self.await_for(
            lambda: self.child1.links.link(self.child1.downstream_client).active(),
            "ERROR in await_quiescent_connections: waiting for child1 to child2 link to be active",
            caller_depth=caller_depth,
        )
        await self.await_for(
            lambda: self.child1.links.link(self.child1.upstream_client).active(),
            "ERROR in await_quiescent_connections: waiting for child1 to parent link to be active",
            caller_depth=caller_depth,
        )
        await self.await_for(
            lambda: self.child1.events_at_rest(),
            "ERROR in await_quiescent_connections: waiting for child1 events to upload to parent",
            caller_depth=caller_depth,
        )

    async def await_parent_at_rest(
        self,
        *,
        exp_parent_pending: int | MinRangeTuple,
        exp_parent_persists: Optional[int | MinRangeTuple] = None,
        exp_total_children_events: Optional[int] = None,  # noqa: ARG002
        exact: bool = False,
        caller_depth: int = 4,
    ) -> None:
        await super().await_parent_at_rest(
            exp_parent_pending=exp_parent_pending,
            exp_parent_persists=exp_parent_persists,
            exact=exact,
            caller_depth=caller_depth + 1,
        )

    # noinspection PyMethodMayBeStatic
    def default_quiesecent_child2_persists(self) -> int:
        # child2 will persist at least 3 events (startup, connect, subscribe),
        # but it could persist more depending on when parent link went active
        # w.r.t the admin and child1. This is not, however, predictable, since,
        # especially in test runs with many tests, mqtt thrash is common,
        # resulting in more events persisted.
        return 3

    # noinspection PyMethodMayBeStatic
    def default_quiescent_total_children_events(self) -> int:
        return sum(
            [
                1,  # child2 startup
                4,  # child2 (child1, admin) x (connect, substribe)
                1,  # child2 peer active
                1,  # child1 startup
                6,  # child1 (parent, child2, admin) x (connect, subscribe)
                2,  # child1 (parent, child2) x peer active
            ]
        )

    def default_quiesecent_child_persists(self) -> int:
        return self.default_quiesecent_child1_persists()

    # noinspection PyMethodMayBeStatic
    def default_quiesecent_child1_persists(self) -> int:
        # child1 will persist at least 3 events (startup, connect, subscribe),
        # but it could persist more depending on when parent link went active
        # w.r.t the admin and child2. This is not, however, predictable, since,
        # especially in test runs with many tests, mqtt thrash is common,
        # resulting in more events persisted.
        return 3

    def default_quiesecent_parent_pending(
        self,
        exp_child_persists: Optional[int | MinRangeTuple] = None,
        exp_total_children_events: Optional[int] = None,
    ) -> int:
        if exp_child_persists is not None:
            raise ValueError(
                "ERROR. TreeLiveTestHelper.default_quiesecent_child_persists() "
                "does not support exp_child_persists because it needs a count "
                "from both childre. Use exp_total_children_events instead."
            )
        return sum(
            [
                exp_total_children_events
                if exp_total_children_events is not None
                else self.default_quiescent_total_children_events(),
                1,  # parent startup
                2,  # parent connect, subscribe
                1,  # child1 peer active
            ]
        )

    def assert_quiescent_tree_event_counts(
        self,
        *,
        exp_child1_persists: int | RangeTuple,
        exp_child2_persists: int | RangeTuple,
        exp_parent_pending: int | RangeTuple,
        exp_parent_persists: int | RangeTuple,
        exact: bool = False,
    ) -> None:
        exp_child1_persists = as_range_tuple(exp_child1_persists, exact)
        exp_child2_persists = as_range_tuple(exp_child2_persists, exact)
        exp_parent_pending = as_range_tuple(exp_parent_pending, exact)
        exp_parent_persists = as_range_tuple(exp_parent_persists, exact)
        summary_str = self.summary_str()
        self.child2.assert_event_counts(
            num_persists=exp_child2_persists,
            all_clear=True,
            tag="child2",
            err_str=summary_str,
        )
        self.child1.assert_event_counts(
            num_persists=exp_child1_persists,
            all_clear=True,
            tag="child1",
            err_str=summary_str,
        )
        self.parent.assert_event_counts(
            num_pending=exp_parent_pending,
            num_persists=exp_parent_persists,
            num_clears=0,
            num_retrieves=0,
            tag="parent",
            err_str=summary_str,
        )

    async def await_quiescent_connections(
        self,
        *,
        exp_child_persists: Optional[int | RangeTuple] = None,
        exp_child1_persists: Optional[int | RangeTuple] = None,
        exp_child2_persists: Optional[int | RangeTuple] = None,
        exp_total_children_events: Optional[int] = None,
        exp_parent_pending: Optional[int | RangeTuple] = None,
        exp_parent_persists: Optional[int | RangeTuple] = None,
        exact: bool = False,
        assert_event_counts: bool = True,
    ) -> None:
        if exp_child_persists is not None and exp_child1_persists is not None:
            raise RuntimeError(
                "Specify 0 or 1 of (exp_child_persists, exp_child1_persists)"
            )
        exp_child1_persists = self._as_tuple(
            exp_child1_persists
            if exp_child1_persists is not None
            else exp_child_persists,
            self.default_quiesecent_child1_persists,
            exact,
        )
        exp_child2_persists = self._as_tuple(
            exp_child2_persists,
            self.default_quiesecent_child2_persists,
            exact,
        )
        if exp_total_children_events is None:
            exp_total_children_events = self.default_quiescent_total_children_events()
        exp_parent_pending = self._as_tuple(
            exp_parent_pending,
            functools.partial(
                self.default_quiesecent_parent_pending,
                exp_total_children_events=exp_total_children_events
                if exp_total_children_events is not None
                else self.default_quiescent_total_children_events(),
            ),
            exact,
        )
        exp_parent_persists = self._as_tuple(
            exp_parent_persists,
            functools.partial(
                self.default_quiesecent_parent_persists,
                exp_parent_pending=exp_parent_pending,
            ),
            exact,
        )

        # Multiple waits for clarity when something goes wrong, rather than
        # one long wait with many possible failures.
        await self.await_child2_at_rest()
        await self.await_child1_at_rest()
        await self.await_parent_at_rest(
            exp_parent_pending=exp_parent_pending,
            exp_parent_persists=exp_parent_persists,
            exp_total_children_events=exp_total_children_events,
            exact=exact,
        )
        if assert_event_counts:
            self.assert_quiescent_tree_event_counts(
                exp_child1_persists=exp_child1_persists,
                exp_child2_persists=exp_child2_persists,
                exp_parent_pending=exp_parent_pending,
                exp_parent_persists=exp_parent_persists,
                exact=exact,
            )
