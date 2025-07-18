import logging
import textwrap
import typing
from typing import Any, Optional, Self

from gwproactor import AppSettings, setup_logging
from gwproactor.app import App
from gwproactor.config import MQTTClient
from gwproactor_test.dummies.tree.atn import DummyAtnApp
from gwproactor_test.dummies.tree.scada1 import DummyScada1App
from gwproactor_test.dummies.tree.scada2 import DummyScada2App
from gwproactor_test.event_consistency_checks import EventAckCounts
from gwproactor_test.instrumented_proactor import InstrumentedProactor, caller_str
from gwproactor_test.live_test_helper import (
    LiveTest,
    get_option_value,
)
from gwproactor_test.logger_guard import LoggerGuards


class TreeLiveTest(LiveTest):
    _child2_app: App
    child2_verbose: bool = False
    child2_on_screen: bool = False
    child2_logger_guards: LoggerGuards

    def __init__(
        self,
        *,
        add_child1: bool = False,
        start_child1: bool = False,
        child1_verbose: Optional[bool] = None,
        child2_app_settings: Optional[AppSettings] = None,
        child2_verbose: Optional[bool] = None,
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
        kwargs["request"] = kwargs.get("request")
        super().__init__(**kwargs)
        self.child2_verbose = get_option_value(
            parameter_value=child2_verbose,
            option_name="--child2-verbose",
            request=kwargs.get("request"),
        )
        self.child2_on_screen = get_option_value(
            parameter_value=child2_on_screen,
            option_name="--child2-on-screen",
            request=kwargs.get("request"),
        )
        self._child2_app = self._make_app(
            self.child2_app_type(), child2_app_settings, app_verbose=self.child2_verbose
        )
        self.setup_child2_logging()
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
            s += textwrap.indent(self.child1.summary_str(), "    ") + "\n"
        if self.child2_app.raw_proactor is None:
            s += "SCADA2: None\n"
        else:
            s += "SCADA2:\n"
            s += textwrap.indent(self.child2.summary_str(), "    ") + "\n"
        if self.parent_app.raw_proactor is None:
            s += "ATN: None\n"
        else:
            s += "ATN:\n"
            s += textwrap.indent(self.parent.summary_str(), "    ") + "\n"
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

    async def await_quiescent_connections(
        self,
        *,
        exp_child_persists: Optional[int | tuple[int | None, int | None]] = None,
        exp_child1_persists: Optional[int | tuple[int | None, int | None]] = None,
        exp_child2_persists: Optional[int | tuple[int | None, int | None]] = None,
        exp_parent_pending: Optional[int] = None,
        exp_parent_persists: Optional[int | tuple[int | None, int | None]] = None,
    ) -> None:
        if exp_child_persists is not None and exp_child1_persists is not None:
            raise RuntimeError(
                "Specify 0 or 1 of (exp_child_persists, exp_child1_persists)"
            )
        if exp_child1_persists is None:
            exp_child1_persists = exp_child_persists

        exp_child2_events = sum(
            [
                1,  # child2 startup
                4,  # child2 (child1, admin) x (connect, substribe)
                1,  # child2 peer active
            ]
        )
        exp_child1_events = sum(
            [
                exp_child2_events,
                1,  # child1 startup
                6,  # child1 (parent, child2, admin) x (connect, subscribe)
                2,  # child1 (parent, child2) x peer active
            ]
        )
        if exp_parent_pending is None:
            exp_parent_pending = sum(
                [
                    exp_child1_events,
                    1,  # parent startup
                    2,  # parent connect, subscribe
                    1,  # child1 peer active
                ]
            )

        child2 = self.child2
        child2_to_child1 = child2.links.link(child2.upstream_client)
        child1 = self.child1
        child1_to_parent = child1.links.link(child1.upstream_client)
        child1_to_child2 = child1.links.link(child1.downstream_client)
        parent = self.parent
        parent_to_child1 = parent.links.link(parent.downstream_client)

        await self.await_for(
            lambda: child2_to_child1.active()
            and child2.events_at_rest()
            and child1_to_child2.active()
            and child1_to_parent.active()
            and child1.events_at_rest()
            and parent_to_child1.active()
            and parent.events_at_rest(num_pending=exp_parent_pending),
            "ERROR waiting for events to be uploaded",
        )
        summary_str = self.summary_str()
        parent.assert_event_counts(
            num_pending=exp_parent_pending,
            num_persists=(
                exp_parent_persists
                if exp_parent_persists is not None
                else exp_parent_pending
            ),
            num_clears=0,
            num_retrieves=0,
            tag="parent",
            err_str=summary_str,
        )

        # child1 will persist between 3 and (exp_child1_events - 1) events,
        # depending on when parent link went active w.r.t the admin and child2
        # links. The '-1' is because the child1-to-parent peer active event
        # isn't persisted by child1
        if exp_child1_persists is None:
            exp_child1_persists = (3, exp_child1_events - 1)
        child1.assert_event_counts(
            num_persists=exp_child_persists,
            all_clear=True,
            tag="child1",
            err_str=summary_str,
        )

        # child2 never persists its own peer active, and whether it persists
        # admin events depends on when child1 link goes active
        if exp_child2_persists is None:
            exp_child2_persists = (3, exp_child2_events - 1)
        child2.assert_event_counts(
            num_persists=exp_child2_persists,
            all_clear=True,
            tag="child2",
            err_str=summary_str,
        )
