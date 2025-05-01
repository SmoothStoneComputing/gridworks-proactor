import argparse
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
from gwproactor_test.instrumented_proactor import InstrumentedProactor
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
                request=kwargs["request"],
            )
        kwargs["request"] = kwargs.get("request")
        super().__init__(**kwargs)
        self._child2_app = self._make_app(self.child2_app_type(), child2_app_settings)
        self.child2_verbose = get_option_value(
            parameter_value=child2_verbose,
            option_name="--child2-verbose",
            request=kwargs["request"],
        )
        self.child2_on_screen = get_option_value(
            parameter_value=child2_on_screen,
            option_name="--child2-on-screen",
            request=kwargs["request"],
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
        if not self.lifecycle_logging and not self.verbose and not self.child2_verbose:
            self.child2_app.config.settings.logging.levels.lifecycle = logging.WARNING
        self.logger_guards.add_loggers(
            list(
                self.child2_app.config.settings.logging.qualified_logger_names().values()
            )
        )
        setup_logging(
            argparse.Namespace(verbose=self.verbose or self.child2_verbose),
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
