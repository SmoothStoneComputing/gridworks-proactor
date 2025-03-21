import argparse
import logging
import textwrap
from pathlib import Path
from typing import Any, Callable, Optional, Self, Type

from gwproactor import Proactor, ProactorSettings, setup_logging
from gwproactor.config import DEFAULT_BASE_NAME, LoggingSettings, MQTTClient, Paths
from gwproactor_test.comm_test_helper import (
    ChildSettingsT,
    ChildT,
    CommTestHelper,
    ParentSettingsT,
    ParentT,
    ProactorTestHelper,
)
from gwproactor_test.dummies import DUMMY_ATN_NAME, DUMMY_SCADA1_NAME, DUMMY_SCADA2_NAME
from gwproactor_test.logger_guard import LoggerGuards
from gwproactor_test.recorder import (
    ProactorT,
    RecorderInterface,
    make_recorder_class,
)


class TreeCommTestHelper(
    CommTestHelper[ParentT, ChildT, ParentSettingsT, ChildSettingsT]
):
    child2_t: Type[Proactor]
    child2_settings_t: Type[ProactorSettings]
    child2_recorder_t: Callable[..., RecorderInterface] = None  # type: ignore[assignment]
    child2_helper: ProactorTestHelper
    child2_verbose: bool = False
    child2_on_screen: bool = False
    child2_logger_guards: LoggerGuards

    def __init__(
        self,
        *,
        add_child1: bool = False,
        start_child1: bool = False,
        child1_verbose: bool = False,
        child2_settings: Optional[ChildSettingsT] = None,
        child2_verbose: bool = False,
        add_child2: bool = False,
        start_child2: bool = False,
        child2_on_screen: bool = False,
        child2_name: str = DUMMY_SCADA2_NAME,
        child_path_name: str = DUMMY_SCADA1_NAME,
        parent_path_name: str = DUMMY_ATN_NAME,
        child2_path_name: str = DUMMY_SCADA2_NAME,
        child2_kwargs: Optional[dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        if not kwargs.get("child_name"):
            kwargs["child_name"] = DUMMY_SCADA1_NAME
        if not kwargs.get("parent_name"):
            kwargs["parent_name"] = DUMMY_ATN_NAME
        kwargs["add_child"] = add_child1 or kwargs.get("add_child", False)
        kwargs["start_child"] = start_child1 or kwargs.get("start_child", False)
        kwargs["child_verbose"] = child1_verbose or kwargs.get("child_verbose", False)
        super().__init__(
            child_path_name=child_path_name,
            parent_path_name=parent_path_name,
            **kwargs,
        )
        self.setup_child2_class()
        self.child2_helper = ProactorTestHelper(
            name=child2_name,
            path_name=child2_path_name,
            settings=(
                self.child2_settings_t(
                    logging=LoggingSettings(
                        base_log_name=f"{child2_path_name}_{DEFAULT_BASE_NAME}"
                    ),
                    paths=Paths(name=Path(child2_path_name)),
                )
                if child2_settings is None
                else child2_settings
            ),
            kwargs={} if child2_kwargs is None else child2_kwargs,
        )
        self.child2_verbose = child2_verbose
        self.child2_on_screen = child2_on_screen
        self.setup_child2_logging()
        if add_child2 or start_child2:
            self.add_child2()
            if start_child2:
                self.start_child2()

    @classmethod
    def setup_child2_class(cls) -> None:
        if cls.child2_recorder_t is None:
            cls.child2_recorder_t = make_recorder_class(cls.child2_t)  # type: ignore[unreachable]

    @property
    def child1(self) -> Optional[ProactorT]:
        return self.child  # type: ignore[return-value]

    def add_child1(self) -> Self:
        return self.add_child()  # type: ignore[return-value]

    def start_child1(
        self,
    ) -> "CommTestHelper[ParentT, ChildT, ParentSettingsT, ChildSettingsT]":
        return self.start_child()

    def make_child2(self) -> RecorderInterface:
        return self._make(self.child2_recorder_t, self.child2_helper)

    @property
    def child2(self) -> Optional[ProactorT]:
        return self.child2_helper.proactor  # type: ignore[return-value]

    def start_child2(self) -> Self:
        if self.child2 is not None:
            self.start_proactor(self.child2)
        return self

    def add_child2(self) -> Self:
        self.child2_helper.proactor = self.make_child2()
        return self

    def remove_child2(self) -> Self:
        self.child2_helper.proactor = None
        return self

    def _get_child2_clients_supporting_tls(self) -> list[MQTTClient]:
        return self._get_clients_supporting_tls(self.child2_helper.settings)

    def set_use_tls(self, use_tls: bool) -> None:
        super().set_use_tls(use_tls)
        self._set_settings_use_tls(use_tls, self._get_child2_clients_supporting_tls())

    def setup_child2_logging(self) -> None:
        self.child2_helper.settings.paths.mkdirs(parents=True)
        errors: list[Exception] = []
        if not self.lifecycle_logging and not self.verbose and not self.child2_verbose:
            self.child2_helper.settings.logging.levels.lifecycle = logging.WARNING
        self.logger_guards.add_loggers(
            list(self.child2_helper.settings.logging.qualified_logger_names().values())
        )
        setup_logging(
            argparse.Namespace(verbose=self.verbose or self.child2_verbose),
            self.child2_helper.settings,
            errors=errors,
            add_screen_handler=self.child2_on_screen,
            root_gets_handlers=False,
        )
        assert not errors

    def get_proactors(self) -> list[RecorderInterface]:
        proactors = super().get_proactors()
        if self.child2_helper.proactor is not None:
            proactors.append(self.child2_helper.proactor)
        return proactors

    def get_log_path_str(self, exc: BaseException) -> str:
        return (
            f"CommTestHelper caught error {exc}.\n"
            "Working log dirs:"
            f"\n\t[{self.child_helper.settings.paths.log_dir}]"
            f"\n\t[{self.parent_helper.settings.paths.log_dir}]"
        )

    def summary_str(self) -> str:
        s = ""
        if self.child1 is None:
            s += "SCADA1: None\n"
        else:
            s += "SCADA1:\n"
            s += textwrap.indent(self.child1.summary_str(), "    ") + "\n"  # type: ignore[attr-defined]
        if self.child2 is None:
            s += "SCADA2: None\n"
        else:
            s += "SCADA2:\n"
            s += textwrap.indent(self.child2.summary_str(), "    ") + "\n"  # type: ignore[attr-defined]
        if self.parent is None:
            s += "ATN: None\n"
        else:
            s += "ATN:\n"
            s += textwrap.indent(self.parent.summary_str(), "    ") + "\n"  # type: ignore[attr-defined]
        return s
