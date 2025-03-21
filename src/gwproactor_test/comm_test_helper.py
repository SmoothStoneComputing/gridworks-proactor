import argparse
import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from pathlib import Path
from types import TracebackType
from typing import Any, Callable, Generic, Optional, Type, TypeVar

from gwproactor import Proactor, ProactorSettings, setup_logging
from gwproactor.config import DEFAULT_BASE_NAME, LoggingSettings, MQTTClient, Paths
from gwproactor_test import copy_keys
from gwproactor_test.certs import uses_tls
from gwproactor_test.logger_guard import LoggerGuards
from gwproactor_test.recorder import (
    RecorderInterface,
    make_recorder_class,
)


@dataclass
class ProactorTestHelper:
    name: str
    path_name: str
    settings: ProactorSettings
    kwargs: dict[str, Any] = field(default_factory=dict)
    proactor: Optional[RecorderInterface] = None


ChildT = TypeVar("ChildT", bound=Proactor)
ParentT = TypeVar("ParentT", bound=Proactor)
ChildSettingsT = TypeVar("ChildSettingsT", bound=ProactorSettings)
ParentSettingsT = TypeVar("ParentSettingsT", bound=ProactorSettings)


class CommTestHelper(Generic[ParentT, ChildT, ParentSettingsT, ChildSettingsT]):
    parent_t: Type[ParentT]
    child_t: Type[ChildT]
    parent_settings_t: Type[ParentSettingsT]
    child_settings_t: Type[ChildSettingsT]

    parent_recorder_t: Callable[..., RecorderInterface] = None  # type:ignore[assignment]
    child_recorder_t: Callable[..., RecorderInterface] = None  # type:ignore[assignment]

    parent_helper: ProactorTestHelper
    child_helper: ProactorTestHelper
    verbose: bool
    child_verbose: bool
    parent_verbose: bool
    parent_on_screen: bool
    lifecycle_logging: bool
    logger_guards: LoggerGuards

    warn_if_multi_subscription_tests_skipped: bool = True

    @classmethod
    def setup_class(cls) -> None:
        if cls.parent_recorder_t is None:
            cls.parent_recorder_t = make_recorder_class(cls.parent_t)  # type: ignore[unreachable]
        if cls.child_recorder_t is None:
            cls.child_recorder_t = make_recorder_class(cls.child_t)  # type: ignore[unreachable]

    def __init__(
        self,
        *,
        child_settings: Optional[ChildSettingsT] = None,
        parent_settings: Optional[ParentSettingsT] = None,
        verbose: bool = False,
        child_verbose: bool = False,
        parent_verbose: bool = False,
        lifecycle_logging: bool = False,
        add_child: bool = False,
        add_parent: bool = False,
        start_child: bool = False,
        start_parent: bool = False,
        parent_on_screen: bool = False,
        child_name: str = "",
        parent_name: str = "",
        child_path_name: str = "child",
        parent_path_name: str = "parent",
        child_kwargs: Optional[dict[str, Any]] = None,
        parent_kwargs: Optional[dict[str, Any]] = None,
    ) -> None:
        self.setup_class()
        self.child_helper = ProactorTestHelper(
            child_name,
            child_path_name,
            (
                self.child_settings_t(
                    logging=LoggingSettings(
                        base_log_name=f"{child_path_name}_{DEFAULT_BASE_NAME}"
                    ),
                    paths=Paths(name=Path(child_path_name)),
                )
                if child_settings is None
                else child_settings
            ),
            {} if child_kwargs is None else child_kwargs,
        )
        self.parent_helper = ProactorTestHelper(
            parent_name,
            parent_path_name,
            (
                self.parent_settings_t(
                    logging=LoggingSettings(
                        base_log_name=f"{parent_path_name}_{DEFAULT_BASE_NAME}"
                    ),
                    paths=Paths(name=Path(parent_path_name)),
                )
                if parent_settings is None
                else parent_settings
            ),
            {} if parent_kwargs is None else parent_kwargs,
        )
        self.verbose = verbose
        self.child_verbose = child_verbose
        self.parent_verbose = parent_verbose
        self.parent_on_screen = parent_on_screen
        self.lifecycle_logging = lifecycle_logging
        self.setup_logging()
        if add_child or start_child:
            self.add_child()
            if start_child:
                self.start_child()
        if add_parent or start_parent:
            self.add_parent()
            if start_parent:
                self.start_parent()

    @classmethod
    def _make(
        cls, recorder_t: Callable[..., RecorderInterface], helper: ProactorTestHelper
    ) -> RecorderInterface:
        if uses_tls(helper.settings):
            copy_keys(helper.path_name, helper.settings)
        return recorder_t(helper.name, helper.settings, **helper.kwargs)

    def make_parent(self) -> RecorderInterface:
        return self._make(self.parent_recorder_t, self.parent_helper)

    def make_child(self) -> RecorderInterface:
        return self._make(self.child_recorder_t, self.child_helper)

    @property
    def parent(self) -> Optional[ParentT]:
        return self.parent_helper.proactor  # type: ignore[return-value]

    @property
    def child(self) -> Optional[ChildT]:
        return self.child_helper.proactor  # type: ignore[return-value]

    def start_child(
        self,
    ) -> "CommTestHelper[ParentT, ChildT, ParentSettingsT, ChildSettingsT]":
        if self.child is not None:
            self.start_proactor(self.child)
        return self

    def start_parent(
        self,
    ) -> "CommTestHelper[ParentT, ChildT, ParentSettingsT, ChildSettingsT]":
        if self.parent is not None:
            self.start_proactor(self.parent)
        return self

    def start_proactor(
        self, proactor: Proactor
    ) -> "CommTestHelper[ParentT, ChildT, ParentSettingsT, ChildSettingsT]":
        asyncio.create_task(proactor.run_forever(), name=f"{proactor.name}_run_forever")  # noqa: RUF006
        return self

    def start(
        self,
    ) -> "CommTestHelper[ParentT, ChildT, ParentSettingsT, ChildSettingsT]":
        return self

    def add_child(
        self,
    ) -> "CommTestHelper[ParentT, ChildT, ParentSettingsT, ChildSettingsT]":
        self.child_helper.proactor = self.make_child()
        return self

    def add_parent(
        self,
    ) -> "CommTestHelper[ParentT, ChildT, ParentSettingsT, ChildSettingsT]":
        self.parent_helper.proactor = self.make_parent()
        return self

    def remove_child(
        self,
    ) -> "CommTestHelper[ParentT, ChildT, ParentSettingsT, ChildSettingsT]":
        self.child_helper.proactor = None
        return self

    def remove_parent(
        self,
    ) -> "CommTestHelper[ParentT, ChildT, ParentSettingsT, ChildSettingsT]":
        self.parent_helper.proactor = None
        return self

    @classmethod
    def _get_clients_supporting_tls(
        cls, settings: ProactorSettings
    ) -> list[MQTTClient]:
        clients = []
        for field_name in settings.model_fields:
            v = getattr(settings, field_name)
            if isinstance(v, MQTTClient):
                clients.append(v)
        return clients

    def _get_child_clients_supporting_tls(self) -> list[MQTTClient]:
        """Overide to filter which MQTT clients of ChildSettingsT are treated as supporting TLS"""
        return self._get_clients_supporting_tls(self.child_helper.settings)

    def _get_parent_clients_supporting_tls(self) -> list[MQTTClient]:
        """Overide to filter which MQTT clients of ParentSettingsT are treated as supporting TLS"""
        return self._get_clients_supporting_tls(self.parent_helper.settings)

    @classmethod
    def _set_settings_use_tls(cls, use_tls: bool, clients: list[MQTTClient]) -> None:
        for client in clients:
            client.tls.use_tls = use_tls

    def set_use_tls(self, use_tls: bool) -> None:
        """Set MQTTClients which support TLS in parent and child settings to use TLS per use_tls. Clients supporting TLS
        is determined by _get_child_clients_supporting_tls() and _get_parent_clients_supporting_tls() which may be
        overriden in derived class.
        """
        self._set_settings_use_tls(use_tls, self._get_child_clients_supporting_tls())
        self._set_settings_use_tls(use_tls, self._get_parent_clients_supporting_tls())

    def setup_logging(self) -> None:
        self.child_helper.settings.paths.mkdirs(parents=True)
        self.parent_helper.settings.paths.mkdirs(parents=True)
        errors: list[Exception] = []
        if not self.lifecycle_logging and not self.verbose:
            if not self.child_verbose:
                self.child_helper.settings.logging.levels.lifecycle = logging.WARNING
            if not self.parent_verbose:
                self.parent_helper.settings.logging.levels.lifecycle = logging.WARNING
        self.logger_guards = LoggerGuards(
            list(self.child_helper.settings.logging.qualified_logger_names().values())
            + list(
                self.parent_helper.settings.logging.qualified_logger_names().values()
            )
        )
        setup_logging(
            argparse.Namespace(verbose=self.verbose or self.child_verbose),
            self.child_helper.settings,
            errors=errors,
            add_screen_handler=True,
            root_gets_handlers=False,
        )
        assert not errors
        setup_logging(
            argparse.Namespace(verbose=self.verbose or self.parent_verbose),
            self.parent_helper.settings,
            errors=errors,
            add_screen_handler=self.parent_on_screen,
            root_gets_handlers=False,
        )
        assert not errors

    def get_proactors(self) -> list[RecorderInterface]:
        return [
            helper.proactor
            for helper in [self.child_helper, self.parent_helper]
            if helper.proactor is not None
        ]

    async def stop_and_join(self) -> None:
        proactors = self.get_proactors()
        for proactor in proactors:
            with contextlib.suppress(Exception):
                proactor.stop()
        for proactor in proactors:
            with contextlib.suppress(Exception):
                await proactor.join()

    async def __aenter__(
        self,
    ) -> "CommTestHelper[ParentT, ChildT, ParentSettingsT, ChildSettingsT]":
        return self

    def get_log_path_str(self, exc: BaseException) -> str:
        return (
            f"CommTestHelper caught error {exc}.\n"
            "Working log dirs:"
            f"\n\t[{self.child_helper.settings.paths.log_dir}]"
            f"\n\t[{self.parent_helper.settings.paths.log_dir}]"
        )

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:  # noqa
        try:
            await self.stop_and_join()
        finally:
            if exc is not None:
                try:
                    s = self.get_log_path_str(exc)
                except Exception as e:  # noqa: BLE001
                    try:
                        s = (
                            f"Caught {type(e)} / <{e}> while logging "
                            f"{type(exc)} / <{exc}>"
                        )
                    except:  # noqa: E722
                        s = "ERRORs upon errors in CommTestHelper cleanup"
                with contextlib.suppress(Exception):
                    logging.getLogger("gridworks").error(s)
            with contextlib.suppress(Exception):
                self.logger_guards.restore()
        return False

    def summary_str(self) -> str:
        s = ""
        if self.child:
            s += "CHILD:\n" f"{self.child.summary_str()}\n"  # type: ignore[attr-defined]
        else:
            s += "CHILD: None\n"
        if self.parent:
            s += "PARENT:\n" f"{self.parent.summary_str()}"  # type: ignore[attr-defined]
        else:
            s += "PARENT: None\n"
        return s
