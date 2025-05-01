import argparse
import asyncio
import contextlib
import logging
import shutil
import typing
from pathlib import Path
from types import TracebackType
from typing import Optional, Self, Type

from pydantic_settings import BaseSettings

from gwproactor import AppSettings, Proactor, setup_logging
from gwproactor.app import App
from gwproactor.config import MQTTClient, Paths
from gwproactor_test import copy_keys
from gwproactor_test.certs import uses_tls
from gwproactor_test.clean import TEST_HARDWARE_LAYOUT_PATH
from gwproactor_test.dummies.pair.child import DummyChildApp
from gwproactor_test.dummies.pair.parent import DummyParentApp
from gwproactor_test.instrumented_proactor import InstrumentedProactor
from gwproactor_test.logger_guard import LoggerGuards


def get_option_value(
    *,
    parameter_value: Optional[bool],
    option_name: str,
    request: Optional[typing.Any],
) -> bool:
    if parameter_value is not None or request is None:
        return bool(parameter_value)
    return bool(request.config.getoption(option_name))


class LiveTest:
    _parent_app: App
    _child_app: App
    verbose: bool
    child_verbose: bool
    parent_verbose: bool
    parent_on_screen: bool
    lifecycle_logging: bool
    logger_guards: LoggerGuards

    def __init__(
        self,
        *,
        child_app_settings: Optional[AppSettings] = None,
        parent_app_settings: Optional[AppSettings] = None,
        verbose: Optional[bool] = None,
        child_verbose: Optional[bool] = None,
        parent_verbose: Optional[bool] = None,
        lifecycle_logging: bool = False,
        add_child: bool = False,
        add_parent: bool = False,
        start_child: bool = False,
        start_parent: bool = False,
        parent_on_screen: Optional[bool] = None,
        request: typing.Any = None,
    ) -> None:
        self._child_app = self._make_app(self.child_app_type(), child_app_settings)
        self._parent_app = self._make_app(self.parent_app_type(), parent_app_settings)
        self.verbose = get_option_value(
            parameter_value=verbose,
            option_name="--live-test-verbose",
            request=request,
        )
        self.child_verbose = get_option_value(
            parameter_value=child_verbose,
            option_name="--child-verbose",
            request=request,
        )
        self.parent_verbose = get_option_value(
            parameter_value=parent_verbose,
            option_name="--parent-verbose",
            request=request,
        )
        self.parent_on_screen = get_option_value(
            parameter_value=parent_on_screen,
            option_name="--parent-on-screen",
            request=request,
        )
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
    def child_app_type(cls) -> type[App]:
        return DummyChildApp

    @property
    def child_app(self) -> App:
        return self._child_app

    @classmethod
    def parent_app_type(cls) -> type[App]:
        return DummyParentApp

    @property
    def parent_app(self) -> App:
        return self._parent_app

    @classmethod
    def _make_app(cls, app_type: type[App], app_settings: Optional[AppSettings]) -> App:
        # Copy hardware layout file.
        paths = Paths(name=app_type.paths_name())
        paths.mkdirs(parents=True, exist_ok=True)
        shutil.copyfile(Path(TEST_HARDWARE_LAYOUT_PATH), paths.hardware_layout)
        # Use an instrumented proactor
        sub_types = app_type.make_subtypes()
        sub_types.proactor_type = InstrumentedProactor
        # Create the app
        app = app_type(
            paths=paths,
            app_settings=app_settings
            if app_settings is None
            else app_settings.with_paths(paths=paths),
            sub_types=sub_types,
        )
        # Copy keys.
        if uses_tls(app.config.settings):
            copy_keys(
                str(app.config.settings.paths.name),
                app.config.settings,
            )
        return app

    @property
    def parent(self) -> InstrumentedProactor:
        if self.parent_app.proactor is None:
            raise RuntimeError(
                "ERROR. CommTestHelper.parent accessed before creating parent."
                "pass add_parent=True to CommTestHelper constructor or call "
                "CommTestHelper.add_parent()"
            )
        return typing.cast(InstrumentedProactor, self.parent_app.proactor)

    @property
    def child(self) -> InstrumentedProactor:
        if self.child_app.proactor is None:
            raise RuntimeError(
                "ERROR. CommTestHelper.child accessed before creating child."
                "pass add_child=True to CommTestHelper constructor or call "
                "CommTestHelper.add_child()"
            )
        return typing.cast(InstrumentedProactor, self.child_app.proactor)

    def start_child(
        self,
    ) -> Self:
        return self.start_proactor(self.child)

    def start_parent(
        self,
    ) -> Self:
        return self.start_proactor(self.parent)

    def start_proactor(self, proactor: Proactor) -> Self:
        asyncio.create_task(proactor.run_forever(), name=f"{proactor.name}_run_forever")  # noqa: RUF006
        return self

    def start(
        self,
    ) -> Self:
        return self

    def add_child(
        self,
    ) -> Self:
        self.child_app.instantiate()
        return self

    def add_parent(
        self,
    ) -> Self:
        self.parent_app.instantiate()
        return self

    def remove_child(
        self,
    ) -> Self:
        self.child_app.raw_proactor = None
        return self

    def remove_parent(
        self,
    ) -> Self:
        self.parent_app.raw_proactor = None
        return self

    @classmethod
    def _get_clients_supporting_tls(cls, settings: BaseSettings) -> list[MQTTClient]:
        clients = []
        for field_name in settings.__pydantic_fields__:
            v = getattr(settings, field_name)
            if isinstance(v, MQTTClient):
                clients.append(v)
        return clients

    def _get_child_clients_supporting_tls(self) -> list[MQTTClient]:
        """Overide to filter which MQTT clients of ChildSettingsT are treated as supporting TLS"""
        return self._get_clients_supporting_tls(self.child_app.config.settings)

    def _get_parent_clients_supporting_tls(self) -> list[MQTTClient]:
        """Overide to filter which MQTT clients of ParentSettingsT are treated as supporting TLS"""
        return self._get_clients_supporting_tls(self.parent_app.config.settings)

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
        child_settings = self.child_app.config.settings
        parent_settings = self.parent_app.config.settings
        child_settings.paths.mkdirs(parents=True)
        parent_settings.paths.mkdirs(parents=True)
        errors: list[Exception] = []
        if not self.lifecycle_logging and not self.verbose:
            if not self.child_verbose:
                child_settings.logging.levels.lifecycle = logging.WARNING
            if not self.parent_verbose:
                parent_settings.logging.levels.lifecycle = logging.WARNING
        self.logger_guards = LoggerGuards(
            list(child_settings.logging.qualified_logger_names().values())
            + list(parent_settings.logging.qualified_logger_names().values())
        )
        setup_logging(
            argparse.Namespace(verbose=self.verbose or self.child_verbose),
            child_settings,
            errors=errors,
            add_screen_handler=True,
            root_gets_handlers=False,
        )
        assert not errors
        setup_logging(
            argparse.Namespace(verbose=self.verbose or self.parent_verbose),
            parent_settings,
            errors=errors,
            add_screen_handler=self.parent_on_screen,
            root_gets_handlers=False,
        )
        assert not errors

    def get_proactors(self) -> list[InstrumentedProactor]:
        proactors = []
        if self.child_app.raw_proactor is not None:
            proactors.append(self.child)
        if self.parent_app.raw_proactor is not None:
            proactors.append(self.parent)
        return proactors

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
    ) -> Self:
        return self

    def get_log_path_str(self, exc: BaseException) -> str:
        return (
            f"CommTestHelper caught error {exc}.\n"
            "Working log dirs:"
            f"\n\t[{self.child_app.config.settings.paths.log_dir}]"
            f"\n\t[{self.parent_app.config.settings.paths.log_dir}]"
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
                self.logger_guards.restore()  # noqa
        return False

    def summary_str(self) -> str:
        s = ""
        if self.child_app.raw_proactor is not None:
            s += "CHILD:\n" f"{self.child.summary_str()}\n"
        else:
            s += "CHILD: None\n"
        if self.parent_app.raw_proactor is not None:
            s += "PARENT:\n" f"{self.parent.summary_str()}"
        else:
            s += "PARENT: None\n"
        return s
