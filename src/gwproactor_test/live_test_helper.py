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
from gwproactor.config import DEFAULT_BASE_NAME as DEFAULT_LOG_BASE_NAME
from gwproactor.config import MQTTClient, Paths
from gwproactor_test.certs import copy_keys, uses_tls
from gwproactor_test.clean import TEST_HARDWARE_LAYOUT_PATH
from gwproactor_test.dummies.pair.child import DummyChildApp
from gwproactor_test.dummies.pair.parent import DummyParentApp
from gwproactor_test.event_consistency_checks import EventAckCounts
from gwproactor_test.instrumented_proactor import InstrumentedProactor, caller_str
from gwproactor_test.logger_guard import LoggerGuards
from gwproactor_test.wait import (
    AwaitablePredicate,
    ErrorStringFunction,
    Predicate,
    await_for,
)


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
        self._child_app = self._make_app(
            self.child_app_type(),
            child_app_settings,
            app_verbose=self.child_verbose,
        )
        self._parent_app = self._make_app(
            self.parent_app_type(),
            parent_app_settings,
            app_verbose=self.parent_verbose,
        )
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
    def test_layout_path(cls) -> Path:
        return TEST_HARDWARE_LAYOUT_PATH

    def _make_app(
        self,
        app_type: type[App],
        app_settings: Optional[AppSettings],
        *,
        app_verbose: bool = False,
    ) -> App:
        # Copy hardware layout file.
        paths = Paths(name=app_type.paths_name())
        paths.mkdirs(parents=True, exist_ok=True)
        shutil.copyfile(self.test_layout_path(), paths.hardware_layout)
        # Use an instrumented proactor
        sub_types = app_type.make_subtypes()
        sub_types.proactor_type = InstrumentedProactor
        app_settings = app_type.update_settings_from_command_line(
            app_type.get_settings(settings=app_settings, paths=paths),
            verbose=self.verbose or app_verbose,
        )
        if not self.lifecycle_logging and not self.verbose and not app_verbose:
            app_settings.logging.levels.lifecycle = logging.WARNING
        if app_settings.logging.base_log_name == str(DEFAULT_LOG_BASE_NAME):
            app_settings.logging.base_log_name = f"{DEFAULT_LOG_BASE_NAME}-{paths.name}"

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
        if self.child_app.raw_proactor is None:
            raise RuntimeError(
                "ERROR. CommTestHelper.child accessed before creating child."
                "pass add_child=True to CommTestHelper constructor or call "
                "CommTestHelper.add_child()"
            )
        return typing.cast(InstrumentedProactor, self.child_app.proactor)

    def start_child(
        self,
    ) -> Self:
        if self.child_app.raw_proactor is None:
            self.add_child()
        return self.start_proactor(self.child)

    def start_parent(
        self,
    ) -> Self:
        if self.parent_app.raw_proactor is None:
            self.add_parent()
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
            child_settings,
            errors=errors,
            add_screen_handler=True,
            root_gets_handlers=False,
        )
        assert not errors
        setup_logging(
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
            f"\n\t{self.child_app.config.settings.paths.log_dir}"
            f"\n\t{self.parent_app.config.settings.paths.log_dir}"
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

    async def await_for(
        self,
        f: Predicate | AwaitablePredicate,
        tag: str = "",
        *,
        timeout: float = 3.0,  # noqa: ASYNC109
        raise_timeout: bool = True,
        retry_duration: float = 0.01,
        err_str_f: Optional[ErrorStringFunction] = None,
        logger: Optional[logging.Logger | logging.LoggerAdapter[logging.Logger]] = None,
        error_dict: Optional[dict[str, typing.Any]] = None,
    ) -> bool:
        if not isinstance(tag, str):
            raise TypeError(
                "ERROR. LiveTest.await_for() received a non-string tag "
                f"(type: {type(tag)}).\n"
                "  Did you pass the timeout as the second parameter?\n\n"
                "  The signature of LiveTest differs from wait.await_for(), "
                "which has timeout as the second, not third parameter."
            )
        if err_str_f is None:
            err_str_f = self.summary_str
        return await await_for(
            f=f,
            timeout=timeout,
            tag=tag,
            raise_timeout=raise_timeout,
            retry_duration=retry_duration,
            err_str_f=err_str_f,
            logger=logger,
            error_dict=error_dict,
        )

    async def child_to_parent_active(self) -> bool:
        return await self.await_for(
            self.child.links.link(self.child.upstream_client).active,
            "ERROR waiting child to parent link to be active",
        )

    async def parent_to_child_active(self) -> bool:
        return await self.await_for(
            self.parent.links.link(self.parent.downstream_client).active,
            "ERROR waiting child to parent link to be active",
        )

    def assert_child_events_at_rest(
        self, *args: typing.Any, **kwargs: typing.Any
    ) -> None:
        self.child.assert_event_counts(*args, **kwargs)

    def assert_child1_events_at_rest(
        self, *args: typing.Any, **kwargs: typing.Any
    ) -> None:
        self.child.assert_event_counts(*args, **kwargs)

    def assert_parent_events_at_rest(
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
    ) -> None:
        called_from_str = (
            f"\nassert_acks_consistent() called from {caller_str(depth=2)}"
        )
        counts = EventAckCounts(parent=self.parent, child=self.child, verbose=verbose)
        if not counts.ok() and raise_errors:
            raise AssertionError(
                f"ERROR {called_from_str}\n{counts.report}\n{self.summary_str()}"
            )
        if verbose or not counts.ok():
            self.child.logger.log(log_level, f"{called_from_str}\n{counts.report}")
        elif print_summary:
            self.child.logger.log(log_level, f"{called_from_str}\n{counts.summary}")

    async def await_quiescent_connections(
        self,
        *,
        exp_child_persists: Optional[int] = None,
        exp_parent_pending: Optional[int] = None,
        exp_parent_persists: Optional[int] = None,
    ) -> None:
        child = self.child
        child_link = child.links.link(child.upstream_client)
        parent = self.parent
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
            exp_parent_persists
            if exp_parent_persists is not None
            else exp_parent_pending
        )
        await self.await_for(
            lambda: child_link.active() and child.events_at_rest(),
            "ERROR waiting for child events upload",
        )
        await self.await_for(
            lambda: parent_link.active()
            and parent.events_at_rest(num_pending=exp_parent_pending),
            f"ERROR waiting for parent to persist {exp_parent_pending} events",
        )
        parent.assert_event_counts(
            num_pending=exp_parent_pending,
            num_persists=exp_parent_persists,
            num_clears=0,
            num_retrieves=0,
            tag="parent",
            err_str=self.summary_str(),
        )
        child.assert_event_counts(
            # child will not persist peer active event
            num_persists=exp_child_persists,
            all_clear=True,
            tag="child",
            err_str=self.summary_str(),
        )
