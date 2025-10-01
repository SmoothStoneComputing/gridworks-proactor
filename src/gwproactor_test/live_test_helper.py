import asyncio
import contextlib
import datetime
import functools
import logging
import shutil
import typing
from pathlib import Path
from types import TracebackType
from typing import Optional, Self, Type

from gwproto import HardwareLayout, MQTTTopic
from gwproto.messages import CommEvent
from pydantic_settings import BaseSettings

from gwproactor import AppSettings, Proactor, setup_logging
from gwproactor.app import App
from gwproactor.config import DEFAULT_BASE_NAME as DEFAULT_LOG_BASE_NAME
from gwproactor.config import MQTTClient, Paths
from gwproactor.links import LinkState
from gwproactor_test.certs import copy_keys, uses_tls
from gwproactor_test.clean import hardware_layout_test_path
from gwproactor_test.dummies.pair.child import DummyChildApp
from gwproactor_test.dummies.pair.parent import DummyParentApp
from gwproactor_test.event_consistency_checks import EventAckCounts
from gwproactor_test.instrumented_proactor import (
    InstrumentedProactor,
    MinRangeTuple,
    RangeTuple,
    as_min_range_tuple,
    as_range_tuple,
    caller_str,
    range_min,
)
from gwproactor_test.instrumented_stats import RecorderLinkStats
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
    message_summary: bool
    child_verbose: bool
    child_message_summary: bool
    parent_verbose: bool
    parent_message_summary: bool
    parent_on_screen: bool
    lifecycle_logging: bool
    logger_guards: LoggerGuards
    ack_tracking: bool

    def __init__(
        self,
        *,
        child_app_settings: Optional[AppSettings] = None,
        parent_app_settings: Optional[AppSettings] = None,
        child_layout: Optional[HardwareLayout] = None,
        parent_layout: Optional[HardwareLayout] = None,
        layout: Optional[HardwareLayout] = None,
        verbose: Optional[bool] = None,
        message_summary: Optional[bool] = None,
        child_verbose: Optional[bool] = None,
        child_message_summary: Optional[bool] = None,
        parent_verbose: Optional[bool] = None,
        parent_message_summary: Optional[bool] = None,
        lifecycle_logging: bool = False,
        parent_on_screen: Optional[bool] = None,
        ack_tracking: Optional[bool] = None,
        add_child: bool = False,
        add_parent: bool = False,
        add_all: bool = False,
        start_child: bool = False,
        start_parent: bool = False,
        start_all: bool = False,
        request: typing.Any = None,
    ) -> None:
        self.verbose = get_option_value(
            parameter_value=verbose,
            option_name="--live-test-verbose",
            request=request,
        )
        self.message_summary = get_option_value(
            parameter_value=message_summary,
            option_name="--live-test-message-summary",
            request=request,
        )
        self.child_verbose = get_option_value(
            parameter_value=child_verbose,
            option_name="--child-verbose",
            request=request,
        )
        self.child_message_summary = get_option_value(
            parameter_value=child_message_summary,
            option_name="--child-message-summary",
            request=request,
        )
        self.parent_verbose = get_option_value(
            parameter_value=parent_verbose,
            option_name="--parent-verbose",
            request=request,
        )
        self.parent_message_summary = get_option_value(
            parameter_value=parent_message_summary,
            option_name="--parent-message-summary",
            request=request,
        )
        self.parent_on_screen = get_option_value(
            parameter_value=parent_on_screen,
            option_name="--parent-on-screen",
            request=request,
        )
        self.lifecycle_logging = lifecycle_logging
        self.ack_tracking = get_option_value(
            parameter_value=ack_tracking,
            option_name="--ack-tracking",
            request=request,
        )
        self._child_app = self._make_app(
            self.child_app_type(),
            child_app_settings,
            app_verbose=self.child_verbose,
            app_message_summary=self.child_message_summary,
            layout=child_layout if child_layout is not None else layout,
        )
        self._parent_app = self._make_app(
            self.parent_app_type(),
            parent_app_settings,
            app_verbose=self.parent_verbose,
            app_message_summary=self.parent_message_summary,
            layout=parent_layout if parent_layout is not None else layout,
        )
        self.setup_logging()
        add_child = add_child or start_child or add_all or start_all
        start_child = start_child or start_all
        if add_child or start_child:
            self.add_child()
            if start_child:
                self.start_child()
        add_parent = add_parent or start_parent or add_all or start_all
        start_parent = start_parent or start_all
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
        return hardware_layout_test_path()

    def _make_app(
        self,
        app_type: type[App],
        app_settings: Optional[AppSettings],
        *,
        app_verbose: bool = False,
        app_message_summary: bool = False,
        layout: Optional[HardwareLayout] = None,
    ) -> App:
        # Copy hardware layout file.
        if app_settings is None:
            paths = Paths(name=app_type.paths_name())
        else:
            if "name" in app_settings.paths.model_dump(
                exclude_unset=True, exclude_defaults=True
            ):
                name = app_settings.paths.name
            else:
                name = str(app_type.paths_name())
            paths = app_settings.paths.duplicate(name=name)
        paths.mkdirs(parents=True, exist_ok=True)
        if not Path(paths.hardware_layout).exists():
            shutil.copyfile(self.test_layout_path(), paths.hardware_layout)

        # Use an instrumented proactor
        sub_types = app_type.make_subtypes()
        sub_types.proactor_type = InstrumentedProactor
        app_settings = app_type.update_settings_from_command_line(
            app_type.get_settings(
                settings=app_settings, paths_name=str(paths.name), paths=paths
            ),
            verbose=self.verbose or app_verbose,
            message_summary=self.message_summary or app_message_summary,
        )
        if not self.lifecycle_logging and not self.verbose and not app_verbose:
            app_settings.logging.levels.lifecycle = logging.WARNING
        if app_settings.logging.base_log_name == str(DEFAULT_LOG_BASE_NAME):
            app_settings.logging.base_log_name = f"{DEFAULT_LOG_BASE_NAME}-{paths.name}"

        # Create the app
        app = app_type(
            paths_name=str(paths.name),
            paths=paths,
            app_settings=app_settings
            if app_settings is None
            else app_settings.with_paths(paths=paths),
            sub_types=sub_types,
            layout=layout,
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

    @property
    def child_to_parent_link(self) -> LinkState:
        return self.child.upstream_link

    @property
    def parent_to_child_link(self) -> LinkState:
        return self.parent.downstream_link

    @property
    def child_to_parent_stats(self) -> RecorderLinkStats:
        return self.child.upstream_stats

    @property
    def parent_to_child_stats(self) -> RecorderLinkStats:
        return self.parent.downstream_stats

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
            f"\nCommTestHelper caught error:\n"
            f"\t{exc}.\n"
            f"\tTime: {datetime.datetime.now()}\n"  # noqa: DTZ005
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
            s += (
                "CHILD:\n" f"{self.child.summary_str(ack_tracking=self.ack_tracking)}\n"
            )
        else:
            s += "CHILD: None\n"
        if self.parent_app.raw_proactor is not None:
            s += (
                "PARENT:\n" f"{self.parent.summary_str(ack_tracking=self.ack_tracking)}"
            )
        else:
            s += "PARENT: None\n"
        return s

    async def await_for(
        self,
        f: Predicate | AwaitablePredicate,
        tag: str = "",
        *,
        timeout: float = 10.0,  # noqa: ASYNC109
        raise_timeout: bool = True,
        retry_duration: float = 0.01,
        err_str_f: Optional[ErrorStringFunction] = None,
        logger: Optional[logging.Logger | logging.LoggerAdapter[logging.Logger]] = None,
        error_dict: Optional[dict[str, typing.Any]] = None,
        caller_depth: int = 2,
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
            caller_depth=caller_depth,
        )

    async def child_to_parent_active(self) -> bool:
        return await self.await_for(
            self.child.links.link(self.child.upstream_client).active,
            "ERROR waiting child to parent link to be active",
            caller_depth=3,
        )

    async def parent_to_child_active(self) -> bool:
        return await self.await_for(
            self.parent.links.link(self.parent.downstream_client).active,
            "ERROR waiting child to parent link to be active",
            caller_depth=3,
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

    async def await_child_at_rest(
        self,
        *,
        exp_child_persists: Optional[int | RangeTuple] = None,  # noqa: ARG002
        exact: bool = False,  # noqa: ARG002
        caller_depth: int = 4,
    ) -> None:
        child = self.child
        child_link = child.links.link(child.upstream_client)
        # Multiple waits for clarity when something goes wrong, rather than
        # one long wait with many possible failures.
        await self.await_for(
            lambda: child_link.active(),
            "ERROR in await_quiescent_connections: waiting for child link to be active",
            caller_depth=3,
        )
        await self.await_for(
            lambda: child.events_at_rest(),
            "ERROR in await_quiescent_connections: waiting for child events to upload",
            caller_depth=caller_depth,
        )

    async def await_parent_at_rest(
        self,
        *,
        exp_parent_pending: int | MinRangeTuple,
        exp_parent_persists: Optional[int | MinRangeTuple] = None,
        exact: bool = False,
        caller_depth: int = 4,
    ) -> None:
        parent = self.parent
        parent_link = parent.links.link(parent.downstream_client)
        await self.await_for(
            lambda: parent_link.active(),
            "ERROR in await_quiescent_connections: waiting for parent link to be active",
            caller_depth=3,
        )
        num_pending = range_min(exp_parent_pending)
        await self.await_for(
            lambda: parent.events_at_rest(
                num_pending=num_pending,
                exact_pending=exact,
                num_persists=None
                if exp_parent_persists is None
                else range_min(exp_parent_persists),
                exact_persists=exact,
            ),
            f"ERROR in await_quiescent_connections: waiting for parent to persist {num_pending} events",
            caller_depth=caller_depth,
        )

    # noinspection PyMethodMayBeStatic
    def default_quiesecent_child_persists(self) -> int:
        return sum(
            [
                1,  # child startup
                2,  # child connect, subscribe
            ]
        )

    def default_quiesecent_parent_pending(
        self,
        exp_child_persists: Optional[int | MinRangeTuple] = None,
    ) -> int:
        return sum(
            [
                range_min(
                    exp_child_persists
                    if exp_child_persists is not None
                    else self.default_quiesecent_child_persists()
                ),
                1,  # child peer active
                1,  # parent startup
                3,  # parent connect, subscribe, peer active
            ]
        )

    def default_quiesecent_parent_persists(
        self,
        exp_parent_pending: Optional[int | MinRangeTuple] = None,
        exp_child_persists: Optional[int | MinRangeTuple] = None,
    ) -> int:
        return range_min(
            exp_parent_pending
            if exp_parent_pending is not None
            else self.default_quiesecent_parent_pending(
                exp_child_persists=exp_child_persists
            )
        )

    def assert_quiescent_event_counts(
        self,
        *,
        exp_child_persists: int | tuple[int | None, int | None],
        exp_parent_pending: int | tuple[int | None, int | None],
        exp_parent_persists: int | tuple[int | None, int | None],
        exact: bool = False,
    ) -> None:
        exp_child_persists = as_range_tuple(exp_child_persists, exact)
        exp_parent_pending = as_range_tuple(exp_parent_pending, exact)
        exp_parent_persists = as_range_tuple(exp_parent_persists, exact)
        self.parent.assert_event_counts(
            num_pending=exp_parent_pending,
            num_persists=exp_parent_persists,
            num_clears=0,
            num_retrieves=0,
            tag="parent",
            err_str=self.summary_str(),
        )
        self.child.assert_event_counts(
            num_persists=exp_child_persists,
            all_clear=True,
            tag="child",
            err_str=self.summary_str(),
        )

    @classmethod
    def _as_tuple(
        cls,
        val: Optional[int | RangeTuple],
        default_func: typing.Callable[[], int],
        exact: bool,
    ) -> MinRangeTuple:
        return (
            as_min_range_tuple(default_func(), exact=exact)
            if val is None
            else as_min_range_tuple(val, exact=exact)
        )

    async def await_quiescent_connections(
        self,
        *,
        exp_child_persists: Optional[int | RangeTuple] = None,
        exp_parent_pending: Optional[int | RangeTuple] = None,
        exp_parent_persists: Optional[int | RangeTuple] = None,
        exact: bool = False,
        assert_event_counts: bool = True,
    ) -> None:
        exp_child_persists = self._as_tuple(
            exp_child_persists, self.default_quiesecent_child_persists, exact
        )
        exp_parent_pending = self._as_tuple(
            exp_parent_pending,
            functools.partial(
                self.default_quiesecent_parent_pending, exp_child_persists[0]
            ),
            exact,
        )
        exp_parent_persists = self._as_tuple(
            exp_parent_persists,
            functools.partial(
                self.default_quiesecent_parent_persists,
                exp_parent_pending=exp_child_persists,
                exp_child_persists=exp_child_persists,
            ),
            exact,
        )
        # Multiple waits for clarity when something goes wrong, rather than
        # one long wait with many possible failures.
        await self.await_child_at_rest(
            exp_child_persists=exp_child_persists,
            exact=exact,
        )
        await self.await_parent_at_rest(
            exp_parent_pending=exp_parent_pending,
            exact=exact,
            exp_parent_persists=exp_parent_persists,
        )
        if assert_event_counts:
            self.assert_quiescent_event_counts(
                exp_child_persists=exp_child_persists,
                exp_parent_pending=exp_parent_pending,
                exp_parent_persists=exp_parent_persists,
                exact=exact,
            )

    def pings_from_parent_topic(self) -> str:
        return MQTTTopic.encode(
            envelope_type="gw",
            src=self.parent.publication_name,
            dst=self.parent.links.topic_dst(self.parent.downstream_client),
            message_type="gridworks-ping",
        )

    def pings_from_child_topic(self) -> str:
        return MQTTTopic.encode(
            envelope_type="gw",
            src=self.child.publication_name,
            dst=self.child.links.topic_dst(self.child.downstream_client),
            message_type="gridworks-ping",
        )

    def pings_from_parent(self) -> int:
        return self.child.stats.link(self.child.upstream_client).num_received_by_topic[
            self.pings_from_parent_topic()
        ]

    def pings_from_child(self) -> int:
        return self.parent.stats.link(
            self.parent.downstream_client
        ).num_received_by_topic[self.pings_from_child_topic()]

    def child_comm_events(self) -> list[CommEvent]:
        return self.child.stats.link(self.child.upstream_client).comm_events

    def parent_comm_events(self) -> list[CommEvent]:
        return self.parent.stats.link(self.parent.downstream_client).comm_events
