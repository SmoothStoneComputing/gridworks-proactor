import abc
import asyncio
import threading
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any, Optional, Self, Sequence, Type

import dotenv
import rich
from aiohttp.typedefs import Handler as HTTPHandler
from gwproto import HardwareLayout, Message, ShNode
from gwproto.messages import EventT
from gwproto.named_types.web_server_gt import WebServerGt
from paho.mqtt.client import MQTTMessageInfo
from result import Result

from gwproactor import actors
from gwproactor.actors.actor import PrimeActor
from gwproactor.callbacks import ProactorCallbackInterface
from gwproactor.codecs import CodecFactory
from gwproactor.config import MQTTClient, Paths
from gwproactor.config.app_settings import AppSettings
from gwproactor.config.links import LinkSettings
from gwproactor.config.proactor_config import ProactorConfig, ProactorName
from gwproactor.external_watchdog import ExternalWatchdogCommandBuilder
from gwproactor.links.link_settings import LinkConfig
from gwproactor.links.mqtt import QOS
from gwproactor.logger import ProactorLogger
from gwproactor.persister import PersisterInterface, StubPersister
from gwproactor.proactor_implementation import Proactor
from gwproactor.proactor_interface import (
    ActorInterface,
    AppInterface,
    CommunicatorInterface,
    IOLoopInterface,
    T,
)
from gwproactor.stats import ProactorStats


@dataclass
class SubTypes:
    proactor_type: type[Proactor] = Proactor
    prime_actor_type: Optional[type[PrimeActor]] = None
    app_settings_type: type[AppSettings] = AppSettings
    actors_module: Optional[ModuleType] = None


@dataclass
class ActorConfig:
    node: ShNode
    constructor_args: dict[str, Any] = field(default_factory=dict)


class App(AppInterface):
    _settings: AppSettings
    config: ProactorConfig
    links: dict[str, LinkSettings]
    sub_types: SubTypes
    _codec_factory: CodecFactory
    _proactor: Optional[Proactor] = None
    _prime_actor: Optional[PrimeActor] = None

    def __init__(  # noqa: PLR0913
        self,
        *,
        paths_name: Optional[str] = None,
        paths: Optional[Paths] = None,
        app_settings: Optional[AppSettings] = None,
        codec_factory: Optional[CodecFactory] = None,
        sub_types: Optional[SubTypes] = None,
        env_file: Optional[str | Path] = None,
    ) -> None:
        self.sub_types = self.make_subtypes() if sub_types is None else sub_types
        self._settings = self.get_settings(
            paths_name=paths_name,
            paths=paths,
            settings=app_settings,
            env_file=env_file,
            settings_type=self.sub_types.app_settings_type,
        )
        if codec_factory is None:
            if self.sub_types.prime_actor_type is not None:
                codec_factory = self.sub_types.prime_actor_type.get_codec_factory()
            else:
                codec_factory = CodecFactory()
        self.codec_factory = CodecFactory() if codec_factory is None else codec_factory
        self.config = self._make_proactor_config()
        self.links = self._get_link_settings(
            name=self.config.name,
            layout=self.config.layout,
            brokers=self.settings.brokers(),
        )

    @property
    def settings(self) -> AppSettings:
        return self._settings

    @property
    def raw_proactor(self) -> Optional[Proactor]:
        return self._proactor

    @raw_proactor.setter
    def raw_proactor(self, proactor: Optional[Proactor]) -> None:
        self._proactor = proactor

    @property
    def proactor(self) -> Proactor:
        if self._proactor is None:
            raise ValueError("proactor accessed before it was initialized")
        return self._proactor

    @property
    def has_prime_actor(self) -> bool:
        return self._prime_actor is not None

    @property
    def prime_actor(self) -> PrimeActor:
        if self._prime_actor is None:
            raise ValueError("prime_actor accessed before it was initialized")
        return self._prime_actor

    @classmethod
    def make_subtypes(cls) -> SubTypes:
        return SubTypes(
            proactor_type=cls.proactor_type(),
            app_settings_type=cls.app_settings_type(),
            prime_actor_type=cls.prime_actor_type(),
            actors_module=cls.actors_module(),
        )

    @classmethod
    def proactor_type(cls) -> type[Proactor]:
        return Proactor

    @classmethod
    def app_settings_type(cls) -> type[AppSettings]:
        return AppSettings

    @classmethod
    def prime_actor_type(cls) -> Optional[type[PrimeActor]]:
        return None

    @classmethod
    def actors_module(cls) -> Optional[ModuleType]:
        return actors

    @classmethod
    def paths_name(cls) -> Optional[str]:
        return None

    def _make_proactor_config(
        self,
    ) -> ProactorConfig:
        layout = self._load_hardware_layout(self.settings.paths.hardware_layout)
        name = self._get_name(layout)
        return ProactorConfig(
            name=name,
            settings=self.settings,
            event_persister=self._make_persister(self.settings),
            hardware_layout=layout,
        )

    def _instantiate_proactor(self) -> Proactor:
        return self.sub_types.proactor_type(self.config)

    def instantiate(self) -> Self:
        self.raw_proactor = self._instantiate_proactor()
        self._connect_links(self.proactor)
        if self.sub_types.prime_actor_type is not None:
            self._prime_actor = self.sub_types.prime_actor_type(
                self.config.name.short_name,
                self.proactor,
            )
        self._load_actors()
        self.proactor.links.log_subscriptions("construction")
        return self

    def run_in_thread(self, *, daemon: bool = True) -> threading.Thread:
        if self.proactor is None:
            raise ValueError("ERROR. Call instantiate() before run_in_thread()")
        return self.proactor.run_in_thread(daemon=daemon)

    @abc.abstractmethod
    def _get_name(self, layout: HardwareLayout) -> ProactorName:
        raise NotImplementedError

    @abc.abstractmethod
    def _get_link_settings(
        self,
        name: ProactorName,
        layout: HardwareLayout,
        brokers: dict[str, MQTTClient],
    ) -> dict[str, LinkSettings]:
        raise NotImplementedError

    def _connect_links(self, proactor: Proactor) -> None:
        for link_name, link_settings in self.links.items():
            if link_settings.enabled:
                proactor.links.add_mqtt_link(
                    LinkConfig(
                        client_name=link_name,
                        gnode_name=link_settings.peer_long_name,
                        spaceheat_name=link_settings.peer_short_name,
                        subscription_name=link_settings.link_subscription_short_name,
                        mqtt=self.settings.broker(link_name),
                        codec=self.codec_factory.get_codec(
                            link_name=link_name,
                            link=link_settings,
                            proactor_name=proactor.name_object,
                            layout=proactor.hardware_layout,
                        ),
                        upstream=link_settings.upstream,
                        downstream=link_settings.downstream,
                    ),
                )

    # noinspection PyMethodMayBeStatic
    def _load_hardware_layout(self, layout_path: str | Path) -> HardwareLayout:
        return HardwareLayout.load(layout_path)

    def _make_persister(self, settings: AppSettings) -> PersisterInterface:  # noqa: ARG002, ARG003
        return StubPersister()

    def _get_actor_nodes(self) -> Sequence[ActorConfig]:
        if self._prime_actor is None or self._prime_actor.node is None:
            return []
        return [
            ActorConfig(node=node)
            for node in self.hardware_layout.nodes.values()
            if (
                node.has_actor
                and self.hardware_layout.parent_node(node) == self.prime_actor.node
            )
        ]

    def _load_actors(self) -> None:
        if self.proactor is None:
            raise ValueError("_load_actors called before proactor instantiated")
        if self.sub_types.actors_module is not None:
            for actor_config in self._get_actor_nodes():
                self.proactor.add_communicator(
                    ActorInterface.load(
                        actor_config.node.Name,
                        actor_config.node.actor_class_str,
                        self,
                        actors_module=self.sub_types.actors_module,
                        **actor_config.constructor_args,
                    )
                )

    @classmethod
    def get_settings(
        cls,
        paths_name: Optional[str] = None,
        paths: Optional[Paths] = None,
        settings: Optional[AppSettings] = None,
        settings_type: Optional[type[AppSettings]] = None,
        env_file: Optional[str | Path] = None,
    ) -> AppSettings:
        if settings_type is None:
            settings_type = cls.app_settings_type()
        return (
            # https://github.com/koxudaxi/pydantic-pycharm-plugin/issues/1013
            settings_type(_env_file=env_file)  # noqa
            if settings is None
            else settings.model_copy(deep=True)
        ).with_paths(name=paths_name if paths_name else cls.paths_name(), paths=paths)

    @classmethod
    def print_settings(
        cls,
        *,
        env_file: str | Path = ".env",
    ) -> None:
        dotenv_file = dotenv.find_dotenv(str(env_file), usecwd=True)
        rich.print(
            f"Env file: <{dotenv_file}>  exists: {bool(dotenv_file and Path(dotenv_file).exists())}"
        )
        app = cls(env_file=env_file)
        rich.print(app.settings)
        missing_tls_paths_ = app.settings.check_tls_paths_present(raise_error=False)
        if missing_tls_paths_:
            rich.print(missing_tls_paths_)

    @property
    def name(self) -> str:
        return self.config.name.name

    def add_communicator(self, communicator: CommunicatorInterface) -> None:
        self.proactor.add_communicator(communicator)

    def get_communicator(self, name: str) -> Optional[CommunicatorInterface]:
        return self.proactor.get_communicator(name)

    def get_communicator_as_type(self, name: str, type_: Type[T]) -> Optional[T]:
        return self.proactor.get_communicator_as_type(name, type_)

    def get_communicator_names(self) -> set[str]:
        return self.proactor.get_communicator_names()

    def send(self, message: Message[Any]) -> None:
        self.proactor.send(message)

    async def await_processing(
        self, message: Message[Any]
    ) -> Result[Any, BaseException]:
        return await self.proactor.await_processing(message)

    def send_threadsafe(self, message: Message[Any]) -> None:
        self.proactor.send_threadsafe(message)

    def wait_for_processing_threadsafe(
        self, message: Message[Any]
    ) -> Result[Any, BaseException]:
        return self.proactor.wait_for_processing_threadsafe(message)

    def add_task(self, task: asyncio.Task[Any]) -> None:
        self.proactor.add_task(task)

    @property
    def async_receive_queue(self) -> Optional[asyncio.Queue[Any]]:
        return self.proactor.async_receive_queue

    @property
    def event_loop(self) -> Optional[asyncio.AbstractEventLoop]:
        return self.proactor.event_loop

    @property
    def io_loop_manager(self) -> IOLoopInterface:
        return self.proactor.io_loop_manager

    def add_web_server_config(
        self, name: str, host: str, port: int, **kwargs: Any
    ) -> None:
        self.proactor.add_web_server_config(name, host, port, **kwargs)

    def add_web_route(
        self,
        server_name: str,
        method: str,
        path: str,
        handler: HTTPHandler,
        **kwargs: Any,
    ) -> None:
        self.proactor.add_web_route(server_name, method, path, handler, **kwargs)

    def get_web_server_route_strings(self) -> dict[str, list[str]]:
        return self.proactor.get_web_server_route_strings()

    def get_web_server_configs(self) -> dict[str, WebServerGt]:
        return self.proactor.get_web_server_configs()

    def generate_event(self, event: EventT) -> Result[bool, Exception]:
        return self.proactor.generate_event(event)

    @property
    def publication_name(self) -> str:
        return self.config.name.publication_name

    @property
    def subscription_name(self) -> str:
        return self.config.name.subscription_name

    def publish_message(  # noqa: PLR0913
        self,
        link_name: str,
        message: Message[Any],
        qos: int = 0,
        context: Any = None,
        *,
        topic: str = "",
        use_link_topic: bool = False,
    ) -> MQTTMessageInfo:
        if self.proactor is None:
            raise ValueError("publish_message called before proactor instantiated")
        return self.proactor.publish_message(
            link_name=link_name,
            message=message,
            qos=qos,
            context=context,
            topic=topic,
            use_link_topic=use_link_topic,
        )

    def publish_upstream(
        self, payload: Any, qos: QOS = QOS.AtMostOnce, **message_args: Any
    ) -> MQTTMessageInfo:
        return self.proactor.publish_upstream(payload=payload, qos=qos, **message_args)

    @property
    def upstream_client(self) -> str:
        return self.proactor.upstream_client

    @property
    def downstream_client(self) -> str:
        return self.proactor.downstream_client

    @property
    def logger(self) -> ProactorLogger:
        return self.config.logger

    @property
    def stats(self) -> ProactorStats:
        return self.proactor.stats

    @property
    def hardware_layout(self) -> HardwareLayout:
        return self.config.layout

    def get_external_watchdog_builder_class(
        self,
    ) -> type[ExternalWatchdogCommandBuilder]:
        return self.proactor.get_external_watchdog_builder_class()

    def add_callbacks(self, callbacks: ProactorCallbackInterface) -> int:
        return self.proactor.add_callbacks(callbacks)

    def remove_callbacks(self, callbacks_id: int) -> None:
        self.proactor.remove_callbacks(callbacks_id)
