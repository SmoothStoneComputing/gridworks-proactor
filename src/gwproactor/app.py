import abc
import threading
from pathlib import Path
from types import ModuleType
from typing import Optional, Self, Sequence, TypeVar

import dotenv
import rich
from gwproto import HardwareLayout, ShNode

from gwproactor.actors.actor import PrimeActor
from gwproactor.codecs import CodecFactory
from gwproactor.config import MQTTClient, Paths
from gwproactor.config.links import LinkSettings
from gwproactor.config.proactor_config import ProactorConfig, ProactorName
from gwproactor.config.proactor_settings import ProactorSettings
from gwproactor.links.link_settings import LinkConfig
from gwproactor.persister import PersisterInterface, StubPersister
from gwproactor.proactor_implementation import Proactor
from gwproactor.proactor_interface import ActorInterface
from gwproactor.stats import ProactorStats

PrimeActorT = TypeVar("PrimeActorT", bound=PrimeActor)


class App(abc.ABC):
    config: ProactorConfig
    proactor_type: type[Proactor] = Proactor
    prime_actor_type: Optional[type[PrimeActor]] = None
    codec_factory: CodecFactory
    actors_module: Optional[ModuleType] = None
    proactor: Optional[Proactor] = None
    prime_actor: Optional[PrimeActor] = None

    def __init__(  # noqa: PLR0913
        self,
        *,
        paths_name: Optional[str] = None,
        paths: Optional[Paths] = None,
        proactor_settings: Optional[ProactorSettings] = None,
        proactor_type: type[Proactor] = Proactor,
        prime_actor_type: Optional[type[PrimeActor]] = None,
        codec_factory: Optional[CodecFactory] = None,
        actors_module: Optional[ModuleType] = None,
        env_file: Optional[str | Path] = None,
    ) -> None:
        self.proactor_type = proactor_type
        self.prime_actor_type = prime_actor_type
        self.codec_factory = CodecFactory() if codec_factory is None else codec_factory
        self.actors_module = actors_module
        self.config = self._make_proactor_config(
            paths_name=paths_name,
            paths=paths,
            proactor_settings=proactor_settings,
            env_file=env_file,
        )

    def _make_proactor_config(
        self,
        paths_name: Optional[str] = None,
        paths: Optional[Paths] = None,
        proactor_settings: Optional[ProactorSettings] = None,
        env_file: Optional[str | Path] = None,
    ) -> ProactorConfig:
        settings = self.get_settings(
            paths_name=paths_name,
            paths=paths,
            settings=proactor_settings,
            env_file=env_file,
        )
        layout = self._load_hardware_layout(settings.paths.hardware_layout)
        name = self._get_name(layout)
        brokers = self._get_mqtt_broker_settings(name, layout)
        for broker_name, broker_settings in brokers.items():
            settings.add_mqtt_broker(broker_name, broker_settings)
        for link_name, link in self._get_link_settings(name, layout, brokers).items():
            settings.add_link(link_name, link)
        return ProactorConfig(
            name=name,
            settings=settings,
            event_persister=self._make_persister(settings),
            stats=self._make_stats(),
            hardware_layout=layout,
        )

    def instantiate(self) -> Self:
        self.proactor = self.proactor_type(self.config)
        self._connect_links(self.proactor)
        if self.prime_actor_type is not None:
            self.prime_actor = self.prime_actor_type(
                self.config.name.long_name,
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
    def _get_mqtt_broker_settings(
        self,
        name: ProactorName,
        layout: HardwareLayout,
    ) -> dict[str, MQTTClient]:
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
        for link_name, link_settings in proactor.settings.links.items():
            if link_settings.enabled:
                proactor.links.add_mqtt_link(
                    LinkConfig(
                        client_name=link_name,
                        gnode_name=link_settings.peer_long_name,
                        spaceheat_name=link_settings.peer_short_name,
                        subscription_name=link_settings.link_subscription_short_name,
                        mqtt=proactor.settings.mqtt_brokers[link_name],
                        codec=self.codec_factory.get_codec(
                            link_name=link_name,
                            proactor_name=proactor.name_object,
                            proactor_settings=proactor.settings,
                            layout=proactor.hardware_layout,
                        ),
                        upstream=link_settings.upstream,
                        downstream=link_settings.downstream,
                    ),
                )

    @property
    def _layout(self) -> HardwareLayout:
        if self.proactor is None:
            raise ValueError("hardware_layout access before proactor instantiated")
        return self.proactor.hardware_layout

    # noinspection PyMethodMayBeStatic
    def _load_hardware_layout(self, layout_path: str | Path) -> HardwareLayout:
        return HardwareLayout.load(layout_path)

    def _make_persister(self, settings: ProactorSettings) -> PersisterInterface:  # noqa: ARG002, ARG003
        return StubPersister()

    def _make_stats(self) -> ProactorStats:
        return ProactorStats()

    def _get_actor_nodes(self) -> Sequence[ShNode]:
        if self.prime_actor is None:
            return []
        return [
            node
            for node in self._layout.nodes.values()
            if (
                node.has_actor
                and self._layout.parent_node(node) == self.prime_actor.node
            )
        ]

    def _load_actors(self) -> None:
        if self.proactor is None:
            raise ValueError("_load_actors called before proactor instantiated")
        if self.actors_module is not None:
            for node in self._get_actor_nodes():
                self.proactor.add_communicator(
                    ActorInterface.load(
                        node.Name,
                        str(node.actor_class),
                        self.proactor,
                        actors_module=self.actors_module,
                    )
                )

    @classmethod
    def get_settings(
        cls,
        paths_name: Optional[str] = None,
        paths: Optional[Paths] = None,
        settings: Optional[ProactorSettings] = None,
        env_file: Optional[str | Path] = None,
    ) -> ProactorSettings:
        return (
            # https://github.com/koxudaxi/pydantic-pycharm-plugin/issues/1013
            ProactorSettings(_env_file=env_file)  # noqa
            if settings is None
            else settings.model_copy(deep=True)
        ).update_paths(name=paths_name, paths=paths)

    @classmethod
    def print_settings(
        cls,
        *,
        env_file: str | Path = ".env",
    ) -> None:
        dotenv_file = dotenv.find_dotenv(str(env_file))
        rich.print(
            f"Env file: <{dotenv_file}>  exists:{env_file and Path(dotenv_file).exists()}"
        )
        app = cls(env_file=env_file)
        settings = app.config.settings
        rich.print(settings)
        missing_tls_paths_ = settings.check_tls_paths_present(raise_error=False)
        if missing_tls_paths_:
            rich.print(missing_tls_paths_)
