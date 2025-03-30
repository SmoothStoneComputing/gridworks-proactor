import abc
from pathlib import Path
from typing import Optional, Self, Type, TypeVar

from gwproactor import Proactor, ProactorSettings
from gwproactor.actors.actor import PrimeActor
from gwproactor.config import MQTTClient, Paths, paths
from gwproactor.config.proactor_config import ProactorConfig, ProactorName
from gwproactor.persister import PersisterInterface, StubPersister
from gwproactor.stats import ProactorStats

PrimeActorT = TypeVar("PrimeActorT", bound=PrimeActor)


class App(abc.ABC):
    config: ProactorConfig
    proactor: Optional[Proactor] = None
    prime_actor_type: Optional[type[PrimeActor]] = None
    prime_actor: Optional[PrimeActor] = None

    def __init__(
        self,
        *,
        name: Optional[ProactorName] = None,
        settings: Optional[ProactorSettings] = None,
        config: Optional[ProactorConfig] = None,
        prime_actor_type: Optional[Type[PrimeActor]] = None,
    ) -> None:
        self.config = (
            self.make_proactor_config(name=name, settings=settings)
            if config is None
            else config
        )
        self.prime_actor_type = prime_actor_type

    @classmethod
    @abc.abstractmethod
    def get_name(cls) -> ProactorName:
        raise NotImplementedError

    @classmethod
    def get_mqtt_clients(cls) -> dict[str, MQTTClient]:
        return {}

    @classmethod
    def get_settings(cls, paths_name: Optional[str | Path] = None) -> ProactorSettings:
        if paths_name is None:
            paths_name = paths.DEFAULT_NAME_DIR
        return ProactorSettings(paths=Paths(name=paths_name))

    @classmethod
    def make_persister(cls, settings: ProactorSettings) -> PersisterInterface:  # noqa: ARG003
        return StubPersister()

    @classmethod
    def make_stats(cls) -> ProactorStats:
        return ProactorStats()

    @classmethod
    def make_proactor_config(
        cls,
        name: Optional[ProactorName] = None,
        settings: Optional[ProactorSettings] = None,
    ) -> ProactorConfig:
        name = cls.get_name() if name is None else name
        settings = (
            cls.get_settings(paths_name=name.paths_name)
            if settings is None
            else settings
        )
        for client_name, client_config in cls.get_mqtt_clients().items():
            settings.add_mqtt_client(client_name, client_config)
        return ProactorConfig(
            name=name,
            settings=settings,
            event_persister=cls.make_persister(settings),
            stats=cls.make_stats(),
        )

    @classmethod
    def instantiate_proactor(cls, config: ProactorConfig) -> Proactor:
        return Proactor(config)

    @abc.abstractmethod
    def connect_proactor(self, proactor: Proactor) -> None:
        raise NotImplementedError

    def instantiate(self) -> Self:
        self.proactor = self.instantiate_proactor(self.config)
        self.connect_proactor(self.proactor)
        if self.prime_actor_type is not None:
            self.prime_actor = self.prime_actor_type(
                self.config.name.long_name,
                self.proactor,
            )

        self.proactor.links.log_subscriptions("construction")
        return self
