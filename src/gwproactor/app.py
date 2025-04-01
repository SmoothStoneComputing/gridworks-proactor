import abc
from pathlib import Path
from typing import Optional, Self, Type, TypeVar

from gwproactor import Proactor, ProactorSettings
from gwproactor.actors.actor import PrimeActor
from gwproactor.codecs import CodecFactory
from gwproactor.config import Paths, paths
from gwproactor.config.proactor_config import ProactorConfig, ProactorName
from gwproactor.links.link_settings import LinkConfig
from gwproactor.persister import PersisterInterface, StubPersister
from gwproactor.stats import ProactorStats

PrimeActorT = TypeVar("PrimeActorT", bound=PrimeActor)


class App(abc.ABC):
    config: ProactorConfig
    proactor: Optional[Proactor] = None
    prime_actor_type: Optional[type[PrimeActor]] = None
    prime_actor: Optional[PrimeActor] = None
    codec_factory: CodecFactory

    def __init__(
        self,
        *,
        name: Optional[ProactorName] = None,
        settings: Optional[ProactorSettings] = None,
        config: Optional[ProactorConfig] = None,
        prime_actor_type: Optional[Type[PrimeActor]] = None,
        codec_factory: Optional[CodecFactory] = None,
    ) -> None:
        self.config = (
            self.make_proactor_config(name=name, settings=settings)
            if config is None
            else config
        )
        self.prime_actor_type = (
            self.get_prime_actor_type()
            if prime_actor_type is None
            else prime_actor_type
        )
        self.codec_factory = (
            self.get_codec_factory() if codec_factory is None else codec_factory
        )

    @classmethod
    @abc.abstractmethod
    def get_name(cls) -> ProactorName:
        raise NotImplementedError

    @classmethod
    def get_prime_actor_type(cls) -> Optional[Type[PrimeActor]]:
        return None

    @classmethod
    def get_codec_factory(cls) -> CodecFactory:
        return CodecFactory()

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
        return ProactorConfig(
            name=name,
            settings=settings,
            event_persister=cls.make_persister(settings),
            stats=cls.make_stats(),
        )

    @classmethod
    def instantiate_proactor(cls, config: ProactorConfig) -> Proactor:
        return Proactor(config)

    def assemble_links(self, proactor: Proactor) -> None:
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

    def instantiate(self) -> Self:
        self.proactor = self.instantiate_proactor(self.config)
        self.assemble_links(self.proactor)
        if self.prime_actor_type is not None:
            self.prime_actor = self.prime_actor_type(
                self.config.name.long_name,
                self.proactor,
            )
        self.proactor.links.log_subscriptions("construction")
        return self
