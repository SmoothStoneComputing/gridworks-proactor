import typing
import uuid
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Mapping, Optional

from gwproto import HardwareLayout

from gwproactor.callbacks import ProactorCallbackFunctions
from gwproactor.config.proactor_settings import ProactorSettings
from gwproactor.logger import ProactorLogger
from gwproactor.persister import PersisterInterface, StubPersister
from gwproactor.stats import ProactorStats


@dataclass
class ProactorName:
    long_name: str
    short_name: str

    @cached_property
    def name(self) -> str:
        return self.long_name

    @cached_property
    def publication_name(self) -> str:
        return self.long_name

    @cached_property
    def subscription_name(self) -> str:
        return self.short_name


class ProactorConfig:
    name: ProactorName
    settings: ProactorSettings
    callback_functions: ProactorCallbackFunctions
    logger: ProactorLogger
    event_persister: PersisterInterface
    stats: ProactorStats
    layout: HardwareLayout

    def __init__(  # noqa: PLR0913
        self,
        name: ProactorName,
        *,
        settings: Optional[ProactorSettings] = None,
        callbacks: Optional[ProactorCallbackFunctions] = None,
        logger: Optional[ProactorLogger] = None,
        event_persister: Optional[PersisterInterface] = None,
        stats: Optional[ProactorStats] = None,
        hardware_layout: Optional[HardwareLayout] = None,
    ) -> None:
        self.name = name
        self.settings = ProactorSettings() if settings is None else settings
        self.callback_functions = (
            ProactorCallbackFunctions() if callbacks is None else callbacks
        )
        self.logger = (
            ProactorLogger(
                **typing.cast(
                    Mapping[str, Any], self.settings.logging.qualified_logger_names()
                )
            )
            if logger is None
            else logger
        )
        self.event_persister = (
            StubPersister() if event_persister is None else event_persister
        )
        self.stats = ProactorStats() if stats is None else stats
        self.layout = (
            (
                HardwareLayout(
                    layout={
                        "ShNodes": [
                            {
                                "ShNodeId": str(uuid.uuid4()),
                                "Name": name.long_name,
                                "ActorClass": "NoActor",
                                "TypeName": "spaceheat.node.gt",
                            }
                        ]
                    },
                    cacs={},
                    components={},
                    nodes={},
                    data_channels={},
                    synth_channels={},
                )
            )
            if hardware_layout is None
            else hardware_layout
        )
