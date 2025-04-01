"""Scada implementation"""

from pathlib import Path
from typing import Any, Optional

from gwproto import Message
from result import Ok, Result

from gwproactor import ProactorSettings
from gwproactor.actors.actor import PrimeActor
from gwproactor.config import Paths, paths
from gwproactor.config.proactor_config import ProactorConfig, ProactorName
from gwproactor.message import DBGPayload
from gwproactor.persister import PersisterInterface, SimpleDirectoryWriter
from gwproactor_test.dummies.names import (
    DUMMY_PARENT_NAME,
    PARENT_SHORT_NAME,
)
from gwproactor_test.dummies.pair.parent_config import DummyParentSettings
from gwproactor_test.instrumented_app import TestApp


class DummyParent(PrimeActor):
    def process_internal_message(self, message: Message[Any]) -> None:
        self.process_message(message)

    def process_message(self, message: Message[Any]) -> Result[bool, Exception]:
        match message.Payload:
            case DBGPayload():
                message.Header.Src = self.services.publication_name
                dst_client = message.Header.Dst
                message.Header.Dst = ""
                self.services.publish_message(dst_client, message)
        return Ok(True)


class ParentApp(TestApp):
    def __init__(
        self,
        *,
        name: Optional[ProactorName] = None,
        settings: Optional[ProactorSettings] = None,
        config: Optional[ProactorConfig] = None,
    ) -> None:
        super().__init__(
            name=name, settings=settings, config=config, prime_actor_type=DummyParent
        )

    @classmethod
    def get_name(cls) -> ProactorName:
        return ProactorName(
            long_name=DUMMY_PARENT_NAME,
            short_name=PARENT_SHORT_NAME,
            paths_name=DUMMY_PARENT_NAME,
        )

    @classmethod
    def get_settings(cls, paths_name: Optional[str | Path] = None) -> ProactorSettings:
        if paths_name is None:
            paths_name = paths.DEFAULT_NAME_DIR
        return DummyParentSettings(paths=Paths(name=paths_name))

    @classmethod
    def make_persister(cls, settings: ProactorSettings) -> PersisterInterface:
        return SimpleDirectoryWriter(settings.paths.event_dir)
