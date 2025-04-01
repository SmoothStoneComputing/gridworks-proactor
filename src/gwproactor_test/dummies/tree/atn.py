"""Scada implementation"""

import typing
from pathlib import Path
from typing import Any, Optional

from gwproto import Message
from gwproto.messages import EventBase

from gwproactor import ProactorSettings
from gwproactor.actors.actor import PrimeActor
from gwproactor.config import Paths, paths
from gwproactor.config.proactor_config import ProactorName
from gwproactor.message import MQTTReceiptPayload
from gwproactor.persister import PersisterInterface, SimpleDirectoryWriter
from gwproactor_test.dummies.names import DUMMY_ATN_NAME, DUMMY_ATN_SHORT_NAME
from gwproactor_test.dummies.tree.atn_settings import DummyAtnSettings
from gwproactor_test.instrumented_app import TestApp


class DummyAtn(PrimeActor):
    def process_mqtt_message(
        self, mqtt_client_message: Message[MQTTReceiptPayload], decoded: Message[Any]
    ) -> None:
        self.services.logger.path(
            f"++{self.name}.process_mqtt_message %s",
            mqtt_client_message.Payload.message.topic,
        )
        path_dbg = 0
        self.services.stats.add_message(decoded)
        match decoded.Payload:
            case EventBase():
                path_dbg |= 0x00000001
            case _:
                path_dbg |= 0x00000002
        self.services.logger.path(
            f"--{self.name}.process_mqtt_message  path:0x%08X", path_dbg
        )


class DummyAtnApp(TestApp):
    @classmethod
    def get_name(cls) -> ProactorName:
        return ProactorName(
            long_name=DUMMY_ATN_NAME,
            short_name=DUMMY_ATN_SHORT_NAME,
            paths_name=DUMMY_ATN_NAME,
        )

    @classmethod
    def get_prime_actor_type(cls) -> typing.Type[DummyAtn]:
        return DummyAtn

    @classmethod
    def get_settings(cls, paths_name: Optional[str | Path] = None) -> ProactorSettings:
        if paths_name is None:
            paths_name = paths.DEFAULT_NAME_DIR
        return DummyAtnSettings(paths=Paths(name=paths_name))

    @classmethod
    def make_persister(cls, settings: ProactorSettings) -> PersisterInterface:
        return SimpleDirectoryWriter(settings.paths.event_dir)
