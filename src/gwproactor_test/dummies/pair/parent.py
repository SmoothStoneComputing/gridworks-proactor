"""Scada implementation"""

from typing import Any, Optional

from gwproto import Message, MQTTCodec, create_message_model
from result import Ok, Result

from gwproactor import Proactor, ProactorSettings
from gwproactor.actors.actor import PrimeActor
from gwproactor.config import MQTTClient
from gwproactor.config.proactor_config import ProactorConfig, ProactorName
from gwproactor.links.link_settings import LinkSettings
from gwproactor.message import DBGPayload
from gwproactor.persister import PersisterInterface, SimpleDirectoryWriter
from gwproactor_test.dummies.names import (
    CHILD_SHORT_NAME,
    DUMMY_CHILD_NAME,
    DUMMY_PARENT_NAME,
    PARENT_SHORT_NAME,
)
from gwproactor_test.instrumented_app import TestApp


class ParentMQTTCodec(MQTTCodec):
    def __init__(self) -> None:
        super().__init__(
            create_message_model(
                model_name="ParentMessageDecoder",
                module_names=["gwproto.messages", "gwproactor.message"],
            )
        )

    def validate_source_and_destination(self, src: str, dst: str) -> None:
        if src != DUMMY_CHILD_NAME or dst != PARENT_SHORT_NAME:
            raise ValueError(
                "ERROR validating src and/or dst\n"
                f"  exp: {DUMMY_CHILD_NAME} -> {PARENT_SHORT_NAME}\n"
                f"  got: {src} -> {dst}"
            )


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
    CHILD_MQTT: str = "child"

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
    def get_mqtt_clients(cls) -> dict[str, MQTTClient]:
        return {ParentApp.CHILD_MQTT: MQTTClient()}

    @classmethod
    def make_persister(cls, settings: ProactorSettings) -> PersisterInterface:
        return SimpleDirectoryWriter(settings.paths.event_dir)

    def connect_proactor(self, proactor: Proactor) -> None:
        if self.proactor is None:
            raise RuntimeError("connect_proactor called before instantiate()")
        proactor.links.add_mqtt_link(
            LinkSettings(
                client_name=self.CHILD_MQTT,
                gnode_name=DUMMY_CHILD_NAME,
                spaceheat_name=CHILD_SHORT_NAME,
                mqtt=proactor.settings.mqtt[self.CHILD_MQTT],
                codec=ParentMQTTCodec(),
                downstream=True,
            )
        )
