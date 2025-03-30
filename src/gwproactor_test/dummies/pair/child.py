from gwproto import Message, MQTTCodec, MQTTTopic, create_message_model

from gwproactor import Proactor, ProactorSettings
from gwproactor.config import MQTTClient
from gwproactor.config.proactor_config import ProactorName
from gwproactor.links import QOS
from gwproactor.links.link_settings import LinkSettings
from gwproactor.persister import PersisterInterface, TimedRollingFilePersister
from gwproactor_test.dummies.names import (
    CHILD_SHORT_NAME,
    DUMMY_CHILD_NAME,
    DUMMY_PARENT_NAME,
    PARENT_SHORT_NAME,
)
from gwproactor_test.instrumented_app import TestApp


class ChildMQTTCodec(MQTTCodec):
    def __init__(self) -> None:
        super().__init__(
            create_message_model(
                "ChildMessageDecoder",
                [
                    "gwproto.messages",
                    "gwproactor.message",
                ],
            )
        )

    def validate_source_and_destination(self, src: str, dst: str) -> None:
        if src != DUMMY_PARENT_NAME or dst != CHILD_SHORT_NAME:
            raise ValueError(
                "ERROR validating src and/or dst\n"
                f"  exp: {DUMMY_PARENT_NAME} -> {CHILD_SHORT_NAME}\n"
                f"  got: {src} -> {dst}"
            )


class DummyChildApp(TestApp):
    PARENT_MQTT: str = "parent"

    @classmethod
    def get_name(cls) -> ProactorName:
        return ProactorName(
            long_name=DUMMY_CHILD_NAME,
            short_name=CHILD_SHORT_NAME,
            paths_name=DUMMY_CHILD_NAME,
        )

    @classmethod
    def get_mqtt_clients(cls) -> dict[str, MQTTClient]:
        return {DummyChildApp.PARENT_MQTT: MQTTClient()}

    @classmethod
    def make_persister(cls, settings: ProactorSettings) -> PersisterInterface:
        return TimedRollingFilePersister(settings.paths.event_dir)

    def connect_proactor(self, proactor: Proactor) -> None:
        proactor.links.add_mqtt_link(
            LinkSettings(
                client_name=DummyChildApp.PARENT_MQTT,
                gnode_name=DUMMY_PARENT_NAME,
                spaceheat_name=PARENT_SHORT_NAME,
                mqtt=proactor.settings.mqtt[self.PARENT_MQTT],
                codec=ChildMQTTCodec(),
                upstream=True,
            )
        )
        for topic in [
            MQTTTopic.encode_subscription(Message.type_name(), "1", "a"),
            MQTTTopic.encode_subscription(Message.type_name(), "2", "b"),
        ]:
            proactor.links.subscribe(self.PARENT_MQTT, topic, QOS.AtMostOnce)
