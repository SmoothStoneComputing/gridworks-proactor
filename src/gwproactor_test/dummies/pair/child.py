import typing
from typing import Optional

from gwproto import Message, MQTTCodec, MQTTTopic, create_message_model

from gwproactor import ProactorSettings
from gwproactor.links import QOS
from gwproactor.links.link_settings import LinkSettings
from gwproactor.persister import TimedRollingFilePersister
from gwproactor.proactor_implementation import Proactor
from gwproactor_test.dummies.names import (
    CHILD_SHORT_NAME,
    DUMMY_CHILD_NAME,
    DUMMY_PARENT_NAME,
    PARENT_SHORT_NAME,
)
from gwproactor_test.dummies.pair.child_config import DummyChildSettings


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


class DummyChild(Proactor):
    PARENT_MQTT = "gridworks"

    def __init__(
        self,
        name: str = "",
        settings: Optional[DummyChildSettings] = None,
    ) -> None:
        super().__init__(
            name=name or DUMMY_CHILD_NAME,
            settings=DummyChildSettings() if settings is None else settings,
        )
        self._links.add_mqtt_link(
            LinkSettings(
                client_name=DummyChild.PARENT_MQTT,
                gnode_name=DUMMY_PARENT_NAME,
                spaceheat_name=PARENT_SHORT_NAME,
                mqtt=self.settings.parent_mqtt,
                codec=ChildMQTTCodec(),
                upstream=True,
            )
        )
        for topic in [
            MQTTTopic.encode_subscription(Message.type_name(), "1", "a"),
            MQTTTopic.encode_subscription(Message.type_name(), "2", "b"),
        ]:
            self._links.subscribe(self.PARENT_MQTT, topic, QOS.AtMostOnce)
        self._links.log_subscriptions("construction")

    @classmethod
    def make_event_persister(
        cls, settings: ProactorSettings
    ) -> TimedRollingFilePersister:
        return TimedRollingFilePersister(settings.paths.event_dir)

    @property
    def publication_name(self) -> str:
        return self.name

    @property
    def subscription_name(self) -> str:
        return CHILD_SHORT_NAME

    @property
    def settings(self) -> DummyChildSettings:
        return typing.cast(DummyChildSettings, self._settings)
