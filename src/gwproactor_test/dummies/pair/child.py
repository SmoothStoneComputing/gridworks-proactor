from typing import Any

from gwproto import HardwareLayout, Message, MQTTTopic

from gwproactor import Proactor, ProactorSettings
from gwproactor.config import MQTTClient, Paths
from gwproactor.config.links import LinkSettings
from gwproactor.config.proactor_config import ProactorName
from gwproactor.links import QOS
from gwproactor.persister import PersisterInterface, TimedRollingFilePersister
from gwproactor_test.dummies import DUMMY_CHILD_NAME, DUMMY_PARENT_NAME
from gwproactor_test.dummies.pair.child_config import DummyChildSettings
from gwproactor_test.instrumented_app import InstrumentedApp


class DummyChildApp(InstrumentedApp):
    PARENT_MQTT: str = DUMMY_PARENT_NAME

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(paths=Paths(name=DUMMY_CHILD_NAME), **kwargs)

    def _get_name(self, layout: HardwareLayout) -> ProactorName:
        return ProactorName(
            long_name=layout.scada_g_node_alias,
            short_name="s",
        )

    def _get_mqtt_broker_settings(
        self,
        name: ProactorName,  # noqa: ARG002
        layout: HardwareLayout,  # noqa: ARG002
    ) -> dict[str, MQTTClient]:
        return {self.PARENT_MQTT: MQTTClient()}

    def _get_link_settings(
        self,
        name: ProactorName,  # noqa: ARG002
        layout: HardwareLayout,
        brokers: dict[str, MQTTClient],  # noqa: ARG002
    ) -> dict[str, LinkSettings]:
        return {
            self.PARENT_MQTT: LinkSettings(
                broker_name=self.PARENT_MQTT,
                peer_long_name=layout.atn_g_node_alias,
                peer_short_name="a",
                upstream=True,
            )
        }

    @classmethod
    def _make_persister(cls, settings: ProactorSettings) -> PersisterInterface:
        return TimedRollingFilePersister(settings.paths.event_dir)

    def _connect_links(self, proactor: Proactor) -> None:
        super()._connect_links(proactor)
        for topic in [
            MQTTTopic.encode_subscription(Message.type_name(), "1", "a"),
            MQTTTopic.encode_subscription(Message.type_name(), "2", "b"),
        ]:
            proactor.links.subscribe(
                DummyChildSettings.PARENT_MQTT, topic, QOS.AtMostOnce
            )
