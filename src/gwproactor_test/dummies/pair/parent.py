"""Scada implementation"""

from typing import Any

from gwproto import HardwareLayout, Message
from result import Ok, Result

from gwproactor import ProactorSettings
from gwproactor.actors.actor import PrimeActor
from gwproactor.config import MQTTClient, Paths
from gwproactor.config.links import LinkSettings
from gwproactor.config.proactor_config import ProactorName
from gwproactor.message import DBGPayload
from gwproactor.persister import PersisterInterface, SimpleDirectoryWriter
from gwproactor_test.dummies import DUMMY_CHILD_NAME, DUMMY_PARENT_NAME
from gwproactor_test.instrumented_app import InstrumentedApp


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


class ParentApp(InstrumentedApp):
    CHILD_MQTT: str = DUMMY_CHILD_NAME

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            paths=Paths(name=DUMMY_PARENT_NAME), prime_actor_type=DummyParent, **kwargs
        )

    def _get_name(self, layout: HardwareLayout) -> ProactorName:
        return ProactorName(
            long_name=layout.atn_g_node_alias,
            short_name="a",
        )

    def _get_mqtt_broker_settings(
        self,
        name: ProactorName,  # noqa: ARG002
        layout: HardwareLayout,  # noqa: ARG002
    ) -> dict[str, MQTTClient]:
        return {self.CHILD_MQTT: MQTTClient()}

    def _get_link_settings(
        self,
        name: ProactorName,  # noqa: ARG002
        layout: HardwareLayout,
        brokers: dict[str, MQTTClient],  # noqa: ARG002
    ) -> dict[str, LinkSettings]:
        return {
            self.CHILD_MQTT: LinkSettings(
                broker_name=self.CHILD_MQTT,
                peer_long_name=layout.scada_g_node_alias,
                peer_short_name="s",
                downstream=True,
            )
        }

    @classmethod
    def _make_persister(cls, settings: ProactorSettings) -> PersisterInterface:
        return SimpleDirectoryWriter(settings.paths.event_dir)
