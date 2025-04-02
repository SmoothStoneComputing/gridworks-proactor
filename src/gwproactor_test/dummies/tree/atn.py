"""Scada implementation"""

from typing import Any

from gwproto import HardwareLayout, Message
from gwproto.messages import EventBase

from gwproactor import ProactorSettings
from gwproactor.actors.actor import PrimeActor
from gwproactor.config import MQTTClient, Paths
from gwproactor.config.links import LinkSettings
from gwproactor.config.proactor_config import ProactorName
from gwproactor.message import MQTTReceiptPayload
from gwproactor.persister import PersisterInterface, SimpleDirectoryWriter
from gwproactor_test.dummies import DUMMY_SCADA1_NAME
from gwproactor_test.dummies.names import DUMMY_ATN_NAME
from gwproactor_test.instrumented_app import InstrumentedApp


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


class DummyAtnApp(InstrumentedApp):
    SCADA1_LINK: str = DUMMY_SCADA1_NAME

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            paths=Paths(name=DUMMY_ATN_NAME), prime_actor_type=DummyAtn, **kwargs
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
        return {self.SCADA1_LINK: MQTTClient()}

    def _get_link_settings(
        self,
        name: ProactorName,  # noqa: ARG002
        layout: HardwareLayout,
        brokers: dict[str, MQTTClient],  # noqa: ARG002
    ) -> dict[str, LinkSettings]:
        return {
            self.SCADA1_LINK: LinkSettings(
                broker_name=self.SCADA1_LINK,
                peer_long_name=layout.scada_g_node_alias,
                peer_short_name="s",
                downstream=True,
            )
        }

    @classmethod
    def _make_persister(cls, settings: ProactorSettings) -> PersisterInterface:
        return SimpleDirectoryWriter(settings.paths.event_dir)
