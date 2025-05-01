"""Scada implementation"""

import typing
from typing import Any, Optional

from gwproto import HardwareLayout, Message
from gwproto.messages import EventBase

from gwproactor import App, AppSettings
from gwproactor.actors.actor import PrimeActor
from gwproactor.config import MQTTClient
from gwproactor.config.links import LinkSettings
from gwproactor.config.proactor_config import ProactorName
from gwproactor.message import MQTTReceiptPayload
from gwproactor.persister import PersisterInterface, SimpleDirectoryWriter
from gwproactor_test.dummies import DUMMY_SCADA1_NAME
from gwproactor_test.dummies.names import DUMMY_ATN_NAME


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


class DummyAtnSettings(AppSettings):
    dummy_scada1: MQTTClient = MQTTClient()


class DummyAtnApp(App):
    SCADA1_LINK: str = DUMMY_SCADA1_NAME

    @classmethod
    def app_settings_type(cls) -> type[AppSettings]:
        return DummyAtnSettings

    @classmethod
    def prime_actor_type(cls) -> type[DummyAtn]:
        return DummyAtn

    @property
    def prime_actor(self) -> DummyAtn:
        return typing.cast(DummyAtn, self._prime_actor)

    @classmethod
    def paths_name(cls) -> Optional[str]:
        return DUMMY_ATN_NAME

    def _get_name(self, layout: HardwareLayout) -> ProactorName:
        return ProactorName(
            long_name=layout.atn_g_node_alias,
            short_name="a",
        )

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
    def _make_persister(cls, settings: AppSettings) -> PersisterInterface:
        return SimpleDirectoryWriter(settings.paths.event_dir)
