"""Scada implementation"""

from typing import Any, Optional

from gwproto import Message
from gwproto.messages import EventBase

from gwproactor import ProactorSettings
from gwproactor.actors.actor import PrimeActor
from gwproactor.config import MQTTClient
from gwproactor.config.proactor_config import ProactorConfig, ProactorName
from gwproactor.links.link_settings import LinkSettings
from gwproactor.message import MQTTReceiptPayload
from gwproactor.persister import PersisterInterface, SimpleDirectoryWriter
from gwproactor.proactor_implementation import Proactor
from gwproactor_test.dummies.names import DUMMY_ATN_NAME, DUMMY_ATN_SHORT_NAME
from gwproactor_test.dummies.tree.atn_settings import Scada1LinkSettings
from gwproactor_test.dummies.tree.codecs import DummyCodec
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
    SCADA1_LINK: str = "scada1_link"
    scada1_link: Scada1LinkSettings

    def __init__(
        self,
        *,
        name: Optional[ProactorName] = None,
        settings: Optional[ProactorSettings] = None,
        config: Optional[ProactorConfig] = None,
    ) -> None:
        super().__init__(
            name=name, settings=settings, config=config, prime_actor_type=DummyAtn
        )
        self.scada1_link = Scada1LinkSettings(
            **self.config.settings.mqtt[self.SCADA1_LINK].model_dump()
        )

    @classmethod
    def get_name(cls) -> ProactorName:
        return ProactorName(
            long_name=DUMMY_ATN_NAME,
            short_name=DUMMY_ATN_SHORT_NAME,
            paths_name=DUMMY_ATN_NAME,
        )

    @classmethod
    def get_mqtt_clients(cls) -> dict[str, MQTTClient]:
        return {cls.SCADA1_LINK: MQTTClient()}

    @classmethod
    def make_persister(cls, settings: ProactorSettings) -> PersisterInterface:
        return SimpleDirectoryWriter(settings.paths.event_dir)

    def connect_proactor(self, proactor: Proactor) -> None:
        if self.proactor is None:
            raise RuntimeError("connect_proactor called before instantiate()")
        proactor.links.add_mqtt_link(
            LinkSettings(
                client_name=self.scada1_link.client_name,
                gnode_name=self.scada1_link.long_name,
                spaceheat_name=self.scada1_link.short_name,
                mqtt=self.scada1_link,
                codec=DummyCodec(
                    src_name=self.scada1_link.long_name,
                    dst_name=DUMMY_ATN_SHORT_NAME,
                    model_name="Scada1ToAtn1Message",
                ),
                downstream=True,
            ),
        )
