"""Scada implementation"""

from typing import Any, Optional, cast

from gwproto import Message
from gwproto.messages import EventBase

from gwproactor import ProactorSettings
from gwproactor.links.link_settings import LinkSettings
from gwproactor.message import MQTTReceiptPayload
from gwproactor.persister import SimpleDirectoryWriter
from gwproactor.proactor_implementation import Proactor
from gwproactor_test.dummies import DUMMY_ATN_NAME
from gwproactor_test.dummies.names import DUMMY_ATN_SHORT_NAME
from gwproactor_test.dummies.tree.atn_settings import DummyAtnSettings
from gwproactor_test.dummies.tree.codecs import DummyCodec


class DummyAtn(Proactor):
    def __init__(
        self,
        name: str = "",
        settings: Optional[DummyAtnSettings] = None,
    ) -> None:
        super().__init__(
            name=name or DUMMY_ATN_NAME,
            settings=DummyAtnSettings() if settings is None else settings,
        )
        self._links.add_mqtt_link(
            LinkSettings(
                client_name=self.settings.scada1_link.client_name,
                gnode_name=self.settings.scada1_link.long_name,
                spaceheat_name=self.settings.scada1_link.short_name,
                mqtt=self.settings.scada1_link,
                codec=DummyCodec(
                    src_name=self.settings.scada1_link.long_name,
                    dst_name=DUMMY_ATN_SHORT_NAME,
                    model_name="Scada1ToAtn1Message",
                ),
                downstream=True,
            ),
        )
        self.links.log_subscriptions()

    @classmethod
    def make_event_persister(cls, settings: ProactorSettings) -> SimpleDirectoryWriter:
        return SimpleDirectoryWriter(settings.paths.event_dir)

    @property
    def publication_name(self) -> str:
        return self.name

    @property
    def subscription_name(self) -> str:
        return "a"

    @property
    def settings(self) -> DummyAtnSettings:
        return cast(DummyAtnSettings, self._settings)

    def _derived_process_mqtt_message(
        self, message: Message[MQTTReceiptPayload], decoded: Message[Any]
    ) -> None:
        self._logger.path(
            f"++{self.name}._derived_process_mqtt_message %s",
            message.Payload.message.topic,
        )
        path_dbg = 0
        self.stats.add_message(decoded)
        match decoded.Payload:
            case EventBase():
                path_dbg |= 0x00000008
            case _:
                path_dbg |= 0x00000040
        self._logger.path(
            f"--{self.name}._derived_process_mqtt_message  path:0x%08X", path_dbg
        )
