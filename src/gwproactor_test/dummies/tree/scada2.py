import typing
from collections import defaultdict
from typing import Optional

import rich
from gwproto import Message

from gwproactor import Proactor, ProactorSettings
from gwproactor.links.link_settings import LinkSettings
from gwproactor.message import MQTTReceiptPayload
from gwproactor.persister import TimedRollingFilePersister
from gwproactor_test.dummies.names import DUMMY_SCADA2_SHORT_NAME
from gwproactor_test.dummies.tree.admin_messages import (
    AdminCommandSetRelay,
    AdminSetRelayEvent,
)
from gwproactor_test.dummies.tree.codecs import AdminCodec, DummyCodec
from gwproactor_test.dummies.tree.messages import (
    RelayInfo,
    RelayReportEvent,
    SetRelay,
)
from gwproactor_test.dummies.tree.scada2_settings import DummyScada2Settings


class DummyScada2(Proactor):
    relays: dict[str, bool]

    def __init__(
        self,
        name: str = "",
        settings: Optional[DummyScada2Settings] = None,
    ) -> None:
        if settings is None:
            settings = DummyScada2Settings()
        self.relays = defaultdict(bool)
        super().__init__(name=name, settings=settings)
        self._links.add_mqtt_link(
            LinkSettings(
                client_name=self.settings.scada1_link.client_name,
                gnode_name=self.settings.scada1_link.long_name,
                spaceheat_name=self.settings.scada1_link.short_name,
                mqtt=self.settings.scada1_link,
                codec=DummyCodec(
                    src_name=self.settings.scada1_link.long_name,
                    dst_name=DUMMY_SCADA2_SHORT_NAME,
                    model_name="Scada1ToScada2Message",
                ),
                upstream=True,
            ),
        )
        if self.settings.admin_link.enabled:
            self._links.add_mqtt_link(
                LinkSettings(
                    client_name=self.settings.admin_link.client_name,
                    gnode_name=self.settings.admin_link.long_name,
                    spaceheat_name=self.settings.admin_link.short_name,
                    mqtt=self.settings.admin_link,
                    codec=AdminCodec(),
                ),
            )
        self.links.log_subscriptions("construction")

    @property
    def settings(self) -> DummyScada2Settings:
        return typing.cast(DummyScada2Settings, self._settings)

    @property
    def subscription_name(self) -> str:
        return DUMMY_SCADA2_SHORT_NAME

    @property
    def admin_client(self) -> str:
        return self.settings.admin_link.client_name

    @classmethod
    def make_event_persister(
        cls, settings: ProactorSettings
    ) -> TimedRollingFilePersister:
        return TimedRollingFilePersister(settings.paths.event_dir)

    def _process_set_relay(self, payload: RelayInfo) -> None:
        self._logger.path(
            f"++{self.name}._process_set_relay "
            f"{payload.RelayName}  "
            f"closed:{payload.Closed}"
        )
        path_dbg = 0
        last_val = self.relays[payload.RelayName]
        event = RelayReportEvent(
            relay_name=payload.RelayName,
            closed=payload.Closed,
            changed=last_val != payload.Closed,
        )
        if event.changed:
            path_dbg |= 0x00000001
            self.relays[payload.RelayName] = event.closed
        self.generate_event(event)
        self._logger.path(
            f"--{self.name}._process_set_relay  "
            f"path:0x{path_dbg:08X}  "
            f"{int(last_val)} -> "
            f"{int(event.closed)}"
        )

    def _process_upstream_mqtt_message(
        self, message: Message[MQTTReceiptPayload], decoded: Message[typing.Any]
    ) -> None:
        self._logger.path(
            f"++{self.name}._process_downstream_mqtt_message {message.Payload.message.topic}",
        )
        path_dbg = 0
        match decoded.Payload:
            case SetRelay():
                path_dbg |= 0x00000001
                self._process_set_relay(decoded.Payload)
            case _:
                path_dbg |= 0x00000002
                rich.print(decoded.Header)
                raise ValueError(
                    f"There is no handler for mqtt message payload type [{type(decoded.Payload)}]\n"
                    f"Received\n\t topic: [{message.Payload.message.topic}]"
                )
        self._logger.path(
            f"--{self.name}._process_downstream_mqtt_message  path:0x{path_dbg:08X}",
        )

    def _process_admin_mqtt_message(
        self, message: Message[MQTTReceiptPayload], decoded: Message[typing.Any]
    ) -> None:
        self._logger.path(
            f"++{self.name}._process_admin_mqtt_message {message.Payload.message.topic}",
        )
        path_dbg = 0
        match decoded.Payload:
            case AdminCommandSetRelay() as command:
                path_dbg |= 0x00000001
                self.generate_event(AdminSetRelayEvent(command=command))
                self._process_set_relay(command.RelayInfo)
            case _:
                raise ValueError(
                    "In this test, since the environment is controlled, "
                    "there is no handler for mqtt message payload type "
                    f"[{type(decoded.Payload)}]\n"
                    f"Received\n\t topic: [{message.Payload.message.topic}]"
                )

        self._logger.path(
            f"--{self.name}._process_admin_mqtt_message  path:0x{path_dbg:08X}",
        )

    def _derived_process_mqtt_message(
        self, message: Message[MQTTReceiptPayload], decoded: Message[typing.Any]
    ) -> None:
        self._logger.path(
            f"++{self.name}._derived_process_mqtt_message {message.Payload.message.topic}",
        )
        path_dbg = 0
        if message.Payload.client_name == self.upstream_client:
            path_dbg |= 0x00000001
            self._process_upstream_mqtt_message(message, decoded)
        elif message.Payload.client_name == self.admin_client:
            path_dbg |= 0x00000002
            self._process_admin_mqtt_message(message, decoded)
        else:
            rich.print(decoded.Header)
            raise ValueError(
                "In this test, since the environment is controlled, "
                "there is no mqtt handler for message from client "
                f"[{message.Payload.client_name}]\n"
                f"Received\n\t topic: [{message.Payload.message.topic}]"
            )
        self._logger.path(
            f"--{self.name}._derived_process_mqtt_message  path:0x{path_dbg:08X}",
        )
