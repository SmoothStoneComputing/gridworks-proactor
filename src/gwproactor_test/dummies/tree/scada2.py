import typing
from collections import defaultdict
from typing import Optional

import rich
from gwproto import Message

from gwproactor import Proactor, ProactorSettings, ServicesInterface
from gwproactor.actors.actor import PrimeActor
from gwproactor.config import MQTTClient
from gwproactor.config.proactor_config import ProactorConfig, ProactorName
from gwproactor.links.link_settings import LinkSettings
from gwproactor.message import MQTTReceiptPayload
from gwproactor.persister import TimedRollingFilePersister
from gwproactor_test.dummies import DUMMY_SCADA2_NAME
from gwproactor_test.dummies.names import DUMMY_SCADA2_SHORT_NAME
from gwproactor_test.dummies.tree.admin_messages import (
    AdminCommandSetRelay,
    AdminSetRelayEvent,
)
from gwproactor_test.dummies.tree.admin_settings import AdminLinkSettings
from gwproactor_test.dummies.tree.codecs import AdminCodec, DummyCodec
from gwproactor_test.dummies.tree.messages import (
    RelayInfo,
    RelayReportEvent,
    SetRelay,
)
from gwproactor_test.dummies.tree.scada1_settings import AtnLinkSettings
from gwproactor_test.dummies.tree.scada2_settings import (
    Scada1LinkSettings,
)
from gwproactor_test.instrumented_app import TestApp


class DummyScada2(PrimeActor):
    ADMIN_LINK: str = "admin_link"

    relays: dict[str, bool]

    def __init__(self, name: str, services: ServicesInterface) -> None:
        super().__init__(name, services)
        self.relays = self.relays = defaultdict(bool)

    @property
    def admin_client(self) -> str:
        return self.ADMIN_LINK

    def _process_set_relay(self, payload: RelayInfo) -> None:
        self.services.logger.path(
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
        self.services.generate_event(event)
        self.services.logger.path(
            f"--{self.name}._process_set_relay  "
            f"path:0x{path_dbg:08X}  "
            f"{int(last_val)} -> "
            f"{int(event.closed)}"
        )

    def _process_upstream_mqtt_message(
        self, message: Message[MQTTReceiptPayload], decoded: Message[typing.Any]
    ) -> None:
        self.services.logger.path(
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
        self.services.logger.path(
            f"--{self.name}._process_downstream_mqtt_message  path:0x{path_dbg:08X}",
        )

    def _process_admin_mqtt_message(
        self, message: Message[MQTTReceiptPayload], decoded: Message[typing.Any]
    ) -> None:
        self.services.logger.path(
            f"++{self.name}._process_admin_mqtt_message {message.Payload.message.topic}",
        )
        path_dbg = 0
        match decoded.Payload:
            case AdminCommandSetRelay() as command:
                path_dbg |= 0x00000001
                self.services.generate_event(AdminSetRelayEvent(command=command))
                self._process_set_relay(command.RelayInfo)
            case _:
                raise ValueError(
                    "In this test, since the environment is controlled, "
                    "there is no handler for mqtt message payload type "
                    f"[{type(decoded.Payload)}]\n"
                    f"Received\n\t topic: [{message.Payload.message.topic}]"
                )

        self.services.logger.path(
            f"--{self.name}._process_admin_mqtt_message  path:0x{path_dbg:08X}",
        )

    def process_mqtt_message(
        self, message: Message[MQTTReceiptPayload], decoded: Message[typing.Any]
    ) -> None:
        self.services.logger.path(
            f"++{self.name}._derived_process_mqtt_message {message.Payload.message.topic}",
        )
        path_dbg = 0
        if message.Payload.client_name == self.services.upstream_client:
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
        self.services.logger.path(
            f"--{self.name}._derived_process_mqtt_message  path:0x{path_dbg:08X}",
        )


class DummyScada2App(TestApp):
    ATN_LINK: str = "atn_link"
    SCADA1_LINK: str = "scada1_link"

    atn_link: AtnLinkSettings
    scada1_link: Scada1LinkSettings
    admin_link: AdminLinkSettings

    def __init__(
        self,
        *,
        name: Optional[ProactorName] = None,
        settings: Optional[ProactorSettings] = None,
        config: Optional[ProactorConfig] = None,
    ) -> None:
        super().__init__(
            name=name, settings=settings, config=config, prime_actor_type=DummyScada2
        )
        self.atn_link = AtnLinkSettings(
            **self.config.settings.mqtt[self.ATN_LINK].model_dump()
        )
        self.scada1_link = Scada1LinkSettings(
            **self.config.settings.mqtt[self.SCADA1_LINK].model_dump()
        )
        self.admin_link = AdminLinkSettings(
            **self.config.settings.mqtt[DummyScada2.ADMIN_LINK].model_dump()
        )

    @classmethod
    def get_name(cls) -> ProactorName:
        return ProactorName(
            long_name=DUMMY_SCADA2_NAME,
            short_name=DUMMY_SCADA2_SHORT_NAME,
            paths_name=DUMMY_SCADA2_NAME,
        )

    @classmethod
    def get_mqtt_clients(cls) -> dict[str, MQTTClient]:
        return {
            cls.ATN_LINK: MQTTClient(),
            cls.SCADA1_LINK: MQTTClient(),
            DummyScada2.ADMIN_LINK: MQTTClient(),
        }

    @classmethod
    def make_persister(cls, settings: ProactorSettings) -> TimedRollingFilePersister:
        return TimedRollingFilePersister(settings.paths.event_dir)

    def connect_proactor(self, proactor: Proactor) -> None:
        proactor.links.add_mqtt_link(
            LinkSettings(
                client_name=self.scada1_link.client_name,
                gnode_name=self.scada1_link.long_name,
                spaceheat_name=self.scada1_link.short_name,
                mqtt=self.scada1_link,
                codec=DummyCodec(
                    src_name=self.scada1_link.long_name,
                    dst_name=DUMMY_SCADA2_SHORT_NAME,
                    model_name="Scada1ToScada2Message",
                ),
                upstream=True,
            ),
        )
        if self.admin_link.enabled:
            proactor.links.add_mqtt_link(
                LinkSettings(
                    client_name=self.admin_link.client_name,
                    gnode_name=self.admin_link.long_name,
                    spaceheat_name=self.admin_link.short_name,
                    mqtt=self.admin_link,
                    codec=AdminCodec(),
                ),
            )
        proactor.links.log_subscriptions("construction")
