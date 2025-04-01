import typing
from collections import defaultdict
from pathlib import Path
from typing import Optional

import rich
from gwproto import Message

from gwproactor import ProactorSettings, ServicesInterface
from gwproactor.actors.actor import PrimeActor
from gwproactor.config import Paths, paths
from gwproactor.config.proactor_config import ProactorName
from gwproactor.message import MQTTReceiptPayload
from gwproactor.persister import TimedRollingFilePersister
from gwproactor_test.dummies import DUMMY_SCADA2_NAME
from gwproactor_test.dummies.names import DUMMY_SCADA2_SHORT_NAME
from gwproactor_test.dummies.tree.admin_messages import (
    AdminCommandSetRelay,
    AdminSetRelayEvent,
)
from gwproactor_test.dummies.tree.codecs import ScadaCodecFactory
from gwproactor_test.dummies.tree.messages import (
    RelayInfo,
    RelayReportEvent,
    SetRelay,
)
from gwproactor_test.dummies.tree.scada2_settings import (
    DummyScada2Settings,
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
    @classmethod
    def get_name(cls) -> ProactorName:
        return ProactorName(
            long_name=DUMMY_SCADA2_NAME,
            short_name=DUMMY_SCADA2_SHORT_NAME,
            paths_name=DUMMY_SCADA2_NAME,
        )

    @classmethod
    def get_prime_actor_type(cls) -> typing.Type[DummyScada2]:
        return DummyScada2

    @classmethod
    def get_settings(cls, paths_name: Optional[str | Path] = None) -> ProactorSettings:
        if paths_name is None:
            paths_name = paths.DEFAULT_NAME_DIR
        return DummyScada2Settings(paths=Paths(name=paths_name))

    @classmethod
    def get_codec_factory(cls) -> ScadaCodecFactory:
        return ScadaCodecFactory()

    @classmethod
    def make_persister(cls, settings: ProactorSettings) -> TimedRollingFilePersister:
        return TimedRollingFilePersister(settings.paths.event_dir)
