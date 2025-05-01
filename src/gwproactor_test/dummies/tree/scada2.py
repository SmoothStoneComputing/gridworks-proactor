import typing
from collections import defaultdict

import rich
from gwproto import HardwareLayout, Message

from gwproactor import App, AppInterface, AppSettings
from gwproactor.actors.actor import PrimeActor
from gwproactor.config import MQTTClient
from gwproactor.config.links import CodecSettings, LinkSettings
from gwproactor.config.proactor_config import ProactorName
from gwproactor.message import MQTTReceiptPayload
from gwproactor.persister import TimedRollingFilePersister
from gwproactor_test.dummies import DUMMY_SCADA1_NAME, DUMMY_SCADA2_NAME
from gwproactor_test.dummies.names import DUMMY_ADMIN_NAME, DUMMY_ADMIN_SHORT_NAME
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


class DummyScada2(PrimeActor):
    ADMIN_LINK: str = "admin_link"

    relays: dict[str, bool]

    def __init__(self, name: str, services: AppInterface) -> None:
        super().__init__(name, services)
        self.relays = self.relays = defaultdict(bool)

    @property
    def admin_client(self) -> str:
        return self.ADMIN_LINK

    @classmethod
    def get_codec_factory(cls) -> ScadaCodecFactory:
        return ScadaCodecFactory()

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


class DummyScada2Settings(AppSettings):
    dummy_scada1: MQTTClient = MQTTClient()
    dummy_admin: MQTTClient = MQTTClient()


class DummyScada2App(App):
    SCADA1_LINK: str = DUMMY_SCADA1_NAME
    ADMIN_LINK: str = DUMMY_ADMIN_NAME

    @classmethod
    def app_settings_type(cls) -> type[DummyScada2Settings]:
        return DummyScada2Settings

    @classmethod
    def prime_actor_type(cls) -> type[DummyScada2]:
        return DummyScada2

    @property
    def prime_actor(self) -> DummyScada2:
        return typing.cast(DummyScada2, self._prime_actor)

    @classmethod
    def paths_name(cls) -> str:
        return DUMMY_SCADA2_NAME

    def _get_name(self, layout: HardwareLayout) -> ProactorName:
        return ProactorName(
            long_name=layout.scada_g_node_alias + ".s2",
            short_name="s2",
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
                upstream=True,
                codec=CodecSettings(
                    message_modules=["gwproactor_test.dummies.tree.messages"]
                ),
            ),
            self.ADMIN_LINK: LinkSettings(
                broker_name=self.ADMIN_LINK,
                peer_long_name=DUMMY_ADMIN_NAME,
                peer_short_name=DUMMY_ADMIN_SHORT_NAME,
                link_subscription_short_name=layout.scada_g_node_alias + ".s2",
            ),
        }

    @classmethod
    def _make_persister(cls, settings: AppSettings) -> TimedRollingFilePersister:
        return TimedRollingFilePersister(settings.paths.event_dir)
