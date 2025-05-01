import typing
from types import ModuleType

import rich
from gwproto import HardwareLayout, Message
from gwproto.messages import EventBase

from gwproactor import App, AppInterface, AppSettings, actors
from gwproactor.actors.actor import PrimeActor
from gwproactor.config import MQTTClient
from gwproactor.config.links import CodecSettings, LinkSettings
from gwproactor.config.proactor_config import ProactorName
from gwproactor.message import MQTTReceiptPayload
from gwproactor.persister import TimedRollingFilePersister
from gwproactor_test.dummies import DUMMY_ATN_NAME, DUMMY_SCADA1_NAME, DUMMY_SCADA2_NAME
from gwproactor_test.dummies.names import DUMMY_ADMIN_NAME, DUMMY_ADMIN_SHORT_NAME
from gwproactor_test.dummies.tree.admin_messages import (
    AdminCommandReadRelays,
    AdminCommandSetRelay,
    AdminSetRelayEvent,
)
from gwproactor_test.dummies.tree.codecs import ScadaCodecFactory
from gwproactor_test.dummies.tree.messages import (
    RelayInfoReported,
    RelayReportEvent,
    RelayReportReceivedEvent,
    RelayStates,
    SetRelay,
    SetRelayMessage,
)


class DummyScada1(PrimeActor):
    relays: RelayStates

    def __init__(self, name: str, services: AppInterface) -> None:
        super().__init__(name, services)
        self.relays = RelayStates()

    def set_relay(self, relay_name: str, closed: bool) -> None:
        self.services.send_threadsafe(
            SetRelayMessage(
                src=self.name,
                dst=self.name,
                relay_name=relay_name,
                closed=closed,
            )
        )

    def process_internal_message(self, message: Message[typing.Any]) -> None:
        self.services.logger.path(
            f"++{self.name}._derived_process_message "
            f"{message.Header.Src}/{message.Header.MessageType}"
        )
        path_dbg = 0
        match message.Payload:
            case SetRelay():
                path_dbg |= 0x00000001
                self.services.publish_message(
                    self.services.downstream_client,
                    SetRelayMessage(
                        src=self.services.publication_name,
                        relay_name=message.Payload.RelayName,
                        closed=message.Payload.Closed,
                        ack_required=True,
                    ),
                )
            case _:
                path_dbg |= 0x00000002
        self.services.logger.path(
            "--{self.name}._derived_process_message  path:0x{path_dbg:08X}"
        )

    def _process_report_relay_event(self, event: RelayReportEvent) -> None:
        self.services.logger.path(
            f"++{self.name}._process_set_relay_event "
            f"{event.relay_name}  "
            f"closed:{event.closed}  "
            f"changed: {event.changed}"
        )
        path_dbg = 0
        if event.relay_name not in self.relays.Relays:
            self.relays.Relays[event.relay_name] = RelayInfoReported()
        last_val = self.relays.Relays[event.relay_name].Closed
        self.relays.Relays[event.relay_name].Closed = event.closed
        changed = last_val != self.relays.Relays[event.relay_name].Closed
        self.services.logger.info(
            f"{event.relay_name}:  {int(last_val)} -> "
            f"{int(self.relays.Relays[event.relay_name].Closed)}  "
            f"changed: {int(changed)}/{int(event.changed)}"
        )
        report_received_event = RelayReportReceivedEvent(
            relay_name=event.relay_name,
            closed=event.closed,
            changed=event.changed,
        )

        if changed != event.changed:
            path_dbg |= 0x00000001
            report_received_event.mismatch = True
            self.relays.Relays[event.relay_name].CurrentChangeMismatch = True
            self.relays.Relays[event.relay_name].MismatchCount += 1
            self.relays.TotalChangeMismatches += 1
            report_received_event.mismatch_count = self.relays.TotalChangeMismatches
            self.services.logger.info(
                f"State change mismatch for {event.relay_name}  "
                f"found: {int(changed)}  reported: {event.changed}  "
                f"total mismatches: {self.relays.TotalChangeMismatches}"
            )
        self.services.generate_event(report_received_event)
        self.services.logger.path(
            f"--{self.name}._process_set_relay_event "
            f"{event.relay_name}  "
            f"closed:{event.closed}  "
            f"changed: {event.changed}  "
            f"path: 0x{path_dbg:08X}"
        )

    def _process_event(self, event: EventBase) -> None:
        self.services.logger.path(
            f"++_process_event  {event.TypeName}  from:{event.Src}",
        )
        self.services.generate_event(event)
        if isinstance(event, RelayReportEvent):
            self._process_report_relay_event(event)
        self.services.logger.path("--_process_event")

    def _process_downstream_mqtt_message(
        self, message: Message[MQTTReceiptPayload], decoded: Message[typing.Any]
    ) -> None:
        self.services.logger.path(
            f"++{self.name}._process_downstream_mqtt_message {message.Payload.message.topic}",
        )
        path_dbg = 0
        match decoded.Payload:
            case EventBase():
                path_dbg |= 0x00000001
                self._process_event(decoded.Payload)
            case _:
                # For testing purposes, this should fail.
                path_dbg |= 0x00000002
                rich.print(decoded.Header)
                raise ValueError(
                    "In this test, since the environment is controlled, "
                    "there is no handler for mqtt message payload type "
                    f"[{type(decoded.Payload)}]\n"
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
                self.services.publish_message(
                    self.services.downstream_client,
                    SetRelayMessage(
                        src=self.services.publication_name,
                        relay_name=command.RelayInfo.RelayName,
                        closed=command.RelayInfo.Closed,
                        ack_required=True,
                    ),
                )
            case AdminCommandReadRelays():
                path_dbg |= 0x00000002
                self.services.publish_message(
                    self.admin_client,
                    Message(
                        Src=self.services.publication_name,
                        Payload=self.relays.model_copy(),
                    ),
                )
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
        if message.Payload.client_name == self.services.downstream_client:
            path_dbg |= 0x00000001
            self._process_downstream_mqtt_message(message, decoded)
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

    @property
    def admin_client(self) -> str:
        return DUMMY_ADMIN_NAME

    @classmethod
    def get_codec_factory(cls) -> ScadaCodecFactory:
        return ScadaCodecFactory()


class DummyScada1Settings(AppSettings):
    dummy_atn1: MQTTClient = MQTTClient()
    dummy_scada2: MQTTClient = MQTTClient()
    dummy_admin: MQTTClient = MQTTClient()


class DummyScada1App(App):
    ATN_LINK: str = DUMMY_ATN_NAME
    SCADA2_LINK: str = DUMMY_SCADA2_NAME
    ADMIN_LINK: str = DUMMY_ADMIN_NAME

    @classmethod
    def app_settings_type(cls) -> type[DummyScada1Settings]:
        return DummyScada1Settings

    @classmethod
    def prime_actor_type(cls) -> type[DummyScada1]:
        return DummyScada1

    @property
    def prime_actor(self) -> DummyScada1:
        return typing.cast(DummyScada1, self._prime_actor)

    @classmethod
    def actors_module(cls) -> ModuleType:
        return actors

    @classmethod
    def paths_name(cls) -> str:
        return DUMMY_SCADA1_NAME

    def _get_name(self, layout: HardwareLayout) -> ProactorName:
        return ProactorName(
            long_name=layout.scada_g_node_alias,
            short_name="s",
        )

    def _get_link_settings(
        self,
        name: ProactorName,  # noqa: ARG002
        layout: HardwareLayout,
        brokers: dict[str, MQTTClient],  # noqa: ARG002
    ) -> dict[str, LinkSettings]:
        return {
            self.ATN_LINK: LinkSettings(
                broker_name=self.ATN_LINK,
                peer_long_name=layout.atn_g_node_alias,
                peer_short_name="a",
                upstream=True,
            ),
            self.SCADA2_LINK: LinkSettings(
                broker_name=self.SCADA2_LINK,
                peer_long_name=layout.scada_g_node_alias + ".s2",
                peer_short_name="s2",
                downstream=True,
                codec=CodecSettings(
                    message_modules=["gwproactor_test.dummies.tree.messages"]
                ),
            ),
            self.ADMIN_LINK: LinkSettings(
                broker_name=self.ADMIN_LINK,
                peer_long_name=DUMMY_ADMIN_NAME,
                peer_short_name=DUMMY_ADMIN_SHORT_NAME,
                link_subscription_short_name=layout.scada_g_node_alias,
            ),
        }

    @classmethod
    def _make_persister(cls, settings: AppSettings) -> TimedRollingFilePersister:
        return TimedRollingFilePersister(settings.paths.event_dir)
