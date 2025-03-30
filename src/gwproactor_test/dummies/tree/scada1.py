import typing
from typing import Optional

import rich
from gwproto import Message
from gwproto.messages import EventBase

from gwproactor import Proactor, ProactorSettings, ServicesInterface
from gwproactor.actors.actor import PrimeActor
from gwproactor.config import MQTTClient
from gwproactor.config.proactor_config import ProactorConfig, ProactorName
from gwproactor.links.link_settings import LinkSettings
from gwproactor.message import MQTTReceiptPayload
from gwproactor.persister import TimedRollingFilePersister
from gwproactor_test.dummies import DUMMY_SCADA1_NAME
from gwproactor_test.dummies.names import DUMMY_SCADA1_SHORT_NAME
from gwproactor_test.dummies.tree.admin_messages import (
    AdminCommandReadRelays,
    AdminCommandSetRelay,
    AdminSetRelayEvent,
)
from gwproactor_test.dummies.tree.admin_settings import AdminLinkSettings
from gwproactor_test.dummies.tree.codecs import AdminCodec, DummyCodec
from gwproactor_test.dummies.tree.messages import (
    RelayInfoReported,
    RelayReportEvent,
    RelayReportReceivedEvent,
    RelayStates,
    SetRelay,
    SetRelayMessage,
)
from gwproactor_test.dummies.tree.scada1_settings import (
    AtnLinkSettings,
    Scada2LinkSettings,
)
from gwproactor_test.instrumented_app import TestApp


class DummyScada1(PrimeActor):
    ADMIN_LINK: str = "admin_link"

    relays: RelayStates

    def __init__(self, name: str, services: ServicesInterface) -> None:
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
        return self.ADMIN_LINK


class DummyScada1App(TestApp):
    ATN_LINK: str = "atn_link"
    SCADA2_LINK: str = "scada2_link"

    atn_link: AtnLinkSettings
    scada2_link: Scada2LinkSettings
    admin_link: AdminLinkSettings

    def __init__(
        self,
        *,
        name: Optional[ProactorName] = None,
        settings: Optional[ProactorSettings] = None,
        config: Optional[ProactorConfig] = None,
    ) -> None:
        super().__init__(
            name=name, settings=settings, config=config, prime_actor_type=DummyScada1
        )
        self.atn_link = AtnLinkSettings(
            **self.config.settings.mqtt[self.ATN_LINK].model_dump()
        )
        self.scada2_link = Scada2LinkSettings(
            **self.config.settings.mqtt[self.SCADA2_LINK].model_dump()
        )
        self.admin_link = AdminLinkSettings(
            **self.config.settings.mqtt[DummyScada1.ADMIN_LINK].model_dump()
        )

    @classmethod
    def get_mqtt_clients(cls) -> dict[str, MQTTClient]:
        return {
            cls.ATN_LINK: MQTTClient(),
            cls.SCADA2_LINK: MQTTClient(),
            DummyScada1.ADMIN_LINK: MQTTClient(),
        }

    @classmethod
    def get_name(cls) -> ProactorName:
        return ProactorName(
            long_name=DUMMY_SCADA1_NAME,
            short_name=DUMMY_SCADA1_SHORT_NAME,
            paths_name=DUMMY_SCADA1_NAME,
        )

    @classmethod
    def make_persister(cls, settings: ProactorSettings) -> TimedRollingFilePersister:
        return TimedRollingFilePersister(settings.paths.event_dir)

    def connect_proactor(self, proactor: Proactor) -> None:
        proactor.links.add_mqtt_link(
            LinkSettings(
                client_name=self.atn_link.client_name,
                gnode_name=self.atn_link.long_name,
                spaceheat_name=self.atn_link.short_name,
                mqtt=self.atn_link,
                codec=DummyCodec(
                    src_name=self.atn_link.long_name,
                    dst_name=DUMMY_SCADA1_SHORT_NAME,
                    model_name="Atn1ToScada1Message",
                ),
                upstream=True,
            ),
        )
        proactor.links.add_mqtt_link(
            LinkSettings(
                client_name=self.scada2_link.client_name,
                gnode_name=self.scada2_link.long_name,
                spaceheat_name=self.scada2_link.short_name,
                mqtt=self.scada2_link,
                codec=DummyCodec(
                    src_name=self.scada2_link.long_name,
                    dst_name=DUMMY_SCADA1_SHORT_NAME,
                    model_name="Scada2ToScada1Message",
                ),
                downstream=True,
            ),
        )
        if self.admin_link.enabled:
            proactor.links.add_mqtt_link(
                LinkSettings(
                    client_name=self.admin_link.client_name,
                    gnode_name=self.admin_link.long_name,
                    spaceheat_name=self.admin_link.long_name,
                    subscription_name=self.config.name.publication_name,
                    mqtt=self.admin_link,
                    codec=AdminCodec(),
                ),
            )
        proactor.links.log_subscriptions("construction")
        proactor.links.enable_mqtt_loggers(proactor.logger.message_summary_logger)
