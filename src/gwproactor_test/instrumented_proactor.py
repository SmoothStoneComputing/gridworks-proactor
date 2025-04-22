# ruff: noqa: ERA001

import dataclasses
import typing
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Optional, Tuple, cast

from gwproto import Message
from gwproto.messages import CommEvent, EventBase, EventT, PingMessage
from paho.mqtt.client import MQTT_ERR_CONN_LOST, MQTT_ERR_SUCCESS, MQTTMessageInfo
from result import Ok, Result

from gwproactor import Proactor
from gwproactor.config import LoggerLevels
from gwproactor.config.proactor_config import ProactorConfig
from gwproactor.links import LinkManager, LinkState, MQTTClients, MQTTClientWrapper
from gwproactor.message import (
    DBGCommands,
    DBGPayload,
    MQTTReceiptPayload,
    MQTTSubackPayload,
)
from gwproactor.stats import LinkStats, ProactorStats


def split_subscriptions(client_wrapper: MQTTClientWrapper) -> Tuple[int, Optional[int]]:
    for topic, qos in client_wrapper.subscription_items():
        MQTTClientWrapper.subscribe(client_wrapper, topic, qos)
    return MQTT_ERR_SUCCESS, None


@dataclass
class RecorderLinkStats(LinkStats):
    comm_events: list[CommEvent] = field(default_factory=list)
    forwarded: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    event_counts: dict[str, dict[str, int]] = field(
        default_factory=lambda: defaultdict(lambda: defaultdict(int))
    )

    def __str__(self) -> str:
        s = super().__str__()
        if self.comm_events:
            s += "\n  Comm events:"
            for comm_event in self.comm_events:
                copy_event = comm_event.model_copy(
                    update={"MessageId": comm_event.MessageId[:6] + "..."}
                )
                s += f"\n    {str(copy_event)[:154]}"
        if self.forwarded:
            s += "\n  Forwarded events *sent* by type:"
            for message_type in sorted(self.forwarded):
                s += f"\n    {self.forwarded[message_type]:3d}: [{message_type}]"
        if self.event_counts:
            s += "\n  Events *received* by src and type:"
            for event_src in sorted(self.event_counts):
                s += f"\n    src: {event_src}"
                forwards_from_src = self.event_counts[event_src]
                for message_type in sorted(forwards_from_src):
                    s += f"\n      {forwards_from_src[message_type]:3d}: [{message_type}]"
        return s


class RecorderStats(ProactorStats):
    @classmethod
    def make_link(cls, link_name: str) -> RecorderLinkStats:
        return RecorderLinkStats(link_name)

    def link(self, name: str) -> RecorderLinkStats:
        return typing.cast(RecorderLinkStats, super().link(name))


@dataclass
class _PausedAck:
    link_name: str
    message: Message[Any]
    qos: int
    context: Optional[Any]


class RecorderLinks(LinkManager):
    acks_paused: bool
    needs_ack: list[_PausedAck]

    # noinspection PyMissingConstructor
    def __init__(self, other: LinkManager) -> None:
        self.__dict__ = other.__dict__
        self.acks_paused = False
        self.needs_ack = []

    def link(self, name: str) -> LinkState:
        link = self._states.link(name)
        if link is None:
            raise RuntimeError(f"Link {name} not found.")
        return link

    def publish_message(
        self,
        link_name: str,
        message: Message[Any],
        qos: int = 0,
        context: Any = None,
        *,
        topic: str = "",
        use_link_topic: bool = False,
    ) -> MQTTMessageInfo:
        if self.acks_paused:
            self.needs_ack.append(_PausedAck(link_name, message, qos, context))
            return MQTTMessageInfo(-1)
        return super().publish_message(
            link_name,
            message,
            qos=qos,
            context=context,
            topic=topic,
            use_link_topic=use_link_topic,
        )

    def release_acks(self, clear: bool = False, num_to_release: int = -1) -> int:
        # self._logger.info(
        #     f"++release_acks: clear:{clear}  num_to_release:{num_to_release}"
        # )
        # path_dbg = 0
        if clear or num_to_release < 1:
            # path_dbg |= 0x00000001
            self.acks_paused = False
            needs_ack = self.needs_ack
            self.needs_ack = []
        else:
            # path_dbg |= 0x00000002
            num_to_release = min(num_to_release, len(self.needs_ack))
            needs_ack = self.needs_ack[:num_to_release]
            self.needs_ack = self.needs_ack[num_to_release:]
            # self._logger.info(f"needs_ack: {needs_ack}")
            # self._logger.info(f"self.needs_ack: {self.needs_ack}")
        if not clear:
            # path_dbg |= 0x00000004
            for paused_ack in needs_ack:
                # path_dbg |= 0x00000008
                super().publish_message(**dataclasses.asdict(paused_ack))  # noqa
        # self._logger.info(
        #     f"--release_acks: clear:{clear}  num_to_release:{num_to_release}  path:0x{path_dbg:08X}"
        # )
        return len(needs_ack)

    def generate_event(self, event: EventT) -> Result[bool, Exception]:
        if not event.Src:
            event.Src = self.publication_name
        if isinstance(event, CommEvent) and event.Src == self.publication_name:
            cast(
                RecorderLinkStats, self._stats.link(event.PeerName)
            ).comm_events.append(event)
        if event.Src != self.publication_name and event.Src in self._stats.links:
            cast(RecorderLinkStats, self._stats.link(event.Src)).forwarded[
                event.TypeName
            ] += 1
        return super().generate_event(event)


class InstrumentedProactor(Proactor):
    _subacks_paused: dict[str, bool]
    _subacks_available: dict[str, list[Message[Any]]]
    _mqtt_messages_dropped: dict[str, bool]

    def __init__(self, config: ProactorConfig) -> None:
        super().__init__(config)
        self._subacks_paused = defaultdict(bool)
        self._subacks_available = defaultdict(list)
        self._mqtt_messages_dropped = defaultdict(bool)
        self._links = RecorderLinks(self.links)

    @classmethod
    def make_stats(cls) -> RecorderStats:
        return RecorderStats()

    @property
    def links(self) -> RecorderLinks:
        return typing.cast(RecorderLinks, self._links)

    @property
    def stats(self) -> RecorderStats:
        return typing.cast(RecorderStats, self._stats)

    @property
    def recorder_links(self) -> RecorderLinks:
        return typing.cast(RecorderLinks, self._links)

    @property
    def needs_ack(self) -> list[_PausedAck]:
        return self.recorder_links.needs_ack

    def force_mqtt_disconnect(self, client_name: str) -> None:
        mqtt_client = self.mqtt_client_wrapper(client_name).mqtt_client
        # noinspection PyProtectedMember
        mqtt_client._loop_rc_handle(MQTT_ERR_CONN_LOST)  # type: ignore[attr-defined]  # noqa: SLF001

    def _process_mqtt_message(
        self, mqtt_receipt_message: Message[MQTTReceiptPayload]
    ) -> Result[Message[Any], Exception]:
        if self._mqtt_messages_dropped[mqtt_receipt_message.Payload.client_name]:
            return Ok(mqtt_receipt_message)
        match decoded_result := super()._process_mqtt_message(mqtt_receipt_message):
            case Ok(decoded):
                match decoded.Payload:
                    case EventBase() as event:
                        stats = cast(
                            RecorderLinkStats,
                            self._stats.link(mqtt_receipt_message.Payload.client_name),
                        )
                        stats.event_counts[event.Src][event.TypeName] += 1
        return decoded_result

    def subacks_paused(self, client_name: str) -> bool:
        return self._subacks_paused[client_name]

    def num_subacks_available(self, client_name: str) -> int:
        return len(self._subacks_available[client_name])

    def clear_subacks(self, client_name: str) -> None:
        self._subacks_available[client_name] = []

    def mqtt_messages_dropped(self, client_name: str) -> bool:
        return self._mqtt_messages_dropped[client_name]

    def upstream_subacks_paused(self) -> bool:
        return self.subacks_paused(self.upstream_client)

    def num_upstream_subacks_available(self) -> int:
        return self.num_subacks_available(self.upstream_client)

    def clear_upstream_subacks(self) -> None:
        self._subacks_available[self.upstream_client] = []

    def upstream_mqtt_messages_dropped(self) -> bool:
        return self.mqtt_messages_dropped(self.upstream_client)

    def split_client_subacks(self, client_name: str) -> None:
        client_wrapper = self.mqtt_client_wrapper(client_name)

        def member_split_subscriptions() -> Tuple[int, Optional[int]]:
            return split_subscriptions(client_wrapper)

        client_wrapper.subscribe_all = member_split_subscriptions  # type: ignore[method-assign]

    def restore_client_subacks(self, client_name: str) -> None:
        client_wrapper = self.mqtt_client_wrapper(client_name)
        client_wrapper.subscribe_all = MQTTClientWrapper.subscribe_all  # type: ignore[method-assign, assignment]

    def pause_subacks(self, client_name: str) -> None:
        self._subacks_paused[client_name] = True

    def pause_upstream_subacks(self) -> None:
        self.pause_subacks(self.upstream_client)

    def release_subacks(self, client_name: str, num_released: int = -1) -> None:
        if self._receive_queue is None:
            raise RuntimeError(
                "ERROR. release_subacks() called before Proactor started."
            )
        self._subacks_paused[client_name] = False
        if num_released < 0:
            num_released = len(self._subacks_available[client_name])
        release = self._subacks_available[client_name][:num_released]
        remaining = self._subacks_available[client_name][num_released:]
        self._subacks_available[client_name] = remaining
        for message in release:
            self._receive_queue.put_nowait(message)

    def release_upstream_subacks(self, num_released: int = -1) -> None:
        self.release_subacks(self.upstream_client, num_released)

    async def async_process_message(self, message: Message[Any]) -> None:
        if (
            isinstance(message.Payload, MQTTSubackPayload)
            and self._subacks_paused[message.Payload.client_name]
        ):
            self._subacks_available[message.Payload.client_name].append(message)
        else:
            await super().async_process_message(message)

    def pause_acks(self) -> None:
        self.recorder_links.acks_paused = True

    def release_acks(self, clear: bool = False, num_to_release: int = -1) -> int:
        return typing.cast(RecorderLinks, self._links).release_acks(
            clear, num_to_release=num_to_release
        )

    def set_ack_timeout_seconds(self, delay: float) -> None:
        self.links.ack_manager._default_delay_seconds = delay  # noqa: SLF001

    def drop_mqtt(self, client_name: str, drop: bool) -> None:
        self._mqtt_messages_dropped[client_name] = drop

    def summary_str(self) -> str:
        s = str(self.stats)
        s += "\nLink states:\n"
        for link_name in self.stats.links:
            link_state = self._links.link_state(link_name)
            if link_state is None:
                raise KeyError("ERROR. LinkManager has no link state for <{link_name}>")
            s += f"  {link_name:10s}  {link_state.value}\n"
        s += self.links.subscription_str().lstrip()
        s += "Pending acks:\n"
        for link_name in self.stats.links:
            s += f"  {link_name:10s}  {self._links.num_acks(link_name):3d}\n"
        s += self._links.get_reuploads_str() + "\n"
        s += "Paused Subacks:"
        for link_name in self.stats.links:
            s += (
                f"  {link_name:10s}  "
                f"subacks paused: {self._subacks_paused[link_name]}  "
                f"subacks available: {len(self._subacks_available[link_name])}\n"
            )
        return s

    def summarize(self) -> None:
        self._logger.info(self.summary_str())

    def force_ping(self, client_name: str) -> None:
        self._links.publish_message(client_name, PingMessage(Src=self.publication_name))

    @property
    def mqtt_clients(self) -> MQTTClients:
        return self._links.mqtt_clients()

    def mqtt_client_wrapper(self, client_name: str) -> MQTTClientWrapper:
        return self._links.mqtt_client_wrapper(client_name)

    def mqtt_subscriptions(self, client_name: str) -> list[str]:
        return [
            item[0]
            for item in self.mqtt_client_wrapper(client_name).subscription_items()
        ]

    def all_mqtt_subscriptions(self) -> list[str]:
        subscriptions = []
        for client_name in self.mqtt_clients.clients:
            subscriptions.extend(self.mqtt_subscriptions(client_name))
        return subscriptions

    def send_dbg(
        self,
        client_name: str,
        message_summary: int = -1,
        lifecycle: int = -1,
        comm_event: int = -1,
        command: Optional[DBGCommands | str] = None,
    ) -> None:
        if isinstance(command, str):
            command = DBGCommands(command)
        self.send_threadsafe(
            Message(
                Src=self.name,
                Dst=client_name,
                Payload=DBGPayload(
                    Levels=LoggerLevels(
                        message_summary=message_summary,
                        lifecycle=lifecycle,
                        comm_event=comm_event,
                    ),
                    Command=command,
                ),
            )
        )

    def mqtt_quiescent(self) -> bool:
        mqtt_quiescent = getattr(super(), "mqtt_quiescent()", None)
        if mqtt_quiescent is not None:
            return typing.cast(bool, mqtt_quiescent())
        link = self._links.link(self.upstream_client)
        return link is None or link.active_for_send()

    def _call_super_if_present(self, function_name: str) -> None:
        if hasattr(super(), function_name):
            getattr(super(), function_name)()

    def disable_derived_events(self) -> None:
        self._call_super_if_present("disable_dervived_events")

    def enable_derived_events(self) -> None:
        self._call_super_if_present("enable_dervived_events")
