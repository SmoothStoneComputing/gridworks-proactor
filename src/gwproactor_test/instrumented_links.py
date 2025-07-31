# ruff: noqa: ERA001

import dataclasses
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Optional, cast

from gwproto import Message
from gwproto.messages import Ack, CommEvent, EventBase, EventT
from paho.mqtt.client import MQTTMessageInfo
from pydantic import BaseModel
from result import Result

from gwproactor.links import LinkManager, LinkState
from gwproactor_test.instrumented_stats import RecorderLinkStats


@dataclass
class _PausedAck:
    link_name: str
    message: Message[Any]
    qos: int
    context: Optional[Any]


class _Ackable(BaseModel):
    message_type: str = ""
    send_count: int = 0
    ack_count: int = 0
    is_ack: bool = False
    ack_paths: list[int] = []


class _AckTracker:
    ackables: dict[str, dict[str, _Ackable]]

    def __init__(self) -> None:
        self.ackables = defaultdict(lambda: defaultdict(_Ackable))

    def __len__(self) -> int:
        return len(self.ackables)

    def track_publish(self, link_name: str, message: Message[Any]) -> None:
        is_ack = isinstance(message.Payload, Ack)
        if is_ack:
            tracked_id = message.Payload.AckMessageID
        else:
            tracked_id = message.Header.MessageId
        if is_ack or message.Header.AckRequired:
            link_tracks = self.ackables[link_name]
            tracked = link_tracks[tracked_id]
            if (
                tracked.message_type
                and tracked.message_type != message.Header.MessageType
            ):
                raise ValueError(
                    f"ERROR. _AckTracker recored {tracked_id} "
                    f"with message type {tracked.message_type} but message "
                    f"resent with type {message.Header.MessageType}"
                )
            tracked.message_type = message.Header.MessageType
            tracked.is_ack = is_ack
            tracked.send_count += 1

    def track_ack(self, link_name: str, message_id: str, path_dbg: int) -> None:
        tracked = self.ackables[link_name][message_id]
        tracked.ack_count += 1
        tracked.ack_paths.append(path_dbg)


class RecorderLinks(LinkManager):
    acks_paused: bool
    needs_ack: list[_PausedAck]
    ack_tracker: _AckTracker

    # noinspection PyMissingConstructor
    def __init__(self, other: LinkManager) -> None:
        self.__dict__ = other.__dict__
        self.acks_paused = False
        self.needs_ack = []
        self.ack_tracker = _AckTracker()

    def link(self, name: str) -> LinkState:
        link = self._states.link(name)
        if link is None:
            raise RuntimeError(f"Link {name} not found.")
        return link

    @property
    def in_flight_events(self) -> dict[str, EventBase]:
        return self._in_flight_events

    @property
    def num_in_flight(self) -> int:
        return len(self._in_flight_events)

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
        self.ack_tracker.track_publish(link_name, message)
        return super().publish_message(
            link_name,
            message,
            qos=qos,
            context=context,
            topic=topic,
            use_link_topic=use_link_topic,
        )

    def process_ack(self, link_name: str, message_id: str) -> int:
        path_dbg = super().process_ack(link_name, message_id)
        self.ack_tracker.track_ack(link_name, message_id, path_dbg)
        return path_dbg

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
