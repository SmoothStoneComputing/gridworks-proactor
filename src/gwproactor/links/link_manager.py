import asyncio
import json
from dataclasses import asdict, dataclass, field
from typing import Any, Optional, Tuple

from gwproto import Message, MQTTCodec, MQTTTopic
from gwproto.messages import (
    Ack,
    CommEvent,
    EventT,
    MQTTConnectEvent,
    MQTTDisconnectEvent,
    MQTTFullySubscribedEvent,
    PeerActiveEvent,
    PingMessage,
    ProblemEvent,
    ResponseTimeoutEvent,
    StartupEvent,
)
from paho.mqtt.client import MQTTMessageInfo
from result import Err, Ok, Result

from gwproactor.config import ProactorSettings
from gwproactor.links import AckWaitInfo
from gwproactor.links.acks import AckManager, AckTimerCallback
from gwproactor.links.link_settings import LinkConfig
from gwproactor.links.link_state import (
    InvalidCommStateInput,
    LinkState,
    LinkStates,
    StateName,
    Transition,
)
from gwproactor.links.message_times import LinkMessageTimes, MessageTimes
from gwproactor.links.mqtt import QOS, MQTTClients, MQTTClientWrapper
from gwproactor.links.reuploads import Reuploads
from gwproactor.links.timer_interface import TimerManagerInterface
from gwproactor.logger import LoggerOrAdapter, ProactorLogger
from gwproactor.message import (
    MQTTConnectFailPayload,
    MQTTConnectPayload,
    MQTTDisconnectPayload,
    MQTTReceiptPayload,
    MQTTSubackPayload,
)
from gwproactor.persister import (
    ByteDecodingError,
    DecodingError,
    FileEmptyWarning,
    JSONDecodingError,
    PersisterInterface,
    UIDMissingWarning,
)
from gwproactor.problems import Problems
from gwproactor.stats import ProactorStats


@dataclass
class LinkManagerTransition(Transition):
    canceled_acks: list[AckWaitInfo] = field(default_factory=list)


class LinkManager:
    PERSISTER_ENCODING = "utf-8"
    publication_name: str
    subscription_name: str
    _settings: ProactorSettings
    _logger: ProactorLogger
    _stats: ProactorStats
    _event_persister: PersisterInterface
    _reuploads: Reuploads
    _mqtt_clients: MQTTClients
    _mqtt_codecs: dict[str, MQTTCodec]
    _states: LinkStates
    _message_times: MessageTimes
    _acks: AckManager

    def __init__(  # noqa: PLR0913, PLR0917
        self,
        publication_name: str,
        subscription_name: str,
        settings: ProactorSettings,
        logger: ProactorLogger,
        stats: ProactorStats,
        event_persister: PersisterInterface,
        timer_manager: TimerManagerInterface,
        ack_timeout_callback: AckTimerCallback,
    ) -> None:
        self.publication_name = publication_name
        self.subscription_name = subscription_name
        self._settings = settings
        self._logger = logger
        self._stats = stats
        self._event_persister = event_persister
        self._reuploads = Reuploads(
            self._logger,
            self._settings.num_initial_event_reuploads,
        )
        self._mqtt_clients = MQTTClients()
        self._mqtt_codecs = {}
        self._states = LinkStates()
        self._message_times = MessageTimes()
        self._acks = AckManager(
            timer_manager, ack_timeout_callback, delay=settings.ack_timeout_seconds
        )

    @property
    def upstream_client(self) -> str:
        return self._mqtt_clients.upstream_client

    @property
    def downstream_client(self) -> str:
        return self._mqtt_clients.downstream_client

    @property
    def num_pending(self) -> int:
        return self._event_persister.num_pending

    @property
    def num_reupload_pending(self) -> int:
        return self._reuploads.num_reupload_pending

    @property
    def num_reuploaded_unacked(self) -> int:
        return self._reuploads.num_reuploaded_unacked

    @property
    def ack_manager(self) -> AckManager:
        return self._acks

    def topic_dst(self, client_name: str) -> str:
        return self._mqtt_clients.topic_dst(client_name)

    def reuploading(self) -> bool:
        return self._reuploads.reuploading()

    def num_acks(self, link_name: str) -> int:
        return self._acks.num_acks(link_name)

    def subscribed(self, link_name: str) -> bool:
        return self._mqtt_clients.subscribed(link_name)

    def mqtt_clients(self) -> MQTTClients:
        return self._mqtt_clients

    def mqtt_client_wrapper(self, client_name: str) -> MQTTClientWrapper:
        return self._mqtt_clients.client_wrapper(client_name)

    def enable_mqtt_loggers(self, logger: Optional[LoggerOrAdapter] = None) -> None:
        self._mqtt_clients.enable_loggers(logger)

    def disable_mqtt_loggers(self) -> None:
        self._mqtt_clients.disable_loggers()

    def decoder(self, link_name: str) -> Optional[MQTTCodec]:
        return self._mqtt_codecs.get(link_name, None)

    def decode(self, link_name: str, topic: str, payload: bytes) -> Message[Any]:
        return self._mqtt_codecs[link_name].decode(topic, payload)

    def link(self, name: str) -> Optional[LinkState]:
        return self._states.link(name)

    def link_state(self, name: str) -> Optional[StateName]:
        return self._states.link_state(name)

    def link_names(self) -> list[str]:
        return self._states.link_names()

    def __contains__(self, name: str) -> bool:
        return name in self._states

    def __getitem__(self, name: str) -> LinkState:
        return self._states[name]

    def get_message_times(self, link_name: str) -> LinkMessageTimes:
        return self._message_times.get_copy(link_name)

    def stopped(self, name: str) -> bool:
        return self._states.stopped(name)

    def add_mqtt_link(self, settings: LinkConfig) -> None:
        self._mqtt_clients.add_client(settings)
        self._mqtt_codecs[settings.client_name] = settings.codec
        self._states.add(settings.client_name)
        self._message_times.add_link(settings.client_name)
        self._stats.add_link(settings.client_name)
        self.subscribe(
            client=settings.client_name,
            topic=settings.subscription_topic(
                settings.subscription_name
                if settings.subscription_name
                else self.subscription_name
            ),
            qos=QOS.AtMostOnce,
        )

    def subscribe(self, client: str, topic: str, qos: int) -> Tuple[int, Optional[int]]:
        return self._mqtt_clients.subscribe(client, topic, qos)

    def subscription_str(self, tag: str = "") -> str:
        tag_str = f"[{tag}]" if tag else ""
        s = f"\nSubscription info for <{self.publication_name}> {tag_str}\n"
        for client_name in self._mqtt_clients.clients:
            client = self._mqtt_clients.client_wrapper(client_name)
            s += f"  Client name: <{client_name}>  topic_dst: <{client.topic_dst}>\n"
            publish_topic = MQTTTopic.encode(
                envelope_type=Message.type_name(),
                src=self.publication_name,
                dst=client.topic_dst,
                message_type="SOME_MESSAGE_TYPE",
            )
            s += f"    Publish to: {publish_topic}\n"
            s += "    Subscriptions:\n"
            for subscription in client.subscription_items():
                s += f"      [{subscription}]\n"
        return s

    def log_subscriptions(self, tag: str = "") -> None:
        if self._logger.lifecycle_enabled:
            self._logger.lifecycle(self.subscription_str(tag=tag))

    def get_reuploads_str(self, verbose: bool = True, num_events: int = 5) -> str:  # noqa: FBT001, FBT002
        return self._reuploads.get_str(verbose=verbose, num_events=num_events)

    def publish_message(  # noqa: PLR0913
        self,
        link_name: str,
        message: Message[Any],
        qos: int = 0,
        context: Any = None,
        *,
        topic: str = "",
        use_link_topic: bool = False,
    ) -> MQTTMessageInfo:
        if topic and use_link_topic:
            raise ValueError(
                f"Error. Specify at most one of use_link_topic "
                f"({use_link_topic}) and topic ({topic})"
            )
        if not message.Header.Dst:
            message.Header.Dst = self._mqtt_clients.topic_dst(link_name)
        if use_link_topic:
            topic = MQTTTopic.encode(
                envelope_type=message.type_name(),
                src=self.publication_name,
                dst=self._mqtt_clients.topic_dst(link_name),
                message_type=message.message_type(),
            )
        elif not topic:
            topic = message.mqtt_topic()
        payload = self._mqtt_codecs[link_name].encode(message)
        self._logger.message_summary(
            direction="OUT mqtt    ",
            src=message.Header.Src,
            dst=message.Header.Dst,
            topic=topic,
            payload_object=message.Payload,
            message_id=message.Payload.AckMessageID
            if isinstance(message.Payload, Ack)
            else message.Header.MessageId,
        )
        if message.Header.AckRequired:
            self._acks.start_ack_timer(
                link_name, message.Header.MessageId, context=context
            )
        self._message_times.update_send(link_name)
        return self._mqtt_clients.publish(link_name, topic, payload, qos)

    def publish_upstream(
        self, payload: Any, qos: QOS = QOS.AtMostOnce, **message_args: Any
    ) -> MQTTMessageInfo:
        message = Message[Any](
            Src=self.publication_name,
            Dst=self._mqtt_clients.upstream_topic_dst,
            Payload=payload,
            **message_args,
        )
        return self.publish_message(
            self._mqtt_clients.upstream_client, message, qos=qos
        )

    def generate_event(self, event: EventT) -> Result[bool, Exception]:
        num_pending_dbg = self._event_persister.num_pending
        self._logger.path("++generate_event %s  %d", event.TypeName, num_pending_dbg)
        path_dbg = 0
        if not event.Src:
            path_dbg |= 0x00000001
            event.Src = self.publication_name
        if isinstance(event, CommEvent) and event.Src == self.publication_name:
            path_dbg |= 0x00000002
            self._stats.link(event.PeerName).comm_event_counts[event.TypeName] += 1
        if isinstance(event, ProblemEvent) and self._logger.path_enabled:
            path_dbg |= 0x00000004
            self._logger.info(event)
        if (
            self._mqtt_clients.upstream_client
            and self._states[self._mqtt_clients.upstream_client].active_for_send()
        ):
            path_dbg |= 0x00000008
            self.publish_upstream(event, AckRequired=True)
        result = self._event_persister.persist(
            event.MessageId,
            event.model_dump_json(indent=2).encode(self.PERSISTER_ENCODING),
        )
        self._logger.path(
            "--generate_event %s  path:0x%08X  %d - %d",
            event.TypeName,
            path_dbg,
            num_pending_dbg,
            self._event_persister.num_pending,
        )
        return result

    def _start_reupload(self) -> None:
        if not self._reuploads.reuploading():
            self._continue_reupload(
                self._reuploads.start_reupload(self._event_persister.pending_ids())
            )

    def _continue_reupload(self, event_ids: list[str]) -> None:
        self._logger.path("++_continue_reupload  %d", len(event_ids))
        path_dbg = 0
        tried_count_dbg = 0
        sent_count_dbg = 0
        continuation_count_dbg = -1

        if event_ids:
            path_dbg |= 0x00000001
            sent_one = False
            # Try to send all requested events. At least send must succeed to
            # continue the reupload, so if all sends fail, get more until
            # one is sent or there are no more reuploads.
            while not sent_one and self._reuploads.reuploading() and event_ids:
                continuation_path_dbg = 0x00000002
                continuation_count_dbg += 1
                next_event_ids = []
                for event_id in event_ids:
                    event_path_dbg = 0x00000004
                    tried_count_dbg += 1
                    problems = Problems()

                    match ret := self._reupload_event(event_id):
                        case Ok():
                            event_path_dbg |= 0x00000008
                            if ret.value:
                                path_dbg |= 0x00000010
                                sent_count_dbg += 1
                                sent_one = True
                            else:
                                event_path_dbg |= 0x00000020
                                problems.add_error(DecodingError(uid=event_id))
                        case Err():
                            event_path_dbg |= 0x00000040
                            problems.add_problems(ret.err())
                    if problems:
                        event_path_dbg |= 0x00000080
                        # There was some error decoding this event.
                        # We generate a new event with information
                        # about decoding failure and delete this event.
                        self.generate_event(
                            problems.problem_event(
                                f"Event decoding error - uid:{event_id}"
                            )
                        )
                        self._event_persister.clear(event_id)
                        if sent_one:
                            event_path_dbg |= 0x00000100
                            self._reuploads.clear_unacked_event(event_id)
                        else:
                            event_path_dbg |= 0x00000200
                            next_event_ids.extend(
                                self._reuploads.process_ack_for_reupload(event_id)
                            )
                    self._logger.path("  1 event path:0x%08X", event_path_dbg)
                    continuation_path_dbg |= event_path_dbg
                self._logger.path("  1 continuation path:0x%08X", continuation_path_dbg)
                event_ids = next_event_ids
                path_dbg |= continuation_path_dbg
        self._logger.path(
            "--_continue_reupload  path:0x%08X  sent:%d  tried:%d  continuations:%d",
            path_dbg,
            sent_count_dbg,
            tried_count_dbg,
            continuation_count_dbg,
        )

    def _reupload_event(self, event_id: str) -> Result[bool, Problems]:
        """Load event for event_id from storage, decoded to JSON and send it.

        Return either Ok(True) or Err(Problems(list of decoding errors)).

        Send errors handled either by exception, which will propagate up, or
        by ack timeout.
        """
        self._logger.path("++_reupload_event  %s", event_id)
        path_dbg = 0
        problems = Problems()
        match self._event_persister.retrieve(event_id):
            case Ok(event_bytes):
                path_dbg |= 0x00000001
                if event_bytes is None:
                    path_dbg |= 0x00000002
                    problems.add_error(
                        UIDMissingWarning("reupload_events", uid=event_id)
                    )
                elif len(event_bytes) == 0:
                    path_dbg |= 0x00000004
                    problems.add_error(
                        FileEmptyWarning("reupload_events", uid=event_id)
                    )
                else:
                    path_dbg |= 0x00000008
                    try:
                        event_str = event_bytes.decode(encoding=self.PERSISTER_ENCODING)
                    except Exception as e:  # noqa: BLE001
                        path_dbg |= 0x00000010
                        problems.add_error(e).add_error(
                            ByteDecodingError("reupload_events", uid=event_id)
                        )
                    else:
                        path_dbg |= 0x00000020
                        try:
                            event = json.loads(event_str)
                        except Exception as e:  # noqa: BLE001
                            path_dbg |= 0x00000040
                            problems.add_error(e).add_error(
                                JSONDecodingError(
                                    f"reupload_events - raw json:\n<\n{event_str}\n>",
                                    uid=event_id,
                                )
                            )
                        else:
                            path_dbg |= 0x00000080
                            self.publish_upstream(event, AckRequired=True)
                            self._logger.path(
                                "--_reupload_event:1  path:0x%08X", path_dbg
                            )
                            return Ok(value=True)
            case Err(error):
                path_dbg |= 0x00000100
                problems.add_problems(error)
        self._logger.path("--_reupload_event:0  path:0x%08X", path_dbg)
        return Err(problems)

    def start(
        self, loop: asyncio.AbstractEventLoop, async_queue: asyncio.Queue[Any]
    ) -> None:
        self._logger.path("++LinkManager.start")
        if self.upstream_client:
            self._reuploads.stats = self._stats.link(self.upstream_client)
        self._mqtt_clients.start(loop, async_queue)
        self.generate_event(StartupEvent())
        self._states.start_all()
        self._logger.path("--LinkManager.start")

    def stop(self) -> Result[bool, Problems]:
        problems: Optional[Problems] = None
        for link_name in self._states.link_names():
            ret = self._states.stop(link_name)
            match ret := self._states.stop(link_name):
                case Err():
                    if problems is None:
                        problems = Problems(errors=[ret.err()])
                    else:
                        problems.errors.append(ret.err())
        self._mqtt_clients.stop()
        if problems is None:
            return Ok(value=True)
        return Err(problems)

    def process_mqtt_connected(
        self, message: Message[MQTTConnectPayload]
    ) -> Result[Transition, InvalidCommStateInput]:
        state_result = self._states.process_mqtt_connected(message)
        if state_result.is_ok():
            self._logger.comm_event(str(state_result.value))
        self.generate_event(MQTTConnectEvent(PeerName=message.Payload.client_name))
        self._mqtt_clients.subscribe_all(message.Payload.client_name)
        return state_result

    def process_mqtt_disconnected(
        self, message: Message[MQTTDisconnectPayload]
    ) -> Result[LinkManagerTransition, InvalidCommStateInput]:
        state_result = self._states.process_mqtt_disconnected(message)
        result: Result[LinkManagerTransition, InvalidCommStateInput]
        match state_result:
            case Ok():
                # noinspection PyTypeChecker
                result = Ok(LinkManagerTransition(**(asdict(state_result.value))))
                self.generate_event(
                    MQTTDisconnectEvent(PeerName=message.Payload.client_name)
                )
                self._logger.comm_event(str(result.value))
                if result.value.recv_deactivated() or result.value.send_deactivated():
                    result.value.canceled_acks = self._acks.cancel_ack_timers(
                        message.Payload.client_name
                    )
                    self._reuploads.clear()
            case _:
                result = state_result
        return result

    def process_mqtt_connect_fail(
        self, message: Message[MQTTConnectFailPayload]
    ) -> Result[Transition, InvalidCommStateInput]:
        return self._states.process_mqtt_connect_fail(message)

    def process_mqtt_message(
        self, message: Message[MQTTReceiptPayload]
    ) -> Result[Transition, InvalidCommStateInput]:
        match result := self._states.process_mqtt_message(message):
            case Ok():
                self.update_recv_time(message.Payload.client_name)
                if result.value:
                    self._logger.comm_event(str(result.value))
                if result.value.recv_activated():
                    self._recv_activated(result.value)
        return result

    def process_ack_timeout(
        self, wait_info: AckWaitInfo
    ) -> Result[LinkManagerTransition, Exception]:
        self._logger.path(
            "++LinkManager.process_ack_timeout  <%s>  <%s>",
            wait_info.link_name,
            wait_info.message_id,
        )
        path_dbg = 0
        self._stats.link(wait_info.link_name).timeouts += 1
        result: Result[LinkManagerTransition, Exception]
        match state_result := self._states.process_ack_timeout(wait_info.link_name):
            case Ok():
                path_dbg |= 0x00000001
                # https://youtrack.jetbrains.com/issue/PY-76059/Incorrect-Type-warning-with-asdict-and-Dataclass
                # noinspection PyTypeChecker
                result = Ok(
                    LinkManagerTransition(
                        canceled_acks=[wait_info], **(asdict(state_result.value))
                    )
                )
                if result.value.deactivated():
                    path_dbg |= 0x00000002
                    self._reuploads.clear()
                    self.generate_event(
                        ResponseTimeoutEvent(PeerName=result.value.link_name)
                    )
                    self._logger.comm_event(str(result.value))
                    result.value.canceled_acks.extend(
                        self._acks.cancel_ack_timers(wait_info.link_name)
                    )
            case _:
                result = Err(state_result.err())
        self._logger.path("--LinkManager.process_ack_timeout path:0x%08X", path_dbg)
        return result

    def process_ack(self, link_name: str, message_id: str) -> None:
        self._logger.path("++LinkManager.process_ack  <%s>  %s", link_name, message_id)
        path_dbg = 0
        wait_info = self._acks.cancel_ack_timer(link_name, message_id)
        if wait_info is not None and message_id in self._event_persister:
            path_dbg |= 0x00000001
            self._event_persister.clear(message_id)
            if self._reuploads.reuploading() and link_name == self.upstream_client:
                path_dbg |= 0x00000002
                self._continue_reupload(
                    self._reuploads.process_ack_for_reupload(message_id)
                )
                if not self._reuploads.reuploading():
                    path_dbg |= 0x00000004
                    self._logger.info("reupload complete.")
        self._logger.path("--LinkManager.process_ack path:0x%08X", path_dbg)

    def send_ack(self, link_name: str, message: Message[Any]) -> None:
        if message.Header.MessageId:
            self.publish_message(
                link_name,
                Message(
                    Src=self.publication_name,
                    Payload=Ack(AckMessageID=message.Header.MessageId),
                ),
            )

    def start_ping_tasks(self) -> list[asyncio.Task[Any]]:
        return [
            asyncio.create_task(
                self.send_ping(link_name), name=f"send_ping<{link_name}>"
            )
            for link_name in self.link_names()
        ]

    async def send_ping(self, link_name: str) -> None:
        while not self._states.stopped(link_name):
            message_times = self._message_times.get_copy(link_name)
            link_state = self._states[link_name]
            if (
                message_times.time_to_send_ping(self._settings.mqtt_link_poll_seconds)
                and link_state.active_for_send()
            ):
                self.publish_message(link_name, PingMessage(Src=self.publication_name))
            await asyncio.sleep(
                message_times.seconds_until_next_ping(
                    self._settings.mqtt_link_poll_seconds
                )
            )

    def update_recv_time(self, link_name: str) -> None:
        self._message_times.update_recv(link_name)

    def _recv_activated(self, transition: Transition) -> None:
        self._logger.path("++LinkManager._recv_activated.<%s>", transition)
        path_dbg = 0
        if transition.link_name == self.upstream_client:
            path_dbg |= 0x00000001
            self._start_reupload()
        self.generate_event(PeerActiveEvent(PeerName=transition.link_name))
        self._logger.path(
            "--LinkManager._recv_activated.<%s>  path:0x%08X", transition, path_dbg
        )

    def process_mqtt_suback(
        self, message: Message[MQTTSubackPayload]
    ) -> Result[Transition, InvalidCommStateInput]:
        self._logger.path(
            "++LinkManager.process_mqtt_suback client:%s", message.Payload.client_name
        )
        path_dbg = 0
        state_result = self._states.process_mqtt_suback(
            message.Payload.client_name,
            self._mqtt_clients.handle_suback(message.Payload),
        )
        if isinstance(state_result, Ok):
            path_dbg |= 0x00000001
            if state_result.value:
                path_dbg |= 0x00000002
                self._logger.comm_event(str(state_result.value))
            if state_result.value.send_activated():
                path_dbg |= 0x00000004
                self.generate_event(
                    MQTTFullySubscribedEvent(PeerName=message.Payload.client_name)
                )
                self.publish_message(
                    message.Payload.client_name,
                    PingMessage(Src=self.publication_name),
                )
            if state_result.value.recv_activated():
                path_dbg |= 0x00000008
                self._recv_activated(state_result.value)
        self._logger.path(
            "--LinkManager.process_mqtt_suback:%d  path:0x%08X",
            state_result.is_ok(),
            path_dbg,
        )
        return state_result
