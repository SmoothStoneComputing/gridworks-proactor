# ruff: noqa: ERA001

import logging
import sys
import typing
from collections import defaultdict
from inspect import getframeinfo, stack
from pathlib import Path
from typing import Any, Optional, Tuple, cast

from gwproto import Message
from gwproto.messages import EventBase, PingMessage
from paho.mqtt.client import MQTT_ERR_CONN_LOST, MQTT_ERR_SUCCESS
from result import Ok, Result

from gwproactor import AppInterface, Proactor
from gwproactor.config import LoggerLevels
from gwproactor.config.proactor_config import ProactorConfig
from gwproactor.links import MQTTClients, MQTTClientWrapper
from gwproactor.message import (
    DBGCommands,
    DBGPayload,
    MQTTReceiptPayload,
    MQTTSubackPayload,
)
from gwproactor.str_tasks import str_tasks
from gwproactor_test.instrumented_links import RecorderLinks, _PausedAck
from gwproactor_test.instrumented_stats import RecorderLinkStats, RecorderStats


def split_subscriptions(client_wrapper: MQTTClientWrapper) -> Tuple[int, Optional[int]]:
    for topic, qos in client_wrapper.subscription_items():
        MQTTClientWrapper.subscribe(client_wrapper, topic, qos)
    return MQTT_ERR_SUCCESS, None


def caller_str(depth: int = 3) -> str:
    caller = getframeinfo(stack()[depth][0])
    caller_filename = Path(caller.filename)
    try:
        if sys.version_info >= (3, 12):
            path_str = Path(caller.filename).relative_to(Path.cwd(), walk_up=True)
        else:
            path_str = Path(caller.filename).relative_to(Path.cwd())
    except:  # noqa
        path_str = caller_filename
    return f"{path_str}:{caller.lineno}, {caller.function}()"


RangeTuple = tuple[int | None, int | None]
MinRangeTuple = tuple[int, int | None]


def as_range_tuple(val: int | RangeTuple, exact: bool) -> RangeTuple:
    if isinstance(val, int):
        val = (val, val) if exact else (val, None)
    return val


def range_min(val: int | RangeTuple) -> int:
    if isinstance(val, int):
        return val
    if isinstance(val[0], int):
        return val[0]
    raise TypeError(
        "ERROR. range_min() requires first entry to be an int. "
        f"Got {type(val[0])} instead."
    )


def as_min_range_tuple(
    val: int | RangeTuple | MinRangeTuple, exact: bool
) -> MinRangeTuple:
    range_tuple = as_range_tuple(val, exact)
    if not isinstance(range_tuple[0], int):
        raise TypeError(
            "ERROR. as_min_range_tuple() requires first entry to be an int. "
            f"Got {type(range_tuple[0])} instead."
        )
    return range_tuple[0], range_tuple[1]


def assert_count(
    exp: Optional[int | RangeTuple],
    got: int,
    tag: str = "",
    err_str: str = "",
) -> None:
    err_str = f"{tag}  exp: {exp}  got: {got}  {caller_str()}\n{err_str}"
    if exp is not None:
        if isinstance(exp, int):
            assert got == exp, err_str
        else:
            if isinstance(exp[0], int):
                assert got >= exp[0], err_str
            if isinstance(exp[1], int):
                assert got <= exp[1], err_str


class InstrumentedProactor(Proactor):
    _subacks_paused: dict[str, bool]
    _subacks_available: dict[str, list[Message[Any]]]
    _mqtt_messages_dropped: dict[str, bool]
    DELIMIT_CHAR = "#"
    DELIMIT_STR = DELIMIT_CHAR * 150

    def __init__(self, services: AppInterface, config: ProactorConfig) -> None:
        super().__init__(services, config)
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
    def recorder_links(self) -> RecorderLinks:
        return typing.cast(RecorderLinks, self._links)

    @property
    def stats(self) -> RecorderStats:
        return typing.cast(RecorderStats, self._stats)

    def link_stats(self, link_name: str) -> RecorderLinkStats:
        return self.stats.link(link_name)

    @property
    def downstream_stats(self) -> RecorderLinkStats:
        return self.link_stats(self.downstream_client)

    @property
    def upstream_stats(self) -> RecorderLinkStats:
        return self.link_stats(self.upstream_client)

    @property
    def needs_ack(self) -> list[_PausedAck]:
        return self.recorder_links.needs_ack

    def force_mqtt_disconnect(self, client_name: str) -> None:
        mqtt_client = self.mqtt_client_wrapper(client_name).mqtt_client
        # noinspection PyProtectedMember
        mqtt_client._loop_rc_handle(MQTT_ERR_CONN_LOST)  # noqa

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

    def restore_ack_timeout_seconds(self) -> None:
        self.links.ack_manager._default_delay_seconds = (  # noqa: SLF001
            self.settings.proactor.ack_timeout_seconds
        )

    def drop_mqtt(self, client_name: str, drop: bool) -> None:
        self._mqtt_messages_dropped[client_name] = drop

    def ack_tracking_str(self) -> str:
        s = "Tracked acks/ackables\n"
        for link_name, tracked_items in self.links.ack_tracker.ackables.items():
            s += f"  {link_name}:{len(tracked_items):3d}\n"
            for message_id, tracked in tracked_items.items():
                rp = int(message_id in self.links._reuploads._reupload_pending)  # noqa
                ru = int(message_id in self.links._reuploads._reuploaded_unacked)  # noqa
                is_ack_str = "*" if tracked.is_ack else " "
                s += (
                    f"    {message_id[:8]}  "
                    f"p:{int(message_id in self.event_persister)}  "
                    f"f:{int(message_id in self.links.in_flight_events)}  "
                    f"rp:{rp}  ru:{ru}  "
                    f"sent: {tracked.send_count:2d}  "
                    f"acked: {tracked.ack_count}  "
                    f"{is_ack_str}  "
                    f"{tracked.message_type:45s}  "
                )
                for ack_path in tracked.ack_paths:
                    s += f"  0x{ack_path:08X}"
                s += "\n"
        return s

    def summary_str(self, *, ack_tracking: bool = False) -> str:
        s = ""
        if self._loop is not None:
            s += str_tasks(self._loop, self.paths_name, self._tasks)
            s += "\n"
        s += str(self.stats)
        s += "\nEvents:\n"
        s += f"  pending: {self.links.num_pending}\n"
        s += f"  in-flight: {self.links.num_in_flight}\n"
        s += f"  persisted: {self.event_persister.num_persists}\n"
        s += f"  retrieved: {self.event_persister.num_retrieves}\n"
        s += f"  cleared: {self.event_persister.num_clears}\n"
        s += "Link states:\n"
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
        s += f"Paused acks: {len(self.needs_ack)}\n"
        s += "Paused Subacks:"
        for link_name in self.stats.links:
            s += (
                f"  {link_name:10s}  "
                f"subacks paused: {self._subacks_paused[link_name]}  "
                f"subacks available: {len(self._subacks_available[link_name])}\n"
            )
        if ack_tracking:
            s += self.ack_tracking_str()
        return s

    def summarize(self) -> None:
        self._logger.info(self.summary_str())

    def delimit(self, text: str = "", log_level: int = logging.INFO) -> None:
        if self._logger.isEnabledFor(log_level):
            self._logger.log(
                log_level,
                f"\n\n{self.DELIMIT_STR}\n"
                f"{self.DELIMIT_CHAR}  {text}\n"
                f"{self.DELIMIT_STR}\n",
            )

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

    def events_at_rest(
        self,
        num_pending: int = 0,
        *,
        exact_pending: bool = True,
        num_persists: Optional[int] = None,
        exact_persists: bool = True,
    ) -> bool:
        if exact_pending:
            pending_check = self.links.num_pending == num_pending
        else:
            pending_check = self.links.num_pending >= num_pending
        if num_persists is None:
            persist_check = True
        elif exact_persists:
            persist_check = self.event_persister.num_persists == num_persists
        else:
            persist_check = self.event_persister.num_persists >= num_persists
        return pending_check and self.links.num_in_flight == 0 and persist_check

    def assert_event_counts(
        self,
        *,
        num_pending: Optional[int | tuple[int | None, int | None]] = 0,
        num_in_flight: Optional[int | tuple[int | None, int | None]] = 0,
        all_pending: bool = False,
        num_persists: Optional[int | tuple[int | None, int | None]] = None,
        num_retrieves: Optional[int | tuple[int | None, int | None]] = None,
        num_clears: Optional[int | tuple[int | None, int | None]] = None,
        all_clear: bool = False,
        tag: str = "",
        err_str: str = "",
    ) -> None:
        assert_count(num_pending, self.links.num_pending, tag + " num_pending", err_str)
        assert_count(
            num_in_flight, self.links.num_in_flight, tag + " num_in_flight", err_str
        )
        p = self.event_persister
        if all_pending:
            assert_count(num_pending, p.num_persists, tag + " num_persists", err_str)
            assert_count(0, p.num_retrieves, tag + " num_retrieves", err_str)
            assert_count(0, p.num_clears, tag + " num_clears", err_str)
        if all_clear:
            if num_pending != 0:
                raise ValueError(
                    f"ERROR. all_clear is True but num_pending ({num_pending}) != 0"
                )
            if num_retrieves is None:
                num_retrieves = num_persists
            if num_clears is None:
                num_clears = num_persists
        assert_count(num_persists, p.num_persists, tag + " num_persists", err_str)
        assert_count(num_retrieves, p.num_retrieves, tag + " num_retrieves", err_str)
        assert_count(num_clears, p.num_clears, tag + " num_clears", err_str)
