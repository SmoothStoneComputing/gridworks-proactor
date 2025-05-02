"""Proactor implementation"""

import asyncio
import sys
import threading
import traceback
from functools import cached_property
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Sequence,
    Type,
    TypeVar,
)

import gwproto
from aiohttp.typedefs import Handler as HTTPHandler
from gwproto import Message
from gwproto.data_classes.components.web_server_component import WebServerComponent
from gwproto.data_classes.hardware_layout import HardwareLayout
from gwproto.data_classes.sh_node import ShNode
from gwproto.messages import Ack, EventBase, EventT, Ping, ProblemEvent, ShutdownEvent
from gwproto.named_types.web_server_gt import WebServerGt
from paho.mqtt.client import MQTTMessageInfo
from result import Err, Ok, Result

from gwproactor.callbacks import (
    CallbackManager,
    ProactorCallbackFunctions,
    ProactorCallbackInterface,
)
from gwproactor.config.app_settings import AppSettings
from gwproactor.config.proactor_config import ProactorConfig, ProactorName
from gwproactor.external_watchdog import (
    ExternalWatchdogCommandBuilder,
    SystemDWatchdogCommandBuilder,
)
from gwproactor.io_loop import IOLoop
from gwproactor.links import (
    AckWaitInfo,
    AsyncioTimerManager,
    LinkManager,
)
from gwproactor.links.mqtt import QOS
from gwproactor.logger import ProactorLogger
from gwproactor.message import (
    DBGCommands,
    DBGEvent,
    DBGPayload,
    MQTTConnectFailPayload,
    MQTTConnectPayload,
    MQTTDisconnectPayload,
    MQTTProblemsPayload,
    MQTTReceiptPayload,
    MQTTSubackPayload,
    PatWatchdog,
    Shutdown,
)
from gwproactor.persister import PersisterInterface
from gwproactor.proactor_interface import (
    AppInterface,
    CommunicatorInterface,
    IOLoopInterface,
    MonitoredName,
    Runnable,
)
from gwproactor.problems import Problems
from gwproactor.stats import ProactorStats
from gwproactor.str_tasks import str_tasks
from gwproactor.watchdog import WatchdogManager
from gwproactor.web_manager import _WebManager

T = TypeVar("T")


class Proactor(AppInterface, Runnable):
    AWAIT_PROCESSING_FUTURE_ATTRIBUTE: str = "_await_processing_future"

    _name: ProactorName
    _settings: AppSettings
    _node: ShNode
    _callbacks: CallbackManager
    _layout: HardwareLayout
    _logger: ProactorLogger
    _stats: ProactorStats
    _event_persister: PersisterInterface
    _reindex_problems: Optional[Problems] = None
    _loop: Optional[asyncio.AbstractEventLoop] = None
    _receive_queue: Optional[asyncio.Queue[Any]] = None
    _processing_futures: set[asyncio.Future[Any]]
    _processing_futures_lock: asyncio.Lock
    _links: LinkManager
    _communicators: Dict[str, CommunicatorInterface]
    _stop_requested: bool
    _stopped: bool
    _tasks: List[asyncio.Task[Any]]
    _io_loop_manager: IOLoop
    _web_manager: _WebManager
    _watchdog: WatchdogManager

    def __init__(self, config: ProactorConfig) -> None:
        self._name = config.name
        self._settings = config.settings
        self._callbacks = CallbackManager(callback_functions=config.callback_functions)
        self._layout = config.layout
        self._node = self._layout.node(self.name)
        self._logger = config.logger
        self._stats = self.make_stats()
        self._event_persister = config.event_persister
        self._logger.lifecycle(f"Proactor <{self._name}> reindexing events")
        reindex_result = self._event_persister.reindex()
        self._logger.lifecycle(
            f"Proactor <{self._name}> reindexing complete.\n"
            f"  {self._event_persister.num_pending} events present for upload, "
            f"using approximately {int(self._event_persister.curr_bytes / 1024)} KB / "
            f"{round(self._event_persister.curr_bytes / 1024 / 1024, 1)} MB "
            f"storage space."
        )
        if reindex_result.is_err():
            self._reindex_problems = reindex_result.err()
            self._logger.error("ERROR in event persister reindex():")
            self._logger.error(reindex_result.err())
        self._links = LinkManager(
            publication_name=self.publication_name,
            subscription_name=self.subscription_name,
            settings=self._settings.proactor,
            logger=self._logger,
            stats=self._stats,
            event_persister=self._event_persister,
            timer_manager=AsyncioTimerManager(),
            ack_timeout_callback=self._process_ack_timeout,
        )
        self._processing_futures = set()
        self._processing_futures_lock = asyncio.Lock()
        self._communicators = {}
        self._tasks = []
        self._stop_requested = False
        self._stopped = False
        self._watchdog = WatchdogManager(9, self)
        self.add_communicator(self._watchdog)
        self._io_loop_manager = IOLoop(self)
        self.add_communicator(self._io_loop_manager)
        self._web_manager = _WebManager(self)
        self.add_communicator(self._web_manager)
        for server_config in self._layout.get_components_by_type(WebServerComponent):
            self._web_manager.add_web_server_config(
                name=server_config.web_server_gt.Name,
                host=server_config.web_server_gt.Host,
                port=server_config.web_server_gt.Port,
                enabled=server_config.web_server_gt.Enabled,
                server_kwargs=server_config.web_server_gt.Kwargs,
            )

    @classmethod
    def make_stats(cls) -> ProactorStats:
        return ProactorStats()

    def send(self, message: Message[Any]) -> None:
        if self._receive_queue is None:
            raise RuntimeError("ERROR. send() called before Proactor started.")
        if not isinstance(message.Payload, PatWatchdog):
            self._logger.message_summary(
                direction="OUT internal",
                src=message.Header.Src,
                dst=message.Header.Dst,
                topic=f"{message.Header.Src}/to/{message.Header.Dst}/{message.Header.MessageType}",
                payload_object=message.Payload,
                message_id=message.Header.MessageId,
            )
        self._receive_queue.put_nowait(message)

    async def _add_processing_future(self, future: asyncio.Future[Any]) -> None:
        async with self._processing_futures_lock:
            self._processing_futures.add(future)

    async def _clear_processing_future(self, future: asyncio.Future[Any]) -> None:
        async with self._processing_futures_lock:
            if future in self._processing_futures:
                self._processing_futures.remove(future)

    async def _clear_processing_futures(self) -> list[asyncio.Future[Any]]:
        futures = []
        async with self._processing_futures_lock:
            futures = list(self._processing_futures)
            self._processing_futures.clear()
        return futures

    async def _get_processing_future(self) -> asyncio.Future[Any]:
        if self._loop is None:
            raise RuntimeError(
                "ERROR. _get_processing_future() called before Proactor started."
            )
        future = self._loop.create_future()
        await self._add_processing_future(future)
        return future

    async def _attach_future_to_message(
        self, message: Message[Any]
    ) -> asyncio.Future[Any]:
        if self._loop is None:
            raise RuntimeError(
                "ERROR. await_processing() called before Proactor started."
            )
        future = getattr(message, self.AWAIT_PROCESSING_FUTURE_ATTRIBUTE, None)
        if future is None:
            future = await self._get_processing_future()
            setattr(message, self.AWAIT_PROCESSING_FUTURE_ATTRIBUTE, future)
        else:
            if not isinstance(future, asyncio.Future):
                raise RuntimeError(
                    "ERROR. await_processing() received message "
                    f"(type: {message.message_type()}) with "
                    f"{self.AWAIT_PROCESSING_FUTURE_ATTRIBUTE} attribute "
                    f"(type: {type(future)}) which is not an instance of "
                    f"asyncio.Future."
                )
            await self._add_processing_future(future)
        return future

    @classmethod
    async def _await_processing(
        cls, future: asyncio.Future[Result[Any, BaseException]]
    ) -> Result[Any, BaseException]:
        try:
            await future
        except asyncio.CancelledError as canceled:
            return Err(canceled)
        e = future.exception()
        if e is not None:
            return Err(e)
        return future.result()

    async def await_processing(
        self, message: Message[Any]
    ) -> Result[Any, BaseException]:
        if self._stop_requested:
            return Ok(value=False)
        future = await self._attach_future_to_message(message)
        self.send(message)
        return await self._await_processing(future)

    def send_threadsafe(self, message: Message[Any]) -> None:
        if self._loop is None or self._receive_queue is None:
            raise RuntimeError(
                "ERROR. send_threadsafe() called before Proactor started."
            )
        self._loop.call_soon_threadsafe(self._receive_queue.put_nowait, message)

    def wait_for_processing_threadsafe(
        self, message: Message[Any]
    ) -> Result[Any, BaseException]:
        if self._loop is None:
            raise RuntimeError(
                "ERROR. wait_for_send_threadsafe() called before Proactor started."
            )
        return asyncio.run_coroutine_threadsafe(
            self.await_processing(message), self._loop
        ).result()

    def get_communicator_names(self) -> set[str]:
        return set(self._communicators.keys())

    def get_communicator(self, name: str) -> Optional[CommunicatorInterface]:
        return self._communicators.get(name, None)

    def get_communicator_as_type(self, name: str, type_: Type[T]) -> Optional[T]:
        communicator = self.get_communicator(name)
        if communicator is not None and not isinstance(communicator, type_):
            raise ValueError(
                f"ERROR. Communicator <{name}> has type {type(communicator)} not {type_}"
            )
        return communicator

    @cached_property
    def name(self) -> str:
        return self._name.name

    @cached_property
    def long_name(self) -> str:
        return self._name.long_name

    @cached_property
    def short_name(self) -> str:
        return self._name.short_name

    @cached_property
    def paths_name(self) -> str:
        return str(self.settings.paths.name)

    @cached_property
    def publication_name(self) -> str:
        return self._name.publication_name

    @cached_property
    def subscription_name(self) -> str:
        return self._name.subscription_name

    @cached_property
    def name_object(self) -> ProactorName:
        return self._name

    @property
    def callback_functions(self) -> ProactorCallbackFunctions:
        return self._callbacks.callback_functions

    @property
    def monitored_names(self) -> Sequence[MonitoredName]:
        return []

    @property
    def settings(self) -> AppSettings:
        return self._settings

    @property
    def logger(self) -> ProactorLogger:
        return self._logger

    @property
    def stats(self) -> ProactorStats:
        return self._stats

    @property
    def links(self) -> LinkManager:
        return self._links

    def add_callbacks(self, callbacks: ProactorCallbackInterface) -> int:
        return self._callbacks.add_callbacks(callbacks)

    def remove_callbacks(self, callbacks_id: int) -> None:
        return self._callbacks.remove_callbacks(callbacks_id)

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
        return self._links.publish_message(
            link_name=link_name,
            message=message,
            qos=qos,
            context=context,
            topic=topic,
            use_link_topic=use_link_topic,
        )

    def publish_upstream(
        self, payload: Any, qos: QOS = QOS.AtMostOnce, **message_args: Any
    ) -> MQTTMessageInfo:
        return self._links.publish_upstream(payload=payload, qos=qos, **message_args)

    @property
    def event_persister(self) -> PersisterInterface:
        return self._event_persister

    @property
    def io_loop_manager(self) -> IOLoopInterface:
        return self._io_loop_manager

    @property
    def hardware_layout(self) -> HardwareLayout:
        return self._layout

    @property
    def upstream_client(self) -> str:
        return self._links.upstream_client

    @property
    def downstream_client(self) -> str:
        return self._links.downstream_client

    @property
    def async_receive_queue(self) -> Optional[asyncio.Queue[Any]]:
        return self._receive_queue

    @property
    def event_loop(self) -> Optional[asyncio.AbstractEventLoop]:
        return self._loop

    def add_web_server_config(
        self, name: str, host: str, port: int, **kwargs: Any
    ) -> None:
        self._web_manager.add_web_server_config(
            name=name, host=host, port=port, **kwargs
        )

    def add_web_route(
        self,
        server_name: str,
        method: str,
        path: str,
        handler: HTTPHandler,
        **kwargs: Any,
    ) -> None:
        self._web_manager.add_web_route(
            server_name=server_name, method=method, path=path, handler=handler, **kwargs
        )

    def get_web_server_route_strings(self) -> dict[str, list[str]]:
        return self._web_manager.get_route_strings()

    def get_web_server_configs(self) -> dict[str, WebServerGt]:
        return self._web_manager.get_configs()

    def get_external_watchdog_builder_class(
        self,
    ) -> type[ExternalWatchdogCommandBuilder]:
        return SystemDWatchdogCommandBuilder

    def _send(self, message: Message[Any]) -> None:
        self.send(message)

    def generate_event(self, event: EventT) -> Result[bool, Exception]:
        return self._links.generate_event(event)

    def _process_ack_timeout(self, wait_info: AckWaitInfo) -> None:
        self._logger.message_enter(
            "++Proactor._process_ack_timeout %s", wait_info.message_id
        )
        path_dbg = 0
        match self._links.process_ack_timeout(wait_info):
            case Ok(transition):
                path_dbg |= 0x00000001
                if transition.deactivated():
                    path_dbg |= 0x00000002
                    self._callbacks.recv_deactivated(transition)
            case Err(exception):
                path_dbg |= 0x00000004
                self._report_error(exception, msg="Proactor._process_ack_timeout")
        self._logger.message_exit(
            "--Proactor._process_ack_timeout path:0x%08X", path_dbg
        )

    def _process_ack(self, link_name: str, message_id: str) -> None:
        self._links.process_ack(link_name, message_id)

    def _process_dbg(self, dbg: DBGPayload) -> None:
        self._logger.path("++_process_dbg")
        path_dbg = 0
        count_dbg = 0
        for logger_name in ["message_summary", "lifecycle", "comm_event"]:
            requested_level = getattr(dbg.Levels, logger_name)
            if requested_level > -1:
                path_dbg |= 0x00000001
                count_dbg += 1
                logger = getattr(self._logger, logger_name + "_logger")
                old_level = logger.getEffectiveLevel()
                logger.setLevel(requested_level)
                self._logger.debug(
                    "%s logger level %s -> %s",
                    logger_name,
                    old_level,
                    logger.getEffectiveLevel(),
                )
        match dbg.Command:
            case DBGCommands.show_subscriptions:
                path_dbg |= 0x00000002
                self._links.log_subscriptions("message")
            case _:
                path_dbg |= 0x00000004
        self.generate_event(
            DBGEvent(Command=dbg, Path=f"0x{path_dbg:08X}", Count=count_dbg, Msg="")
        )
        self._logger.path("--_process_dbg  path:0x%08X  count:%d", path_dbg, count_dbg)

    def add_communicator(self, communicator: CommunicatorInterface) -> None:
        if communicator.name in self._communicators:
            raise ValueError(
                f"ERROR. Communicator with name [{communicator.name}] already present"
            )
        self._communicators[communicator.name] = communicator
        for monitored in communicator.monitored_names:
            self._watchdog.add_monitored_name(monitored)

    async def process_messages(self) -> None:  # noqa: C901
        if self._receive_queue is None:
            raise RuntimeError(
                "ERROR. process_messages() called before Proactor started."
            )
        try:
            self._callbacks.start_processing_messages()
            while not self._stop_requested:
                message = await self._receive_queue.get()
                if not self._stop_requested:
                    await self.async_process_message(message)
                self._receive_queue.task_done()
        except Exception as e:
            if not isinstance(e, asyncio.exceptions.CancelledError):
                self._logger.exception("ERROR in process_message")
                self._logger.error("Stopping proactor")  # noqa: TRY400
                try:
                    self.generate_event(
                        ShutdownEvent(
                            Reason=(
                                f"ERROR in process_message {e}\n"
                                f"{traceback.format_exception(e)}"
                            )
                        )
                    )
                except:  # noqa: E722
                    self._logger.exception("ERROR generating exception event")
        try:
            message_futures = await self._clear_processing_futures()
            for future in message_futures:
                if not future.done():
                    future.cancel()
        except:  # noqa: E722
            ...
        try:
            self.stop()
        except:  # noqa: E722
            self._logger.exception("ERROR stopping proactor")

    def add_task(self, task: asyncio.Task[Any]) -> None:
        self._tasks.append(task)

    def start_tasks(self) -> None:
        self._tasks = [
            asyncio.create_task(self.process_messages(), name="process_messages"),
            *self._links.start_ping_tasks(),
        ]
        self._tasks.extend(self._callbacks.start_tasks())

    @classmethod
    def _second_caller(cls) -> str:
        try:
            # noinspection PyProtectedMember,PyUnresolvedReferences
            return sys._getframe(2).f_back.f_code.co_name  # type: ignore[union-attr] # noqa: SLF001
        except Exception as e:  # noqa: BLE001
            return f"[ERROR extracting caller of _report_errors: {e}"

    def _report_error(self, error: Exception, msg: str = "") -> Result[bool, Exception]:
        try:
            if not msg:
                msg = self._second_caller()
            self._report_errors([error], msg)
        except Exception as e2:  # noqa: BLE001
            return Err(e2)
        return Ok()

    def _report_errors(
        self, errors: Sequence[Exception], msg: str = ""
    ) -> Result[bool, Exception]:
        try:
            if not msg:
                msg = self._second_caller()
            self.generate_event(Problems(errors=errors).problem_event(msg))
        except Exception as e2:  # noqa: BLE001
            return Err(e2)
        return Ok()

    def process_message(self, message: Message[Any]) -> Result[bool, Exception]:
        raise NotImplementedError(
            "Proactor does not implement process_message, "
            "but instead async_process_message."
        )

    async def async_process_message(self, message: Message[Any]) -> None:  # noqa: C901, PLR0912
        if not isinstance(message.Payload, PatWatchdog):
            self._logger.message_enter(
                "++Proactor.process_message %s/%s",
                message.Header.Src,
                message.Header.MessageType,
            )
        path_dbg = 0
        if not isinstance(message.Payload, (MQTTReceiptPayload, PatWatchdog)):
            path_dbg |= 0x00000001
            self._logger.message_summary(
                direction="IN  internal",
                src=message.src(),
                dst=message.dst(),
                topic=f"{message.src()}/to/{message.dst()}/{message.Header.MessageType}",
                payload_object=message.Payload,
                message_id=message.Header.MessageId,
            )
        self._stats.add_message(message)
        match message.Payload:
            case MQTTReceiptPayload():
                path_dbg |= 0x00000002
                self._process_mqtt_message(message)
            case MQTTConnectPayload():
                path_dbg |= 0x00000004
                self._process_mqtt_connected(message)
            case MQTTDisconnectPayload():
                path_dbg |= 0x00000008
                self._process_mqtt_disconnected(message)
            case MQTTConnectFailPayload():
                path_dbg |= 0x00000010
                self._process_mqtt_connect_fail(message)
            case MQTTSubackPayload():
                path_dbg |= 0x00000020
                self._process_mqtt_suback(message)
            case MQTTProblemsPayload():
                path_dbg |= 0x00000040
                self._process_mqtt_problems(message)
            case PatWatchdog():
                path_dbg |= 0x00000080
                self._watchdog.process_message(message)
            case Shutdown():
                path_dbg |= 0x00000100
                self._process_shutdown_message(message)
            case EventBase():
                path_dbg |= 0x00000200
                self.generate_event(message.Payload)
            case _:
                path_dbg |= 0x00000400
                self._callbacks.process_internal_message(message)
        await self._notify_message_future(message)
        if not isinstance(message.Payload, PatWatchdog):
            self._logger.message_exit(
                "--Proactor.process_message  path:0x%08X", path_dbg
            )

    async def _notify_message_future(self, message: Message[Any]) -> None:
        future = getattr(message, self.AWAIT_PROCESSING_FUTURE_ATTRIBUTE, None)
        if future is not None and isinstance(future, asyncio.Future):
            await self._clear_processing_future(future)
            future.set_result(Ok(value=True))

    def _decode_mqtt_message(
        self, mqtt_payload: MQTTReceiptPayload
    ) -> Result[Message[Any], Exception]:
        try:
            result: Result[Message[Any], Exception] = Ok(
                self._links.decode(
                    mqtt_payload.client_name,
                    mqtt_payload.message.topic,
                    mqtt_payload.message.payload,
                )
            )
        except Exception as e:
            self._logger.exception("ERROR decoding [%s]", mqtt_payload)
            clip_len = 70
            self.generate_event(
                ProblemEvent(
                    ProblemType=gwproto.messages.Problems.warning,
                    Summary=f"Decoding error topic [{mqtt_payload.message.topic}]  error [{type(e)}]",
                    Details=(
                        f"Topic: {mqtt_payload.message.topic}\n"
                        f"Message: {mqtt_payload.message.payload[:clip_len]!r}"
                        f"{'...' if len(mqtt_payload.message.payload) > clip_len else ''}\n"
                        f"{traceback.format_exception(e)}\n"
                        f"Exception: {e}"
                    ),
                )
            )
            result = Err(e)
        return result

    def _process_mqtt_message(  # noqa: C901, PLR0912
        self, mqtt_receipt_message: Message[MQTTReceiptPayload]
    ) -> Result[Message[Any], Exception]:
        self._logger.path(
            "++Proactor._process_mqtt_message %s/%s",
            mqtt_receipt_message.Header.Src,
            mqtt_receipt_message.Header.MessageType,
        )
        path_dbg = 0
        self._stats.add_mqtt_message(mqtt_receipt_message)
        match decode_result := self._decode_mqtt_message(mqtt_receipt_message.Payload):
            case Ok(decoded_message):
                path_dbg |= 0x00000001
                decoded_message = decode_result.value
                self._stats.add_decoded_mqtt_message_type(
                    mqtt_receipt_message.Payload.client_name,
                    decoded_message.message_type(),
                )
                if self._logger.message_summary_enabled:
                    if isinstance(decoded_message.Payload, Ack):
                        message_id = decoded_message.Payload.AckMessageID
                    else:
                        message_id = decoded_message.Header.MessageId
                    self._logger.message_summary(
                        direction="IN  mqtt    ",
                        src=decoded_message.src(),
                        dst=decoded_message.dst(),
                        topic=mqtt_receipt_message.Payload.message.topic,
                        payload_object=decoded_message.Payload,
                        message_id=message_id,
                    )
                match self._links.process_mqtt_message(mqtt_receipt_message):
                    case Ok(transition):
                        path_dbg |= 0x00000002
                        if transition.recv_activated():
                            path_dbg |= 0x00000004
                            self._callbacks.recv_activated(transition)
                    case Err(error):
                        path_dbg |= 0x00000008
                        self._report_error(
                            error,
                            "_process_mqtt_message/_link_states.process_mqtt_message",
                        )
                match decoded_message.Payload:
                    case Ack():
                        path_dbg |= 0x00000010
                        self._process_ack(
                            mqtt_receipt_message.Payload.client_name,
                            decoded_message.Payload.AckMessageID,
                        )
                    case Ping():
                        path_dbg |= 0x00000020
                    case DBGPayload():
                        path_dbg |= 0x00000040
                        self._process_dbg(decoded_message.Payload)
                    case _:
                        path_dbg |= 0x00000080
                        self._callbacks.process_mqtt_message(
                            mqtt_receipt_message, decoded_message
                        )
                if decoded_message.Header.AckRequired:
                    path_dbg |= 0x00000200
                    self._links.send_ack(
                        mqtt_receipt_message.Payload.client_name, decoded_message
                    )
        self._logger.path(
            "--Proactor._process_mqtt_message:%s  path:0x%08X",
            int(decode_result.is_ok()),
            path_dbg,
        )
        return decode_result

    def _process_mqtt_connected(self, message: Message[MQTTConnectPayload]) -> None:
        match self._links.process_mqtt_connected(message):
            case Err(error):
                self._report_error(error, "_process_mqtt_connected")

    def _process_mqtt_disconnected(
        self, message: Message[MQTTDisconnectPayload]
    ) -> Result[bool, Exception]:
        result: Result[bool, Exception] = Ok()
        match self._links.process_mqtt_disconnected(message):
            case Ok(transition):
                if transition.recv_deactivated():
                    self._callbacks.recv_deactivated(transition)
                result = Ok(value=True)
            case Err(error):
                result = Err(error)
        return result

    def _process_mqtt_connect_fail(
        self, message: Message[MQTTConnectFailPayload]
    ) -> Result[bool, Exception]:
        result: Result[bool, Exception] = Ok()
        match self._links.process_mqtt_connect_fail(message):
            case Err(error):
                result = Err(error)
        return result

    def _process_mqtt_suback(
        self, message: Message[MQTTSubackPayload]
    ) -> Result[bool, Exception]:
        self._logger.path(
            "++Proactor._process_mqtt_suback client:%s", message.Payload.client_name
        )
        path_dbg = 0
        result: Result[bool, Exception] = Ok()
        match self._links.process_mqtt_suback(message):
            case Ok(transition):
                path_dbg |= 0x00000001
                if transition.recv_activated():
                    path_dbg |= 0x00000002
                    self._callbacks.recv_activated(transition)
                    result = Ok(value=True)
            case Err(error):
                path_dbg |= 0x00000004
                result = Err(error)
        self._logger.path(
            "--Proactor._process_mqtt_suback:%d  path:0x%08X",
            result.is_ok(),
            path_dbg,
        )
        return result

    def _process_mqtt_problems(
        self, message: Message[MQTTProblemsPayload]
    ) -> Result[bool, Exception]:
        self.generate_event(
            ProblemEvent(
                ProblemType=gwproto.messages.Problems.error,
                Summary=f"Error in mqtt event loop for client [{message.Payload.client_name}]",
                Details=(
                    f"{message.Payload.problems}\n"
                    f"{message.Payload.problems.error_traceback_str()}"
                ),
            )
        )
        return Ok()

    def _process_shutdown_message(self, message: Message[Shutdown]) -> None:
        self._stop_requested = True
        self.generate_event(ShutdownEvent(Reason=message.Payload.Reason))
        self._logger.lifecycle(
            f"Shutting down due to ShutdownMessage, [{message.Payload.Reason}]"
        )

    def _start(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._receive_queue = asyncio.Queue()
        self._links.start(self._loop, self._receive_queue)
        if self._reindex_problems is not None:
            self.generate_event(
                self._reindex_problems.problem_event("Startup event reindex() problems")
            )
        self._reindex_problems = None
        self._callbacks.pre_child_start()
        for communicator in self._communicators.values():
            if isinstance(communicator, Runnable):
                communicator.start()
        self.start_tasks()

    async def run_forever(self) -> None:
        self._start()
        await self.join()

    def run_in_thread(self, *, daemon: bool = True) -> threading.Thread:
        async def _async_run_forever() -> None:
            try:
                await self.run_forever()

            finally:
                self.stop()

        def _run_forever() -> None:
            asyncio.run(_async_run_forever())

        thread = threading.Thread(target=_run_forever, daemon=daemon)
        thread.start()
        return thread

    def start(self) -> None:
        raise RuntimeError("ERROR. Proactor must be started by awaiting run_forever()")

    def stop(self) -> None:
        self._stop_requested = True
        for task in self._tasks:
            if not task.done():
                task.cancel()
        self._links.stop()
        for communicator in self._communicators.values():
            if isinstance(communicator, Runnable):
                try:  # noqa: SIM105
                    communicator.stop()
                except:  # noqa: E722, S110
                    pass

    async def join(self) -> None:
        self._logger.lifecycle("++Proactor.join()  proactor: <%s>", self.name)
        if self._stopped:
            self._logger.lifecycle(
                "--Proactor.join()  proactor: <%s>  (already stopped)", self.name
            )
            return
        if self._loop is None:
            raise ValueError("Proactor cannot be joined until it is started")
        self._logger.lifecycle(str_tasks(self._loop, "Proactor.join() - all tasks"))
        running: set[asyncio.Task[Any]] = set(self._tasks)
        for communicator in self._communicators.values():
            communicator_name = communicator.name
            if isinstance(communicator, Runnable):
                running.add(
                    self._loop.create_task(
                        communicator.join(), name=f"{communicator_name}.join"
                    )
                )

        try:
            while running:
                self._logger.lifecycle(
                    str_tasks(self._loop, "WAITING FOR", tasks=running)
                )
                done, running = await asyncio.wait(
                    running, return_when="FIRST_COMPLETED"
                )
                self._logger.lifecycle(str_tasks(self._loop, tag="DONE", tasks=done))
                self._logger.lifecycle(
                    str_tasks(self._loop, tag="PENDING", tasks=running)
                )
                for task in done:
                    if not task.cancelled() and (exception := task.exception()):
                        self._logger.error(
                            "EXCEPTION in task <%(name)s?>  <%(exception)s>",
                            {
                                "name": task.get_name(),
                                "exception": exception,
                            },
                        )
                        self._logger.error(traceback.format_tb(exception.__traceback__))
            self._stopped = True
        except Exception:
            self._logger.exception("ERROR in Proactor.join")
        self._logger.lifecycle("--Proactor.join()  proactor: <%s>", self.name)
