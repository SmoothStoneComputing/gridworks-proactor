import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Optional, Sequence

from gwproto import Message

from gwproactor.links import Transition
from gwproactor.message import MQTTReceiptPayload

PreChildStartCallback = Callable[[], None]
StartTasksCallback = Callable[[], Sequence[asyncio.Task[Any]]]
StartProcessingMessagesCallback = Callable[[], None]
ProcessMessageCallback = Callable[[Message[Any]], None]
ProcessMQTTMessageCallback = Callable[[Message[MQTTReceiptPayload], Message[Any]], None]
RecvDeactivatedCallback = Callable[[Transition], None]
RecvActivatedCallback = Callable[[Transition], None]


@dataclass
class ProactorCallbackFunctions:
    pre_child_start: Optional[PreChildStartCallback] = None
    start_tasks: Optional[StartTasksCallback] = None
    start_processing_messages: Optional[StartProcessingMessagesCallback] = None
    process_internal_message: Optional[ProcessMessageCallback] = None
    process_mqtt_message: Optional[ProcessMQTTMessageCallback] = None
    recv_activated: Optional[RecvActivatedCallback] = None
    recv_deactivated: Optional[RecvDeactivatedCallback] = None


class ProactorCallbackInterface:
    def pre_child_start(self) -> None: ...
    def start_tasks(self) -> Sequence[asyncio.Task[Any]]:
        return []

    def start_processing_messages(self) -> None: ...
    def process_internal_message(self, message: Message[Any]) -> None: ...
    def process_mqtt_message(
        self, mqtt_client_message: Message[MQTTReceiptPayload], decoded: Message[Any]
    ) -> None: ...
    def recv_activated(self, transition: Transition) -> None: ...
    def recv_deactivated(self, transition: Transition) -> None: ...


class CallbackManager(ProactorCallbackInterface):
    callback_functions: ProactorCallbackFunctions
    callback_objects: dict[int, ProactorCallbackInterface]
    _next_callback_id: int = -1

    def __init__(
        self,
        callback_functions: Optional[ProactorCallbackFunctions] = None,
        callback_objects: Optional[list[ProactorCallbackInterface]] = None,
    ) -> None:
        self.callback_functions = callback_functions or ProactorCallbackFunctions()
        self.callback_objects = {}
        if callback_objects is not None:
            for callback_object in callback_objects:
                self.add_callbacks(callback_object)

    def add_callbacks(self, callbacks: ProactorCallbackInterface) -> int:
        self._next_callback_id += 1
        self.callback_objects[self._next_callback_id] = callbacks
        return self._next_callback_id

    def remove_callbacks(self, callback_id: int) -> None:
        self.callback_objects.pop(callback_id, None)

    def pre_child_start(self) -> None:
        self._call_callbacks("pre_child_start")

    def start_tasks(self) -> Sequence[asyncio.Task[Any]]:
        tasks: list[asyncio.Task[Any]] = []
        if (
            self.callback_functions is not None
            and self.callback_functions.start_tasks is not None
        ):
            tasks.extend(self.callback_functions.start_tasks())
        for callback_object in self.callback_objects.values():
            tasks.extend(callback_object.start_tasks())
        return tasks

    def start_processing_messages(self) -> None:
        self._call_callbacks("start_processing_messages")

    def process_internal_message(self, message: Message[Any]) -> None:
        self._call_callbacks("process_internal_message", message)

    def process_mqtt_message(
        self, mqtt_client_message: Message[MQTTReceiptPayload], decoded: Message[Any]
    ) -> None:
        self._call_callbacks("process_mqtt_message", mqtt_client_message, decoded)

    def recv_activated(self, transition: Transition) -> None:
        self._call_callbacks("recv_activated", transition)

    def recv_deactivated(self, transition: Transition) -> None:
        self._call_callbacks("recv_deactivated", transition)

    def _call_callbacks(self, callback_name: str, *args: Any, **kwargs: Any) -> None:
        if not hasattr(self.callback_functions, callback_name):
            raise RuntimeError(
                f"callback <{callback_name}> is not an attribute of ProactorCallbackFunctions"
            )
        if cb_function := getattr(self.callback_functions, callback_name, None):
            cb_function(*args, **kwargs)
        for callback_object in self.callback_objects.values():
            if (cb_method := getattr(callback_object, callback_name, None)) is None:
                raise RuntimeError(
                    f"callback <{callback_name}> is not an attribute of ProactorCallbackInterface"
                )
            cb_method(*args, **kwargs)
