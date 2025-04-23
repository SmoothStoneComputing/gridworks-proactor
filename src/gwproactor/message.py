"""Message structures for use between proactor and its sub-objects."""

import uuid
from enum import Enum
from typing import Any, Generic, Literal, Optional, Sequence, TypeVar

from gwproto import as_enum
from gwproto.message import Header, Message, ensure_arg
from gwproto.messages import EventBase
from paho.mqtt.client import MQTT_ERR_UNKNOWN, MQTTMessage
from pydantic import BaseModel, ConfigDict, field_validator

from gwproactor.config import LoggerLevels
from gwproactor.problems import Problems


class MessageType(Enum):
    invalid = "invalid"
    mqtt_subscribe = "mqtt_subscribe"
    mqtt_message = "mqtt_message"
    mqtt_connected = "mqtt_connected"
    mqtt_disconnected = "mqtt_disconnected"
    mqtt_connect_failed = "mqtt_connect_failed"
    mqtt_suback = "mqtt_suback"
    mqtt_problems = "mqtt_problems"


class KnownNames(Enum):
    proactor = "proactor"
    mqtt_clients = "mqtt_clients"
    watchdog_manager = "watchdog_manager"
    io_loop_manager = "io_loop_manager"


class MQTTClientsPayload(BaseModel):
    client_name: str
    userdata: Optional[Any] = None


MQTTClientsPayloadT = TypeVar("MQTTClientsPayloadT", bound=MQTTClientsPayload)


class MQTTClientMessage(Message[MQTTClientsPayloadT], Generic[MQTTClientsPayloadT]):
    def __init__(
        self,
        message_type: MessageType,
        payload: MQTTClientsPayloadT,
    ) -> None:
        super().__init__(
            Header=Header(
                Src=KnownNames.mqtt_clients.value,
                Dst=KnownNames.proactor.value,
                MessageType=message_type.value,
            ),
            Payload=payload,
        )


class MQTTMessageModel(BaseModel):
    timestamp: float = 0
    state: int = 0
    dup: bool = False
    mid: int = 0
    topic: str = ""
    payload: bytes = b""
    qos: int = 0
    retain: bool = False

    @classmethod
    def from_mqtt_message(cls, message: MQTTMessage) -> "MQTTMessageModel":
        model = MQTTMessageModel()
        for field_name in model.__pydantic_fields__:
            setattr(model, field_name, getattr(message, field_name))
        return model


class MQTTReceiptPayload(MQTTClientsPayload):
    message: MQTTMessageModel


class MQTTReceiptMessage(MQTTClientMessage[MQTTReceiptPayload]):
    def __init__(
        self,
        client_name: str,
        userdata: Optional[Any],
        message: MQTTMessage,
    ) -> None:
        super().__init__(
            message_type=MessageType.mqtt_message,
            payload=MQTTReceiptPayload(
                client_name=client_name,
                userdata=userdata,
                message=MQTTMessageModel.from_mqtt_message(message),
            ),
        )


class MQTTSubackPayload(MQTTClientsPayload):
    mid: int
    granted_qos: Sequence[int]


class MQTTSubackMessage(MQTTClientMessage[MQTTSubackPayload]):
    def __init__(
        self,
        client_name: str,
        userdata: Optional[Any],
        mid: int,
        granted_qos: Sequence[int],
    ) -> None:
        super().__init__(
            message_type=MessageType.mqtt_suback,
            payload=MQTTSubackPayload(
                client_name=client_name,
                userdata=userdata,
                mid=mid,
                granted_qos=granted_qos,
            ),
        )


class MQTTCommEventPayload(MQTTClientsPayload):
    rc: int


class MQTTConnectPayload(MQTTCommEventPayload):
    flags: dict[str, Any]


class MQTTConnectMessage(MQTTClientMessage[MQTTConnectPayload]):
    def __init__(
        self,
        client_name: str,
        userdata: Optional[Any],
        flags: dict[str, Any],
        rc: int,
    ) -> None:
        super().__init__(
            message_type=MessageType.mqtt_connected,
            payload=MQTTConnectPayload(
                client_name=client_name,
                userdata=userdata,
                flags=flags,
                rc=rc,
            ),
        )


class MQTTConnectFailPayload(MQTTClientsPayload):
    pass


class MQTTConnectFailMessage(MQTTClientMessage[MQTTConnectFailPayload]):
    def __init__(self, client_name: str, userdata: Optional[Any]) -> None:
        super().__init__(
            message_type=MessageType.mqtt_connect_failed,
            payload=MQTTConnectFailPayload(
                client_name=client_name,
                userdata=userdata,
            ),
        )


class MQTTDisconnectPayload(MQTTCommEventPayload):
    pass


class MQTTDisconnectMessage(MQTTClientMessage[MQTTDisconnectPayload]):
    def __init__(self, client_name: str, userdata: Optional[Any], rc: int) -> None:
        super().__init__(
            message_type=MessageType.mqtt_disconnected,
            payload=MQTTDisconnectPayload(
                client_name=client_name,
                userdata=userdata,
                rc=rc,
            ),
        )


class MQTTProblemsPayload(MQTTCommEventPayload):
    problems: Problems
    model_config = ConfigDict(arbitrary_types_allowed=True)


class MQTTProblemsMessage(MQTTClientMessage[MQTTCommEventPayload]):
    def __init__(
        self, client_name: str, problems: Problems, rc: int = MQTT_ERR_UNKNOWN
    ) -> None:
        super().__init__(
            message_type=MessageType.mqtt_problems,
            payload=MQTTProblemsPayload(
                client_name=client_name, rc=rc, problems=problems
            ),
        )


class PatWatchdog(BaseModel): ...


class PatInternalWatchdog(PatWatchdog):
    TypeName: Literal["gridworks.watchdog.pat.internal"] = (
        "gridworks.watchdog.pat.internal"
    )


class PatExternalWatchdog(PatWatchdog):
    TypeName: Literal["gridworks.watchdog.pat.external"] = (
        "gridworks.watchdog.pat.external"
    )


class PatInternalWatchdogMessage(Message[PatInternalWatchdog]):
    def __init__(self, src: str) -> None:
        super().__init__(
            Src=src,
            Dst=KnownNames.watchdog_manager.value,
            Payload=PatInternalWatchdog(),
        )


class PatExternalWatchdogMessage(Message[PatExternalWatchdog]):
    def __init__(self) -> None:
        super().__init__(
            Src=KnownNames.watchdog_manager.value,
            Dst=KnownNames.watchdog_manager.value,
            Payload=PatExternalWatchdog(),
        )


class Command(BaseModel): ...


CommandT = TypeVar("CommandT", bound=Command)


class CommandMessage(Message[CommandT], Generic[CommandT]):
    def __init__(self, *, AckRequired: bool = True, **kwargs: Any) -> None:
        ensure_arg("MessageId", str(uuid.uuid4()), kwargs)
        super().__init__(AckRequired=AckRequired, **kwargs)


class Shutdown(Command):
    Reason: str = ""
    TypeName: Literal["gridworks.shutdown"] = "gridworks.shutdown"


class ShutdownMessage(CommandMessage[Shutdown]):
    def __init__(self, *, Reason: str = "", **data: Any) -> None:
        ensure_arg("Payload", Shutdown(Reason=Reason), data)
        super().__init__(**data)


class InternalShutdownMessage(ShutdownMessage):
    def __init__(self, *, AckRequired: bool = False, **data: Any) -> None:
        super().__init__(AckRequired=AckRequired, **data)


class DBGCommands(Enum):
    show_subscriptions = "show_subscriptions"


class DBGPayload(BaseModel):
    Levels: LoggerLevels = LoggerLevels(
        message_summary=-1,
        lifecycle=-1,
        comm_event=-1,
    )
    Command: Optional[DBGCommands] = None
    TypeName: Literal["gridworks.proactor.dbg"] = "gridworks.proactor.dbg"

    @field_validator("Command", mode="before")
    @classmethod
    def command_value(cls, v: Any) -> Optional[DBGCommands]:
        return as_enum(v, DBGCommands)


class DBGEvent(EventBase):
    Command: DBGPayload
    Path: str = ""
    Count: int = 0
    Msg: str = ""
    TypeName: Literal["gridworks.event.proactor.dbg"] = "gridworks.event.proactor.dbg"
