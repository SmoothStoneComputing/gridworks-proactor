"""Message structures for use between proactor and its sub-objects."""

import uuid
from enum import Enum
from typing import Any, Generic, Literal, Optional, Sequence, TypeVar
from gw.named_types import GwBase
from gwproto import as_enum
from gwproto.message import Message, ensure_arg
from gwproto.messages import EventBase
from paho.mqtt.client import ConnectFlags, MQTTMessage
from paho.mqtt.reasoncodes import ReasonCode as PahoReasonCode
from pydantic import BaseModel, ConfigDict, field_validator

from gw.named_types import GwBase
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


class MQTTClientsPayload(GwBase):
    client_name: str
    userdata: Optional[Any] = None
    type_name: Literal["gridworks.mqtt.clients.payload"] = "gridworks.mqtt.clients.payload"


MQTTClientsPayloadT = TypeVar("MQTTClientsPayloadT", bound=MQTTClientsPayload)


class MQTTClientMessage(Message[MQTTClientsPayloadT], Generic[MQTTClientsPayloadT]):
    def __init__(
        self,
        payload: MQTTClientsPayloadT,
    ) -> None:
        super().__init__(
            src=KnownNames.mqtt_clients.value,
            dst=KnownNames.proactor.value,
            payload=payload,
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
    type_name: Literal["gridworks.mqtt.receipt.payload"] = "gridworks.mqtt.receipt.payload"
 

class MQTTReceiptMessage(MQTTClientMessage[MQTTReceiptPayload]):
    def __init__(
        self,
        client_name: str,
        userdata: Optional[Any],
        message: MQTTMessage,
    ) -> None:
        super().__init__(
            payload=MQTTReceiptPayload(
                client_name=client_name,
                userdata=userdata,
                message=MQTTMessageModel.from_mqtt_message(message),
            ),
        )


class SerializedReasonCode(BaseModel):
    packet_type: int
    code: int
    string: str

    @classmethod
    def from_paho_reason_code(
        cls, reason_code: PahoReasonCode
    ) -> "SerializedReasonCode":
        return SerializedReasonCode(
            packet_type=reason_code.packetType,
            code=reason_code.value,
            string=str(reason_code),
        )


class MQTTSubackPayload(MQTTClientsPayload):
    mid: int
    reason_codes: Sequence[SerializedReasonCode]
    type_name: Literal["gridworks.mqtt.suback.payload"] = "gridworks.mqtt.suback.payload"

class MQTTSubackMessage(MQTTClientMessage[MQTTSubackPayload]):
    def __init__(
        self,
        client_name: str,
        userdata: Optional[Any],
        mid: int,
        reason_codes: Sequence[PahoReasonCode],
    ) -> None:
        super().__init__(
            payload=MQTTSubackPayload(
                client_name=client_name,
                userdata=userdata,
                mid=mid,
                reason_codes=[
                    SerializedReasonCode.from_paho_reason_code(reason_code)
                    for reason_code in reason_codes
                ],
            ),
        )


class MQTTCommEventPayload(MQTTClientsPayload):
    rc: Optional[SerializedReasonCode]
    type_name: Literal["gridworks.mqtt.comm.event.payload"] = "gridworks.mqtt.comm.event.payload"

class MQTTConnectPayload(MQTTCommEventPayload):
    flags: ConnectFlags
    type_name: Literal["gridworks.mqtt.connect.payload"] = "gridworks.mqtt.connect.payload"

class MQTTConnectMessage(MQTTClientMessage[MQTTConnectPayload]):
    def __init__(
        self,
        client_name: str,
        userdata: Optional[Any],
        flags: ConnectFlags,
        rc: PahoReasonCode,
    ) -> None:
        super().__init__(
            payload=MQTTConnectPayload(
                client_name=client_name,
                userdata=userdata,
                flags=flags,
                rc=SerializedReasonCode.from_paho_reason_code(rc),
            ),
        )


class MQTTConnectFailPayload(MQTTClientsPayload):
    type_name: Literal["gridworks.mqtt.connect.fail.payload"] = "gridworks.mqtt.connect.fail.payload"


class MQTTConnectFailMessage(MQTTClientMessage[MQTTConnectFailPayload]):
    def __init__(self, client_name: str, userdata: Optional[Any]) -> None:
        super().__init__(
            payload=MQTTConnectFailPayload(
                client_name=client_name,
                userdata=userdata,
            ),
        )


class MQTTDisconnectPayload(MQTTCommEventPayload):
    type_name: Literal["gridworks.mqtt.disconnect.payload"] = "gridworks.mqtt.disconnect.payload"


class MQTTDisconnectMessage(MQTTClientMessage[MQTTDisconnectPayload]):
    def __init__(
        self, client_name: str, userdata: Optional[Any], rc: PahoReasonCode
    ) -> None:
        super().__init__(
            payload=MQTTDisconnectPayload(
                client_name=client_name,
                userdata=userdata,
                rc=SerializedReasonCode.from_paho_reason_code(rc),
            ),
        )


class MQTTProblemsPayload(MQTTCommEventPayload):
    problems: Problems
    model_config = ConfigDict(arbitrary_types_allowed=True)
    type_name: Literal["gridworks.mqtt.problems.payload"] = "gridworks.mqtt.problems.payload"

class MQTTProblemsMessage(MQTTClientMessage[MQTTCommEventPayload]):
    def __init__(
        self, client_name: str, problems: Problems, rc: Optional[PahoReasonCode] = None
    ) -> None:
        super().__init__(
            payload=MQTTProblemsPayload(
                client_name=client_name,
                rc=SerializedReasonCode.from_paho_reason_code(rc)
                if rc is not None
                else rc,
                problems=problems,
            ),
        )


class PatWatchdog(GwBase):
    type_name: Literal["gridworks.watchdog.pat"] = "gridworks.watchdog.pat"


class PatInternalWatchdog(PatWatchdog):
    type_name: Literal["gridworks.watchdog.pat.internal"] = (
        "gridworks.watchdog.pat.internal"
    )


class PatExternalWatchdog(PatWatchdog):
    type_name: Literal["gridworks.watchdog.pat.external"] = (
        "gridworks.watchdog.pat.external"
    )


class PatInternalWatchdogMessage(Message[PatInternalWatchdog]):
    def __init__(self, src: str) -> None:
        super().__init__(
            src=src,
            dst=KnownNames.watchdog_manager.value,
            payload=PatInternalWatchdog(),
        )


class PatExternalWatchdogMessage(Message[PatExternalWatchdog]):
    def __init__(self) -> None:
        super().__init__(
            src=KnownNames.watchdog_manager.value,
            dst=KnownNames.watchdog_manager.value,
            payload=PatExternalWatchdog(),
        )


class Command(BaseModel): ...


CommandT = TypeVar("CommandT", bound=Command)


class CommandMessage(Message[CommandT], Generic[CommandT]):
    def __init__(self, *, ack_required: bool = True, **kwargs: Any) -> None:
        ensure_arg("message_id", str(uuid.uuid4()), kwargs)
        super().__init__(ack_required=ack_required, **kwargs)


class Shutdown(Command, GwBase):
    reason: str = ""
    type_name: Literal["gridworks.shutdown"] = "gridworks.shutdown"


class ShutdownMessage(CommandMessage[Shutdown]):
    def __init__(self, *, reason: str = "", **data: Any) -> None:
        ensure_arg("payload", Shutdown(reason=reason), data)
        super().__init__(**data)


class InternalShutdownMessage(ShutdownMessage):
    def __init__(self, *, ack_required: bool = False, **data: Any) -> None:
        super().__init__(ack_required=ack_required, **data)


class DBGCommands(Enum):
    show_subscriptions = "show_subscriptions"


class DBGPayload(GwBase):
    levels: LoggerLevels = LoggerLevels(
        message_summary=-1,
        lifecycle=-1,
        comm_event=-1,
    )
    command: Optional[DBGCommands] = None
    type_name: Literal["gridworks.proactor.dbg"] = "gridworks.proactor.dbg"

    @field_validator("command", mode="before")
    @classmethod
    def command_value(cls, v: Any) -> Optional[DBGCommands]:
        return as_enum(v, DBGCommands)


class DBGEvent(EventBase):
    command: DBGPayload
    path: str = ""
    count: int = 0
    msg: str = ""
    type_name: Literal["gridworks.event.proactor.dbg"] = "gridworks.event.proactor.dbg"
