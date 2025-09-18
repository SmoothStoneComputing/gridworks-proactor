import uuid
from typing import Literal
from gw.named_types import GwBase
from gwproto import Message
from gwproto.messages import EventBase
from pydantic import  Field


class RelayInfo(GwBase):
    relay_name: str = ""
    closed: bool = False
    type_name: Literal["gridworks.dummy.relay.info"] = "gridworks.dummy.relay.info"

class RelayInfoReported(RelayInfo):
    current_change_mismatch: bool = False
    mismatch_count: int = 0
    type_name: Literal["gridworks.dummy.relay.info.reported"] = "gridworks.dummy.relay.info.reported"


class RelayStates(GwBase):
    total_change_mismatches: int = 0
    relays: dict[str, RelayInfoReported] = {}
    type_name: Literal["gridworks.dummy.relay.states"] = "gridworks.dummy.relay.states"


class SetRelay(GwBase):
    relay_name: str = ""
    closed: bool = False
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type_name: Literal["gridworks.dummy.set.relay"] = "gridworks.dummy.set.relay"


class SetRelayMessage(Message[SetRelay]):
    def __init__(
        self,
        *,
        src: str,
        relay_name: str,
        closed: bool,
        dst: str = "",
        ack_required: bool = False,
    ) -> None:
        super().__init__(
            src=src,
            dst=dst,
            ack_required=ack_required,
            payload=SetRelay(relay_name=relay_name, continuelosed=closed),
        )


class RelayReportEvent(EventBase):
    """Dummy event, scada2 -> scada1"""

    relay_name: str = ""
    closed: bool = False
    changed: bool = False
    TypeName: Literal["gridworks.event.relay.report"] = "gridworks.event.relay.report"


class RelayReportReceivedEvent(RelayReportEvent):
    """Dummy event, scada1 *received* RelayReportEvent"""

    mismatch: bool = False
    mismatch_count: int = 0
    TypeName: Literal["gridworks.event.relay.report.received"] = (
        "gridworks.event.relay.report.received"  # type: ignore[assignment]
    )
