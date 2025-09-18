import uuid
from typing import Literal
from gw.named_types import GwBase
from gwproto.messages import EventBase
from pydantic import BaseModel, Field

from gwproactor_test.dummies.tree.messages import RelayInfo


class AdminInfo(GwBase):
    user: str
    src_machine: str
    type_name: Literal["gridworks.dummy.admin.info"]


class AdminCommandSetRelay(GwBase):
    command_info: AdminInfo
    relay_info: RelayInfo
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type_name: Literal["gridworks.dummy.admin.command.set.relay"] = (
        "gridworks.dummy.admin.command.set.relay"
    )


class AdminCommandReadRelays(GwBase):
    command_info: AdminInfo
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type_name: Literal["gridworks.dummy.admin.command.read.relays"] = (
        "gridworks.dummy.admin.command.read.relays"
    )


class AdminSetRelayEvent(EventBase):
    command: AdminCommandSetRelay
    type_name: Literal["gridworks.event.admin.command.set.relay"] = (
        "gridworks.event.admin.command.set.relay"
    )
