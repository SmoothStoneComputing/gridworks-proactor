from dataclasses import dataclass
from typing import Any, Optional, Sequence

import pytest
from paho.mqtt.client import ConnectFlags, MQTTMessage
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.reasoncodes import ReasonCode
from result import Result

from gwproactor.links import (
    InvalidCommStateInput,
    LinkStates,
    StateName,
    Transition,
    TransitionName,
)
from gwproactor.message import (
    MQTTConnectFailMessage,
    MQTTConnectMessage,
    MQTTDisconnectMessage,
    MQTTReceiptMessage,
)


def assert_transition(got: Transition, exp: Transition) -> None:
    assert got.__dict__ == exp.__dict__


@dataclass
class _Case:
    start: StateName
    input: TransitionName
    end: StateName
    ok: bool = True
    err: Optional[InvalidCommStateInput] = None
    input_content: Any = None

    def exp(self, name: str) -> Transition:
        return Transition(
            name,
            self.input,
            old_state=self.start,
            new_state=self.end,
        )

    def __str__(self) -> str:
        return (
            f"{self.start.value}--{self.input.value}-->{self.end.value}---ok:{self.ok}"
        )

    def get_input_content(self, name: str) -> Any:
        content = self.input_content
        if content is None or isinstance(content, int):
            match self.input:
                case TransitionName.mqtt_connected:
                    content = MQTTConnectMessage(
                        client_name=name,
                        userdata=None,
                        flags=ConnectFlags(session_present=False),
                        rc=ReasonCode(PacketTypes.CONNACK, "Success"),
                    )
                case TransitionName.mqtt_connect_failed:
                    content = MQTTConnectFailMessage(client_name=name, userdata=None)
                case TransitionName.mqtt_disconnected:
                    content = MQTTDisconnectMessage(
                        client_name=name,
                        userdata=None,
                        rc=ReasonCode(PacketTypes.DISCONNECT, "Success"),
                    )
                case TransitionName.mqtt_suback:
                    if content is None:
                        content = 1
                    content = name, content
                case TransitionName.message_from_peer:
                    content = MQTTReceiptMessage(
                        client_name=name, userdata=None, message=MQTTMessage()
                    )
                case _:
                    pass
        return content

    def assert_case(
        self,
        links: LinkStates,
        name: str,
        got: Result[Transition, InvalidCommStateInput],
    ) -> None:
        assert links[name].name == name
        link = links.link(name)
        assert link is not None
        assert link is not None
        assert link.name == name
        if self.ok:
            got_transition = got.unwrap()
            assert_transition(got_transition, self.exp(got_transition.link_name))
            assert links.link_state(got_transition.link_name) == self.end
        else:
            got_err: InvalidCommStateInput = got.unwrap_err()
            if self.err is not None:
                assert isinstance(got_err, self.err.__class__)
            assert links[name].state == self.start

    def _test(self) -> None:
        name = "a"
        links = LinkStates([name])
        link = links[name]
        assert link.state == StateName.not_started
        link.curr_state = link.states[self.start]
        assert link.state == self.start
        match self.input:
            case TransitionName.start_called:
                self.assert_case(links, name, links.start(name))
            case TransitionName.mqtt_connected:
                self.assert_case(
                    links,
                    name,
                    links.process_mqtt_connected(self.get_input_content(name)),
                )
            case TransitionName.mqtt_connect_failed:
                self.assert_case(
                    links,
                    name,
                    links.process_mqtt_connect_fail(self.get_input_content(name)),
                )
            case TransitionName.mqtt_disconnected:
                self.assert_case(
                    links,
                    name,
                    links.process_mqtt_disconnected(self.get_input_content(name)),
                )
            case TransitionName.mqtt_suback:
                self.assert_case(
                    links,
                    name,
                    links.process_mqtt_suback(*self.get_input_content(name)),
                )
            case TransitionName.message_from_peer:
                self.assert_case(
                    links,
                    name,
                    links.process_mqtt_message(self.get_input_content(name)),
                )
            case TransitionName.response_timeout:
                self.assert_case(links, name, links.process_ack_timeout(name))
            case TransitionName.stop_called:
                self.assert_case(links, name, links.stop(name))
            case _:
                raise ValueError(f"ERROR. Unexpected transition {self.input}")


class _State:
    start: StateName
    transitions: dict[TransitionName, list[_Case]]

    def __init__(self, start: StateName) -> None:
        self.start = start
        # By default, all transitions illegal
        self.transitions = {
            transition: [_Case(start, transition, start, False)]
            for transition in TransitionName
            if transition != TransitionName.none
        }

    def set_case(self, case: _Case | list[_Case]) -> None:
        cases: list[_Case]
        if isinstance(case, _Case):
            start = case.start
            input_ = case.input
            cases = [case]
        else:
            cases = case[:]
            starts = [case.start for case in cases]
            inputs = [case.input for case in cases]
            if any(start != cases[0].start for start in starts):
                raise ValueError(
                    f"If multiple cases added they must share the same start state. Found states: {starts}"
                )
            if any(input_ != cases[0].input for input_ in inputs):
                raise ValueError(
                    f"If multiple cases added they must share the same input. Found inputs: {inputs}"
                )
            start = cases[0].start
            input_ = cases[0].input
        if start != self.start:
            raise ValueError(f"Whoops {start} != {self.start}")
        self.transitions[input_] = cases


class _Cases:
    states: dict[StateName, _State]

    def __init__(self, cases: Optional[list[_Case | list[_Case]]] = None) -> None:
        # Disallow all transitions by default.
        self.states = {
            state: _State(state) for state in StateName if state != StateName.none
        }
        # Set explicit cases
        if cases is not None:
            for case in cases:
                self.set_case(case)

    def set_case(self, case: _Case | list[_Case]) -> None:
        if isinstance(case, _Case):
            start = case.start
        else:
            cases: list[_Case] = case
            starts = [case.start for case in cases]
            if any(start != cases[0].start for start in starts):
                raise ValueError(
                    f"If multiple cases added they must share the same start state. Found states: {starts}"
                )
            start = cases[0].start
        self.states[start].set_case(case)

    def cases(self) -> Sequence[_Case]:
        cases = []
        for state in self.states.values():
            for transition_cases in state.transitions.values():
                cases.extend(transition_cases)
        return cases


all_cases = _Cases(
    [
        _Case(StateName.not_started, TransitionName.start_called, StateName.connecting),
        _Case(StateName.not_started, TransitionName.stop_called, StateName.stopped),
        _Case(
            StateName.connecting,
            TransitionName.mqtt_connected,
            StateName.awaiting_setup_and_peer,
        ),
        _Case(
            StateName.connecting,
            TransitionName.mqtt_connect_failed,
            StateName.connecting,
        ),
        _Case(StateName.connecting, TransitionName.stop_called, StateName.stopped),
        [
            _Case(
                StateName.awaiting_setup_and_peer,
                TransitionName.mqtt_suback,
                StateName.awaiting_setup_and_peer,
                input_content=1,
            ),
            _Case(
                StateName.awaiting_setup_and_peer,
                TransitionName.mqtt_suback,
                StateName.awaiting_peer,
                input_content=0,
            ),
        ],
        _Case(
            StateName.awaiting_setup_and_peer,
            TransitionName.message_from_peer,
            StateName.awaiting_setup,
        ),
        _Case(
            StateName.awaiting_setup_and_peer,
            TransitionName.mqtt_disconnected,
            StateName.connecting,
        ),
        _Case(
            StateName.awaiting_setup_and_peer,
            TransitionName.stop_called,
            StateName.stopped,
        ),
        [
            _Case(
                StateName.awaiting_setup,
                TransitionName.mqtt_suback,
                StateName.awaiting_setup,
                input_content=1,
            ),
            _Case(
                StateName.awaiting_setup,
                TransitionName.mqtt_suback,
                StateName.active,
                input_content=0,
            ),
        ],
        _Case(
            StateName.awaiting_setup,
            TransitionName.mqtt_disconnected,
            StateName.connecting,
        ),
        _Case(
            StateName.awaiting_setup,
            TransitionName.message_from_peer,
            StateName.awaiting_setup,
        ),
        _Case(StateName.awaiting_setup, TransitionName.stop_called, StateName.stopped),
        _Case(
            StateName.awaiting_peer, TransitionName.message_from_peer, StateName.active
        ),
        _Case(
            StateName.awaiting_peer,
            TransitionName.mqtt_disconnected,
            StateName.connecting,
        ),
        _Case(
            StateName.awaiting_peer,
            TransitionName.response_timeout,
            StateName.awaiting_peer,
        ),
        _Case(StateName.awaiting_peer, TransitionName.stop_called, StateName.stopped),
        _Case(StateName.active, TransitionName.message_from_peer, StateName.active),
        _Case(
            StateName.active, TransitionName.response_timeout, StateName.awaiting_peer
        ),
        _Case(StateName.active, TransitionName.mqtt_disconnected, StateName.connecting),
        _Case(StateName.active, TransitionName.stop_called, StateName.stopped),
        _Case(StateName.stopped, TransitionName.stop_called, StateName.stopped),
    ]
)


@pytest.mark.parametrize("case", all_cases.cases(), ids=_Case.__str__)
def test_transitions(case: _Case) -> None:  # noqa: ANN001
    case._test()  # noqa: SLF001
