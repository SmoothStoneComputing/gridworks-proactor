import functools
from dataclasses import dataclass
from typing import Any, Callable, Optional

from gwproactor.links.timer_interface import TimerManagerInterface


@dataclass
class AckWaitInfo:
    link_name: str
    message_id: str
    timer_handle: Any
    context: Any = None


AckTimerCallback = Callable[[AckWaitInfo], None]

DEFAULT_ACK_DELAY = 5.0


class AckManager:
    _acks: dict[str, dict[str, AckWaitInfo]]
    _timer_mgr: TimerManagerInterface
    _callback: AckTimerCallback
    _default_delay_seconds: float

    def __init__(
        self,
        timer_mgr: TimerManagerInterface,
        callback: AckTimerCallback,
        delay: float = DEFAULT_ACK_DELAY,
    ) -> None:
        self._acks = {}
        self._timer_mgr = timer_mgr
        self._user_callback = callback
        self._default_delay_seconds = delay

    def start_ack_timer(
        self,
        link_name: str,
        message_id: str,
        context: Optional[Any] = None,
        delay_seconds: Optional[float] = None,
    ) -> AckWaitInfo:
        self.cancel_ack_timer(link_name, message_id)
        delay_seconds = (
            self._default_delay_seconds if delay_seconds is None else delay_seconds
        )
        wait_info = AckWaitInfo(
            link_name=link_name,
            message_id=message_id,
            timer_handle=self._timer_mgr.start_timer(
                self._default_delay_seconds if delay_seconds is None else delay_seconds,
                functools.partial(self._timeout, link_name, message_id),
            ),
            context=context,
        )
        if link_name not in self._acks:
            self._acks[link_name] = {}
        self._acks[link_name][message_id] = wait_info
        return wait_info

    def add_link(self, link_name: str) -> None:
        self._acks[link_name] = {}

    def _pop_wait_info(self, link_name: str, message_id: str) -> Optional[AckWaitInfo]:
        if (client_acks := self._acks.get(link_name, None)) is not None:
            return client_acks.pop(message_id, None)
        return None

    def _timeout(self, link_name: str, message_id: str) -> None:
        if (wait_info := self._pop_wait_info(link_name, message_id)) is not None:
            self._user_callback(wait_info)

    def cancel_ack_timer(
        self, link_name: str, message_id: str
    ) -> Optional[AckWaitInfo]:
        if (wait_info := self._pop_wait_info(link_name, message_id)) is not None:
            self._timer_mgr.cancel_timer(wait_info.timer_handle)
        return wait_info

    def cancel_ack_timers(self, link_name: str) -> list[AckWaitInfo]:
        if link_name in self._acks:
            wait_infos = list(self._acks[link_name].values())
            self._acks[link_name] = {}
            for wait_info in wait_infos:
                self._timer_mgr.cancel_timer(wait_info.timer_handle)
        else:
            wait_infos = []
        return wait_infos

    def num_acks(self, link_name: str) -> int:
        if (client_acks := self._acks.get(link_name, None)) is not None:
            return len(client_acks)
        return 0

    @property
    def default_delay_seconds(self) -> float:
        return self._default_delay_seconds
