"""Classes providing interaction between synchronous and asynchronous code"""

import asyncio
import logging
import queue
import threading
import time
import traceback
import typing
from abc import ABC
from typing import Any, Optional

from gwproactor.logger import LoggerOrAdapter
from gwproactor.message import InternalShutdownMessage, PatInternalWatchdogMessage

DEFAULT_STEP_DURATION = 0.1

JOIN_NEVER_TIMEOUT = 1000000000.0  # > 30 years
JOIN_CHECK_THREAD_SECONDS = 1.0


async def async_polling_thread_join(
    t: Optional[threading.Thread],
    timeout_seconds: float = JOIN_NEVER_TIMEOUT,
    *,
    check_thread_seconds: float = JOIN_CHECK_THREAD_SECONDS,
    raise_errors: bool = False,
) -> Optional[Exception]:
    if t is None:
        return None
    if timeout_seconds is None:
        timeout_seconds = JOIN_NEVER_TIMEOUT  # type: ignore[unreachable]
    end = time.time() + timeout_seconds
    remaining = timeout_seconds
    returned_e: Optional[Exception] = None
    try:
        while t.is_alive() and remaining > 0:
            await asyncio.sleep(min(check_thread_seconds, remaining))
            remaining = end - time.time()
    except Exception as e:
        returned_e = e
        if raise_errors:
            raise
    return returned_e


def responsive_sleep(
    obj: Any,
    seconds: float,
    *,
    step_duration: float = DEFAULT_STEP_DURATION,
    running_field_name: str = "_main_loop_running",
    running_field: bool = True,
) -> bool:
    """Sleep in way that is more responsive to thread termination: sleep in step_duration increments up to
    specificed seconds, at after each step checking obj._main_loop_running. If the designated running_field_name actually
    indicates that a stop has been requested (e.g. what you would expect from a field named '_stop_requested'),
    set running_field parameter to False."""
    end_time = time.time() + seconds
    while (
        getattr(obj, running_field_name) == running_field
        and (now := time.time()) < end_time
    ):
        time.sleep(min(end_time - now, step_duration))
    return typing.cast(bool, getattr(obj, running_field_name)) == running_field


class AsyncQueueWriter:
    """Allow synchronous code to write to an asyncio Queue.

    It is assumed the asynchronous reader has access to the asyncio Queue "await get()" from directly from it.
    """

    _loop: Optional[asyncio.AbstractEventLoop] = None
    _async_queue: Optional[asyncio.Queue[Any]] = None

    def set_async_loop(
        self, loop: asyncio.AbstractEventLoop, async_queue: asyncio.Queue[Any]
    ) -> None:
        self._loop = loop
        self._async_queue = async_queue

    def put(self, item: Any) -> None:
        """Write to asyncio queue in a threadsafe way."""
        if self._loop is None or self._async_queue is None:
            raise ValueError(
                "ERROR. start(loop, async_queue) must be called prior to put(item)"
            )
        self._loop.call_soon_threadsafe(self._async_queue.put_nowait, item)


class SyncAsyncQueueWriter:
    """Provide a full duplex communication "channel" between synchronous and asynchronous code.

    It is assumed the asynchronous reader has access to the asyncio Queue "await get()" from directly from it.
    """

    _loop: Optional[asyncio.AbstractEventLoop] = None
    _async_queue: Optional[asyncio.Queue[Any]] = None
    sync_queue: Optional[queue.Queue[Any]]

    def __init__(self, sync_queue: Optional[queue.Queue[Any]] = None) -> None:
        self.sync_queue = sync_queue

    def set_async_loop(
        self, loop: asyncio.AbstractEventLoop, async_queue: asyncio.Queue[Any]
    ) -> None:
        self._loop = loop
        self._async_queue = async_queue

    def put_to_sync_queue(
        self, item: Any, *, block: bool = True, timeout: Optional[float] = None
    ) -> None:
        """Write to synchronous queue in a threadsafe way."""
        if self.sync_queue is None:
            raise ValueError(
                "SyncAsyncQueueWriter sync_queue is None and cannot be written to"
            )
        self.sync_queue.put(item, block=block, timeout=timeout)

    def put_to_async_queue(self, item: Any) -> None:
        """Write to asynchronous queue in a threadsafe way."""
        if self._loop is None or self._async_queue is None:
            raise ValueError(
                "ERROR. start(loop, async_queue) must be called prior to put(item)"
            )
        self._loop.call_soon_threadsafe(self._async_queue.put_nowait, item)

    def get_from_sync_queue(
        self, *, block: bool = True, timeout: Optional[float] = None
    ) -> Any:
        """Read from synchronous queue in a threadsafe way."""
        if self.sync_queue is None:
            raise ValueError(
                "SyncAsyncQueueWriter sync_queue is None and cannot be written to"
            )
        return self.sync_queue.get(block=block, timeout=timeout)


class SyncAsyncInteractionThread(threading.Thread, ABC):
    """A thread wrapper providing an async-sync communication channel and simple "iterate, sleep, read message"
    semantics.
    """

    SLEEP_STEP_SECONDS = 0.1
    PAT_TIMEOUT = 20
    JOIN_CHECK_THREAD_SECONDS = 1.0

    _channel: SyncAsyncQueueWriter
    running: Optional[bool]
    _iterate_sleep_seconds: Optional[float]
    _responsive_sleep_step_seconds: float
    pat_timeout: Optional[float]
    _last_pat_time: float
    _logger: LoggerOrAdapter

    def __init__(  # noqa: PLR0913
        self,
        channel: Optional[SyncAsyncQueueWriter] = None,
        name: Optional[str] = None,
        *,
        iterate_sleep_seconds: Optional[float] = None,
        responsive_sleep_step_seconds: float = SLEEP_STEP_SECONDS,
        pat_timeout: Optional[float] = PAT_TIMEOUT,
        daemon: bool = True,
        logger: Optional[LoggerOrAdapter] = None,
    ) -> None:
        super().__init__(name=name, daemon=daemon)
        if channel is None:
            self._channel = SyncAsyncQueueWriter()
        else:
            self._channel = channel
        self._iterate_sleep_seconds = iterate_sleep_seconds
        self._responsive_sleep_step_seconds = responsive_sleep_step_seconds
        self.running = None
        self.pat_timeout = pat_timeout
        self._last_pat_time = 0.0
        if logger is None:
            self._logger = logging.getLogger(__package__)
        else:
            self._logger = logger

    def _preiterate(self) -> None:
        pass

    def _iterate(self) -> None:
        pass

    def _handle_message(self, message: Any) -> None:
        pass

    def _handle_exception(self, exception: Exception) -> bool:  # noqa
        return False

    def request_stop(self) -> None:
        self.running = False

    def set_async_loop(
        self, loop: asyncio.AbstractEventLoop, async_queue: asyncio.Queue[Any]
    ) -> None:
        self._channel.set_async_loop(loop, async_queue)

    def set_async_loop_and_start(
        self, loop: asyncio.AbstractEventLoop, async_queue: asyncio.Queue[Any]
    ) -> None:
        self.set_async_loop(loop, async_queue)
        self.start()

    def put_to_sync_queue(
        self, message: Any, *, block: bool = True, timeout: Optional[float] = None
    ) -> None:
        self._channel.put_to_sync_queue(message, block=block, timeout=timeout)

    def _put_to_async_queue(self, message: Any) -> None:
        self._channel.put_to_async_queue(message)

    def run(self) -> None:  # noqa: C901
        if self.running is None:  # noqa: PLR1702
            self.running = True
            self._last_pat_time = time.time()
            self._preiterate()
            while self.running:
                try:
                    if self._iterate_sleep_seconds is not None:
                        responsive_sleep(
                            self,
                            self._iterate_sleep_seconds,
                            running_field_name="running",
                            step_duration=self._responsive_sleep_step_seconds,
                        )
                    if self.running:
                        self._iterate()
                    if self.running and self.time_to_pat():
                        self.pat_watchdog()
                    if self.running and self._channel.sync_queue is not None:
                        try:
                            message = self._channel.get_from_sync_queue(block=False)
                            if self.running:
                                self._handle_message(message)
                        except queue.Empty:
                            pass
                except Exception as e:  # noqa: BLE001, PERF203
                    handle_exception_str = ""
                    try:
                        handled = self._handle_exception(e)
                    except Exception as e2:  # noqa: BLE001
                        handled = False
                        handle_exception_str = "".join(traceback.format_exception(e2))
                    if not handled:
                        self.running = False
                        reason = (
                            f"SyncAsyncInteractionThread ({self.name}) caught exception:\n"
                            f"{''.join(traceback.format_exception(e))}\n"
                        )
                        if handle_exception_str:
                            reason += (
                                "While handling that exception, _handle_exception caused exception:\n"
                                f"{handle_exception_str}\n"
                            )
                        self._put_to_async_queue(
                            InternalShutdownMessage(Src=self.name, Reason=reason)
                        )

    def time_to_pat(self) -> bool:
        return self.pat_timeout is not None and time.time() >= (
            self._last_pat_time + (self.pat_timeout / 2)
        )

    def pat_watchdog(self) -> None:
        self._last_pat_time = time.time()
        self._put_to_async_queue(PatInternalWatchdogMessage(src=self.name))

    async def async_join(self, timeout: Optional[float] = None) -> None:  # noqa: ASYNC109
        if timeout is None:
            timeout = JOIN_NEVER_TIMEOUT
        await async_polling_thread_join(self, timeout)
