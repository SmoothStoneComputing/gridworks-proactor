# ruff: noqa: ERA001
import asyncio
import contextlib
import subprocess  # noqa: S404
import time
from typing import Any, Optional

from gwproto import Message
from result import Ok, Result

from gwproactor.message import (
    InternalShutdownMessage,
    KnownNames,
    PatExternalWatchdog,
    PatExternalWatchdogMessage,
    PatInternalWatchdog,
)
from gwproactor.proactor_interface import (
    AppInterface,
    Communicator,
    MonitoredName,
    Runnable,
)


class _MonitoredName(MonitoredName):
    last_pat: float = 0.0


class WatchdogManager(Communicator, Runnable):
    _watchdog_task: Optional[asyncio.Task[Any]] = None
    _seconds_per_pat: float
    _monitored_names: dict[str, _MonitoredName]
    _pat_external_watchdog_process_args: list[str]

    def __init__(self, seconds_per_pat: float, services: AppInterface) -> None:
        super().__init__(KnownNames.watchdog_manager.value, services)
        self.lg = services.logger
        self._seconds_per_pat = seconds_per_pat
        self._monitored_names = {}
        self._pat_external_watchdog_process_args = []

    def start(self) -> None:
        if self._watchdog_task is None:
            self._pat_external_watchdog_process_args = (
                self._services.get_external_watchdog_builder_class().pat_args(
                    str(self._services.settings.paths.name),
                )
            )
            self.lg.info(
                f"WatchdogManager: [{' '.join(self._pat_external_watchdog_process_args)}]"
            )
            now = time.time()
            for monitored in self._monitored_names.values():
                monitored.last_pat = now
            self._watchdog_task = asyncio.create_task(
                self._check_pats(), name="pat_watchdog"
            )

    def stop(self) -> None:
        if self._watchdog_task is not None and not self._watchdog_task.done():
            self._watchdog_task.cancel()

    async def join(self) -> None:
        if self._watchdog_task is not None:
            with contextlib.suppress(asyncio.CancelledError):
                await self._watchdog_task

    def process_message(self, message: Message[Any]) -> Result[bool, Exception]:
        # self.lg.path("++WatchdogManager.process_message")
        path_dbg = 0
        match message.Payload:
            case PatInternalWatchdog():
                path_dbg |= 0x00000001
                self._pat_internal_watchdog(message.src())
            case PatExternalWatchdog():
                path_dbg |= 0x00000002
                self._pat_external_watchdog()
            case _:
                path_dbg |= 0x00000004
                raise ValueError(
                    f"WatchdogManager does not handle message payloads of type {type(message.Payload)}"
                )
        # self.lg.path(f"--WatchdogManager.process_message  0x{path_dbg:08X}")
        return Ok()

    def _pat_internal_watchdog(self, name: str) -> None:
        if name not in self._monitored_names:
            raise ValueError(
                f"ERROR. Received interal watchdog pat from unmonitored name: {name}. Monistored names: {list(self._monitored_names.keys())}"
            )
        self._monitored_names[name].last_pat = time.time()

    def add_monitored_name(self, monitored: MonitoredName) -> None:
        if monitored.timeout_seconds <= self._seconds_per_pat / 2:
            raise ValueError(
                f"ERROR. WatchdogManager cannot reliably monitor a timeout of {monitored.timeout_seconds} "
                f"(requested for {monitored.name}) because "
                f"WatchdogManager's _seconds_per_pat (sample rate) is {self._seconds_per_pat}"
            )
        if monitored.name in self._monitored_names:
            raise ValueError(
                f"ERROR. Name {monitored.name} is already being monitored with {self._monitored_names[monitored.name]}"
            )
        self._monitored_names[monitored.name] = _MonitoredName(
            monitored.name, monitored.timeout_seconds
        )

    def _check_for_timeout_expired(self) -> Optional[_MonitoredName]:
        # self.lg.path("++_check_for_timeout_expired")
        # path_dbg = 0
        expired: Optional[_MonitoredName] = None
        now = time.time()
        for monitored in self._monitored_names.values():
            # path_dbg |= 0x0000001
            required_pat_time = monitored.last_pat + monitored.timeout_seconds
            # self.lg.info(
            #     f"  {monitored.name:50s}  "
            #     f"{monitored.timeout_seconds:4d}  last_pat:{monitored.last_pat:11.1f}  "
            #     f"required_pat_time: {required_pat_time:11.1f}  "
            #     f"now:{now:11.1f}  "
            #     f"remaining: {int(required_pat_time - now):4d}  "
            #     f"required_pat_time < now ? {int(required_pat_time < now)}"
            # )
            if required_pat_time < now:
                # path_dbg |= 0x0000002
                expired = monitored
                break
        # self.lg.path(f"--_check_for_timeout_expired: count:{int(expired is not None)}  0x{path_dbg:08X}")
        return expired

    async def _check_pats(self) -> None:
        while (expired := self._check_for_timeout_expired()) is None:
            self._send(PatExternalWatchdogMessage())
            await asyncio.sleep(self._seconds_per_pat)
        self._send(
            InternalShutdownMessage(
                Src=self.name,
                Reason=(
                    f"Monitored object ({expired.name}) failed to pat internal watchdog.  \n"
                    f"  Last pat from {expired.name}: {int(time.time() - expired.last_pat)} seconds ago\n"
                    f"  Allowed seconds: {int(expired.timeout_seconds)}"
                ),
            )
        )

    def _pat_external_watchdog(self) -> None:
        if self._pat_external_watchdog_process_args:
            subprocess.run(self._pat_external_watchdog_process_args, check=True)  # noqa: S603
