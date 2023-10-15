"""Code for actors that use a simple rest interaction, converting the response to one or more
REST commands into a message posted to main processing thread.

"""
import asyncio
from dataclasses import dataclass
from dataclasses import field
from typing import Awaitable
from typing import Callable
from typing import Optional

import aiohttp as aiohttp
import yarl
from aiohttp import ClientResponse
from aiohttp import ClientSession
from aiohttp import ClientTimeout
from gwproto import Message
from gwproto.data_classes.components.rest_poller_component import RESTPollerComponent
from gwproto.types import AioHttpClientTimeout
from gwproto.types import RESTPollerSettings
from gwproto.types import URLConfig
from result import Result

from gwproactor import Actor
from gwproactor import ServicesInterface
from gwproactor.proactor_interface import INVALID_IO_TASK_HANDLE
from gwproactor.proactor_interface import IOLoopInterface


Converter = Callable[[ClientResponse], Awaitable[Optional[Message]]]
ThreadSafeForwarder = Callable[[Message], ...]


async def null_converter(response: ClientResponse) -> Optional[Message]:  # noqa
    return None


def null_fowarder(message: Message) -> None:  # noqa
    return None


def to_client_timeout(
    timeout: Optional[AioHttpClientTimeout],
) -> Optional[ClientTimeout]:
    if timeout is not None:
        return ClientTimeout(**timeout.dict())
    return None


@dataclass
class SessionArgs:
    base_url: Optional[yarl.URL] = None
    kwargs: dict = field(default_factory=dict)


@dataclass
class RequestArgs:
    method: str = "GET"
    url: Optional[yarl.URL] = None
    kwargs: dict = field(default_factory=dict)


class RESTPoller:
    _name: str
    _rest: RESTPollerSettings
    _io_loop_manager: IOLoopInterface
    _task_id: int
    _session_args: Optional[SessionArgs] = None
    _request_args: Optional[RequestArgs] = None

    def __init__(
        self,
        name: str,
        rest: RESTPollerSettings,
        loop_manager: IOLoopInterface,
        convert: Converter = null_converter,
        forward: ThreadSafeForwarder = null_fowarder,
        cache_request_args: bool = True,
    ):
        self._name = name
        self._task_id = INVALID_IO_TASK_HANDLE
        self._rest = rest
        self._io_loop_manager = loop_manager
        self._convert = convert
        self._forward = forward
        if cache_request_args:
            self._session_args = self._make_session_args()
            self._request_args = self._make_request_args()

    def _make_base_url(self) -> Optional[yarl.URL]:
        return URLConfig.make_url(self._rest.session.base_url)

    def _make_url(self) -> Optional[yarl.URL]:
        return URLConfig.make_url(self._rest.request.url)

    def _make_session_args(self) -> SessionArgs:
        return SessionArgs(
            self._make_base_url(),
            dict(
                self._rest.session.dict(
                    exclude={"base_url", "timeout"},
                    exclude_unset=True,
                ),
                timeout=to_client_timeout(self._rest.session.timeout),
            ),
        )

    def _make_request_args(self) -> RequestArgs:
        return RequestArgs(
            self._rest.request.method,
            self._make_url(),
            dict(
                self._rest.request.dict(
                    exclude={"method", "url", "timeout"},
                    exclude_unset=True,
                ),
                timeout=to_client_timeout(self._rest.request.timeout),
            ),
        )

    async def _make_request(self, session: ClientSession) -> ClientResponse:
        args = self._request_args
        if args is None:
            args = self._make_request_args()
        response = await session.request(args.method, args.url, **args.kwargs)
        return response

    def _get_next_sleep_seconds(self) -> float:
        return self._rest.poll_period_seconds

    async def _run(self):
        try:
            reconnect = True
            while reconnect:
                args = self._session_args
                if args is None:
                    args = self._make_session_args()
                async with aiohttp.ClientSession(
                    args.base_url, **args.kwargs
                ) as session:
                    while True:
                        try:
                            async with await self._make_request(session) as response:
                                message = await self._convert(response)
                            if message is not None:
                                self._forward(message)
                            sleep_seconds = self._get_next_sleep_seconds()
                            await asyncio.sleep(sleep_seconds)
                        except BaseException as e:
                            raise e

        except BaseException as e:
            raise e

    def start(self) -> None:
        self._task_id = self._io_loop_manager.add_io_coroutine(self._run())

    def stop(self) -> None:
        self._io_loop_manager.cancel_io_routine(self._task_id)


class RESTPollerActor(Actor):
    _poller: RESTPoller

    def __init__(
        self,
        name: str,
        services: ServicesInterface,
        convert: Converter = null_converter,
        forward: ThreadSafeForwarder = null_fowarder,
        cache_request_args: bool = True,
    ):
        super().__init__(name, services)
        component = services.hardware_layout.component(self.name)
        if not isinstance(component, RESTPollerComponent):
            display_name = getattr(
                component, "display_name", "MISSING ATTRIBUTE display_name"
            )
            raise ValueError(
                f"ERROR. Component <{display_name}> has type {type(component)}. "
                f"Expected RESTPollerComponent.\n"
                f"  Node: {self.name}\n"
                f"  Component id: {component.component_id}"
            )
        self._poller = RESTPoller(
            name,
            component.rest,
            services.io_loop_manager,
            convert=convert,
            forward=forward,
            cache_request_args=cache_request_args,
        )

    def process_message(self, message: Message) -> Result[bool, BaseException]:
        pass

    def start(self) -> None:
        self._poller.start()

    def stop(self) -> None:
        self._poller.stop()

    async def join(self) -> None:
        """IOLoop will take care of shutting down the associated task."""
