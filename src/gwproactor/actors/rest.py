"""Code for actors that use a simple rest interaction, converting the response to one or more
REST commands into a message posted to main processing thread.

"""

import asyncio
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

import aiohttp
import yarl
from aiohttp import ClientResponse, ClientSession, ClientTimeout
from gwproto import Message
from gwproto.data_classes.components.rest_poller_component import RESTPollerComponent
from gwproto.type_helpers import AioHttpClientTimeout, RESTPollerSettings, URLConfig
from result import Ok, Result

from gwproactor import Actor, AppInterface, Problems
from gwproactor.proactor_interface import INVALID_IO_TASK_HANDLE, IOLoopInterface

Converter = Callable[[ClientResponse], Awaitable[Optional[Message[Any]]]]
ThreadSafeForwarder = Callable[[Message[Any]], Any]


async def null_converter(_: ClientResponse) -> Optional[Message[Any]]:
    return None


def null_forwarder(_: Message[Any]) -> None:
    return None


def to_client_timeout(
    timeout: Optional[AioHttpClientTimeout],
) -> Optional[ClientTimeout]:
    if timeout is not None:
        return ClientTimeout(**timeout.model_dump())
    return None


@dataclass
class SessionArgs:
    base_url: Optional[yarl.URL] = None
    kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class RequestArgs:
    method: str = "GET"
    url: Optional[yarl.URL] = None
    kwargs: dict[str, Any] = field(default_factory=dict)


class RESTPoller:
    _name: str
    _rest: RESTPollerSettings
    _io_loop_manager: IOLoopInterface
    _task_id: int
    _session_args: Optional[SessionArgs] = None
    _request_args: Optional[RequestArgs] = None
    _converter: Converter
    _forward: ThreadSafeForwarder

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        rest: RESTPollerSettings,
        loop_manager: IOLoopInterface,
        *,
        convert: Converter = null_converter,
        forward: ThreadSafeForwarder = null_forwarder,
        cache_request_args: bool = True,
    ) -> None:
        self._name = name
        self._task_id = INVALID_IO_TASK_HANDLE
        self._rest = rest
        self._io_loop_manager = loop_manager
        setattr(self, "_convert", convert)  # noqa: B010
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
                self._rest.session.model_dump(
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
                self._rest.request.model_dump(
                    exclude={"method", "url", "timeout"},
                    exclude_unset=True,
                ),
                timeout=to_client_timeout(self._rest.request.timeout),
            ),
        )

    async def _make_request(self, session: ClientSession) -> Optional[ClientResponse]:
        try:
            args = self._request_args
            if args is None:
                args = self._make_request_args()
            if args.url is None:
                raise ValueError(f"ERROR. args <{args}> produced url of None")  # noqa: TRY301
            response = await session.request(args.method, args.url, **args.kwargs)
            if self._rest.errors.request.error_for_http_status:
                response.raise_for_status()
        except Exception as e:
            response = None
            if self._rest.errors.request.report:
                try:  # noqa: SIM105
                    self._forward(
                        Message(
                            Payload=Problems(errors=[e]).problem_event(
                                summary=(
                                    f"Request error for <{self._name}>: {type(e)} <{e}>"
                                ),
                            )
                        )
                    )
                except:  # noqa: E722, S110
                    pass
            if self._rest.errors.request.raise_exception:
                raise
        return response

    async def _convert(self, response: ClientResponse) -> Optional[Message[Any]]:
        try:
            message = await self._converter(response)
        except Exception as convert_exception:
            message = None
            if self._rest.errors.convert.report:
                try:  # noqa: SIM105
                    self._forward(
                        Message(
                            Payload=Problems(errors=[convert_exception]).problem_event(
                                summary=(
                                    f"Convert error for <{self._name}>: {type(convert_exception)} <{convert_exception}>"
                                ),
                            )
                        )
                    )
                except:  # noqa: E722, S110
                    pass
            if self._rest.errors.convert.raise_exception:
                raise
        return message

    def _get_next_sleep_seconds(self) -> float:
        return self._rest.poll_period_seconds

    async def _run(self) -> None:
        reconnect = True
        while reconnect:
            args = self._session_args
            if args is None:
                args = self._make_session_args()
            async with aiohttp.ClientSession(args.base_url, **args.kwargs) as session:
                while True:
                    response = await self._make_request(session)
                    if response is not None:
                        async with response:
                            message = await self._convert(response)
                        if message is not None:
                            self._forward(message)
                    sleep_seconds = self._get_next_sleep_seconds()
                    await asyncio.sleep(sleep_seconds)

    def start(self) -> None:
        self._task_id = self._io_loop_manager.add_io_coroutine(
            self._run(),
            name=self._name,
        )

    def stop(self) -> None:
        self._io_loop_manager.cancel_io_routine(self._task_id)


class RESTPollerActor(Actor):
    _poller: RESTPoller

    def __init__(
        self,
        name: str,
        services: AppInterface,
        *,
        convert: Converter = null_converter,
        forward: ThreadSafeForwarder = null_forwarder,
        cache_request_args: bool = True,
    ) -> None:
        super().__init__(name, services)
        component = services.hardware_layout.component(self.name)
        if not isinstance(component, RESTPollerComponent):
            display_name = getattr(
                component, "display_name", "MISSING ATTRIBUTE display_name"
            )
            raise TypeError(
                f"ERROR. Component <{display_name}> has type {type(component)}. "
                f"Expected RESTPollerComponent.\n"
                f"  Node: {self.name}\n"
                f"  Component id: {getattr(component, 'component_id', None)}"
            )
        self._poller = RESTPoller(
            name,
            component.rest,
            services.io_loop_manager,
            convert=convert,
            forward=forward,
            cache_request_args=cache_request_args,
        )

    def process_message(self, _: Message[Any]) -> Result[bool, Exception]:
        return Ok()

    def start(self) -> None:
        self._poller.start()

    def stop(self) -> None:
        self._poller.stop()

    async def join(self) -> None:
        """IOLoop will take care of shutting down the associated task."""
