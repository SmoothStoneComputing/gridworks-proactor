"""Proactor interfaces, separate from implementations to clarify how users of this package interact with it and to
create forward references for implementation hiearchies
"""

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from types import ModuleType
from typing import Any, Coroutine, Optional, Sequence, Type, TypeVar

from aiohttp.typedefs import Handler as HTTPHandler
from gwproto import HardwareLayout, Message, ShNode
from gwproto.messages import EventT
from gwproto.named_types.web_server_gt import WebServerGt
from paho.mqtt.client import MQTTMessageInfo
from result import Result

from gwproactor.callbacks import ProactorCallbackInterface
from gwproactor.config.app_settings import AppSettings
from gwproactor.external_watchdog import ExternalWatchdogCommandBuilder
from gwproactor.links.mqtt import QOS
from gwproactor.logger import ProactorLogger
from gwproactor.stats import ProactorStats

T = TypeVar("T")


@dataclass
class MonitoredName:
    name: str
    timeout_seconds: float


class CommunicatorInterface(ABC):
    """Pure interface necessary for interaction between a sub-object and the system services proactor"""

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def _send(self, message: Message[Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def process_message(self, message: Message[Any]) -> Result[bool, Exception]:
        raise NotImplementedError

    @property
    @abstractmethod
    def monitored_names(self) -> Sequence[MonitoredName]:
        raise NotImplementedError


class Communicator(CommunicatorInterface, ABC):
    """A partial implementation of CommunicatorInterface which supplies the trivial implementations"""

    _name: str
    _services: "AppInterface"

    def __init__(self, name: str, services: "AppInterface") -> None:
        self._name = name
        self._services = services

    @property
    def name(self) -> str:
        return self._name

    def _send(self, message: Message[Any]) -> None:
        self._services.send(message)

    @property
    def monitored_names(self) -> Sequence[MonitoredName]:
        return []

    @property
    def services(self) -> "AppInterface":
        return self._services


class Runnable(ABC):
    """Pure interface to an object which is expected to support starting, stopping and joining."""

    @abstractmethod
    def start(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def stop(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def join(self) -> None:
        raise NotImplementedError

    async def stop_and_join(self) -> None:
        self.stop()
        await self.join()


class ActorInterface(CommunicatorInterface, Runnable, ABC):
    """Pure interface for a proactor sub-object (an Actor) which can communicate
    and has a GridWorks ShNode.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def node(self) -> ShNode:
        raise NotImplementedError

    @abstractmethod
    def init(self) -> None:
        """Called after constructor so derived functions can be used in setup."""

    @classmethod
    @abstractmethod
    def instantiate(
        cls, name: str, services: "AppInterface", **contstructor_kwargs: Any
    ) -> "ActorInterface":
        raise NotImplementedError

    @classmethod
    def load(
        cls,
        name: str,
        actor_class_name: str,
        services: "AppInterface",
        actors_module: ModuleType,
        **constructor_kwargs: Any,
    ) -> "ActorInterface":
        actor_class = getattr(actors_module, actor_class_name)
        if not issubclass(actor_class, ActorInterface):
            raise ValueError(  # noqa: TRY004
                f"ERROR. Imported class <{actor_class}> "
                f"from module <{actors_module.__name__}> "
                f"with via requested name <{actor_class_name} "
                "does not implement ActorInterface",
            )
        actor = actor_class.instantiate(name, services, **constructor_kwargs)
        if not isinstance(actor, ActorInterface):
            raise ValueError(  # noqa: TRY004
                f"ERROR. Constructed object with type {type(actor)} "
                f"is not instance of ActorInterface",
            )
        actor.init()
        return actor


INVALID_IO_TASK_HANDLE = -1


class IOLoopInterface(CommunicatorInterface, Runnable, ABC):
    """Interface to an asyncio event loop running a seperate thread meant io-only
    routines which have minimal CPU bound work.
    """

    @abstractmethod
    def add_io_coroutine(self, coro: Coroutine[Any, Any, Any], name: str = "") -> int:
        """Add a couroutine that will be run as a task in the io event loop.

        May be called before or after IOLoopInterface.start(). No tasks will actually
        run until IOLoopInterface.start() is called.

        This routine is thread safe.

        Args:
            coro: The coroutine to run as a task.
            name: Optional name assigned to task for use in debugging.

        Returns:
            an integer handle which may be passed to cancel_io_coroutine() to cancel
            the task running the coroutine.

        """

    @abstractmethod
    def cancel_io_routine(self, handle: int) -> None:
        """Cancel the task represented by the handle.

        This routine may be called multiple times for the same handle with no effect.
        This routine is thread safe.

        Args:
            handle: The handle returned by previous call to add_io_routine().

        """


class AppInterface(ABC):
    """Interface to system services (the proactor)"""

    @abstractmethod
    def add_communicator(self, communicator: CommunicatorInterface) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_communicator(self, name: str) -> Optional[CommunicatorInterface]:
        raise NotImplementedError

    @abstractmethod
    def get_communicator_as_type(self, name: str, type_: Type[T]) -> Optional[T]:
        raise NotImplementedError

    @abstractmethod
    def get_communicator_names(self) -> set[str]:
        raise NotImplementedError

    @abstractmethod
    def send(self, message: Message[Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    async def await_processing(
        self, message: Message[Any]
    ) -> Result[Any, BaseException]:
        raise NotImplementedError

    @abstractmethod
    def send_threadsafe(self, message: Message[Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def wait_for_processing_threadsafe(
        self, message: Message[Any]
    ) -> Result[Any, BaseException]:
        raise NotImplementedError

    @abstractmethod
    def add_task(self, task: asyncio.Task[Any]) -> None:
        raise NotImplementedError

    @property
    @abstractmethod
    def async_receive_queue(self) -> Optional[asyncio.Queue[Any]]:
        raise NotImplementedError

    @property
    @abstractmethod
    def event_loop(self) -> Optional[asyncio.AbstractEventLoop]:
        raise NotImplementedError

    @property
    @abstractmethod
    def io_loop_manager(self) -> IOLoopInterface:
        raise NotImplementedError

    @abstractmethod
    def add_web_server_config(
        self,
        name: str,
        host: str,
        port: int,
        **kwargs: Any,
    ) -> None:
        """Adds configuration for web server which will be started when start() is called.

        Not thread safe.
        """
        raise NotImplementedError

    @abstractmethod
    def add_web_route(
        self,
        server_name: str,
        method: str,
        path: str,
        handler: HTTPHandler,
        **kwargs: Any,
    ) -> None:
        """Adds configuration for web server route which will be available after start() is called.

        May be called even if associated web server is not configured, in which case this route
        will simply be ignored.

        Not thread safe.
        """
        raise NotImplementedError

    @abstractmethod
    def get_web_server_route_strings(self) -> dict[str, list[str]]:
        raise NotImplementedError

    @abstractmethod
    def get_web_server_configs(self) -> dict[str, WebServerGt]:
        raise NotImplementedError

    @abstractmethod
    def generate_event(self, event: EventT) -> Result[bool, Exception]:
        raise NotImplementedError

    @property
    @abstractmethod
    def publication_name(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def subscription_name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def publish_message(  # noqa: PLR0913
        self,
        link_name: str,
        message: Message[Any],
        qos: int = 0,
        context: Any = None,
        *,
        topic: str = "",
        use_link_topic: bool = False,
    ) -> MQTTMessageInfo:
        raise NotImplementedError

    def publish_upstream(
        self, payload: Any, qos: QOS = QOS.AtMostOnce, **message_args: Any
    ) -> MQTTMessageInfo:
        raise NotImplementedError

    @property
    def upstream_client(self) -> str:
        raise NotImplementedError

    @property
    def downstream_client(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def settings(self) -> AppSettings:
        raise NotImplementedError

    @property
    @abstractmethod
    def logger(self) -> ProactorLogger:
        raise NotImplementedError

    @property
    @abstractmethod
    def stats(self) -> ProactorStats:
        raise NotImplementedError

    @property
    @abstractmethod
    def hardware_layout(self) -> HardwareLayout: ...

    @abstractmethod
    def get_external_watchdog_builder_class(
        self,
    ) -> type[ExternalWatchdogCommandBuilder]:
        raise NotImplementedError

    @abstractmethod
    def add_callbacks(self, callbacks: ProactorCallbackInterface) -> int:
        raise NotImplementedError

    @abstractmethod
    def remove_callbacks(self, callbacks_id: int) -> None:
        raise NotImplementedError
