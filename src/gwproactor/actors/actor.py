"""Actor: A partial implementation of ActorInterface which supplies the trivial implementations.

SyncThreadActor: An actor which orchestrates starting, stopping and communicating with a passed in
SyncAsyncInteractionThread
"""

from abc import ABC
from typing import Any, Generic, Sequence, TypeVar

from gwproto import Message, ShNode
from result import Ok, Result

from gwproactor.callbacks import ProactorCallbackInterface
from gwproactor.codecs import CodecFactory
from gwproactor.proactor_interface import (
    ActorInterface,
    AppInterface,
    Communicator,
    MonitoredName,
)
from gwproactor.sync_thread import SyncAsyncInteractionThread


class Actor(ActorInterface, Communicator, ABC):
    _node: ShNode

    def __init__(self, name: str, services: AppInterface) -> None:
        self._node = services.hardware_layout.node(name)
        super().__init__(name, services)

    @classmethod
    def instantiate(
        cls, name: str, services: "AppInterface", **constructor_args: Any
    ) -> "ActorInterface":
        return cls(
            name,
            services,
            **constructor_args,  # noqa
        )

    @property
    def name(self) -> str:
        return self._name

    @property
    def node(self) -> ShNode:
        return self._node

    def init(self) -> None:
        """Called after constructor so derived functions can be used in setup."""


SyncThreadT = TypeVar("SyncThreadT", bound=SyncAsyncInteractionThread)


class SyncThreadActor(Actor, Generic[SyncThreadT]):
    _sync_thread: SyncAsyncInteractionThread

    def __init__(
        self,
        name: str,
        services: AppInterface,
        sync_thread: SyncAsyncInteractionThread,
    ) -> None:
        super().__init__(name, services)
        self._sync_thread = sync_thread

    def process_message(self, message: Message[Any]) -> Result[bool, Exception]:
        raise ValueError(
            f"Error. {self.__class__.__name__} does not process any messages. Received {message.Header}"
        )

    def send_driver_message(self, message: Any) -> None:
        self._sync_thread.put_to_sync_queue(message)

    def start(self) -> None:
        if (
            self.services.event_loop is None
            or self.services.async_receive_queue is None
        ):
            raise ValueError("ERROR. Actor started before ServicesInterface started")
        self._sync_thread.set_async_loop_and_start(
            self.services.event_loop, self.services.async_receive_queue
        )

    def stop(self) -> None:
        self._sync_thread.request_stop()

    async def join(self) -> None:
        await self._sync_thread.async_join()

    @property
    def monitored_names(self) -> Sequence[MonitoredName]:
        monitored_names = []
        if self._sync_thread.pat_timeout is not None:
            monitored_names.append(
                MonitoredName(self.name, self._sync_thread.pat_timeout)
            )
        return monitored_names


class PrimeActor(ProactorCallbackInterface, Actor):
    def __init__(self, name: str, services: AppInterface) -> None:
        super().__init__(name, services)
        services.add_callbacks(self)
        services.add_communicator(self)

    def process_message(self, _: Message[Any]) -> Result[bool, Exception]:
        return Ok(value=True)

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass

    async def join(self) -> None:
        pass

    @classmethod
    def get_codec_factory(cls) -> CodecFactory:
        return CodecFactory()


class NullPrimeActor(PrimeActor): ...
