"""This packages provides infrastructure for running a proactor on top of asyncio with support multiple MQTT clients
and and sub-objects which support their own threads for synchronous operations.

This packages is not GridWorks-aware (except that it links actors with multiple mqtt clients). This separation between
communication / action infrastructure and GridWorks semantics is intended to allow the latter to be more focussed.

This package is not polished and the separation is up for debate.

Particular questions:

* Is the programming model still clean after more concrete actors are implemented and more infrastructure are added.
* Does the separation add value or just complicate analysis.
* MQTTClients should be made async.
* Semantics of building message type namespaces should be spelled out / further worked out.
* Test support should be implemented / cleaner.
"""

from gwproactor.actors import Actor, SyncThreadActor, SyncThreadT, WebEventListener
from gwproactor.actors.actor import PrimeActor
from gwproactor.app import App
from gwproactor.callbacks import ProactorCallbackFunctions, ProactorCallbackInterface
from gwproactor.codecs import CodecFactory, ProactorCodec
from gwproactor.command_line_utils import get_app, run_async_main
from gwproactor.config import AppSettings, ProactorSettings
from gwproactor.config.links import CodecSettings, LinkSettings
from gwproactor.config.proactor_config import ProactorConfig, ProactorName
from gwproactor.external_watchdog import ExternalWatchdogCommandBuilder
from gwproactor.links.mqtt import QOS, MQTTClients, MQTTClientWrapper, Subscription
from gwproactor.logger import ProactorLogger
from gwproactor.logging_setup import format_exceptions, setup_logging
from gwproactor.proactor_implementation import Proactor
from gwproactor.proactor_interface import (
    INVALID_IO_TASK_HANDLE,
    ActorInterface,
    AppInterface,
    Communicator,
    CommunicatorInterface,
    MonitoredName,
    Runnable,
)
from gwproactor.problems import Problems
from gwproactor.sync_thread import (
    AsyncQueueWriter,
    SyncAsyncInteractionThread,
    SyncAsyncQueueWriter,
    responsive_sleep,
)

__all__ = [
    "INVALID_IO_TASK_HANDLE",
    "QOS",
    "Actor",
    "ActorInterface",
    "App",
    "AppSettings",
    "AsyncQueueWriter",
    "CodecFactory",
    "CodecSettings",
    "Communicator",
    "CommunicatorInterface",
    "ExternalWatchdogCommandBuilder",
    "LinkSettings",
    "MQTTClientWrapper",
    "MQTTClients",
    "MonitoredName",
    "PrimeActor",
    "Proactor",
    "ProactorCallbackFunctions",
    "ProactorCallbackInterface",
    "ProactorCodec",
    "ProactorLogger",
    "ProactorConfig",
    "ProactorName",
    "ProactorSettings",
    "Problems",
    "Runnable",
    "AppInterface",
    "Subscription",
    "SyncAsyncInteractionThread",
    "SyncAsyncQueueWriter",
    "SyncThreadActor",
    "SyncThreadT",
    "WebEventListener",
    "format_exceptions",
    "get_app",
    "responsive_sleep",
    "run_async_main",
    "setup_logging",
]
