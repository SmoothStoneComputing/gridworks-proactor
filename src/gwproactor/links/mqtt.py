"""MQTT infrastructure providing support for multiple MTQTT clients

TODO: Replace synchronous use of Paho MQTT Client with asyncio usage, per Paho documentation or external library

Main current limitation: each interaction between asyncio code and the mqtt clients must either have thread locking
(as is provided inside paho for certain functions such as publish()) or an explicit message based API.

"""

import asyncio
import contextlib
import enum
import logging
import ssl
import threading
import typing
import uuid
from typing import Any, Dict, List, NamedTuple, Optional, Set, Tuple, Union, cast

from paho.mqtt.client import MQTT_ERR_SUCCESS, MQTTMessage, MQTTMessageInfo
from paho.mqtt.client import Client as PahoMQTTClient

from gwproactor import config
from gwproactor.links.link_settings import LinkConfig
from gwproactor.message import (
    MQTTConnectFailMessage,
    MQTTConnectMessage,
    MQTTDisconnectMessage,
    MQTTProblemsMessage,
    MQTTReceiptMessage,
    MQTTSubackMessage,
    MQTTSubackPayload,
)
from gwproactor.problems import Problems
from gwproactor.sync_thread import AsyncQueueWriter, responsive_sleep


class QOS(enum.IntEnum):
    AtMostOnce = 0
    AtLeastOnce = 1
    ExactlyOnce = 2


class Subscription(NamedTuple):
    Topic: str
    Qos: QOS


class MQTTClientWrapper:
    _client_name: str
    topic_dst: str
    _client_config: config.MQTTClient
    _client: PahoMQTTClient
    _stop_requested: bool
    _receive_queue: AsyncQueueWriter
    _subscriptions: Dict[str, int]
    _pending_subscriptions: Set[str]
    _pending_subacks: Dict[int, List[str]]

    def __init__(
        self,
        client_name: str,
        topic_dst: str,
        client_config: config.MQTTClient,
        receive_queue: AsyncQueueWriter,
    ) -> None:
        self._client_name = client_name
        self.topic_dst = topic_dst
        self._client_config = client_config
        self._receive_queue = receive_queue
        self._client = PahoMQTTClient("-".join(str(uuid.uuid4()).split("-")[:-1]))
        if self._client_config.username is not None:
            self._client.username_pw_set(
                username=self._client_config.username,
                password=self._client_config.password.get_secret_value(),
            )
        tls_config = self._client_config.tls
        if tls_config.use_tls:
            try:
                self._client.tls_set(
                    ca_certs=str(tls_config.paths.ca_cert_path),
                    certfile=str(tls_config.paths.cert_path),
                    keyfile=str(tls_config.paths.private_key_path),
                    cert_reqs=tls_config.cert_reqs,
                    tls_version=ssl.PROTOCOL_TLS_CLIENT,
                    ciphers=tls_config.ciphers,
                    keyfile_password=tls_config.keyfile_password.get_secret_value(),
                )
            except Exception as e:
                raise RuntimeError(
                    f"ERROR. Client.tls_set() caught exception <{e}> "
                    f"from tls_config: {tls_config}"
                ) from e

        self._client.on_message = self.on_message
        self._client.on_connect = self.on_connect
        self._client.on_connect_fail = self.on_connect_fail
        self._client.on_disconnect = self.on_disconnect
        self._client.on_subscribe = self.on_subscribe
        self._subscriptions = {}
        self._pending_subscriptions = set()
        self._pending_subacks = {}
        self._thread = threading.Thread(
            target=self._client_thread, name=f"MQTT-client-thread-{self._client_name}"
        )
        self._stop_requested = False

    def _client_thread(self) -> None:
        max_back_off = 1024
        backoff = 1
        while not self._stop_requested:
            try:
                self._client.connect(
                    self._client_config.host, port=self._client_config.effective_port()
                )
                self._client.loop_forever(retry_first_connection=True)
            except Exception as e:  # noqa: BLE001
                self._receive_queue.put(
                    MQTTProblemsMessage(
                        client_name=self._client_name, problems=Problems(errors=[e])
                    )
                )
            finally:
                with contextlib.suppress(Exception):
                    self._client.disconnect()
            if not self._stop_requested:
                if backoff >= max_back_off:
                    backoff = 1
                else:
                    backoff = min(backoff * 2, max_back_off)
                responsive_sleep(
                    self,
                    backoff,
                    running_field_name="_stop_requested",
                    running_field=False,
                )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop_requested = True
        try:
            self._client.disconnect()
            self._thread.join()
        except:  # noqa: E722, S110
            pass

    def publish(self, topic: str, payload: bytes, qos: int) -> MQTTMessageInfo:
        return self._client.publish(topic, payload, qos)

    def subscribe(self, topic: str, qos: int) -> tuple[int, Optional[int]]:
        self._subscriptions[topic] = qos
        self._pending_subscriptions.add(topic)
        subscribe_result = self._client.subscribe(topic, qos)
        if subscribe_result[0] == MQTT_ERR_SUCCESS:
            self._pending_subacks[subscribe_result[1]] = [topic]
        return typing.cast(tuple[int, Optional[int]], subscribe_result)

    def subscribe_all(self) -> tuple[int, Optional[int]]:
        subscribe_result: tuple[int, Optional[int]]
        if self._subscriptions:
            topics = list(self._subscriptions.keys())
            for topic in topics:
                self._pending_subscriptions.add(topic)
            subscribe_result = self._client.subscribe(
                list(self._subscriptions.items()), 0
            )
            if (
                subscribe_result[0] == MQTT_ERR_SUCCESS
                and subscribe_result[1] is not None
            ):
                self._pending_subacks[subscribe_result[1]] = topics
        else:
            subscribe_result = MQTT_ERR_SUCCESS, None
        return subscribe_result

    def unsubscribe(self, topic: str) -> Tuple[int, Optional[int]]:
        self._subscriptions.pop(topic, None)
        return typing.cast(tuple[int, Optional[int]], self._client.unsubscribe(topic))

    def connected(self) -> bool:
        return self._client.is_connected()

    def num_subscriptions(self) -> int:
        return len(self._subscriptions)

    def num_pending_subscriptions(self) -> int:
        return len(self._pending_subscriptions)

    def subscribed(self) -> bool:
        return self.connected() and (
            not self.num_subscriptions() or not self.num_pending_subscriptions()
        )

    def subscription_items(self) -> list[Tuple[str, int]]:
        return list(cast(list[Tuple[str, int]], self._subscriptions.items()))

    @property
    def mqtt_client(self) -> PahoMQTTClient:
        return self._client

    def on_message(self, _: Any, userdata: Any, message: MQTTMessage) -> None:
        self._receive_queue.put(
            MQTTReceiptMessage(
                client_name=self._client_name,
                userdata=userdata,
                message=message,
            )
        )

    def handle_suback(self, suback: MQTTSubackPayload) -> int:
        topics = self._pending_subacks.pop(suback.mid, [])
        if topics:
            for topic in topics:
                self._pending_subscriptions.remove(topic)
        return len(self._pending_subscriptions)

    def on_subscribe(
        self,
        _: PahoMQTTClient,
        userdata: Any,
        mid: int,
        granted_qos: typing.Sequence[int],
    ) -> None:
        self._receive_queue.put(
            MQTTSubackMessage(
                client_name=self._client_name,
                userdata=userdata,
                mid=mid,
                granted_qos=granted_qos,
            )
        )

    def on_connect(self, _: Any, userdata: Any, flags: dict[str, Any], rc: int) -> None:
        self._receive_queue.put(
            MQTTConnectMessage(
                client_name=self._client_name,
                userdata=userdata,
                flags=flags,
                rc=rc,
            )
        )

    def on_connect_fail(self, _: Any, userdata: Any) -> None:
        self._receive_queue.put(
            MQTTConnectFailMessage(
                client_name=self._client_name,
                userdata=userdata,
            )
        )

    def on_disconnect(self, _: Any, userdata: Any, rc: int) -> None:
        self._pending_subscriptions = set(self._subscriptions.keys())
        self._receive_queue.put(
            MQTTDisconnectMessage(
                client_name=self._client_name,
                userdata=userdata,
                rc=rc,
            )
        )

    def enable_logger(
        self,
        logger: Optional[
            Union[logging.Logger, logging.LoggerAdapter[logging.Logger]]
        ] = None,
    ) -> None:
        self._client.enable_logger(typing.cast(logging.Logger | None, logger))

    def disable_logger(self) -> None:
        self._client.disable_logger()


class MQTTClients:
    clients: Dict[str, MQTTClientWrapper]
    _send_queue: AsyncQueueWriter
    upstream_client: str = ""
    upstream_topic_dst: str = ""
    downstream_client: str = ""

    def __init__(self) -> None:
        self._send_queue = AsyncQueueWriter()
        self.clients = {}

    def add_client(self, settings: LinkConfig) -> None:
        if settings.client_name in self.clients:
            raise ValueError(
                f"ERROR. MQTT client named {settings.client_name} already exists"
            )
        if settings.upstream:
            if self.upstream_client:
                raise ValueError(
                    f"ERROR. upstream client already set as {self.upstream_client}. "
                    f"Client {settings.client_name} may not be set as upstream."
                )
            self.upstream_client = settings.client_name
            self.upstream_topic_dst = settings.spaceheat_name
        if settings.downstream:
            if self.downstream_client:
                raise ValueError(
                    f"ERROR. primary peer client already set as {self.downstream_client}. "
                    f"Client {settings.client_name} may not be set as primary peer."
                )
            self.downstream_client = settings.client_name
        self.clients[settings.client_name] = MQTTClientWrapper(
            client_name=settings.client_name,
            topic_dst=settings.spaceheat_name,
            client_config=settings.mqtt,
            receive_queue=self._send_queue,
        )

    def publish(
        self, client: str, topic: str, payload: bytes, qos: int
    ) -> MQTTMessageInfo:
        return self.clients[client].publish(topic, payload, qos)

    def subscribe(self, client: str, topic: str, qos: int) -> Tuple[int, Optional[int]]:
        return self.clients[client].subscribe(topic, qos)

    def subscribe_all(self, client: str) -> Tuple[int, Optional[int]]:
        return self.clients[client].subscribe_all()

    def unsubscribe(self, client: str, topic: str) -> Tuple[int, Optional[int]]:
        return self.clients[client].unsubscribe(topic)

    def handle_suback(self, suback: MQTTSubackPayload) -> int:
        return self.clients[suback.client_name].handle_suback(suback)

    def stop(self) -> None:
        for client in self.clients.values():
            client.stop()

    def start(
        self, loop: asyncio.AbstractEventLoop, async_queue: asyncio.Queue[Any]
    ) -> None:
        self._send_queue.set_async_loop(loop, async_queue)
        for client in self.clients.values():
            client.start()

    def connected(self, client: str) -> bool:
        return self.clients[client].connected()

    def num_subscriptions(self, client: str) -> int:
        return self.clients[client].num_subscriptions()

    def num_pending_subscriptions(self, client: str) -> int:
        return self.clients[client].num_pending_subscriptions()

    def subscribed(self, client: str) -> bool:
        return self.clients[client].subscribed()

    def enable_loggers(
        self,
        logger: Optional[
            Union[logging.Logger, logging.LoggerAdapter[logging.Logger]]
        ] = None,
    ) -> None:
        for client_name in self.clients:
            self.clients[client_name].enable_logger(logger)

    def disable_loggers(self) -> None:
        for client_name in self.clients:
            self.clients[client_name].disable_logger()

    def client_wrapper(self, client: str) -> MQTTClientWrapper:
        return self.clients[client]

    def upstream(self) -> MQTTClientWrapper:
        return self.clients[self.upstream_client]

    def primary_peer(self) -> MQTTClientWrapper:
        return self.clients[self.downstream_client]

    def topic_dst(self, client: str) -> str:
        if client in self.clients:
            return self.clients[client].topic_dst
        return ""
