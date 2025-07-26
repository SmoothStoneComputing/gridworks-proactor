import time
from typing import Literal, Sequence

import aiohttp
import pytest
from gwproto.messages import EventBase

from gwproactor import Proactor, WebEventListener
from gwproactor.actors.web_event_listener import WebEventListenerSettings
from gwproactor.app import ActorConfig
from gwproactor.logging_setup import enable_aiohttp_logging
from gwproactor_test import LiveTest, await_for
from gwproactor_test.dummies import DummyChildApp


class _UploaderApp(DummyChildApp):
    SERVER_NAME = "uploader"
    TEST_PORT = 8082

    def _instantiate_proactor(self) -> Proactor:
        proactor = super()._instantiate_proactor()
        proactor.add_web_server_config(
            name=self.SERVER_NAME,
            host="127.0.0.1",
            port=self.TEST_PORT,
            enabled=True,
            server_kwargs={},
        )
        return proactor

    def _get_actor_nodes(self) -> Sequence[ActorConfig]:
        return [
            ActorConfig(
                node=self.hardware_layout.add_node(
                    node=WebEventListener.default_node(),
                ),
                constructor_args={
                    "settings": WebEventListenerSettings(server_name=self.SERVER_NAME)
                },
            )
        ]


class SomeData(EventBase):
    TimestampUTC: float
    Reading: float
    TypeName: Literal["gridworks.event.some.data"] = "gridworks.event.some.data"


class _UploaderLiveTest(LiveTest):
    @classmethod
    def child_app_type(cls) -> type[_UploaderApp]:
        return _UploaderApp


@pytest.mark.asyncio
async def test_http_event_upload(request: pytest.FixtureRequest) -> None:
    async with _UploaderLiveTest(request=request, add_child=True, add_parent=True) as t:
        enable_aiohttp_logging()
        linkU2I = t.child.links.link(t.child.upstream_client)
        statsI2U = t.parent.stats.link(t.parent.downstream_client)

        t.start_child()
        t.start_parent()

        # wait for link to go active
        await await_for(
            linkU2I.active,
            1,
            "ERROR waiting for uploader to connect to ingester",
            err_str_f=t.summary_str,
        )
        t.child.logger.info("4")
        async with aiohttp.ClientSession() as client:
            response = await client.post(
                f"http://127.0.0.1:{_UploaderApp.TEST_PORT}/events",
                json=SomeData(
                    TimestampUTC=round(time.time(), 3),
                    Reading=38.00,
                ).model_dump(),
            )
        assert response.ok, response.text
        await t.await_for(
            lambda: statsI2U.num_received_by_type["gridworks.event.some.data"] == 1,
            "ERROR waiting for ingester to receive event from uploader",
        )
