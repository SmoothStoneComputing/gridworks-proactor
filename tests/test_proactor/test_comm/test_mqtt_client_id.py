"""The upstream link's MQTT client_id is the proactor's instance id: one uuid4
minted per process, never regenerated across reconnects. Every other link
carries its own uuid4, so links sharing a broker do not evict each other."""

import pytest
from gwproto.property_format import is_uuid4_str

from gwproactor.links import StateName
from gwproactor_test.live_test_helper import LiveTest
from gwproactor_test.wait import await_for


@pytest.mark.asyncio
async def test_mqtt_client_id_is_the_instance_id(
    request: pytest.FixtureRequest,
) -> None:
    async with LiveTest(add_child=True, add_parent=True, request=request) as h:
        child = h.child
        parent = h.parent

        # a uuid4, distinct per proactor
        is_uuid4_str(child.instance_id)
        is_uuid4_str(parent.instance_id)
        assert child.instance_id != parent.instance_id

        # the upstream link carries it as its client_id; the rest are distinct uuid4s
        client_ids = {
            name: wrapper.client_id
            for name, wrapper in child.links.mqtt_clients().clients.items()
        }
        assert client_ids[child.upstream_client] == child.instance_id
        for client_id in client_ids.values():
            is_uuid4_str(client_id)
        assert len(set(client_ids.values())) == len(client_ids)

        # unchanged across a disconnect and reconnect
        link = child.links.link(child.upstream_client)
        before = child.instance_id
        connects_after_reconnect = 2
        h.start_child()
        await await_for(
            lambda: link.state not in (StateName.not_started, StateName.connecting),
            3,
            "ERROR waiting for first connect",
            err_str_f=h.summary_str,
        )
        child.force_mqtt_disconnect(child.upstream_client)
        await await_for(
            lambda: child.stats.link(child.upstream_client).comm_event_counts[
                "gridworks.event.comm.mqtt.connect"
            ]
            == connects_after_reconnect,
            3,
            "ERROR waiting for reconnect",
            err_str_f=h.summary_str,
        )
        assert child.instance_id == before
        assert (
            child.links.mqtt_client_wrapper(child.upstream_client).client_id == before
        )
