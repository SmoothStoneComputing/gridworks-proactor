"""A broker that refuses the CONNECT (bad password) must not read as connected.

Runs a private mosquitto with a password file on an ephemeral port, so the
refusal is real: the shared test broker on 1883 allows anonymous clients and
accepts any password.
"""

import shutil
import socket
import subprocess
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import SecretStr

from gwproactor.config import MQTTClient
from gwproactor.config.mqtt import TLSInfo
from gwproactor.links import StateName
from gwproactor_test import LiveTest
from gwproactor_test.dummies.pair.child import DummyChildSettings

MOSQUITTO_USER = "child"
MOSQUITTO_PASSWORD = "right-password"  # noqa: S105
WRONG_PASSWORD = "wrong-password"  # noqa: S105


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
def password_broker_port(tmp_path: Path) -> Iterator[int]:
    """Start a mosquitto that requires MOSQUITTO_USER / MOSQUITTO_PASSWORD."""
    mosquitto = shutil.which("mosquitto")
    mosquitto_passwd = shutil.which("mosquitto_passwd")
    if mosquitto is None or mosquitto_passwd is None:
        pytest.skip("mosquitto and mosquitto_passwd must be on the PATH")
    port = free_port()
    password_file = tmp_path / "passwd"
    subprocess.run(  # noqa: S603
        [
            mosquitto_passwd,
            "-c",
            "-b",
            str(password_file),
            MOSQUITTO_USER,
            MOSQUITTO_PASSWORD,
        ],
        check=True,
        capture_output=True,
    )
    conf = tmp_path / "mosquitto.conf"
    conf.write_text(
        f"listener {port} 127.0.0.1\n"
        "allow_anonymous false\n"
        f"password_file {password_file}\n"
    )
    proc = subprocess.Popen(  # noqa: S603
        [mosquitto, "-c", str(conf)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.time() + 5
        while time.time() < deadline:
            with socket.socket() as s:
                s.settimeout(0.2)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(0.05)
        else:
            raise RuntimeError(f"mosquitto did not open port {port}")
        yield port
    finally:
        proc.terminate()
        proc.wait(timeout=5)


@pytest.mark.asyncio
async def test_refused_connect_is_not_a_connection(
    password_broker_port: int,
    request: pytest.FixtureRequest,
) -> None:
    """
    Test:
        (connecting -> mqtt_connect_failed -> connecting) on a refused CONNACK;
        the refusal is logged with its reason code and no connect event fires.
    """
    async with LiveTest(
        add_child=True,
        child_app_settings=DummyChildSettings(
            parent=MQTTClient(
                port=password_broker_port,
                username=MOSQUITTO_USER,
                password=SecretStr(WRONG_PASSWORD),
                tls=TLSInfo(use_tls=False),
            )
        ),
        request=request,
    ) as h:
        link = h.child_to_parent_link
        counts = h.child_to_parent_stats.comm_event_counts
        h.start_child()
        await h.await_for(
            lambda: counts["gridworks.event.comm.mqtt.connect.failed"] >= 1,
            "ERROR waiting for the refused connect to be reported",
        )
        assert counts["gridworks.event.comm.mqtt.connect"] == 0
        assert link.state == StateName.connecting
        assert not link.active_for_send()
        log_file = Path(h.child.settings.paths.log_dir) / "proactor.log"
        await h.await_for(
            lambda: log_file.exists() and "Not authorized" in log_file.read_text(),
            "ERROR waiting for the refusal reason to reach the log",
        )
