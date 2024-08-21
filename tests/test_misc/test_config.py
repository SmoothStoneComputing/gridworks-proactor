"""Test config module"""

import shutil
import ssl
from pathlib import Path
from typing import Any

import pytest
from pydantic import SecretStr

from gwproactor.config import MQTTClient, Paths
from gwproactor.config.mqtt import TLSInfo
from gwproactor.config.paths import TLSPaths
from gwproactor_test.dummies import DummyChildSettings


def test_tls_paths():
    # unitialized TLSPaths
    exp = dict(
        ca_cert_path=None,
        cert_path=None,
        private_key_path=None,
    )
    paths = TLSPaths()
    paths_d = paths.dict()
    for k, v in exp.items():
        assert paths_d[k] == v
        assert getattr(paths, k) == v

    # defaults, given a certs_dir and a name
    certs_dir = Path("foo/certs")
    name = "bar"
    exp = dict(
        ca_cert_path=certs_dir / name / "ca.crt",
        cert_path=certs_dir / name / f"{name}.crt",
        private_key_path=certs_dir / name / "private" / f"{name}.pem",
    )
    paths = TLSPaths.defaults(certs_dir, name)
    paths_d = paths.dict()
    for k, v in exp.items():
        assert paths_d[k] == v
        assert getattr(paths, k) == v

    # a value set explicitly
    ca_cert_path = Path("bla/bla/bla")
    exp = dict(
        ca_cert_path=ca_cert_path,
        cert_path=None,
        private_key_path=None,
    )
    paths = TLSPaths(ca_cert_path=ca_cert_path)
    paths_d = paths.dict()
    for k, v in exp.items():
        assert paths_d[k] == v
        assert getattr(paths, k) == v

    # updates for unset values, given a certs_dir and a name
    certs_dir = Path("foo/certs")
    name = "bar"
    exp = dict(
        ca_cert_path=ca_cert_path,
        cert_path=certs_dir / name / f"{name}.crt",
        private_key_path=certs_dir / name / "private" / f"{name}.pem",
    )
    paths = paths.effective_paths(certs_dir, name)
    paths_d = paths.dict()
    for k, v in exp.items():
        assert paths_d[k] == v
        assert getattr(paths, k) == v


def test_tls_paths_mkdirs(clean_test_env, tmp_path) -> None:
    with pytest.raises(ValueError):
        TLSPaths().mkdirs()
    paths = Paths()
    # Get rid of the config dir created inside of tmp_path by clean_test_env
    if paths.config_dir.exists():
        shutil.rmtree(paths.config_dir)
    name = "foo"
    ca_cert_dir = tmp_path / "ca_dir"
    ca_cert_path = ca_cert_dir / "ca.pem"
    tls_paths = TLSPaths(ca_cert_path=ca_cert_path).effective_paths(
        paths.certs_dir, name
    )
    assert not paths.config_dir.exists()
    assert not paths.certs_dir.exists()
    assert not ca_cert_dir.exists()
    assert tls_paths.ca_cert_path == ca_cert_path
    assert not tls_paths.cert_path.parent.exists()
    assert not tls_paths.private_key_path.parent.exists()
    tls_paths.mkdirs()
    assert paths.config_dir.exists()
    assert paths.certs_dir.exists()
    assert tls_paths.ca_cert_path.parent.exists()
    assert tls_paths.cert_path.parent.exists()
    assert tls_paths.private_key_path.parent.exists()


def test_tls_info():
    # unitialized TLSInfo
    exp: dict = dict(
        use_tls=True,
        port=8883,
        paths=dict(
            ca_cert_path=None,
            cert_path=None,
            private_key_path=None,
        ),
        cert_reqs=ssl.CERT_REQUIRED,
        ciphers=None,
        keyfile_password=SecretStr(""),
    )
    info = TLSInfo()
    info_d = info.dict()
    for k, v in exp.items():
        assert info_d[k] == v
        assert getattr(info, k) == v

    # path updates, given a certs_dir and a name
    certs_dir = Path("foo/certs")
    name = "bar"
    info.update_tls_paths(certs_dir, name)
    exp["paths"] = dict(
        ca_cert_path=certs_dir / name / "ca.crt",
        cert_path=certs_dir / name / f"{name}.crt",
        private_key_path=certs_dir / name / "private" / f"{name}.pem",
    )
    info_d = info.dict()
    for k, v in exp.items():
        assert info_d[k] == v
        assert getattr(info, k) == v


def test_mqtt_client_settings():
    """Test MQTTClient"""
    password = "d"
    port = 1883
    exp: dict = dict(
        host="a",
        keepalive=1,
        bind_address="b",
        bind_port=2,
        username="c",
        password=SecretStr(password),
        tls=dict(
            use_tls=True,
            port=8883,
            paths=dict(
                ca_cert_path=None,
                cert_path=None,
                private_key_path=None,
            ),
            cert_reqs=ssl.CERT_REQUIRED,
            ciphers=None,
            keyfile_password=SecretStr(""),
        ),
    )
    settings = MQTTClient(**exp)
    d = settings.dict()
    assert d == dict(exp, port=port)
    for k, v in exp.items():
        assert d[k] == v
        assert getattr(settings, k) == v
    assert settings.port == port
    assert settings.password.get_secret_value() == password

    # path updates, given a cert_dir and a name
    certs_dir = Path("foo/certs")
    name = "bar"
    settings.update_tls_paths(certs_dir, name)
    exp["tls"]["paths"] = dict(
        ca_cert_path=certs_dir / name / "ca.crt",
        cert_path=certs_dir / name / f"{name}.crt",
        private_key_path=certs_dir / name / "private" / f"{name}.pem",
    )
    d = settings.dict()
    assert d == dict(exp, port=port)
    for k, v in exp.items():
        assert d[k] == v
        assert getattr(settings, k) == v


def exp_paths_dict(**kwargs) -> dict:
    default_base = Path("gridworks")
    default_name = Path("scada")
    default_relative_path = default_base / default_name
    home = kwargs.pop("home", Path.home())
    default_data_home = home / ".local" / "share"
    default_state_home = home / ".local" / "state"
    default_config_home = home / ".config"
    default_config_dir = default_config_home / default_relative_path
    exp = dict(
        base=default_base,
        name=default_name,
        relative_path=default_relative_path,
        data_home=default_data_home,
        state_home=default_state_home,
        config_home=default_config_home,
        data_dir=default_data_home / default_relative_path,
        config_dir=default_config_dir,
        certs_dir=default_config_dir / "certs",
        event_dir=default_data_home / default_relative_path / "event",
        log_dir=default_state_home / default_relative_path / "log",
        hardware_layout=default_config_dir / "hardware-layout.json",
    )
    exp.update(**kwargs)
    return exp


def assert_paths(paths: Paths, **kwargs):
    exp = exp_paths_dict(**kwargs)
    for field, exp_value in exp.items():
        got_value = getattr(paths, field)
        if isinstance(got_value, Path) and not isinstance(exp_value, Path):
            exp_value = Path(exp_value)
            exp[field] = exp_value
        assert (
            got_value == exp_value
        ), f"Paths.{field}\n\texp: {exp_value}\n\tgot: {got_value}"
    assert paths.dict() == exp


def test_paths_defaults(clean_test_env, tmp_path):
    assert_paths(Paths(), home=tmp_path)


def test_paths(clean_test_env, tmp_path):
    # base, name
    assert_paths(
        Paths(base="foo", name="bar"),
        home=tmp_path,
        base=Path("foo"),
        name=Path("bar"),
        relative_path=Path("foo/bar"),
        data_dir=tmp_path / ".local/share/foo/bar",
        config_dir=tmp_path / ".config/foo/bar",
        certs_dir=tmp_path / ".config/foo/bar/certs",
        event_dir=tmp_path / ".local/share/foo/bar/event",
        log_dir=tmp_path / ".local/state/foo/bar/log",
        hardware_layout=tmp_path / ".config/foo/bar/hardware-layout.json",
    )

    # explicit relative_path
    assert_paths(
        Paths(relative_path="foo/bar"),
        home=tmp_path,
        relative_path=Path("foo/bar"),
        data_dir=tmp_path / ".local/share/foo/bar",
        config_dir=tmp_path / ".config/foo/bar",
        certs_dir=tmp_path / ".config/foo/bar/certs",
        event_dir=tmp_path / ".local/share/foo/bar/event",
        log_dir=tmp_path / ".local/state/foo/bar/log",
        hardware_layout=tmp_path / ".config/foo/bar/hardware-layout.json",
    )

    # explicit xdg dirs
    assert_paths(
        Paths(data_home="x", state_home="y", config_home="z"),
        home=tmp_path,
        data_home="x",
        state_home="y",
        config_home="z",
        data_dir="x/gridworks/scada",
        event_dir="x/gridworks/scada/event",
        log_dir="y/gridworks/scada/log",
        config_dir="z/gridworks/scada",
        certs_dir="z/gridworks/scada/certs",
        hardware_layout="z/gridworks/scada/hardware-layout.json",
    )

    # explicit working dirs
    assert_paths(
        Paths(data_dir="x", log_dir="y", config_dir="z", event_dir="q"),
        home=tmp_path,
        data_dir="x",
        log_dir="y",
        config_dir="z",
        certs_dir="z/certs",
        event_dir="q",
        hardware_layout="z/hardware-layout.json",
    )

    # explicit hardware_layout
    assert_paths(
        Paths(hardware_layout="foo.json"),
        home=tmp_path,
        hardware_layout="foo.json",
    )

    # set xdg through environment
    clean_test_env.setenv("XDG_DATA_HOME", "/x")
    clean_test_env.setenv("XDG_STATE_HOME", "/y")
    clean_test_env.setenv("XDG_CONFIG_HOME", "/z")
    paths = Paths()
    assert_paths(
        paths,
        home=tmp_path,
        data_home="/x",
        state_home="/y",
        config_home="/z",
        data_dir="/x/gridworks/scada",
        log_dir="/y/gridworks/scada/log",
        config_dir="/z/gridworks/scada",
        certs_dir="/z/gridworks/scada/certs",
        event_dir="/x/gridworks/scada/event",
        hardware_layout="/z/gridworks/scada/hardware-layout.json",
    )

    paths2 = paths.copy(name="foo")
    assert_paths(
        paths2,
        home=tmp_path,
        data_home="/x",
        state_home="/y",
        config_home="/z",
        name="foo",
        relative_path="gridworks/foo",
        data_dir="/x/gridworks/foo",
        log_dir="/y/gridworks/foo/log",
        config_dir="/z/gridworks/foo",
        certs_dir="/z/gridworks/foo/certs",
        event_dir="/x/gridworks/foo/event",
        hardware_layout="/z/gridworks/foo/hardware-layout.json",
    )


def test_paths_mkdirs(clean_test_env, tmp_path):  # noqa
    paths = Paths()
    assert not paths.data_dir.exists()
    # Get rid of the config dir created inside of tmp_path by clean_test_env
    if paths.config_dir.exists():
        shutil.rmtree(paths.config_dir)
    assert not paths.config_dir.exists()
    assert not paths.log_dir.exists()
    paths.mkdirs()
    assert paths.data_dir.exists()
    assert paths.config_dir.exists()
    assert paths.log_dir.exists()


def _assert_eq(
    tag: str,
    field_name: str,
    exp: Any,
    got: Any,
):
    assert exp == got, (
        f"ERROR on field <{field_name}> for test {tag}\n"
        f"\texp: {exp}\n"
        f"\tgot: {got}"
    )


def _assert_child_paths_update(
    child: DummyChildSettings,
    test_name: str,
    param_type: str,
    certs_dir: Path | str,
    ca_cert_path: Path | str,
    cert_path: Path | str,
    private_key_path: Path | str,
):
    tag = f"[{test_name}], with param type: {param_type}"
    _assert_eq(tag, "certs_dir", Path(certs_dir), child.paths.certs_dir)
    _assert_eq(
        tag,
        "ca_cert_path",
        Path(ca_cert_path),
        child.parent_mqtt.tls.paths.ca_cert_path,
    )
    _assert_eq(tag, "cert_path", Path(cert_path), child.parent_mqtt.tls.paths.cert_path)
    _assert_eq(
        tag,
        "private_key_path",
        Path(private_key_path),
        child.parent_mqtt.tls.paths.private_key_path,
    )


def test_proactor_settings_root_validators(clean_test_env) -> None:
    clean_test_env.setenv("XDG_CONFIG_HOME", "/z")

    # no paths specification
    child = DummyChildSettings()
    assert child.paths.certs_dir == Path("/z/gridworks/child/certs")
    assert child.parent_mqtt.tls.paths.ca_cert_path == Path(
        "/z/gridworks/child/certs/parent_mqtt/ca.crt"
    )
    assert child.parent_mqtt.tls.paths.cert_path == Path(
        "/z/gridworks/child/certs/parent_mqtt/parent_mqtt.crt"
    )
    assert child.parent_mqtt.tls.paths.private_key_path == Path(
        "/z/gridworks/child/certs/parent_mqtt/private/parent_mqtt.pem"
    )

    # Test path parameter setting, using Paths objects and dicts, which happens when variables set in .env files.
    explicit_ca_cert_path = Path("/q/ca_cert.pem")
    for (
        test_name,
        certs_dir,
        ca_cert_path,
        cert_path,
        private_key_path,
        children,
    ) in [
        [
            "Defaults",
            "/z/gridworks/child/certs",
            "/z/gridworks/child/certs/parent_mqtt/ca.crt",
            "/z/gridworks/child/certs/parent_mqtt/parent_mqtt.crt",
            "/z/gridworks/child/certs/parent_mqtt/private/parent_mqtt.pem",
            [("no params", DummyChildSettings())],
        ],
        [
            "Parameters set, but with defaults",
            "/z/gridworks/child/certs",
            "/z/gridworks/child/certs/parent_mqtt/ca.crt",
            "/z/gridworks/child/certs/parent_mqtt/parent_mqtt.crt",
            "/z/gridworks/child/certs/parent_mqtt/private/parent_mqtt.pem",
            [
                ("obj", DummyChildSettings(paths=Paths())),
                ("dict", DummyChildSettings(paths={})),  # noqa
            ],
        ],
        [
            "Path name specified",
            "/z/gridworks/foo/certs",
            "/z/gridworks/foo/certs/parent_mqtt/ca.crt",
            "/z/gridworks/foo/certs/parent_mqtt/parent_mqtt.crt",
            "/z/gridworks/foo/certs/parent_mqtt/private/parent_mqtt.pem",
            [
                ("obj", DummyChildSettings(paths=Paths(name="foo"))),
                ("dict", DummyChildSettings(paths=dict(name="foo"))),  # noqa
            ],
        ],
        [
            "Paths with name specified *and* explicit CA cert path",
            "/z/gridworks/foo/certs",
            explicit_ca_cert_path,
            "/z/gridworks/foo/certs/parent_mqtt/parent_mqtt.crt",
            "/z/gridworks/foo/certs/parent_mqtt/private/parent_mqtt.pem",
            [
                (
                    "obj",
                    DummyChildSettings(
                        paths=Paths(name="foo"),
                        parent_mqtt=MQTTClient(
                            tls=TLSInfo(
                                paths=TLSPaths(ca_cert_path=explicit_ca_cert_path)
                            )
                        ),
                    ),
                ),
                (
                    "dict",
                    DummyChildSettings(
                        paths=dict(name="foo"),  # noqa
                        parent_mqtt=dict(  # noqa
                            tls=dict(paths=dict(ca_cert_path=explicit_ca_cert_path))
                        ),
                    ),
                ),
            ],
        ],
    ]:
        for param_type, child in children:
            _assert_child_paths_update(
                child=child,
                test_name=test_name,
                param_type=param_type,
                certs_dir=certs_dir,
                ca_cert_path=ca_cert_path,
                cert_path=cert_path,
                private_key_path=private_key_path,
            )
