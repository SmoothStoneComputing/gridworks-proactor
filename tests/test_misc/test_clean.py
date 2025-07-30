import os
from pathlib import Path
from typing import Sequence

import pytest
from gwproto import HardwareLayout

from gwproactor.config.paths import Paths
from gwproactor_test import (
    DefaultTestEnv,
    hardware_layout_test_path,
    set_hardware_layout_test_path,
)
from gwproactor_test.clean import DUMMY_HARDWARE_LAYOUT_PATH
from tests.test_misc.test_config import assert_paths

DUMMY_SCADA_DISPLAY_NAME = "Scada in dummy hardware layout file"
DEFAULT_TEST_SCADA_DISPLAY_NAME = "Little Orange House Main Scada"


def assert_paths_exist(paths: Sequence[str | Path], expect_exists: bool) -> None:
    for entry in paths:
        path = Path(entry)
        assert (
            path.exists() == expect_exists
        ), f"Expected {path}.exists() == {expect_exists}"


def assert_default_env_paths(home: Path | str) -> None:
    home = Path(home)
    paths = Paths()
    assert_paths(
        paths,
        home=home,
        base=Path("gridworks"),
        name=Path("scada"),
        relative_path=Path("gridworks/scada"),
        data_dir=home / ".local/share/gridworks/scada",
        config_dir=home / ".config/gridworks/scada",
        certs_dir=home / ".config/gridworks/scada/certs",
        event_dir=home / ".local/share/gridworks/scada/event",
        log_dir=home / ".local/state/gridworks/scada/log",
        hardware_layout=home / ".config/gridworks/scada/hardware-layout.json",
    )
    assert str(paths.config_home) == os.environ["XDG_CONFIG_HOME"]
    assert str(paths.data_home) == os.environ["XDG_DATA_HOME"]
    assert str(paths.state_home) == os.environ["XDG_STATE_HOME"]
    assert_paths_exist(
        [
            paths.config_home,
            paths.config_dir,
            paths.hardware_layout,
        ],
        True,
    )
    assert_paths_exist([paths.certs_dir], False)
    created_by_mkdirs = [
        paths.data_home,
        paths.data_dir,
        paths.state_home,
        paths.event_dir,
        paths.log_dir,
    ]
    assert_paths_exist(created_by_mkdirs, False)


def test_default_env(tmp_path: Path) -> None:
    assert_default_env_paths(tmp_path)


def test_change_default_env(tmp_path: Path) -> None:
    assert_default_env_paths(tmp_path)

    new_home = tmp_path / "new_home"
    with DefaultTestEnv(new_home).context():
        assert_default_env_paths(new_home)

    assert_default_env_paths(tmp_path)


@pytest.mark.parametrize(
    "default_test_env",
    [(DefaultTestEnv(src_test_layout=DUMMY_HARDWARE_LAYOUT_PATH))],
    indirect=True,
)
def test_parametrize_hardware_layout_test_path(
    default_test_env: DefaultTestEnv,
) -> None:
    hardware_layout_path = Path(Paths().hardware_layout)
    assert (
        hardware_layout_path.open().read() == DUMMY_HARDWARE_LAYOUT_PATH.open().read()
    )
    layout = HardwareLayout.load(hardware_layout_path)
    assert layout.node("s").DisplayName == DUMMY_SCADA_DISPLAY_NAME


def assert_expected_layout(
    home: Path, expected_scada_display_name: str = DEFAULT_TEST_SCADA_DISPLAY_NAME
) -> None:
    default_paths = Paths()
    assert (
        Path(default_paths.hardware_layout)
        == home / ".config/gridworks/scada/hardware-layout.json"
    )
    default_layout = HardwareLayout.load(default_paths.hardware_layout)
    assert default_layout.node("s").DisplayName == expected_scada_display_name


def test_hardware_layout_test_path(tmp_path: Path) -> None:
    # Verify default layout
    assert_expected_layout(tmp_path)

    # Create a new test env which uses DUMMY_HARDWARE_LAYOUT_PATH
    original_layout_path = hardware_layout_test_path()
    new_home = tmp_path / "new_home"
    try:
        assert (
            set_hardware_layout_test_path(DUMMY_HARDWARE_LAYOUT_PATH)
            == DUMMY_HARDWARE_LAYOUT_PATH
        )
        with DefaultTestEnv(new_home).context():
            assert_expected_layout(new_home, DUMMY_SCADA_DISPLAY_NAME)
    finally:
        set_hardware_layout_test_path(original_layout_path)
