import shutil
import textwrap
from pathlib import Path

import pytest
from typer.testing import CliRunner

from gwproactor.config import Paths
from gwproactor_test.clean import hardware_layout_test_path
from gwproactor_test.cli import app as cli_app
from gwproactor_test.dummies.tree.atn import DummyAtnApp
from gwproactor_test.dummies.tree.scada1 import DummyScada1App
from gwproactor_test.dummies.tree.scada2 import DummyScada2App

runner = CliRunner()


def test_cli_completes(request: pytest.FixtureRequest) -> None:
    """This test just verifies that clis can execute dry-runs and help without
    exception. It does not attempt to test content of execution."""
    for app_type in [DummyAtnApp, DummyScada1App, DummyScada2App]:
        paths = Paths(name=app_type.paths_name())
        paths.mkdirs(parents=True, exist_ok=True)
        shutil.copyfile(Path(hardware_layout_test_path()), paths.hardware_layout)
    command: list[str]
    for command in [
        [],
        ["gen-dummy-certs", "--dry-run"],
        ["admin"],
        ["admin", "config"],
        ["admin", "run", "--help"],
        ["admin", "set-relay", "--help"],
        ["atn", "config"],
        ["atn", "run", "--dry-run"],
        ["scada1", "config"],
        ["scada1", "run", "--dry-run"],
        ["scada2", "config"],
        ["scada2", "run", "--dry-run"],
    ]:
        result = runner.invoke(cli_app, command)
        result_str = (
            f"exit code: {result.exit_code}\n"
            f"\t{result!s} from command\n"
            f"\t<gwtest {' '.join(command)}> with output\n"
            f"{textwrap.indent(result.output, '        ')}"
        )
        assert result.exit_code == 0, result_str
