"""Local pytest configuration"""

import json
from pathlib import Path
from typing import Any

import pytest
import rich
from _pytest._code.code import ExceptionChainRepr, ReprEntry  # noqa
from _pytest.nodes import Item
from _pytest.reports import TestReport
from _pytest.runner import CallInfo

from gwproactor_test import (
    clean_test_env,  # noqa: F401
    default_test_env,  # noqa: F401
    restore_loggers,  # noqa: F401
)
from gwproactor_test.certs import set_test_certificate_cache_dir
from gwproactor_test.pytest_options import add_live_test_options

set_test_certificate_cache_dir(Path(__file__).parent / ".certificate_cache")


def pytest_addoption(parser: pytest.Parser) -> None:
    add_live_test_options(parser, include_tree=True)


@pytest.fixture(autouse=True)
def always_restore_loggers(restore_loggers: Any) -> None: ...  # noqa: F811


@pytest.hookimpl(wrapper=True, tryfirst=True)
def pytest_runtest_makereport(item: Item, call: CallInfo[None]) -> TestReport | None:  # type: ignore[misc]
    rep = yield  # noqa
    try:
        fail_dict: dict[str, Any]
        if rep.when == "call" and rep.failed:
            fail_file = Path("output/failed_tests.json")
            if not fail_file.parent.exists():
                fail_file.parent.mkdir(parents=True)
            if not fail_file.exists():
                fail_dict = {"total": 0}
            else:
                with fail_file.open() as f:
                    try:
                        fail_dict = json.loads(f.read())
                    except Exception as e:  # noqa: BLE001
                        rich.print(
                            f"ERROR treating {fail_file} as json. Truncating. "
                            f"Excetpion: {type(e)}, {e}"
                        )
                        fail_dict = {"total": 0}
            if "total" not in fail_dict:
                fail_dict["total"] = 0
            fail_dict["total"] += 1
            lineno = f"{rep.location[1] if rep.location[1] is not None else 0}"
            test_file_path = Path(rep.location[0])
            if (
                isinstance(rep.longrepr, ExceptionChainRepr)
                and rep.longrepr.reprtraceback is not None
            ):
                reprtraceback = rep.longrepr.reprtraceback
                if reprtraceback.reprentries is not None:
                    repentries = reprtraceback.reprentries
                    if (
                        len(repentries)
                        and isinstance(repentries[0], ReprEntry)
                        and repentries[0].reprfileloc is not None
                    ):
                        loc = repentries[0].reprfileloc
                        error_path = Path(loc.path)
                        if error_path.name == test_file_path.name:
                            lineno = f"{loc.lineno}"
                        else:
                            lineno = f"{error_path.name}:{loc.lineno}"
            line_key = "failed_lines"
            if rep.nodeid not in fail_dict:
                fail_dict[rep.nodeid] = {"total": 0, line_key: {}}
            if lineno not in fail_dict[rep.nodeid][line_key]:
                fail_dict[rep.nodeid][line_key][lineno] = 0
            fail_dict[rep.nodeid]["total"] += 1
            fail_dict[rep.nodeid][line_key][lineno] += 1
            with fail_file.open("w") as f:
                f.write(json.dumps(fail_dict, sort_keys=True, indent=2))
                f.write("\n")
    except Exception as e:  # noqa: BLE001
        rich.print(f"ERROR in pytest_runtest_makereport. " f"Exception: {type(e)}, {e}")
    return rep
