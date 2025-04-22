"""Local pytest configuration"""

from pathlib import Path
from typing import Any

import pytest

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
