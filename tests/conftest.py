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

set_test_certificate_cache_dir(Path(__file__).parent / ".certificate_cache")


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--comm-test-verbose",
        action="store_true",
        help="Pass verbose=True to CommTestHelper",
    )
    parser.addoption(
        "--child-verbose",
        action="store_true",
        help="Pass child_verbose=True to CommTestHelper",
    )
    parser.addoption(
        "--parent-verbose",
        action="store_true",
        help="Pass parent_verbose=True to CommTestHelper",
    )
    parser.addoption(
        "--parent-on-screen",
        action="store_true",
        help="Pass parent_on_screen=True to CommTestHelper",
    )
    parser.addoption(
        "--child1-verbose",
        action="store_true",
        help="Pass child1_verbose=True to TreeCommTestHelper",
    )
    parser.addoption(
        "--child2-verbose",
        action="store_true",
        help="Pass child2_verbose=True to TreeCommTestHelper",
    )
    parser.addoption(
        "--child2-on-screen",
        action="store_true",
        help="Pass child2_on_screen=True to TreeCommTestHelper",
    )


@pytest.fixture(autouse=True)
def always_restore_loggers(restore_loggers: Any) -> None: ...  # noqa: F811
