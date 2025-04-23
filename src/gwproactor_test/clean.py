"""Local pytest configuration"""

import contextlib
import os
import shutil
from pathlib import Path
from typing import Any, Generator, Optional

import dotenv
import pytest
from _pytest.monkeypatch import MonkeyPatch

from gwproactor.config import DEFAULT_LAYOUT_FILE, Paths
from gwproactor_test.dummies import DUMMY_CHILD_ENV_PREFIX, DUMMY_PARENT_ENV_PREFIX

TEST_DOTENV_PATH = "tests/.env-gwproactor-test"
TEST_DOTENV_PATH_VAR = "GWPROACTOR_TEST_DOTENV_PATH"
TEST_HARDWARE_LAYOUT_PATH = Path(__file__).parent / "config" / DEFAULT_LAYOUT_FILE
DUMMY_TEST_HARDWARE_LAYOUT_PATH = (
    Path(__file__).parent / "config" / "dummy-hardware-layout.json"
)
DEFAULT_PREFIXES = [
    DUMMY_CHILD_ENV_PREFIX,
    DUMMY_PARENT_ENV_PREFIX,
]


class DefaultTestEnv:
    """Context manager for monkeypatched environment with:
        - all vars starting with any entry in DEFAULT_PREFIXES removed
        - vars loaded from test env file, if specified
        - xdg vars set relative to passed in xdg_home parameter
        - working config directory created via xdg_home
        - test hardware layout file copied into working config directory.

    >>> tmp_path = Path("/home/bla")
    >>> with DefaultTestEnv(tmp_path).context() as mpatch:
    ...     assert AppSettings().paths.hardware_layout == Path("/home/bla/.config/gridworks/scada/hardware-layout.json")
    ...     assert AppSettings().paths.hardware_layout.exists()


    The default test env file is tests/.env-gwproactor-test. This path can be overridden with the environment variable
    GWPROACTOR_TEST_DOTENV_PATH. The test env file will be ignored if the GWPROACTOR_TEST_DOTENV_PATH environment
    variable exists but is empty or the specified path does not exist.

    Hardware file copying can be suppressed by passing copy_test_layout as False.

    Working test directory creation can be suppressed by passing xdg_home as None.
    """

    DEFAULT_PREFIXES = DEFAULT_PREFIXES

    xdg_home: Path | None = None
    src_test_layout: Path = TEST_HARDWARE_LAYOUT_PATH
    copy_test_layout: bool = True
    use_test_dotenv: bool = True
    prefixes: list[str]

    def __init__(
        self,
        *,
        xdg_home: Path | str | None = None,
        src_test_layout: Path = TEST_HARDWARE_LAYOUT_PATH,
        copy_test_layout: bool = True,
        use_test_dotenv: bool = True,
        prefixes: Optional[list[str]] = None,
    ) -> None:
        if isinstance(xdg_home, str):
            xdg_home = Path(xdg_home) if xdg_home else None
        self.xdg_home = xdg_home
        self.src_test_layout = src_test_layout
        self.copy_test_layout = copy_test_layout
        self.use_test_dotenv = use_test_dotenv
        if prefixes is None:
            self.prefixes = self.get_default_prefixes()
        else:
            self.prefixes = prefixes[:]

    @classmethod
    def get_default_prefixes(cls) -> list[str]:
        return cls.DEFAULT_PREFIXES[:]

    @contextlib.contextmanager
    def context(self) -> Generator[MonkeyPatch, None, None]:
        """Produce monkeypatch context manager from this object"""
        mpatch = MonkeyPatch()
        with mpatch.context() as m:
            self.clean_env(m)
            self.load_test_dotenv()
            self.setup_text_xdg_home(m)
            yield m

    def setup_text_xdg_home(self, m: MonkeyPatch) -> None:
        if self.xdg_home is not None:
            m.setenv("XDG_DATA_HOME", str(self.xdg_home / ".local" / "share"))
            m.setenv("XDG_STATE_HOME", str(self.xdg_home / ".local" / "state"))
            m.setenv("XDG_CONFIG_HOME", str(self.xdg_home / ".config"))
            if self.copy_test_layout:
                paths = Paths()
                Path(paths.hardware_layout).parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(self.src_test_layout, paths.hardware_layout)

    def clean_env(self, m: MonkeyPatch) -> None:
        for env_var in os.environ:
            for prefix in self.prefixes:
                if env_var.startswith(prefix):
                    m.delenv(env_var)

    def load_test_dotenv(self) -> None:
        if self.use_test_dotenv:
            test_dotenv_file = os.getenv(TEST_DOTENV_PATH_VAR)
            if test_dotenv_file is None:
                test_dotenv_file = TEST_DOTENV_PATH
            if test_dotenv_file:
                test_dotenv_path = Path(test_dotenv_file)
                if test_dotenv_path.exists():
                    dotenv.load_dotenv(dotenv_path=test_dotenv_path)


@pytest.fixture(autouse=True)
def default_test_env(
    request: Any, tmp_path: Path
) -> Generator[MonkeyPatch, None, None]:
    """Automatically used fixture producing monkeypatched environment with:
        - all vars starting with any entry in DefaultTestEnv.DEFAULT_PREFIXES removed
        - vars loaded from test env file, if specified
        - xdg vars set relative to passed in xdg_home parameter
        - working config directory created via xdg_home
        - test hardware layout file copied into working config directory.

    Note that this fixture is run before _every_ test.

    The behavior of this fixture can be customized by:
        1. Modifying the contents of tests/.env-gwproactor-test.
        2. Changing the the path to the test dotenv file via the GWPROACTOR_TEST_DOTENV_PATH environment variable.
        3. Explicitly passing and parametrizing this fixture. For example, to run a test with a different hardware
            layout file, such as DUMMY_TEST_HARDWARE_LAYOUT_PATH:

        >>> from gwproactor_test.clean import DUMMY_TEST_HARDWARE_LAYOUT_PATH
        >>> @pytest.mark.parametrize("default_test_env", [(DefaultTestEnv(src_test_layout=DUMMY_TEST_HARDWARE_LAYOUT_PATH)], indirect=True)
        >>> def test_something(default_test_env):
        >>>    assert Paths().hardware_layout.open().read() == DUMMY_TEST_HARDWARE_LAYOUT_PATH.open().read()


    """
    test_env = getattr(request, "param", DefaultTestEnv())
    if test_env.xdg_home is None:
        test_env.xdg_home = tmp_path
    with test_env.context() as mpatch:
        yield mpatch


@pytest.fixture
def clean_test_env(request: Any, tmp_path: Path) -> Generator[MonkeyPatch, None, None]:
    """Get a monkeypatched environment with all vars starting with any entry in DEFAULT_PREFIXES *removed* (and none
    loaded from any dotenv file)."""
    test_env = getattr(request, "param", DefaultTestEnv(use_test_dotenv=False))
    if test_env.xdg_home is None:
        test_env.xdg_home = tmp_path
    with test_env.context() as mpatch:
        yield mpatch
