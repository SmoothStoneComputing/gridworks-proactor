import logging
import logging.handlers
from pathlib import Path
from typing import Any, Optional

import pytest

from gwproactor import AppSettings, setup_logging
from gwproactor.command_line_utils import command_line_update
from gwproactor.config import (
    DEFAULT_LOG_FILE_NAME,
    LoggingSettings,
    Paths,
    RotatingFileHandlerSettings,
)
from tests.test_misc.test_logging_config import get_exp_formatted_time


def test_get_default_logging_config(
    caplog: pytest.LogCaptureFixture, capsys: pytest.LogCaptureFixture
) -> None:
    paths = Paths()
    paths.mkdirs()
    settings = AppSettings(logging=LoggingSettings(base_log_level=logging.INFO))
    assert paths == settings.paths
    root = logging.getLogger()
    old_root_level = root.getEffectiveLevel()
    pytest_root_handlers = len(root.handlers)
    errors: list[Exception] = []

    setup_logging(command_line_update(settings, message_summary=True), errors=errors)
    assert len(errors) == 0

    # root logger changes
    assert root.getEffectiveLevel() == old_root_level
    assert len(root.handlers) == pytest_root_handlers + 2
    stream_handler: Optional[logging.StreamHandler[Any]] = None
    file_handler: Optional[logging.handlers.RotatingFileHandler] = None
    for i in range(-1, -3, -1):
        handler = root.handlers[i]
        if isinstance(handler, logging.StreamHandler):
            stream_handler = handler
        if isinstance(handler, logging.handlers.RotatingFileHandler):
            file_handler = handler
    assert stream_handler is not None
    assert file_handler is not None
    assert (
        logging.getLogger("gridworks").getEffectiveLevel()
        == settings.logging.base_log_level
    )
    # Sub-logger levels
    logger_names = settings.logging.qualified_logger_names()

    # Check if loggers have been added or renamed
    assert set(LoggingSettings().levels.__pydantic_fields__.keys()) == {
        "message_summary",
        "lifecycle",
        "comm_event",
        "io_loop",
    }
    for field_name in settings.logging.levels.__pydantic_fields__:
        logger_level = logging.getLogger(logger_names[field_name]).level
        settings_level = getattr(settings.logging.levels, field_name)
        assert logger_level == settings_level
    assert (
        logging.getLogger(logger_names["base"]).level == settings.logging.base_log_level
    )

    assert len(caplog.records) == 0

    # Check logger filter by level and message formatting.
    formatter = settings.logging.formatter.create()
    text = ""
    for i, logger_name in enumerate(
        [settings.logging.base_log_name, *list(logger_names.values())]
    ):
        logger = logging.getLogger(logger_name)
        msg = "%d: %s"
        logger.debug(msg, i, logger.name)
        assert len(caplog.records) == 0
        logger.info(msg, i, logger.name)
        # io_loop does not propagate logs, so caplog doesn't see them.
        # (https://github.com/pytest-dev/pytest/issues/3697)
        if logger_name.endswith("io_loop"):
            assert len(caplog.records) == 0
        else:
            assert len(caplog.records) == 1, logger_name
            exp_time = get_exp_formatted_time(
                caplog.records[-1],
                formatter,
                settings.logging.formatter.use_utc,
            )
            exp_msg = f"{exp_time} {msg % (i, logger.name)}\n"
            assert capsys.readouterr().err == exp_msg  # type: ignore[attr-defined]
            text += exp_msg
        caplog.clear()

    # Check file contents
    log_path = Path(settings.paths.log_dir) / DEFAULT_LOG_FILE_NAME
    with log_path.open() as f:
        log_contents = f.read()
    assert log_contents == text

    # Check file contents
    io_loop_log_path = (
        Path(settings.paths.log_dir) / settings.logging.io_loop.file_handler.filename
    )
    with io_loop_log_path.open() as f:
        io_loop_log_contents = f.read()
    assert f"{settings.logging.base_log_name}.io_loop" in io_loop_log_contents


def test_rollover() -> None:
    paths = Paths()
    paths.mkdirs()

    def _log_dir_size() -> int:
        return sum(
            f.stat().st_size
            for f in Path(paths.log_dir).glob("**/*")
            if bool(f.is_file())
        )

    bytes_per_log_file = 50
    num_log_files = 3
    settings = AppSettings(
        logging=LoggingSettings(
            file_handler=RotatingFileHandlerSettings(
                bytes_per_log_file=bytes_per_log_file,
                num_log_files=num_log_files,
            )
        )
    )
    errors: list[Exception] = []
    setup_logging(
        command_line_update(settings, verbose=True),
        errors=errors,
        add_screen_handler=False,
    )
    assert len(errors) == 0
    assert _log_dir_size() == 0
    logger = logging.getLogger("gridworks.general")
    for _ in range(300):
        logger.info("12345678901234567890")
        assert _log_dir_size() <= bytes_per_log_file * num_log_files
    exp_log_log_files = num_log_files + 1  # +1 for io_loop.log
    files_in_log_dir = list(Path(paths.log_dir).glob("**/*"))
    if len(files_in_log_dir) != exp_log_log_files:
        s = f"ERROR. Expected {exp_log_log_files} but got {len(files_in_log_dir)}.\n"
        s += f"Files in {paths.log_dir}:\n"
        for path in files_in_log_dir:
            s += f"\t{path.name}\n"
        raise ValueError(s)
