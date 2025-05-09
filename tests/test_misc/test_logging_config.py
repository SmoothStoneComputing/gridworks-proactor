# ruff: noqa: PLR2004, ERA001

import logging
import time
from pathlib import Path

import pytest
from pydantic import ValidationError

from gwproactor.config import (
    DEFAULT_BASE_NAME,
    DEFAULT_BYTES_PER_LOG_FILE,
    DEFAULT_LOG_FILE_NAME,
    DEFAULT_NUM_LOG_FILES,
    FormatterSettings,
    LoggerLevels,
    LoggingSettings,
    RotatingFileHandlerSettings,
)


def test_logger_levels() -> None:
    # Check if fields have been added or renamed
    assert set(LoggerLevels().__pydantic_fields__.keys()) == {
        "message_summary",
        "lifecycle",
        "comm_event",
        "io_loop",
    }

    # Defaults
    levels = LoggerLevels()
    assert levels.message_summary == logging.WARNING
    assert levels.lifecycle == logging.INFO
    assert levels.comm_event == logging.INFO
    assert levels.io_loop == logging.INFO

    # Set parameters
    levels = LoggerLevels(
        message_summary=2,
        lifecycle=3,
        comm_event=4,
        io_loop=5,
    )
    assert levels.message_summary == 2
    assert levels.lifecycle == 3
    assert levels.comm_event == 4
    assert levels.io_loop == 5

    # Level conversion
    with pytest.raises(ValidationError):
        LoggerLevels(message_summary="FOO")

    levels = LoggerLevels(
        message_summary="Critical",
        lifecycle="DEBUG",
        comm_event="debug",
        io_loop="WarNing",
    )
    assert levels.message_summary == logging.CRITICAL
    assert levels.lifecycle == logging.DEBUG
    assert levels.comm_event == logging.DEBUG
    assert levels.io_loop == logging.WARNING

    # qualified_names()
    base_name = "foo"
    assert levels.qualified_logger_names(base_name) == {
        field_name: f"{base_name}.{field_name}"
        for field_name in levels.__pydantic_fields__
    }

    # logger_names_to_levels()
    assert levels.logger_names_to_levels(base_name) == {
        "foo.message_summary": {"level": 50},
        "foo.lifecycle": {"level": 10},
        "foo.comm_event": {"level": 10},
        "foo.io_loop": {"level": 30},
    }

    # set_logger_names_to_levels() - all fields set
    assert levels.set_logger_names_to_levels(
        base_name
    ) == levels.logger_names_to_levels(base_name)
    # only some fields set
    levels = LoggerLevels(comm_event=2)
    assert levels.set_logger_names_to_levels(base_name) == {
        "foo.comm_event": {"level": 2},
    }
    # no fields set
    assert LoggerLevels().set_logger_names_to_levels(base_name) == {}


def test_logging_settings() -> None:
    # Check if loggers have been added or renamed
    assert set(LoggingSettings().levels.__pydantic_fields__.keys()) == {
        "message_summary",
        "lifecycle",
        "comm_event",
        "io_loop",
    }

    # Defaults
    logging_settings = LoggingSettings()
    assert logging_settings.base_log_name == DEFAULT_BASE_NAME
    assert logging_settings.base_log_level == logging.WARNING
    assert logging_settings.levels.message_summary == logging.WARNING
    assert logging_settings.levels.lifecycle == logging.INFO
    assert logging_settings.levels.comm_event == logging.INFO
    assert logging_settings.levels.io_loop == logging.INFO

    # constructor settings
    logging_settings = LoggingSettings(
        base_log_name="foo",
        base_log_level=1,
        levels=LoggerLevels(
            message_summary=2,
            lifecycle=3,
            comm_event=4,
            io_loop=5,
        ),
    )
    assert logging_settings.base_log_name == "foo"
    assert logging_settings.base_log_level == 1
    assert logging_settings.levels.message_summary == 2
    assert logging_settings.levels.lifecycle == 3
    assert logging_settings.levels.comm_event == 4
    assert logging_settings.levels.io_loop == 5

    # qualified_names()
    logging_settings = LoggingSettings()
    exp_logger_names = {
        field_name: f"gridworks.{field_name}"
        for field_name in logging_settings.levels.__pydantic_fields__
    }
    exp_logger_names["base"] = logging_settings.base_log_name
    assert logging_settings.qualified_logger_names() == exp_logger_names

    # logger_levels()
    assert logging_settings.logger_levels() == {
        "gridworks": {"level": 30},
        "gridworks.message_summary": {"level": 30},
        "gridworks.lifecycle": {"level": 20},
        "gridworks.comm_event": {"level": 20},
        "gridworks.io_loop": {"level": 20},
    }

    # set_logger_levels() - no fields set
    assert logging_settings.set_logger_levels() == {}

    # some fields set
    logging_settings = LoggingSettings(levels=LoggerLevels(lifecycle=2))
    assert logging_settings.set_logger_levels() == {
        "gridworks.lifecycle": {"level": 2},
    }

    # custom base name and level dicts
    logging_settings = LoggingSettings(
        base_log_name="foo", base_log_level=0, levels=LoggerLevels(message_summary=1)
    )
    assert logging_settings.qualified_logger_names() == {
        "base": "foo",
        "message_summary": "foo.message_summary",
        "lifecycle": "foo.lifecycle",
        "comm_event": "foo.comm_event",
        "io_loop": "foo.io_loop",
    }
    assert logging_settings.logger_levels() == {
        "foo": {"level": 0},
        "foo.message_summary": {"level": 1},
        "foo.lifecycle": {"level": 20},
        "foo.comm_event": {"level": 20},
        "foo.io_loop": {"level": 20},
    }
    assert logging_settings.set_logger_levels() == {"foo.message_summary": {"level": 1}}

    # verbose()
    logging_settings = LoggingSettings()
    assert not logging_settings.verbose()
    logging_settings.base_log_level = logging.INFO
    assert logging_settings.verbose()

    # message_summary_enabled()
    assert not logging_settings.message_summary_enabled()
    logging_settings.levels.message_summary = 20
    assert logging_settings.message_summary_enabled()


def get_exp_formatted_time(
    record: logging.LogRecord, formatter: logging.Formatter, use_utc: bool
) -> str:
    time_used = (
        time.gmtime(record.created) if use_utc else time.localtime(record.created)
    )
    if formatter.default_msec_format is not None:
        return formatter.default_msec_format % (
            time.strftime(formatter.default_time_format, time_used),
            record.msecs,
        )
    return ""


def _test_formatter_settings(settings: FormatterSettings) -> None:
    formatter = settings.create()
    record = logging.makeLogRecord({"msg": "bla %s %d", "args": ("biz", 1)})
    got_formatted_time = formatter.formatTime(record, formatter.datefmt)
    if settings.use_utc:
        created_time_struct = time.gmtime(record.created)
    else:
        created_time_struct = time.localtime(record.created)
    strftimed = time.strftime(
        logging.Formatter.default_time_format, created_time_struct
    )
    assert got_formatted_time.startswith(strftimed)
    exp_formatted_time = get_exp_formatted_time(record, formatter, settings.use_utc)
    assert got_formatted_time == exp_formatted_time
    formatted = formatter.format(record)
    assert formatted.startswith(exp_formatted_time)
    assert formatted.endswith(record.msg % record.args)


def test_formatter_settings() -> None:
    settings = FormatterSettings()
    assert settings.use_utc is False
    _test_formatter_settings(settings)
    settings.use_utc = True
    _test_formatter_settings(settings)


def test_rotating_file_handler_settings(tmp_path: Path) -> None:
    settings = RotatingFileHandlerSettings()
    handler = settings.create(tmp_path, FormatterSettings().create())
    assert handler.level == logging.NOTSET
    assert handler.maxBytes == DEFAULT_BYTES_PER_LOG_FILE
    assert handler.backupCount == DEFAULT_NUM_LOG_FILES - 1
    assert handler.stream.name == str(tmp_path / DEFAULT_LOG_FILE_NAME)
    bytes_per_log_file = 10
    num_log_files = 3
    settings = RotatingFileHandlerSettings(
        level=logging.INFO,
        bytes_per_log_file=bytes_per_log_file,
        num_log_files=num_log_files,
    )
    handler = settings.create(tmp_path, FormatterSettings().create())
    assert handler.level == logging.INFO
    assert handler.maxBytes == bytes_per_log_file
    assert handler.backupCount == num_log_files - 1
    assert handler.stream.name == str(tmp_path / DEFAULT_LOG_FILE_NAME)
