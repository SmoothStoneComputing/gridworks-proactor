import argparse  # noqa: I001
import logging
import typing
import warnings
from typing import Any, Mapping

import pytest

from gwproactor import ProactorLogger, ProactorSettings, setup_logging
from gwproactor.config import Paths
from gwproactor_test import LoggerGuards


def _prlogger(settings: ProactorSettings) -> ProactorLogger:
    return ProactorLogger(
        **typing.cast(Mapping[str, Any], settings.logging.qualified_logger_names()),
    )


def test_proactor_logger(caplog: Any) -> None:
    paths = Paths()
    paths.mkdirs()
    settings = ProactorSettings()
    with LoggerGuards():
        errors: list[Exception] = []
        setup_logging(argparse.Namespace(), settings, errors=errors)
        assert len(errors) == 0
        logger = _prlogger(settings)
        assert not logger.isEnabledFor(logging.INFO)
        assert not logger.message_summary_enabled
        assert logger.message_summary_logger.level == logging.WARNING
        assert not logger.path_enabled
        logger.message_summary(direction="", src="", dst="", topic="")
        assert len(caplog.records) == 0
        logger.path("foo")
        assert len(caplog.records) == 0
        assert logger.lifecycle_enabled
        assert logger.comm_event_enabled

    errors = []
    setup_logging(
        argparse.Namespace(verbose=True),
        settings,
        errors=errors,
        add_screen_handler=False,
    )
    assert len(errors) == 0
    logger = _prlogger(settings)
    assert logger.isEnabledFor(logging.INFO)
    assert logger.message_summary_enabled
    assert logger.message_summary_logger.level == logging.DEBUG
    assert logger.path_enabled
    assert logger.lifecycle_enabled
    assert logger.comm_event_enabled
    logger.info("info")
    if len(caplog.records) != 1:
        warnings.warn(
            f"len(caplog.records) ({len(caplog.records)}) != 1  (#1)",
            stacklevel=2,
        )
    caplog.clear()
    for function_name in ["path", "lifecycle", "comm_event"]:
        getattr(logger, function_name)(function_name)
        if len(caplog.records) != 1:
            warnings.warn(
                f"len(caplog.records) ({len(caplog.records)}) != 1  (#2)",
                stacklevel=2,
            )
        caplog.clear()
    logger.message_summary(
        direction="IN",
        src="x",
        dst="y",
        topic="z",
    )
    if len(caplog.records) != 1:
        warnings.warn(
            f"len(caplog.records) ({len(caplog.records)}) != 1  (#3)",
            stacklevel=2,
        )


def test_category_logger() -> None:
    # default - no categories
    settings = ProactorSettings()
    prlogger = _prlogger(settings)
    assert not prlogger.category_loggers

    # One cat logger in constructor
    cat_name = "Spots"
    prlogger = ProactorLogger(
        category_logger_names=[cat_name],
        **typing.cast(Mapping[str, Any], settings.logging.qualified_logger_names()),
    )
    assert len(prlogger.category_loggers) == 1
    logger = prlogger.category_logger(cat_name)
    assert logger is not None
    assert logger.getEffectiveLevel() == logging.INFO
    assert hasattr(logger, "disabled")
    assert not logger.disabled
    assert logger.name == f"{prlogger.name}.{cat_name}"

    # Check valid arguments for add
    with pytest.raises(ValueError):
        prlogger.add_category_logger()

    # query for missing logger does not crash
    prlogger = _prlogger(settings)
    assert prlogger.category_logger("foo") is None

    # Add cat loggers in various ways
    # Add by name
    cat_name = "Max"
    logger = prlogger.add_category_logger(cat_name, level=logging.DEBUG)
    assert logger is not None
    assert logger is prlogger.category_logger(cat_name)
    assert logger.getEffectiveLevel() == logging.DEBUG
    assert hasattr(logger, "disabled")
    assert not logger.disabled
    assert logger.name == f"{prlogger.name}.{cat_name}"

    # Add by an explicit logger
    cat_name = "Sandy"
    qualified_name = f"{prlogger.name}.{cat_name}"
    explicit_logger = logging.getLogger(qualified_name)
    explicit_logger.setLevel(logging.WARNING)
    explicit_logger.disabled = True
    logger = prlogger.add_category_logger(logger=explicit_logger)
    assert logger is explicit_logger
    assert logger is prlogger.category_logger(cat_name)
    assert logger.getEffectiveLevel() == logging.WARNING
    assert hasattr(logger, "disabled")
    assert logger.disabled
    assert logger.name == f"{prlogger.name}.{cat_name}"

    # Add by an explicit logger with name not qualifed on ProactorLogger's
    # base name
    cat_name = "Oreo"
    explicit_logger = logging.getLogger(cat_name)
    explicit_logger.setLevel(logging.ERROR)
    logger = prlogger.add_category_logger(logger=explicit_logger)
    assert logger is explicit_logger
    assert logger is prlogger.category_logger(cat_name)
    assert logger.getEffectiveLevel() == logging.ERROR
    assert hasattr(logger, "disabled")
    assert not logger.disabled
    assert logger.name == cat_name

    # Test resetting the category logger levels
    Max = prlogger.category_logger("Max")
    assert Max is not None
    assert hasattr(Max, "disabled")
    Max.setLevel(logging.INFO)
    Max.disabled = True
    Sandy = prlogger.category_logger("Sandy")
    assert Sandy is not None
    assert hasattr(Sandy, "disabled")
    Sandy.setLevel(logging.INFO)
    Sandy.disabled = False
    Oreo = prlogger.category_logger("Oreo")
    assert Oreo is not None
    assert hasattr(Oreo, "disabled")
    Oreo.setLevel(logging.INFO)
    Oreo.disabled = True
    prlogger.reset_default_category_levels()
    assert Max.getEffectiveLevel() == logging.DEBUG
    assert not Max.disabled
    assert Sandy.getEffectiveLevel() == logging.WARNING
    assert Sandy.disabled
    assert Oreo.getEffectiveLevel() == logging.ERROR
    assert not Oreo.disabled
