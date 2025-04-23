# ruff: noqa: S101

import logging
import sys
from types import TracebackType
from typing import Iterator, Optional, Self, Sequence, Type

import pytest

from gwproactor.config import DEFAULT_BASE_NAME, LoggerLevels


class LoggerGuard:
    level: int
    propagate: bool
    disabled: bool
    handlers: set[logging.Handler]
    filters: set[logging.Filter]

    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self.level = logger.level
        self.propagate = logger.propagate
        self.disabled = logger.disabled
        self.handlers = set(logger.handlers)
        self.filters = set(logger.filters)  # type: ignore[arg-type]

    def restore(self) -> None:
        screen_handlers = [
            h
            for h in self.logger.handlers
            if isinstance(h, logging.StreamHandler)
            and (h.stream is sys.stderr or h.stream is sys.stdout)
        ]
        if len(screen_handlers) > 1:
            raise ValueError(
                "More than 1 screen handlers  "
                f"{self.logger.name}  {len(screen_handlers)}  "
                f"stream handlers: {screen_handlers},  "
                f"from all handlers {self.logger.handlers}"
            )
        self.logger.setLevel(self.level)
        self.logger.propagate = self.propagate
        self.logger.disabled = self.disabled
        curr_handlers = set(self.logger.handlers)
        for handler in curr_handlers - self.handlers:
            self.logger.removeHandler(handler)
        for handler in self.handlers - curr_handlers:
            self.logger.addHandler(handler)
        curr_filters: set[logging.Filter] = set(self.logger.filters)  # type: ignore[arg-type]
        for filter_ in curr_filters - self.filters:
            self.logger.removeFilter(filter_)
        for filter_ in self.filters - curr_filters:
            self.logger.addFilter(filter_)
        assert set(self.logger.handlers) == self.handlers
        assert set(self.logger.filters) == self.filters

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        self.restore()
        return True


class LoggerGuards:
    guards: dict[str, LoggerGuard]

    def __init__(self, extra_logger_names: Optional[Sequence[str]] = None) -> None:
        logger_names = self.default_logger_names()
        if extra_logger_names is not None:
            logger_names = logger_names.union(set(extra_logger_names))
        self.guards = {
            logger_name: LoggerGuard(logging.getLogger(logger_name))
            for logger_name in logger_names
        }

    def add_loggers(self, logger_names: Sequence[str]) -> None:
        for logger_name in logger_names:
            if logger_name not in self.guards:
                self.guards[logger_name] = LoggerGuard(logging.getLogger(logger_name))

    def restore(self) -> None:
        for guard in self.guards.values():
            guard.restore()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc: Optional[BaseException],
        tb: Optional[TracebackType],
    ) -> bool:
        self.restore()
        return True

    @classmethod
    def default_logger_names(cls) -> set[str]:
        return {"root"}.union(
            LoggerLevels().qualified_logger_names(DEFAULT_BASE_NAME).values()
        ).union(logging.root.manager.loggerDict.keys())


@pytest.fixture
def restore_loggers() -> Iterator[LoggerGuards]:
    num_root_handlers = len(logging.getLogger().handlers)
    guards = LoggerGuards()
    yield guards
    guards.restore()
    new_num_root_handlers = len(logging.getLogger().handlers)
    if new_num_root_handlers != num_root_handlers:
        raise ValueError(
            f"ARRG. root handlers: {num_root_handlers} -> {new_num_root_handlers}  handlers:\n\t"
            f"{logging.getLogger().handlers}"
        )
