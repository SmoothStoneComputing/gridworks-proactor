import logging
import time
import typing
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Iterable

from pydantic import BaseModel, field_validator

from .paths import DEFAULT_BASE_NAME

DEFAULT_LOGGING_FORMAT = "%(asctime)s %(message)s"
DEFAULT_FRACTIONAL_SECOND_FORMAT = "%s.%03d"
DEFAULT_LOG_FILE_NAME = "proactor.log"
DEFAULT_BYTES_PER_LOG_FILE = 2 * 1024 * 1024
DEFAULT_NUM_LOG_FILES = 10


class FormatterSettings(BaseModel):
    fmt: str = DEFAULT_LOGGING_FORMAT
    datefmt: str = ""
    default_msec_format: str = DEFAULT_FRACTIONAL_SECOND_FORMAT
    use_utc: bool = False

    def create(self) -> logging.Formatter:
        formatter = logging.Formatter(
            fmt=self.fmt,
            datefmt=self.datefmt,
        )
        formatter.default_msec_format = self.default_msec_format
        if self.use_utc:
            formatter.converter = time.gmtime
        return formatter


class RotatingFileHandlerSettings(BaseModel):
    filename: str = DEFAULT_LOG_FILE_NAME
    bytes_per_log_file: int = DEFAULT_BYTES_PER_LOG_FILE
    num_log_files: int = DEFAULT_NUM_LOG_FILES
    level: int = logging.NOTSET

    def create(
        self, log_dir: Path | str, formatter: logging.Formatter | None
    ) -> RotatingFileHandler:
        handler = logging.handlers.RotatingFileHandler(
            filename=Path(log_dir) / self.filename,
            maxBytes=self.bytes_per_log_file,
            backupCount=self.num_log_files - 1,
        )
        if formatter is not None:
            handler.setFormatter(formatter)
        if self.level != logging.NOTSET:
            handler.setLevel(self.level)
        return handler


class LoggerLevels(BaseModel):
    message_summary: int | str = logging.WARNING
    lifecycle: int | str = logging.INFO
    comm_event: int | str = logging.INFO

    def qualified_logger_names(self, base_log_name: str) -> dict[str, str]:
        return {
            field_name: f"{base_log_name}.{field_name}"
            for field_name in self.__pydantic_fields__
        }

    def _logger_levels(
        self, base_log_name: str, fields: Iterable[str]
    ) -> dict[str, dict[str, int]]:
        return {
            f"{base_log_name}.{field_name}": {"level": getattr(self, field_name)}
            for field_name in fields
        }

    def logger_names_to_levels(self, base_log_name: str) -> dict[str, dict[str, int]]:
        return self._logger_levels(base_log_name, self.__pydantic_fields__)

    def set_logger_names_to_levels(
        self, base_log_name: str
    ) -> dict[str, dict[str, int]]:
        return self._logger_levels(base_log_name, self.model_fields_set)

    @field_validator("*")
    @classmethod
    def logging_level_str_to_int(cls, v: int | str) -> int:
        try:
            int_v = int(v)
        except:  # noqa: E722
            if hasattr(v, "upper"):
                v = v.upper()
            got_v = logging.getLevelName(v)
            if not isinstance(got_v, int):
                raise ValueError(  # noqa: B904, TRY004
                    f"Could not convert level ({v}/{type(v)}) to an int, either by cast or by logging.getLevelName()"
                )
            int_v = got_v
        return int_v


class LoggingSettings(BaseModel):
    base_log_name: str = DEFAULT_BASE_NAME
    base_log_level: int = logging.WARNING
    levels: LoggerLevels = LoggerLevels()
    formatter: FormatterSettings = FormatterSettings()
    file_handler: RotatingFileHandlerSettings = RotatingFileHandlerSettings()

    def qualified_logger_names(self) -> typing.Mapping[str, str]:
        return dict(
            self.levels.qualified_logger_names(self.base_log_name),
            base=self.base_log_name,
        )

    def logger_levels(self) -> dict[str, dict[str, int]]:
        d = dict(
            self.levels.logger_names_to_levels(self.base_log_name),
        )
        d[self.base_log_name] = {"level": self.base_log_level}
        return d

    def set_logger_levels(self) -> dict[str, dict[str, int]]:
        return self.levels.set_logger_names_to_levels(self.base_log_name)

    def verbose(self) -> bool:
        return self.base_log_level <= logging.INFO

    def message_summary_enabled(self) -> bool:
        return typing.cast(int, self.levels.message_summary) <= logging.INFO
