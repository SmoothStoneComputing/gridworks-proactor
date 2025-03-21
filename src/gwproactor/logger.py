import contextlib
import datetime
import logging
from typing import Any, Mapping, Optional, Sequence, TypeAlias


class MessageSummary:
    """Helper class for formating message summaries message receipt/publication single line summaries."""

    DEFAULT_FORMAT = (
        "  {direction:15s}  {src_dst:50s}  {broker_flag}  {arrow:2s}  {topic:80s}"
        "  {payload_type:25s}{message_id}"
    )

    @classmethod
    def format(  # noqa: PLR0913
        cls,
        direction: str,
        src: str,
        dst: str,
        topic: str,
        *,
        payload_object: Any = None,
        broker_flag: str = " ",
        timestamp: Optional[datetime.datetime] = None,
        include_timestamp: bool = False,
        message_id: str = "",
    ) -> str:
        """
        Formats a single line summary of message receipt/publication.

        Args:
            direction: "IN" or "OUT"
            src: The node name of the sending or receiving actor.
            dst: The node name of the inteded recipient
            topic: The destination or source topic.
            payload_object: The payload of the message.
            broker_flag: "*" for the "gw" broker.
            timestamp: datetime.dateime.now(tz=datetime.timezone.utc) by default.
            include_timestamp: whether timestamp is prepended to output.
            message_id: The message id. Ignored if empty.

        Returns:
            Formatted string.
        """
        with contextlib.suppress(Exception):
            if timestamp is None:
                timestamp = datetime.datetime.now(tz=datetime.timezone.utc)
            if include_timestamp:
                format_ = "{timestamp}  " + cls.DEFAULT_FORMAT
            else:
                format_ = cls.DEFAULT_FORMAT
            direction = direction.strip()
            if direction.startswith(("OUT", "SND")):
                arrow = "->"
            elif direction.startswith(("IN", "RCV")):  # noqa: PIE810
                arrow = "<-"
            else:
                arrow = "? "
            if hasattr(payload_object, "payload"):
                payload_object = payload_object.payload
            if hasattr(payload_object, "__class__"):
                payload_str = payload_object.__class__.__name__
            else:
                payload_str = type(payload_object)
            max_msg_id_len = 11
            if message_id and len(message_id) > max_msg_id_len:
                message_id = f" {message_id[:max_msg_id_len-3]}..."
            src_dst = f"{src} to {dst}"
            return format_.format(
                timestamp=timestamp.isoformat(),
                direction=direction,
                src_dst=src_dst,
                broker_flag=broker_flag,
                arrow=arrow,
                topic=f"[{topic}]",
                payload_type=payload_str,
                message_id=message_id,
            )
        return ""


LoggerAdapterT = logging.LoggerAdapter[logging.Logger]
LoggerOrAdapter: TypeAlias = logging.Logger | logging.LoggerAdapter[logging.Logger]


class CategoryLoggerInfo:
    logger: LoggerOrAdapter
    default_level: int = logging.INFO
    default_disabled = False

    def __init__(
        self, logger: LoggerOrAdapter, default_level: int = logging.INFO
    ) -> None:
        self.logger = logger
        self.default_level = default_level
        self.default_disabled = getattr(logger, "disabled", False)


class ProactorLogger(LoggerAdapterT):
    MESSAGE_DELIMITER_WIDTH = 88
    MESSAGE_ENTRY_DELIMITER = "+" * MESSAGE_DELIMITER_WIDTH
    MESSAGE_EXIT_DELIMITER = "-" * MESSAGE_DELIMITER_WIDTH

    message_summary_logger: logging.Logger
    lifecycle_logger: logging.Logger
    comm_event_logger: logging.Logger
    category_loggers: dict[str, CategoryLoggerInfo]

    def __init__(  # noqa: PLR0913
        self,
        base: str,
        message_summary: str,
        lifecycle: str,
        comm_event: str,
        *,
        extra: Optional[dict[str, Any]] = None,
        category_logger_names: Optional[Sequence[str]] = None,
        **_kwargs: Mapping[str, Any],
    ) -> None:
        super().__init__(logging.getLogger(base), extra=extra)
        self.message_summary_logger = logging.getLogger(message_summary)
        self.lifecycle_logger = logging.getLogger(lifecycle)
        self.comm_event_logger = logging.getLogger(comm_event)
        self.category_loggers = {}
        if category_logger_names is not None:
            for category_logger_name in category_logger_names:
                self.add_category_logger(category_logger_name)

    @property
    def general_enabled(self) -> bool:
        return self.logger.isEnabledFor(logging.INFO)

    @property
    def message_summary_enabled(self) -> bool:
        return self.message_summary_logger.isEnabledFor(logging.INFO)

    @property
    def path_enabled(self) -> bool:
        return self.message_summary_logger.isEnabledFor(logging.DEBUG)

    @property
    def lifecycle_enabled(self) -> bool:
        return self.lifecycle_logger.isEnabledFor(logging.INFO)

    @property
    def comm_event_enabled(self) -> bool:
        return self.comm_event_logger.isEnabledFor(logging.INFO)

    def message_summary(  # noqa: PLR0913
        self,
        *,
        direction: str,
        src: str,
        dst: str,
        topic: str,
        payload_object: Any = None,
        broker_flag: str = " ",
        timestamp: Optional[datetime.datetime] = None,
        message_id: str = "",
    ) -> None:
        if self.message_summary_logger.isEnabledFor(logging.INFO):
            self.message_summary_logger.info(
                MessageSummary.format(
                    direction=direction,
                    src=src,
                    dst=dst,
                    topic=topic,
                    payload_object=payload_object,
                    broker_flag=broker_flag,
                    timestamp=timestamp,
                    message_id=message_id,
                )
            )

    def path(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.message_summary_logger.debug(msg, *args, **kwargs)

    def lifecycle(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.lifecycle_logger.info(msg, *args, **kwargs)

    def comm_event(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self.comm_event_logger.info(msg, *args, **kwargs)

    def message_enter(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if self.path_enabled:
            self.path("")
            self.path(self.MESSAGE_ENTRY_DELIMITER)
            self.path(msg, *args, **kwargs)

    def message_exit(self, msg: str, *args: Any, **kwargs: Any) -> None:
        if self.path_enabled:
            self.path(msg, *args, **kwargs)
            self.path(self.MESSAGE_EXIT_DELIMITER)

    def category_logger_name(self, category: str) -> str:
        return f"{self.name}.{category}"

    def category_logger(self, category: str) -> Optional[LoggerOrAdapter]:
        """Get existing category logger"""
        logger_info = self.category_loggers.get(category)
        if logger_info is not None:
            return logger_info.logger
        return None

    def add_category_logger(
        self,
        category: str = "",
        level: int = logging.INFO,
        logger: Optional[LoggerOrAdapter] = None,
    ) -> LoggerOrAdapter:
        if logger is None:
            if not category:
                raise ValueError(
                    "ERROR. add_category_logger() requires category value "
                    "unless logger is provided."
                )
            logger = self.category_logger(category)
            if logger is None:
                logger = logging.getLogger(self.category_logger_name(category))
                logger.setLevel(level)
                self.category_loggers[category] = CategoryLoggerInfo(
                    logger=logger,
                    default_level=level,
                )
        else:
            if not category:
                category = logger.name
                self_prefix = f"{self.name}."
                if category.startswith(self_prefix):
                    category = category[len(self_prefix) :]
            if category in self.category_loggers:
                raise ValueError(
                    "ERROR. add_category_logger() got explicit logger "
                    f"named {logger.name}, categorized as {category}, but "
                    f"logger for that category is already present."
                )
            level = logger.getEffectiveLevel()
            self.category_loggers[category] = CategoryLoggerInfo(
                logger=logger,
                default_level=level,
            )
        return logger

    def reset_default_category_levels(self) -> None:
        for logger_info in self.category_loggers.values():
            logger_info.logger.setLevel(logger_info.default_level)
            if hasattr(logger_info.logger, "disabled"):
                logger_info.logger.disabled = logger_info.default_disabled

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} "
            f"{self.logger.name}, "
            f"{self.message_summary_logger.name}, "
            f"{self.lifecycle_logger.name}, "
            f"{self.comm_event_logger.name}>"
        )
