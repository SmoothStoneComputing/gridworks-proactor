import argparse
import contextlib
import logging
import logging.config
import sys
import syslog
import traceback
from typing import Optional

from gwproactor.config.app_settings import AppSettings


def enable_aiohttp_logging() -> None:
    import logging

    for logger_name in [
        "aiohttp.access",
        "aiohttp.client",
        "aiohttp.internal",
        "aiohttp.server",
        "aiohttp.web",
        "aiohttp.websocket",
    ]:
        logger_ = logging.getLogger(logger_name)
        handler_ = logging.StreamHandler()
        handler_.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s.%(msecs)03d   %(message)s",
                datefmt="%Y-%m-%d  %H:%M:%S",
            )
        )
        logger_.addHandler(handler_)
        logger_.setLevel(logging.INFO)
        logger_.setLevel(logging.DEBUG)


def format_exceptions(exceptions: list[Exception]) -> str:
    s = ""
    try:
        if exceptions:
            for i, exception in enumerate(exceptions):
                s += f"++ Exception {i + 1:2d} / {len(exceptions)} ++++++++++++++++++++++++++++++++++++++++++++++++++++\n"
                try:
                    s += "".join(traceback.format_exception(exception))
                    s += "\n"
                except Exception as e:  # noqa: BLE001
                    try:
                        s += f"ERROR formatting traceback for {e}\n"
                    except:  # noqa: E722
                        s += "UNEXPECTED ERROR formatting exception.\n"
                s += f"-- Exception {i + 1:2d} / {len(exceptions)} ----------------------------------------------------\n\n"
    except:  # noqa: E722
        s += "UNEXPECTED ERROR formatting exception.\n"
    return s


def setup_logging(  # noqa: C901, PLR0912, PLR0915
    args: argparse.Namespace,
    settings: AppSettings,
    *,
    errors: Optional[list[Exception]] = None,
    add_screen_handler: bool = True,
    root_gets_handlers: bool = True,
) -> None:
    """Get python logging config based on parsed command line args, defaults, environment variables and logging config file.

    The order of precedence is:

        1. Command line arguments
        2. Environment
        3. Defaults

    """
    if errors is None:
        errors = []
    else:
        errors.clear()
    config_finished = False
    try:
        # Take any arguments from command line
        try:
            if getattr(args, "verbose", None):
                settings.logging.base_log_level = logging.INFO
                settings.logging.levels.message_summary = logging.DEBUG
            elif getattr(args, "message_summary", None):
                settings.logging.levels.message_summary = logging.INFO
        except Exception as e:  # noqa: BLE001
            errors.append(e)

        # Create formatter from settings
        try:
            formatter = settings.logging.formatter.create()
        except Exception as e:  # noqa: BLE001
            formatter = None
            errors.append(e)

        # Set application logger levels
        for logger_name, logger_settings in settings.logging.logger_levels().items():
            try:
                logger = logging.getLogger(logger_name)
                logger.setLevel(logger_settings["level"])
            except Exception as e:  # noqa: BLE001, PERF203
                errors.append(e)

        # Create handlers from settings, add them to root logger
        if root_gets_handlers:
            base_logger = logging.getLogger()
        else:
            base_logger = logging.getLogger(settings.logging.base_log_name)
            base_logger.propagate = False
        if add_screen_handler:
            try:
                # try not to add more than one screen handler.
                if not any(
                    h
                    for h in base_logger.handlers
                    if isinstance(h, logging.StreamHandler)
                    and (h.stream is sys.stderr or h.stream is sys.stdout)
                ):
                    screen_handler = logging.StreamHandler()
                    if formatter is not None:
                        screen_handler.setFormatter(formatter)
                    base_logger.addHandler(screen_handler)
            except Exception as e:  # noqa: BLE001
                errors.append(e)
        screen_handlers = [
            h
            for h in base_logger.handlers
            if isinstance(h, logging.StreamHandler)
            and (h.stream is sys.stderr or h.stream is sys.stdout)
        ]
        if len(screen_handlers) > 1:
            errors.append(
                ValueError(
                    "x More than 1 screen handlers  "
                    f"{base_logger.name}  {len(screen_handlers)}  "
                    f"\n\tstream handlers: {screen_handlers},  "
                    f"\n\tstream handlers: {[type(x) for x in screen_handlers]},  "
                    f"from all handlers {base_logger.handlers}"
                )
            )
        else:
            try:
                file_handler = settings.logging.file_handler.create(
                    settings.paths.log_dir, formatter
                )
                if formatter is not None:
                    file_handler.setFormatter(formatter)
                base_logger.addHandler(file_handler)
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        # Enable IOLoop logging if requested
        if getattr(args, "aiohttp_logging", None):
            enable_aiohttp_logging()

        config_finished = True
    except Exception as e:  # noqa: BLE001
        config_finished = False
        errors.append(e)
    finally:
        # Try to tell user if something went wrong
        if errors:
            with contextlib.suppress(Exception):
                s = "ERROR in setup_logging():\n" + format_exceptions(errors)
                if config_finished:
                    logging.error(s)
                else:
                    syslog.syslog(s)
                    print(s)  # noqa: T201
