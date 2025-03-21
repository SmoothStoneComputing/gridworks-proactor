import argparse
import logging
import sys
import traceback
from pathlib import Path
from typing import Optional, Type, TypeVar

import dotenv
import rich
from pydantic import BaseModel
from pydantic_settings import BaseSettings

from gwproactor import Proactor, ProactorSettings, setup_logging
from gwproactor.config import MQTTClient
from gwproactor.config.paths import TLSPaths

LOGGING_FORMAT = "%(asctime)s %(message)s"


def missing_tls_paths(paths: TLSPaths) -> list[tuple[str, Optional[Path]]]:
    missing = []
    for path_name in paths.model_fields:
        path = getattr(paths, path_name)
        if path is None or not Path(path).exists():
            missing.append((path_name, path))
    return missing


def check_tls_paths_present(
    model: BaseModel | BaseSettings, *, raise_error: bool = True
) -> str:
    missing_str = ""
    for k in model.model_fields:
        v = getattr(model, k)
        if isinstance(v, MQTTClient) and v.tls.use_tls:
            missing_paths = missing_tls_paths(v.tls.paths)
            if missing_paths:
                missing_str += f"client {k}\n"
                for path_name, path in missing_paths:
                    missing_str += f"  {path_name:20s}  {path}\n"
    if missing_str:
        error_str = f"ERROR. TLS usage requested but the following files are missing:\n{missing_str}"
        if raise_error:
            raise ValueError(error_str)
    else:
        error_str = ""
    return error_str


ProactorT = TypeVar("ProactorT", bound=Proactor)
ProactorSettingsT = TypeVar("ProactorSettingsT", bound=ProactorSettings)


def get_settings(
    *,
    settings_type: Optional[Type[ProactorSettingsT]] = None,
    settings: Optional[ProactorSettingsT] = None,
    env_file: str | Path = ".env",
) -> ProactorSettingsT:
    if settings_type is None:
        if settings is None:
            raise ValueError("ERROR. Specifiy exactly one of (settings_type, settings)")
    else:
        if settings is not None:
            raise ValueError("ERROR. Specify exactly one of (settings_type, settings)")
        settings = settings_type(_env_file=str(env_file))
    return settings


def print_settings(
    *,
    settings_type: Optional[Type[ProactorSettingsT]] = None,
    settings: Optional[ProactorSettingsT] = None,
    env_file: str | Path = ".env",
) -> None:
    dotenv_file = dotenv.find_dotenv(str(env_file))
    rich.print(
        f"Env file: <{dotenv_file}>  exists:{env_file and Path(dotenv_file).exists()}"
    )
    settings = get_settings(
        settings_type=settings_type, settings=settings, env_file=dotenv_file
    )
    rich.print(settings)
    missing_tls_paths_ = check_tls_paths_present(settings, raise_error=False)
    if missing_tls_paths_:
        rich.print(missing_tls_paths_)


def get_proactor(  # noqa: PLR0913
    name: str,
    proactor_type: Type[ProactorT],
    *,
    settings_type: Optional[Type[ProactorSettingsT]] = None,
    settings: Optional[ProactorSettingsT] = None,
    dry_run: bool = False,
    verbose: bool = False,
    message_summary: bool = False,
    env_file: str | Path = ".env",
    run_in_thread: bool = False,
    add_screen_handler: bool = True,
) -> ProactorT:
    dotenv_file = dotenv.find_dotenv(str(env_file))
    dotenv_file_debug_str = (
        f"Env file: <{dotenv_file}>  exists:{Path(dotenv_file).exists()}"
    )
    settings = get_settings(
        settings_type=settings_type, settings=settings, env_file=dotenv_file
    )
    if dry_run:
        rich.print(dotenv_file_debug_str)
        rich.print(settings)
        missing_tls_paths_ = check_tls_paths_present(settings, raise_error=False)
        if missing_tls_paths_:
            rich.print(missing_tls_paths_)
        rich.print("Dry run. Doing nothing.")
        sys.exit(0)
    else:
        settings.paths.mkdirs()
        args = argparse.Namespace(
            verbose=verbose,
            message_summary=message_summary,
        )
        setup_logging(args, settings, add_screen_handler=add_screen_handler)
        logger = logging.getLogger(
            settings.logging.qualified_logger_names()["lifecycle"]
        )
        logger.info("")
        logger.info(dotenv_file_debug_str)
        logger.info("Settings:")
        logger.info(settings.model_dump_json(indent=2))
        rich.print(settings)
        check_tls_paths_present(settings)
        proactor = proactor_type(name=name, settings=settings)
        if run_in_thread:
            logger.info("run_async_actors_main() starting")
            proactor.run_in_thread()
    return proactor


async def run_async_main(  # noqa: PLR0913
    name: str,
    proactor_type: Type[ProactorT],
    *,
    settings_type: Optional[Type[ProactorSettingsT]] = None,
    settings: Optional[ProactorSettingsT] = None,
    env_file: str | Path = ".env",
    dry_run: bool = False,
    verbose: bool = False,
    message_summary: bool = False,
) -> None:
    settings = get_settings(
        settings_type=settings_type,
        settings=settings,
        env_file=dotenv.find_dotenv(str(env_file)),
    )
    exception_logger: logging.Logger | logging.LoggerAdapter[logging.Logger] = (
        logging.getLogger(settings.logging.base_log_name)
    )
    try:
        proactor = get_proactor(
            name=name,
            proactor_type=proactor_type,
            settings=settings,
            dry_run=dry_run,
            verbose=verbose,
            message_summary=message_summary,
        )
        # noinspection PyTypeChecker
        exception_logger = proactor.logger
        try:
            await proactor.run_forever()
        finally:
            proactor.stop()
    except SystemExit:
        pass
    except KeyboardInterrupt:
        pass
    except BaseException as e:
        try:
            exception_logger.exception(
                "ERROR in run_async_actors_main. Shutting down: " "[%s] / [%s]",
                e,  # noqa: TRY401
                type(e),  # noqa: TRY401
            )
        except:  # noqa: E722
            traceback.print_exception(e)
        raise
