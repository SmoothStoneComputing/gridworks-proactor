import asyncio
import logging
import traceback
from pathlib import Path
from typing import Any, Optional

import dotenv
import rich
import typer

from gwproactor.app import App, SubTypes
from gwproactor.codecs import CodecFactory
from gwproactor.config import Paths
from gwproactor.config.app_settings import AppSettings
from gwproactor.logging_setup import setup_logging


def command_line_update(  # noqa: PLR0913
    app_settings: AppSettings,
    *,
    verbose: bool = False,
    message_summary: bool = False,
    io_loop_verbose: bool = False,
    io_loop_on_screen: bool = False,
    aiohttp_logging: bool = False,
    paho_logging: bool = False,
    **kwargs: Any,  # noqa: ARG001
) -> AppSettings:
    if verbose:
        app_settings.logging.base_log_level = logging.INFO
        app_settings.logging.levels.message_summary = logging.DEBUG
    elif message_summary:
        app_settings.logging.levels.message_summary = logging.INFO
    if io_loop_verbose:
        app_settings.logging.levels.io_loop = logging.DEBUG
    if io_loop_on_screen:
        app_settings.logging.io_loop.on_screen = True
    if aiohttp_logging:
        app_settings.logging.aiohttp_logging = True
    if paho_logging:
        app_settings.logging.paho_logging = True
    return app_settings


def make_app_for_cli(  # noqa: PLR0913
    *,
    app_settings: AppSettings,
    app_type: type[App] = App,
    codec_factory: Optional[CodecFactory] = None,
    sub_types: Optional[SubTypes] = None,
    env_file: Optional[str | Path] = ".env",
    dry_run: bool = False,
    add_screen_handler: bool = True,
) -> App:
    dotenv_file = dotenv.find_dotenv(str(env_file), usecwd=True)
    app = app_type(
        app_settings=app_settings,
        codec_factory=codec_factory,
        sub_types=sub_types,
        env_file=env_file,
    )
    dotenv_file_debug_str = (
        f"Env file: <{dotenv_file}>  exists:{Path(dotenv_file).exists()}"
    )
    if dry_run:
        rich.print(dotenv_file_debug_str)
        rich.print(app.settings)
        missing_tls_paths_ = app.settings.check_tls_paths_present(raise_error=False)
        if missing_tls_paths_:
            rich.print(missing_tls_paths_)
        rich.print("Dry run. Doing nothing.")
    else:
        app.settings.paths.mkdirs()
        setup_logging(app.settings, add_screen_handler=add_screen_handler)
        logger = logging.getLogger(
            app.settings.logging.qualified_logger_names()["lifecycle"]
        )
        logger.info("")
        logger.info(dotenv_file_debug_str)
        logger.info("Settings:")
        logger.info(app.settings.model_dump_json(indent=2))
        rich.print(app.settings)
        app.settings.check_tls_paths_present()
        app.instantiate()
    return app


def app_main(  # noqa: PLR0913
    *,
    paths_name: Optional[str] = None,
    paths: Optional[Paths] = None,
    app_settings: Optional[AppSettings] = None,
    app_type: type[App] = App,
    codec_factory: Optional[CodecFactory] = None,
    sub_types: Optional[SubTypes] = None,
    env_file: Optional[str | Path] = ".env",
    dry_run: bool = False,
    verbose: bool = False,
    message_summary: bool = False,
    io_loop_verbose: bool = False,
    io_loop_on_screen: bool = False,
    aiohttp_logging: bool = False,
    paho_logging: bool = False,
    add_screen_handler: bool = True,
    run_in_thread: bool = False,
    return_int: bool = False,
) -> int:
    dotenv_file = dotenv.find_dotenv(str(env_file), usecwd=True)
    if app_settings is None:
        app_settings = app_type.get_settings(
            paths_name=paths_name,
            paths=paths,
            env_file=dotenv_file,
        )
    app_settings = command_line_update(
        app_settings=app_settings,
        verbose=verbose,
        message_summary=message_summary,
        io_loop_verbose=io_loop_verbose,
        io_loop_on_screen=io_loop_on_screen,
        aiohttp_logging=aiohttp_logging,
        paho_logging=paho_logging,
    )
    exception_logger: logging.Logger | logging.LoggerAdapter[logging.Logger] = (
        logging.getLogger(app_settings.logging.base_log_name)
    )
    ret = 0
    try:
        app = make_app_for_cli(
            app_type=app_type,
            app_settings=app_settings,
            codec_factory=codec_factory,
            sub_types=sub_types,
            env_file=dotenv_file,
            dry_run=dry_run,
            add_screen_handler=add_screen_handler,
        )
        if not dry_run:
            if run_in_thread:
                app.run_in_thread()
            else:
                try:
                    asyncio.run(app.proactor.run_forever())
                finally:
                    app.proactor.stop()
    except SystemExit:
        ret = 1
    except KeyboardInterrupt:
        ret = 2
    except BaseException as e:  # noqa: BLE001
        ret = 3
        try:
            exception_logger.exception(
                "ERROR in app_main. Shutting down: [%s] / [%s]",
                e,  # noqa: TRY401
                type(e),  # noqa: TRY401
            )
        except:  # noqa: E722
            traceback.print_exception(e)
    if not return_int:
        raise typer.Exit(code=ret)
    return ret
