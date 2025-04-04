import argparse
import logging
import sys
import traceback
from pathlib import Path
from types import ModuleType
from typing import Optional

import dotenv
import rich

from gwproactor.actors.actor import PrimeActor
from gwproactor.app import App
from gwproactor.codecs import CodecFactory
from gwproactor.config import Paths
from gwproactor.config.proactor_settings import ProactorSettings
from gwproactor.logging_setup import setup_logging


def get_app(  # noqa: PLR0913
    *,
    paths_name: Optional[str] = None,
    paths: Optional[Paths] = None,
    proactor_settings: Optional[ProactorSettings] = None,
    app_type: type[App] = App,
    prime_actor_type: Optional[type[PrimeActor]] = None,
    codec_factory: Optional[CodecFactory] = None,
    actors_module: Optional[ModuleType] = None,
    env_file: Optional[str | Path] = ".env",
    dry_run: bool = False,
    verbose: bool = False,
    message_summary: bool = False,
    run_in_thread: bool = False,
    add_screen_handler: bool = True,
) -> App:
    dotenv_file = dotenv.find_dotenv(str(env_file))
    dotenv_file_debug_str = (
        f"Env file: <{dotenv_file}>  exists:{Path(dotenv_file).exists()}"
    )
    app = app_type(
        paths_name=paths_name,
        paths=paths,
        proactor_settings=proactor_settings,
        prime_actor_type=prime_actor_type,
        codec_factory=codec_factory,
        actors_module=actors_module,
        env_file=env_file,
    )
    settings = app.config.settings
    if dry_run:
        rich.print(dotenv_file_debug_str)
        rich.print(settings)
        missing_tls_paths_ = settings.check_tls_paths_present(raise_error=False)
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
        settings.check_tls_paths_present()
        app.instantiate()
        if run_in_thread:
            logger.info("run_async_actors_main() starting")
            app.run_in_thread()
    return app


async def run_async_main(  # noqa: PLR0913
    *,
    paths_name: Optional[str] = None,
    paths: Optional[Paths] = None,
    proactor_settings: Optional[ProactorSettings] = None,
    app_type: type[App] = App,
    prime_actor_type: Optional[type[PrimeActor]] = None,
    codec_factory: Optional[CodecFactory] = None,
    actors_module: Optional[ModuleType] = None,
    env_file: Optional[str | Path] = ".env",
    dry_run: bool = False,
    verbose: bool = False,
    message_summary: bool = False,
    add_screen_handler: bool = True,
) -> None:
    settings = app_type.get_settings(
        paths_name=paths_name,
        paths=paths,
        settings=proactor_settings,
        env_file=env_file,
    )
    exception_logger: logging.Logger | logging.LoggerAdapter[logging.Logger] = (
        logging.getLogger(settings.logging.base_log_name)
    )
    try:
        app = get_app(
            paths_name=paths_name,
            paths=paths,
            proactor_settings=proactor_settings,
            app_type=app_type,
            prime_actor_type=prime_actor_type,
            codec_factory=codec_factory,
            actors_module=actors_module,
            env_file=env_file,
            dry_run=dry_run,
            verbose=verbose,
            message_summary=message_summary,
            run_in_thread=False,
            add_screen_handler=add_screen_handler,
        )
        if app.proactor is None:
            raise RuntimeError("ERROR. app.proactor is unexpectedly None")  # noqa: TRY301
        exception_logger = app.config.logger
        try:
            await app.proactor.run_forever()
        finally:
            app.proactor.stop()
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
