import asyncio
import typing

import typer

from gwproactor.command_line_utils import run_async_main
from gwproactor.logging_setup import enable_aiohttp_logging
from gwproactor_test.dummies.tree.scada1 import DummyScada1App

app = typer.Typer(
    no_args_is_help=True,
    pretty_exceptions_enable=False,
    rich_markup_mode="rich",
    help="GridWorks Dummy Scada1",
)


class DummyScada1CLIApp(DummyScada1App):
    def __init__(self, **kwargs: typing.Any) -> None:
        kwargs["src_layout_path"] = None
        super().__init__(**kwargs)


@app.command()
def run(
    env_file: str = ".env",
    dry_run: bool = False,
    verbose: bool = False,
    message_summary: bool = False,
    aiohttp_verbose: bool = False,
) -> None:
    if aiohttp_verbose:
        enable_aiohttp_logging()
    asyncio.run(
        run_async_main(
            app_type=DummyScada1CLIApp,
            env_file=env_file,
            dry_run=dry_run,
            verbose=verbose,
            message_summary=message_summary,
        )
    )


@app.command()
def config(
    env_file: str = ".env",
) -> None:
    DummyScada1App.print_settings(env_file=env_file)


@app.callback()
def _main() -> None: ...


if __name__ == "__main__":
    app()
