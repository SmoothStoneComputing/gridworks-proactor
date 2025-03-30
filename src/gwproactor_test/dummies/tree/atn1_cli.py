import asyncio

import typer

from gwproactor.command_line_utils import (
    print_settings,
    run_async_main,
)
from gwproactor_test.dummies import DUMMY_ATN_NAME
from gwproactor_test.dummies.tree.atn import DummyAtnApp
from gwproactor_test.dummies.tree.atn_settings import DummyAtnSettings

app = typer.Typer(
    no_args_is_help=True,
    pretty_exceptions_enable=False,
    rich_markup_mode="rich",
    help="GridWorks Dummy Scada1",
)


@app.command()
def run(
    env_file: str = ".env",
    dry_run: bool = False,
    verbose: bool = False,
    message_summary: bool = False,
) -> None:
    raise NotImplementedError("Reimplement CLIs using App class")
    asyncio.run(  # type: ignore[unreachable]
        run_async_main(
            name=DUMMY_ATN_NAME,
            proactor_type=DummyAtnApp,
            settings_type=DummyAtnSettings,
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
    print_settings(settings_type=DummyAtnSettings, env_file=env_file)


@app.callback()
def _main() -> None: ...


if __name__ == "__main__":
    app()
