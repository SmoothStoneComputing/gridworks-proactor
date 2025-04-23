import asyncio

import typer

from gwproactor.command_line_utils import run_async_main
from gwproactor_test.dummies.tree.atn import DummyAtnApp

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
    asyncio.run(
        run_async_main(
            app_type=DummyAtnApp,
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
    DummyAtnApp.print_settings(env_file=env_file)


@app.callback()
def _main() -> None: ...


if __name__ == "__main__":
    app()
