import typer

from gwproactor_test.dummies.tree.scada1 import DummyScada1App

app = typer.Typer(
    no_args_is_help=True,
    pretty_exceptions_enable=False,
    rich_markup_mode="rich",
    help="GridWorks Dummy Scada1",
)


@app.command()
def run(
    env_file: str = "",
    dry_run: bool = False,
    verbose: bool = False,
    message_summary: bool = False,
    aiohttp_verbose: bool = False,
    io_loop_verbose: bool = False,
    io_loop_on_screen: bool = False,
) -> None:
    DummyScada1App.main(
        env_file=env_file,
        dry_run=dry_run,
        verbose=verbose,
        message_summary=message_summary,
        io_loop_verbose=io_loop_verbose,
        io_loop_on_screen=io_loop_on_screen,
        aiohttp_logging=aiohttp_verbose,
    )


@app.command()
def config(
    env_file: str = "",
) -> None:
    DummyScada1App.print_settings(env_file=env_file)


@app.callback()
def _main() -> None: ...


if __name__ == "__main__":
    app()
