import typer
from trogon import Trogon
from typer.main import get_group

from gwproactor.config import Paths
from gwproactor_test import persister_cli
from gwproactor_test.certs import generate_dummy_certs
from gwproactor_test.dummies.tree import admin_cli, atn1_cli, scada1_cli, scada2_cli
from gwproactor_test.dummies.tree.admin_settings import (
    AdminLinkSettings,
    DummyAdminSettings,
)
from gwproactor_test.dummies.tree.atn import DummyAtnApp
from gwproactor_test.dummies.tree.scada1 import DummyScada1App
from gwproactor_test.dummies.tree.scada2 import DummyScada2App

app = typer.Typer(
    no_args_is_help=True,
    pretty_exceptions_enable=False,
    rich_markup_mode="rich",
    help="GridWorks Proactor Test CLI",
)

app.add_typer(scada1_cli.app, name="scada1", help="Use dummy scada1")
app.add_typer(scada2_cli.app, name="scada2", help="Use dummy scada1")
app.add_typer(atn1_cli.app, name="atn", help="Use dummy scada1")
app.add_typer(admin_cli.app, name="admin", help="Use dummy admin")
app.add_typer(persister_cli.app, name="mp", help="Measure persister timings.")


@app.command()
def gen_dummy_certs(dry_run: bool = False, only: str = "") -> None:
    """Generate certs for dummy proactors."""
    for app_name, settings in [
        ("atn", DummyAtnApp(env_file=DummyAtnApp.default_env_path()).settings),
        ("scada1", DummyScada1App(env_file=DummyScada1App.default_env_path()).settings),
        ("scada2", DummyScada2App(env_file=DummyScada2App.default_env_path()).settings),
        (
            "admin",
            DummyAdminSettings(
                _env_file=Paths.default_env_path(  # noqa
                    name=AdminLinkSettings.DUMMY_ADMIN_PATHS_NAME
                )
            ),
        ),  # noqa
    ]:
        if only and only != app_name:
            continue
        generate_dummy_certs(settings=settings, dry_run=dry_run)


@app.command()
def commands(ctx: typer.Context) -> None:
    """CLI command builder."""
    Trogon(get_group(app), click_context=ctx).run()


@app.callback()
def main_app_callback() -> None: ...


# For sphinx:
typer_click_object = typer.main.get_command(app)

if __name__ == "__main__":
    app()
