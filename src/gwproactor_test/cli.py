import dotenv
import typer
from trogon import Trogon
from typer.main import get_group

from gwproactor_test.certs import generate_dummy_certs
from gwproactor_test.dummies.tree import admin_cli, atn1_cli, scada1_cli, scada2_cli
from gwproactor_test.dummies.tree.admin_settings import DummyAdminSettings
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


@app.command()
def gen_dummy_certs(
    dry_run: bool = False, env_file: str = ".env", only: str = ""
) -> None:
    """Generate certs for dummy proactors."""
    env_file = dotenv.find_dotenv(env_file, usecwd=True)
    for app_name, settings in [
        ("atn", DummyAtnApp(env_file=env_file).settings),
        ("scada1", DummyScada1App(env_file=env_file).settings),
        ("scada2", DummyScada2App(env_file=env_file).settings),
        ("admin", DummyAdminSettings(_env_file=env_file)),  # noqa
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
