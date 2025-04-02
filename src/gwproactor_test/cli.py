import subprocess
import sys

import dotenv
import rich
import typer
from trogon import Trogon
from typer.main import get_group

from gwproactor_test import test_ca_certificate_path, test_ca_private_key_path
from gwproactor_test.certs import mqtt_client_fields
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
    console = rich.console.Console()
    env_file = dotenv.find_dotenv(env_file)
    ca_cert_path = test_ca_certificate_path()
    ca_private_key_path = test_ca_private_key_path()
    for app_name, settings in [
        ("atn", DummyAtnApp(env_file=env_file).config.settings),
        ("scada1", DummyScada1App(env_file=env_file).config.settings),
        ("scada2", DummyScada2App(env_file=env_file).config.settings),
        ("admin", DummyAdminSettings(_env_file=env_file)),  # noqa
    ]:
        if only and only != app_name:
            continue
        commands_ = []
        for _, client_config in mqtt_client_fields(settings):
            commands_.append(
                [
                    "gwcert",
                    "key",
                    "add",
                    "--ca-certificate-path",
                    str(ca_cert_path),
                    "--ca-private-key-path",
                    str(ca_private_key_path),
                    "--force",
                    str(client_config.tls.paths.cert_path),
                ]
            )
        if dry_run:
            console.print(f"Showing key generation commands for {type(settings)}")
            for command in commands_:
                console.print(f"  {' '.join(command)}", soft_wrap=True)
            console.print("\n\n\n")
        else:
            console.print(f"Generating keys with for {type(settings)}")
            for command in commands_:
                console.print("Generating keys with command:")
                console.print(f"  {' '.join(command)}", soft_wrap=True)
                try:
                    result = subprocess.run(command, capture_output=True, check=True)  # noqa: S603
                    print(result.stdout.decode("utf-8"))  # noqa
                except subprocess.CalledProcessError as e:
                    print(e.stderr.decode("utf-8"))  # noqa
                    sys.exit(e.returncode)
            console.print("\n")


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
