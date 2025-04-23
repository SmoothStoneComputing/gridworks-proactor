from enum import StrEnum
from pathlib import Path

import dotenv
import rich
import typer

from gwproactor_test.dummies.tree.admin import MQTTAdmin
from gwproactor_test.dummies.tree.admin_settings import (
    DummyAdminSettings,
)

app = typer.Typer(
    no_args_is_help=True,
    pretty_exceptions_enable=False,
    rich_markup_mode="rich",
    help="GridWorks Dummy Admin Client",
)


class RelayState(StrEnum):
    open = "0"
    closed = "1"


def _set_relay(
    *,
    target: str,
    relay_name: str,
    closed: RelayState,
    user: str = "HeatpumpWizard",
    json: bool = False,
) -> None:
    settings = DummyAdminSettings(target_gnode=target)
    if not json:
        rich.print(settings)
    admin = MQTTAdmin(
        settings=settings,
        relay_name=relay_name,
        closed=closed == RelayState.closed,
        user=user,
        json=json,
    )
    admin.run()


@app.command()
def set_relay(
    target: str,
    relay_name: str,
    closed: RelayState,
    user: str = "HeatpumpWizard",
    json: bool = False,
) -> None:
    _set_relay(
        target=target,
        relay_name=relay_name,
        closed=closed,
        user=user,
        json=json,
    )


@app.command()
def run(
    target: str = DummyAdminSettings.DEFAULT_TARGET,
    relay_name: str = "relay0",
    closed: RelayState = RelayState.closed,
    user: str = "HeatpumpWizard",
    json: bool = False,
) -> None:
    _set_relay(
        target=target,
        relay_name=relay_name,
        closed=closed,
        user=user,
        json=json,
    )


@app.command()
def config(
    target: str = DummyAdminSettings.DEFAULT_TARGET,
    env_file: str = ".env",
) -> None:
    settings = DummyAdminSettings(_env_file=env_file, target_gnode=target)  # noqa
    dotenv_file = dotenv.find_dotenv(str(env_file))
    rich.print(
        f"Env file: <{dotenv_file}>  exists:{env_file and Path(dotenv_file).exists()}"
    )
    rich.print(settings)
    missing_tls_paths_ = settings.check_tls_paths_present(raise_error=False)
    if missing_tls_paths_:
        rich.print(missing_tls_paths_)


@app.callback()
def _main() -> None: ...


if __name__ == "__main__":
    app()
