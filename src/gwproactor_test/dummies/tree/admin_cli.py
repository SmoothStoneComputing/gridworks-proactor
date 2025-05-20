from enum import StrEnum
from pathlib import Path

import rich
import typer

from gwproactor.config import Paths
from gwproactor_test.dummies.tree.admin import MQTTAdmin
from gwproactor_test.dummies.tree.admin_settings import (
    AdminLinkSettings,
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
    env_file: str = "",
) -> None:
    env_path = Path(
        env_file
        if env_file
        else Paths.default_env_path(name=AdminLinkSettings.DUMMY_ADMIN_PATHS_NAME)
    ).absolute()
    rich.print(
        f"Env file: <{env_path!s}>  exists: {bool(env_file and Path(env_path).exists())}"
    )
    settings = DummyAdminSettings(_env_file=str(env_path), target_gnode=target)  # noqa

    rich.print(settings)
    missing_tls_paths_ = settings.check_tls_paths_present(raise_error=False)
    if missing_tls_paths_:
        rich.print(missing_tls_paths_)


@app.callback()
def _main() -> None: ...


if __name__ == "__main__":
    app()
