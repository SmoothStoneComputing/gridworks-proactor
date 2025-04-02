import typing
from typing import Any, Self

from pydantic import Field, model_validator
from pydantic_settings import SettingsConfigDict

from gwproactor import ProactorSettings
from gwproactor.config import Paths
from gwproactor_test.dummies.names import DUMMY_ADMIN_NAME, DUMMY_ADMIN_SHORT_NAME
from gwproactor_test.dummies.tree.link_settings import TreeLinkSettings


class AdminLinkSettings(TreeLinkSettings):
    DUMMY_ADMIN_CLIENT_NAME: typing.ClassVar[str] = "dummy_admin"
    DUMMY_ADMIN_PATHS_NAME: typing.ClassVar[str] = "dummy_admin"

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            client_name=self.DUMMY_ADMIN_CLIENT_NAME,
            long_name=DUMMY_ADMIN_NAME,
            short_name=DUMMY_ADMIN_SHORT_NAME,
            **kwargs,
        )


class DummyAdminSettings(ProactorSettings):
    DEFAULT_TARGET: typing.ClassVar[str] = "d1.isone.ct.newhaven.orange1.scada"
    target_gnode: str = DEFAULT_TARGET
    paths: Paths = Field(default=typing.cast(Paths, {}), validate_default=True)
    link: AdminLinkSettings = AdminLinkSettings()
    model_config = SettingsConfigDict(env_prefix="GWADMIN_", env_nested_delimiter="__")

    @model_validator(mode="before")
    @classmethod
    def pre_root_validator(cls, values: dict[str, Any]) -> dict[str, Any]:
        return ProactorSettings.update_paths_name_validator(
            values, AdminLinkSettings.DUMMY_ADMIN_PATHS_NAME
        )

    @model_validator(mode="after")
    def validate_(self) -> Self:
        self.link.update_tls_paths(self.paths.certs_dir, self.link.client_name)
        return self
