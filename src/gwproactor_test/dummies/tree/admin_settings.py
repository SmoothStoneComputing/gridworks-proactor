import typing
from typing import Any, Self

from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

from gwproactor import AppSettings
from gwproactor_test.dummies.names import DUMMY_ADMIN_NAME, DUMMY_ADMIN_SHORT_NAME
from gwproactor_test.dummies.tree.link_settings import TreeLinkSettings


class AdminLinkSettings(TreeLinkSettings):
    DUMMY_ADMIN_CLIENT_NAME: typing.ClassVar[str] = "dummy_admin"
    DUMMY_ADMIN_PATHS_NAME: typing.ClassVar[str] = "dummy_admin"

    def __init__(self, **kwargs: Any) -> None:
        kwargs["client_name"] = kwargs.get("client_name", self.DUMMY_ADMIN_CLIENT_NAME)
        kwargs["long_name"] = kwargs.get("long_name", DUMMY_ADMIN_NAME)
        kwargs["short_name"] = kwargs.get("short_name", DUMMY_ADMIN_SHORT_NAME)
        super().__init__(**kwargs)


class DummyAdminSettings(AppSettings):
    DEFAULT_TARGET: typing.ClassVar[str] = "d1.isone.ct.newhaven.orange1.scada"
    target_gnode: str = DEFAULT_TARGET
    link: AdminLinkSettings = AdminLinkSettings()
    model_config = SettingsConfigDict(env_prefix="GWADMIN_", env_nested_delimiter="__")

    @model_validator(mode="before")
    @classmethod
    def pre_root_validator(cls, values: dict[str, Any]) -> dict[str, Any]:
        return cls.update_paths_name_before_validator(
            values, AdminLinkSettings.DUMMY_ADMIN_PATHS_NAME
        )

    @model_validator(mode="after")
    def validate_(self) -> Self:
        self.link.update_tls_paths(self.paths.certs_dir, self.link.client_name)
        return self
