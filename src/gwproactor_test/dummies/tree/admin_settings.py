from typing import Any, Self

from pydantic import Field, model_validator
from pydantic_settings import SettingsConfigDict

from gwproactor import ProactorSettings
from gwproactor.config import Paths
from gwproactor.config.mqtt import TLSInfo
from gwproactor_test.dummies.names import DUMMY_ADMIN_NAME, DUMMY_ADMIN_SHORT_NAME
from gwproactor_test.dummies.tree.link_settings import TreeLinkSettings


class AdminLinkSettings(TreeLinkSettings):
    def __init__(self, **kwargs: Any) -> None:
        if "tls" not in kwargs:
            kwargs["tls"] = TLSInfo(use_tls=False)
        super().__init__(
            client_name=DUMMY_ADMIN_NAME,
            long_name=DUMMY_ADMIN_NAME,
            short_name=DUMMY_ADMIN_SHORT_NAME,
            **kwargs,
        )


class DummyAdminSettings(ProactorSettings):
    target_gnode: str = ""
    paths: Paths = Field({}, validate_default=True)
    link: AdminLinkSettings = AdminLinkSettings()
    model_config = SettingsConfigDict(env_prefix="GWADMIN_", env_nested_delimiter="__")

    @model_validator(mode="before")
    @classmethod
    def pre_root_validator(cls, values: dict) -> dict:
        return ProactorSettings.update_paths_name(values, DUMMY_ADMIN_NAME)

    @model_validator(mode="after")
    def validate(self) -> Self:
        self.link.update_tls_paths(self.paths.certs_dir, self.link.client_name)
        return self
