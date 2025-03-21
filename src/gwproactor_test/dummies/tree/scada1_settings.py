from typing import Any, Self

from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

from gwproactor import ProactorSettings
from gwproactor_test.dummies import (
    DUMMY_ATN_NAME,
    DUMMY_SCADA1_ENV_PREFIX,
    DUMMY_SCADA1_NAME,
    DUMMY_SCADA2_NAME,
)
from gwproactor_test.dummies.names import DUMMY_ATN_SHORT_NAME, DUMMY_SCADA2_SHORT_NAME
from gwproactor_test.dummies.tree.admin_settings import AdminLinkSettings
from gwproactor_test.dummies.tree.link_settings import TreeLinkSettings


class AtnLinkSettings(TreeLinkSettings):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            client_name=DUMMY_ATN_NAME,
            long_name=DUMMY_ATN_NAME,
            short_name=DUMMY_ATN_SHORT_NAME,
            **kwargs,
        )


class Scada2LinkSettings(TreeLinkSettings):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            client_name=DUMMY_SCADA2_NAME,
            long_name=DUMMY_SCADA2_NAME,
            short_name=DUMMY_SCADA2_SHORT_NAME,
            **kwargs,
        )


class DummyScada1Settings(ProactorSettings):
    atn_link: AtnLinkSettings = AtnLinkSettings()
    scada2_link: Scada2LinkSettings = Scada2LinkSettings()
    admin_link: AdminLinkSettings = AdminLinkSettings()

    model_config = SettingsConfigDict(env_prefix=DUMMY_SCADA1_ENV_PREFIX)

    @model_validator(mode="before")
    @classmethod
    def pre_root_validator(cls, values: dict[str, Any]) -> dict[str, Any]:
        return ProactorSettings.update_paths_name(values, DUMMY_SCADA1_NAME)

    @model_validator(mode="after")
    def validate_(self) -> Self:
        self.atn_link.update_tls_paths(self.paths.certs_dir, self.atn_link.client_name)
        self.scada2_link.update_tls_paths(
            self.paths.certs_dir, self.scada2_link.client_name
        )
        return self
