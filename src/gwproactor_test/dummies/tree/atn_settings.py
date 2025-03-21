from typing import Any, Self

from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

from gwproactor import ProactorSettings
from gwproactor_test.dummies import DUMMY_ATN_NAME, DUMMY_SCADA1_NAME
from gwproactor_test.dummies.names import (
    DUMMY_PARENT_ENV_PREFIX,
    DUMMY_SCADA1_SHORT_NAME,
)
from gwproactor_test.dummies.tree.link_settings import TreeLinkSettings


class Scada1LinkSettings(TreeLinkSettings):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(
            client_name=DUMMY_SCADA1_NAME,
            long_name=DUMMY_SCADA1_NAME,
            short_name=DUMMY_SCADA1_SHORT_NAME,
            upstream=False,
            **kwargs,
        )


class DummyAtnSettings(ProactorSettings):
    scada1_link: Scada1LinkSettings = Scada1LinkSettings()

    model_config = SettingsConfigDict(env_prefix=DUMMY_PARENT_ENV_PREFIX)

    @model_validator(mode="before")
    @classmethod
    def pre_root_validator(cls, values: dict[str, Any]) -> dict[str, Any]:
        return ProactorSettings.update_paths_name(values, DUMMY_ATN_NAME)

    @model_validator(mode="after")
    def validate_(self) -> Self:
        self.scada1_link.update_tls_paths(
            self.paths.certs_dir, self.scada1_link.client_name
        )
        return self
