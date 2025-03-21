from typing import Any

from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

from gwproactor import ProactorSettings
from gwproactor.config import MQTTClient
from gwproactor_test.dummies.names import DUMMY_PARENT_ENV_PREFIX, DUMMY_PARENT_NAME


class DummyParentSettings(ProactorSettings):
    child_mqtt: MQTTClient = MQTTClient()

    model_config = SettingsConfigDict(env_prefix=DUMMY_PARENT_ENV_PREFIX)

    @model_validator(mode="before")
    @classmethod
    def pre_root_validator(cls, values: dict[str, Any]) -> dict[str, Any]:
        return ProactorSettings.update_paths_name(values, DUMMY_PARENT_NAME)
