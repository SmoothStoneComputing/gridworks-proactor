from typing import Any

from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

from gwproactor import ProactorSettings
from gwproactor.config import MQTTClient
from gwproactor_test.dummies.names import DUMMY_CHILD_ENV_PREFIX, DUMMY_CHILD_NAME


class DummyChildSettings(ProactorSettings):
    parent_mqtt: MQTTClient = MQTTClient()
    seconds_per_report: int = 300
    async_power_reporting_threshold: float = 0.02

    model_config = SettingsConfigDict(env_prefix=DUMMY_CHILD_ENV_PREFIX)

    @model_validator(mode="before")
    @classmethod
    def pre_root_validator(cls, values: dict[str, Any]) -> dict[str, Any]:
        return ProactorSettings.update_paths_name(values, DUMMY_CHILD_NAME)
