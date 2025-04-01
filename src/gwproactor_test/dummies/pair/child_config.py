from typing import Any, ClassVar

from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

from gwproactor import ProactorSettings
from gwproactor.config import MQTTClient
from gwproactor.config.links import LinkSettings
from gwproactor_test.dummies import DUMMY_PARENT_NAME
from gwproactor_test.dummies.names import DUMMY_CHILD_ENV_PREFIX, PARENT_SHORT_NAME


class DummyChildSettings(ProactorSettings):
    PARENT_MQTT: ClassVar[str] = DUMMY_PARENT_NAME

    model_config = SettingsConfigDict(env_prefix=DUMMY_CHILD_ENV_PREFIX)

    @model_validator(mode="before")
    @classmethod
    def add_links(cls, values: dict[str, Any]) -> dict[str, Any]:
        return ProactorSettings.update_brokers_and_links(
            values,
            default_brokers={cls.PARENT_MQTT: MQTTClient()},
            default_links={
                cls.PARENT_MQTT: LinkSettings(
                    broker_name=cls.PARENT_MQTT,
                    peer_long_name=DUMMY_PARENT_NAME,
                    peer_short_name=PARENT_SHORT_NAME,
                    upstream=True,
                )
            },
        )
