from typing import Any, ClassVar

from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

from gwproactor import ProactorSettings
from gwproactor.config import MQTTClient
from gwproactor.config.links import LinkSettings
from gwproactor_test.dummies import DUMMY_CHILD_NAME
from gwproactor_test.dummies.names import CHILD_SHORT_NAME, DUMMY_PARENT_ENV_PREFIX


class DummyParentSettings(ProactorSettings):
    CHILD_MQTT: ClassVar[str] = DUMMY_CHILD_NAME

    model_config = SettingsConfigDict(env_prefix=DUMMY_PARENT_ENV_PREFIX)

    @model_validator(mode="before")
    @classmethod
    def add_links(cls, values: dict[str, Any]) -> dict[str, Any]:
        return ProactorSettings.update_brokers_and_links(
            values,
            default_brokers={cls.CHILD_MQTT: MQTTClient()},
            default_links={
                cls.CHILD_MQTT: LinkSettings(
                    broker_name=cls.CHILD_MQTT,
                    peer_long_name=DUMMY_CHILD_NAME,
                    peer_short_name=CHILD_SHORT_NAME,
                    downstream=True,
                )
            },
        )
