from pydantic_settings import BaseSettings, SettingsConfigDict

from gwproactor.config import MQTTClient
from gwproactor_test.dummies.names import DUMMY_PARENT_ENV_PREFIX


class DummyParentSettings(BaseSettings):
    child_mqtt: MQTTClient = MQTTClient()

    model_config = SettingsConfigDict(env_prefix=DUMMY_PARENT_ENV_PREFIX)
