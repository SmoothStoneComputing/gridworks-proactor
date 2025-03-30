from pydantic_settings import BaseSettings, SettingsConfigDict

from gwproactor.config import MQTTClient
from gwproactor_test.dummies.names import DUMMY_CHILD_ENV_PREFIX


class DummyChildSettings(BaseSettings):
    parent_mqtt: MQTTClient = MQTTClient()

    model_config = SettingsConfigDict(env_prefix=DUMMY_CHILD_ENV_PREFIX)
