from typing import Any, ClassVar

from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

from gwproactor import ProactorSettings
from gwproactor.config import MQTTClient
from gwproactor.config.links import LinkSettings
from gwproactor_test.dummies import DUMMY_SCADA1_NAME
from gwproactor_test.dummies.names import (
    DUMMY_PARENT_ENV_PREFIX,
    DUMMY_SCADA1_SHORT_NAME,
)


class DummyAtnSettings(ProactorSettings):
    SCADA1_LINK: ClassVar[str] = DUMMY_SCADA1_NAME

    model_config = SettingsConfigDict(env_prefix=DUMMY_PARENT_ENV_PREFIX)

    @model_validator(mode="before")
    @classmethod
    def add_links(cls, values: dict[str, Any]) -> dict[str, Any]:
        return ProactorSettings.update_brokers_and_links(
            values,
            default_brokers={
                link_name: MQTTClient() for link_name in [cls.SCADA1_LINK]
            },
            default_links={
                cls.SCADA1_LINK: LinkSettings(
                    broker_name=cls.SCADA1_LINK,
                    peer_long_name=DUMMY_SCADA1_NAME,
                    peer_short_name=DUMMY_SCADA1_SHORT_NAME,
                    downstream=True,
                ),
            },
        )
