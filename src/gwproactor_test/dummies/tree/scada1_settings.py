from typing import Any, ClassVar

from pydantic import model_validator
from pydantic_settings import SettingsConfigDict

from gwproactor import ProactorSettings
from gwproactor.config import MQTTClient
from gwproactor.config.links import CodecSettings, LinkSettings
from gwproactor_test.dummies import (
    DUMMY_ATN_NAME,
    DUMMY_SCADA1_ENV_PREFIX,
    DUMMY_SCADA1_NAME,
    DUMMY_SCADA2_NAME,
)
from gwproactor_test.dummies.names import (
    DUMMY_ADMIN_NAME,
    DUMMY_ADMIN_SHORT_NAME,
    DUMMY_ATN_SHORT_NAME,
    DUMMY_SCADA2_SHORT_NAME,
)


class DummyScada1Settings(ProactorSettings):
    ATN_LINK: ClassVar[str] = DUMMY_ATN_NAME
    SCADA2_LINK: ClassVar[str] = DUMMY_SCADA2_NAME
    ADMIN_LINK: ClassVar[str] = DUMMY_ADMIN_NAME

    model_config = SettingsConfigDict(env_prefix=DUMMY_SCADA1_ENV_PREFIX)

    @model_validator(mode="before")
    @classmethod
    def add_links(cls, values: dict[str, Any]) -> dict[str, Any]:
        return ProactorSettings.update_brokers_and_links(
            values,
            default_brokers={
                link_name: MQTTClient()
                for link_name in [cls.ATN_LINK, cls.SCADA2_LINK, cls.ADMIN_LINK]
            },
            default_links={
                cls.ATN_LINK: LinkSettings(
                    broker_name=cls.ATN_LINK,
                    peer_long_name=DUMMY_ATN_NAME,
                    peer_short_name=DUMMY_ATN_SHORT_NAME,
                    upstream=True,
                ),
                cls.SCADA2_LINK: LinkSettings(
                    broker_name=cls.SCADA2_LINK,
                    peer_long_name=DUMMY_SCADA2_NAME,
                    peer_short_name=DUMMY_SCADA2_SHORT_NAME,
                    downstream=True,
                    codec=CodecSettings(
                        message_modules=["gwproactor_test.dummies.tree.messages"]
                    ),
                ),
                cls.ADMIN_LINK: LinkSettings(
                    broker_name=cls.ADMIN_LINK,
                    peer_long_name=DUMMY_ADMIN_NAME,
                    peer_short_name=DUMMY_ADMIN_SHORT_NAME,
                    link_subscription_short_name=DUMMY_SCADA1_NAME,
                ),
            },
        )
