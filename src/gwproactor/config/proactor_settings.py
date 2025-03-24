import typing
from pathlib import Path
from typing import Any, Self

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from gwproactor.config.logging import LoggingSettings
from gwproactor.config.mqtt import MQTTClient
from gwproactor.config.paths import Paths

MQTT_LINK_POLL_SECONDS = 60.0
ACK_TIMEOUT_SECONDS = 5.0
NUM_INITIAL_EVENT_REUPLOADS: int = 5


class ProactorSettings(BaseSettings):
    paths: Paths = Field(default=typing.cast(Paths, {}), validate_default=True)
    logging: LoggingSettings = LoggingSettings()
    mqtt_link_poll_seconds: float = MQTT_LINK_POLL_SECONDS
    ack_timeout_seconds: float = ACK_TIMEOUT_SECONDS
    num_initial_event_reuploads: int = NUM_INITIAL_EVENT_REUPLOADS
    model_config = SettingsConfigDict(env_prefix="PROACTOR_", env_nested_delimiter="__")

    @field_validator("paths")
    @classmethod
    def get_paths(cls, v: Paths) -> Paths:
        if not v:
            v = Paths()
        return v

    @classmethod
    def update_paths_name(cls, values: dict[str, Any], name: str) -> dict[str, Any]:
        """Update paths member with a new 'name' attribute, e.g., a name known by a derived class.

        This is meant to be called in a 'pre=True' root validator of a derived class.
        """
        if "paths" not in values:
            values["paths"] = Paths(name=name)
        elif isinstance(values["paths"], Paths):
            if "name" not in values["paths"].model_fields_set:
                values["paths"] = Paths(
                    name=name, **values["paths"].model_dump(exclude_unset=True)
                )
        elif "name" not in values["paths"]:
            values["paths"]["name"] = name
        return values

    @model_validator(mode="after")
    def post_root_validator(self) -> Self:
        """Update unset paths of any member MQTTClient's TLS paths based on ProactorSettings 'paths' member."""
        if not isinstance(self.paths, Paths):
            raise ValueError(  # noqa: TRY004
                f"ERROR. 'paths' member must be instance of Paths. Got: {type(self.paths)}"
            )
        for field_name in self.model_fields:
            v = getattr(self, field_name)
            if isinstance(v, MQTTClient):
                v.update_tls_paths(Path(self.paths.certs_dir), field_name)
        return self
