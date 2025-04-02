import copy
import typing
from pathlib import Path
from typing import Any, Self

from pydantic import BaseModel, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from gwproactor.config.links import LinkSettings
from gwproactor.config.logging import LoggingSettings
from gwproactor.config.mqtt import MQTTClient
from gwproactor.config.paths import Paths

MQTT_LINK_POLL_SECONDS = 60.0
ACK_TIMEOUT_SECONDS = 5.0
NUM_INITIAL_EVENT_REUPLOADS: int = 5


class ProactorSettings(BaseSettings):
    paths: Paths = Field(default=typing.cast(Paths, {}), validate_default=True)
    mqtt_brokers: dict[str, MQTTClient] = {}
    links: dict[str, LinkSettings] = {}
    mqtt_link_poll_seconds: float = MQTT_LINK_POLL_SECONDS
    logging: LoggingSettings = LoggingSettings()
    ack_timeout_seconds: float = ACK_TIMEOUT_SECONDS
    num_initial_event_reuploads: int = NUM_INITIAL_EVENT_REUPLOADS
    model_config = SettingsConfigDict(env_prefix="PROACTOR_", env_nested_delimiter="__")

    @field_validator("paths")
    @classmethod
    def get_paths(cls, v: Paths) -> Paths:
        if not v:
            v = Paths()
        return v

    def update_paths_name(self, name: str | Path) -> Self:
        self.paths = Paths(
            name=name, **self.paths.model_dump(exclude={"name"}, exclude_unset=True)
        )
        self.update_tls_paths()
        return self

    @classmethod
    def update_paths_name_validator(
        cls, values: dict[str, Any], name: str
    ) -> dict[str, Any]:
        """Update paths member with a new 'name' attribute, e.g., a name known by a derived class.

        This may be called in a mode="before" root validator of a derived class.
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

    @classmethod
    def update_sub_models(
        cls,
        values: dict[str, Any],
        dict_name: str,
        defaults: typing.Mapping[str, BaseModel],
    ) -> dict[str, Any]:
        """Update a dict of models

        This is meant to be called in a 'pre=True' root validator of a derived class.
        """
        if defaults:
            if dict_name not in values:
                values[dict_name] = copy.deepcopy(defaults)
            else:
                for name, default_value in defaults.items():
                    if name not in values[dict_name]:
                        values[dict_name][name] = default_value.model_copy(deep=True)
                    else:
                        values[dict_name][name] = default_value.model_copy(
                            update=default_value.model_validate(
                                **values[dict_name][name]
                            ).model_dump(exclude_unset=True)
                        )
        return values

    @classmethod
    def update_brokers(
        cls, values: dict[str, Any], defaults: dict[str, MQTTClient]
    ) -> dict[str, Any]:
        """Update default mqtt brokers.

        This is meant to be called in a 'pre=True' root validator of a derived class.
        """
        return cls.update_sub_models(
            values, dict_name="mqtt_brokers", defaults=defaults
        )

    @classmethod
    def update_links(
        cls, values: dict[str, Any], defaults: dict[str, LinkSettings]
    ) -> dict[str, Any]:
        """Update default links.

        This is meant to be called in a 'pre=True' root validator of a derived class.
        """
        return cls.update_sub_models(values, dict_name="links", defaults=defaults)

    @classmethod
    def update_brokers_and_links(
        cls,
        values: dict[str, Any],
        default_brokers: dict[str, MQTTClient],
        default_links: dict[str, LinkSettings],
    ) -> dict[str, Any]:
        """Update default mqtt brokers and default links.

        This is meant to be called in a 'pre=True' root validator of a derived class.
        """
        values = cls.update_brokers(values, defaults=default_brokers)
        return cls.update_links(values, defaults=default_links)

    @model_validator(mode="after")
    def post_root_validator(self) -> Self:
        """Update unset paths of any member MQTTClient's TLS paths based on ProactorSettings 'paths' member."""
        if not isinstance(self.paths, Paths):
            raise ValueError(  # noqa: TRY004
                f"ERROR. 'paths' member must be instance of Paths. Got: {type(self.paths)}"
            )
        self.update_tls_paths()
        for link_name, link in self.links.items():
            if link.enabled and link.broker_name not in self.mqtt_brokers:
                raise ValueError(
                    f"ERROR. Link <{link_name}> is enabled but uses "
                    f"missing broker <{link.broker_name}>. "
                )
        return self

    def add_mqtt_broker(
        self, name: str, client: typing.Optional[MQTTClient] = None
    ) -> Self:
        if name in self.mqtt_brokers:
            raise ValueError(f"MQTT client with name <{name}> is already present")
        self.mqtt_brokers[name] = MQTTClient() if client is None else client
        self.mqtt_brokers[name].update_tls_paths(Path(self.paths.certs_dir), name)
        return self

    def add_link(self, name: str, link: LinkSettings) -> Self:
        if name in self.links:
            raise ValueError(f"Link with name <{name}> is already present")
        if link.enabled and link.broker_name not in self.mqtt_brokers:
            raise ValueError(
                f"ERROR. Link <{name}> is enabled but uses "
                f"missing broker <{link.broker_name}>. "
            )
        self.links[name] = link
        return self

    def update_tls_paths(self) -> Self:
        for client_name, client in self.mqtt_brokers.items():
            client.update_tls_paths(Path(self.paths.certs_dir), client_name)
        return self

    def update_paths(
        self,
        *,
        name: typing.Optional[str] = None,
        paths: typing.Optional[Paths] = None,
    ) -> Self:
        if name is not None or paths is not None:
            name_update = {}
            if name is not None:
                name_update["name"] = name
            self.paths = (self.paths if paths is None else paths).copy(**name_update)
            self.update_tls_paths()
        return self

    def uses_tls(self) -> bool:
        return any(client.tls.use_tls for client in self.mqtt_brokers.values())

    def check_tls_paths_present(self, *, raise_error: bool = True) -> str:
        missing_str = ""
        for broker_name, broker in self.mqtt_brokers.items():
            if broker.tls.use_tls:
                missing_paths = broker.tls.paths.missing_paths()
                if missing_paths:
                    missing_str += f"broker {broker_name}\n"
                    for path_name, path in missing_paths:
                        missing_str += f"  {path_name:20s}  {path}\n"
        if missing_str:
            error_str = f"ERROR. TLS usage requested but the following files are missing:\n{missing_str}"
            if raise_error:
                raise ValueError(error_str)
        else:
            error_str = ""
        return error_str
