from pathlib import Path
from typing import Any, Optional, Self, TypeVar

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from gwproactor.config.logging import LoggingSettings
from gwproactor.config.mqtt import MQTTClient
from gwproactor.config.paths import Paths
from gwproactor.config.proactor_settings import ProactorSettings

FieldT = TypeVar("FieldT")


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="PROACTOR_APP_",
        env_nested_delimiter="__",
        nested_model_default_partial_update=True,
        extra="ignore",
    )
    paths: Paths = Field(default_factory=Paths, validate_default=True)
    logging: LoggingSettings = LoggingSettings()
    proactor: ProactorSettings = ProactorSettings()

    def brokers(self) -> dict[str, MQTTClient]:
        return self._fields_of_type(MQTTClient)

    def broker(self, name: str) -> MQTTClient:
        return self._field_as_type(name, MQTTClient)

    def update_paths_name(self, name: str | Path) -> Self:
        self.paths = Paths(
            name=name, **self.paths.model_dump(exclude={"name"}, exclude_unset=True)
        )
        self.update_tls_paths()
        return self

    def with_paths(
        self,
        *,
        paths: Optional[Paths] = None,
        exclude_unset: bool = True,
        exclude_defaults: bool = True,
        **kwargs: Any,
    ) -> Self:
        if paths is None:
            paths = self.paths
        self.paths = paths.duplicate(
            exclude=set(kwargs.keys()),
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            **kwargs,
        )
        self.update_tls_paths()
        return self

    def with_paths_name(self, name: str) -> Self:
        return self.with_paths(name=name)

    @classmethod
    def update_paths_name_before_validator(
        cls, values: dict[str, Any], name: str
    ) -> dict[str, Any]:
        """Update paths member with a new 'name' attribute, e.g., a name known by a derived class.

        This may be called in a mode="before" root validator of a derived class.
        """
        if "paths" not in values:
            values["paths"] = Paths(name=name)
        elif isinstance(values["paths"], Paths):
            if "name" not in values["paths"].model_fields_set:
                values["paths"] = values["paths"].duplicate(name=name)
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
        self.update_tls_paths()
        return self

    def update_tls_paths(self) -> Self:
        for client_name, client in self.brokers().items():
            client.update_tls_paths(Path(self.paths.certs_dir), client_name)
        return self

    def uses_tls(self) -> bool:
        return any(client.tls.use_tls for client in self.brokers().values())

    def check_tls_paths_present(self, *, raise_error: bool = True) -> str:
        missing_str = ""
        for broker_name, broker in self.brokers().items():
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

    def _fields_of_type(self, field_type: type[FieldT]) -> dict[str, FieldT]:
        return {
            field_name: getattr(self, field_name)
            for field_name in self.__pydantic_fields__
            if isinstance(getattr(self, field_name, None), field_type)
        }

    def _field_as_type(self, field_name: str, field_type: type[FieldT]) -> FieldT:
        if field_name not in self.__pydantic_fields__:
            raise ValueError(
                f"ERROR. AppSettings field <{field_name}> not present. Was expected as type {field_type}"
            )
        field_value = getattr(self, field_name, None)
        if not isinstance(field_value, field_type):
            raise ValueError(  # noqa: TRY004
                f"ERROR. Field name <{field_type}> is present (type: {type(field_value)} "
                f"but is not an instance of {field_type}."
            )
        return field_value
