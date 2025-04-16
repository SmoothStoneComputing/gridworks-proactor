from pathlib import Path
from typing import Any, Optional

import xdg
from pydantic import BaseModel, Field, field_validator
from pydantic_core.core_schema import ValidationInfo

DEFAULT_BASE_NAME = "gridworks"
DEFAULT_BASE_DIR = Path(DEFAULT_BASE_NAME)
DEFAULT_NAME = "scada"
DEFAULT_NAME_DIR = Path(DEFAULT_NAME)
DEFAULT_LAYOUT_FILE = Path("hardware-layout.json")


class TLSPaths(BaseModel):
    ca_cert_path: Optional[str | Path] = None
    cert_path: Optional[str | Path] = None
    private_key_path: Optional[str | Path] = None

    @classmethod
    def defaults(cls, certs_dir: Path, client_name: str) -> "TLSPaths":
        """Calcuate defaults given a certs_dir and client name. Meant to be called in context where those are known,
        e.g. a validator on a higher-level model which has access to a Paths object and a named MQTT configuration.
        """
        client_dir = Path(certs_dir) / client_name
        return TLSPaths(
            ca_cert_path=client_dir / "ca.crt",
            cert_path=client_dir / f"{client_name}.crt",
            private_key_path=client_dir / "private" / f"{client_name}.pem",
        )

    def effective_paths(self, certs_dir: str | Path, client_name: str) -> "TLSPaths":
        """Re-calculate paths given a certs_dir and client name. Meant to be called in context where those are
        known, e.g. a validator on a higher-level model which has access to a Paths object and a named MQTT
        configuration."""
        fields = self.model_dump(exclude_none=True)
        fields.update(self.defaults(Path(certs_dir), client_name).model_dump())
        return TLSPaths(**fields)

    def mkdirs(
        self, *, mode: int = 0o777, parents: bool = True, exist_ok: bool = True
    ) -> None:
        """Create directories that will be used to store certificates and private keys."""
        if (
            self.ca_cert_path is None
            or self.cert_path is None
            or self.private_key_path is None
        ):
            raise ValueError(
                "ERROR. TLSPaths.mkdirs() requires all paths to nave non-None values. "
                f"Current values: {self.model_dump()}"
            )
        Path(self.ca_cert_path).parent.mkdir(
            mode=mode, parents=parents, exist_ok=exist_ok
        )
        Path(self.cert_path).parent.mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
        Path(self.private_key_path).parent.mkdir(
            mode=mode, parents=parents, exist_ok=exist_ok
        )

    def missing_paths(self) -> list[tuple[str, Optional[Path]]]:
        missing = []
        for path_name in self.__pydantic_fields__:
            path = getattr(self, path_name)
            if path is None or not Path(path).exists():
                missing.append((path_name, path))
        return missing


class Paths(BaseModel):
    # Relative offsets used under home directories
    base: str | Path = Field(default=DEFAULT_BASE_DIR, validate_default=True)
    name: str | Path = Field(default=DEFAULT_NAME_DIR, validate_default=True)
    relative_path: str | Path = Field(default="", validate_default=True)

    # Home directories (defaulting to https://specifications.freedesktop.org/basedir-spec/latest/)
    data_home: str | Path = Field(default="", validate_default=True)
    state_home: str | Path = Field(default="", validate_default=True)
    config_home: str | Path = Field(default="", validate_default=True)

    # Base working paths, defaulting to xdg home/relative_path/...
    data_dir: str | Path = Field(default="", validate_default=True)
    config_dir: str | Path = Field(default="", validate_default=True)
    certs_dir: str | Path = Field(default="", validate_default=True)
    event_dir: str | Path = Field(default="", validate_default=True)
    log_dir: str | Path = Field(default="", validate_default=True)
    hardware_layout: str | Path = Field(default="", validate_default=True)

    @field_validator("base")
    @classmethod
    def get_base(cls, v: str | Path) -> Path:
        return Path(v)

    @field_validator("name")
    @classmethod
    def get_name(cls, v: str | Path) -> Path:
        return Path(v)

    @field_validator("data_home")
    @classmethod
    def get_data_home(cls, v: str | Path) -> Path:
        return Path(v or xdg.xdg_data_home())

    @field_validator("state_home")
    @classmethod
    def get_state_home(cls, v: str | Path) -> Path:
        return Path(v or xdg.xdg_state_home())

    @field_validator("config_home")
    @classmethod
    def get_config_home(cls, v: str | Path) -> Path:
        return Path(v or xdg.xdg_config_home())

    @field_validator("config_dir")
    @classmethod
    def get_config_dir(cls, v: str | Path, info: ValidationInfo) -> Path:
        if not v:
            v = Path(info.data["config_home"]) / info.data["relative_path"]
        return Path(v)

    @field_validator("certs_dir")
    @classmethod
    def get_certs_dir(cls, v: str | Path, info: ValidationInfo) -> Path:
        if not v:
            v = Path(info.data["config_dir"]) / "certs"
        return Path(v)

    @field_validator("data_dir")
    @classmethod
    def get_data_dir(cls, v: str | Path, info: ValidationInfo) -> Path:
        if not v:
            v = Path(info.data["data_home"]) / info.data["relative_path"]
        return Path(v)

    @field_validator("event_dir")
    @classmethod
    def get_event_dir(cls, v: str | Path, info: ValidationInfo) -> Path:
        if not v:
            v = Path(info.data["data_dir"]) / "event"
        return Path(v)

    @field_validator("log_dir")
    @classmethod
    def get_log_dir(cls, v: str | Path, info: ValidationInfo) -> Path:
        if not v:
            v = Path(info.data["state_home"]) / info.data["relative_path"] / "log"
        return Path(v)

    @field_validator("hardware_layout")
    @classmethod
    def get_hardware_layout(cls, v: Any, info: ValidationInfo) -> Path:
        if not v:
            v = Path(info.data["config_dir"]) / DEFAULT_LAYOUT_FILE
        return Path(v)

    @field_validator("relative_path")
    @classmethod
    def get_relative_path(cls, v: str | Path, info: ValidationInfo) -> Path:
        if not v:
            v = Path(info.data["base"]) / info.data["name"]
        return Path(v)

    def mkdirs(
        self, *, mode: int = 0o777, parents: bool = True, exist_ok: bool = True
    ) -> None:
        Path(self.data_dir).mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
        Path(self.config_dir).mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
        Path(self.event_dir).mkdir(mode=mode, parents=parents, exist_ok=exist_ok)
        Path(self.log_dir).mkdir(mode=mode, parents=parents, exist_ok=exist_ok)

    def duplicate(
        self,
        *,
        exclude_unset: bool = True,
        exclude_defaults: bool = True,
        exclude: Optional[set[str]] = None,
        **kwargs: Any,
    ) -> "Paths":
        fields = self.model_dump(
            exclude_unset=exclude_unset,
            exclude_defaults=exclude_defaults,
            exclude=exclude,
        )
        fields.update(**kwargs)
        return Paths(**fields)
