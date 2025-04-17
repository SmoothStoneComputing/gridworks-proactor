"""Generate or copy test certificates for MQTT using TLS."""

# ruff: noqa: T201, S101

import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Optional

import rich
from gwcert import DEFAULT_CA_DIR
from pydantic import BaseModel
from pydantic_settings import BaseSettings

from gwproactor.config.mqtt import MQTTClient
from gwproactor.config.paths import TLSPaths

TEST_CA_CERTIFICATE_PATH_VAR = "GWPROACTOR_TEST_CA_CERT_PATH"
"""Environment variable containing the path to the CA certificate used by the tests."""

TEST_CA_PRIVATE_KEY_VAR = "GWPROACTOR_TEST_CA_KEY_PATH"
"""Environment variable containing the the path to the CA private key used by the tests."""

TEST_CERTIFICATE_CACHE_VAR = "GWPROACTOR_TEST_CERTIFICATE_CACHE"
"""Environment variable containing the path to the cache of TLS certificates used by the tests."""

TEST_CA_CERTIFICATE_PATH = DEFAULT_CA_DIR / "ca.crt"
TEST_CA_PRIVATE_KEY_PATH = DEFAULT_CA_DIR / "private" / "ca_key.pem"


def _key_path(tag: str, var_name: str, default: Path) -> Path:
    """Return the path to a specified key, specified by environment variable or default. Raise an
    error if that a file that path does not exist."""
    env_val = os.getenv(var_name)
    if env_val:
        key_path = Path(env_val)
        if not key_path.exists():
            raise ValueError(
                f"ERROR. Key <{tag}> does not exist at path: {key_path}.\n"
                f"  Key path specified via environment variable {var_name}"
            )
    else:
        key_path = default
        if not key_path.exists():
            raise ValueError(
                f"ERROR. Key <{tag}> does not exist at path: {key_path}.\n"
                f"  Key path is the default path for <{tag}> because environment variable {var_name} is empty or missing."
            )
    return key_path


def test_ca_certificate_path() -> Path:
    """Return the path to the CA certificate used in testing, specified by environment variable or default. Raise an
    error if that a file that path does not exist.
    """
    return _key_path(
        "CA certificate", TEST_CA_CERTIFICATE_PATH_VAR, TEST_CA_CERTIFICATE_PATH
    )


def test_ca_private_key_path() -> Path:
    """Return the path to the CA private key used in testing, specified by environment variable or default. Raise an
    error if that a file that path does not exist.
    """
    return _key_path(
        "CA private key", TEST_CA_PRIVATE_KEY_VAR, TEST_CA_PRIVATE_KEY_PATH
    )


def _copy_keys(test_cert_dir: Path, dst_paths: TLSPaths) -> None:
    ca_cert_path = test_ca_certificate_path()
    ca_private_key_path = test_ca_private_key_path()
    if (
        dst_paths.cert_path is None
        or dst_paths.private_key_path is None
        or dst_paths.ca_cert_path is None
    ):
        raise ValueError(
            f"ERROR. _copy_keys dst_paths ({dst_paths}) must have non-None paths"
        )
    src_paths = TLSPaths(
        ca_cert_path=ca_cert_path,
        cert_path=test_cert_dir / Path(dst_paths.cert_path).name,
        private_key_path=test_cert_dir
        / "private"
        / Path(dst_paths.private_key_path).name,
    )
    assert isinstance(src_paths.ca_cert_path, Path)
    assert isinstance(src_paths.cert_path, Path)
    assert isinstance(src_paths.private_key_path, Path)
    if not src_paths.cert_path.exists() or not src_paths.private_key_path.exists():
        print(
            f"One or more TLS test keys at {test_cert_dir} does not exist. Recreating."
        )
        print(
            f"  Cert path        exists:{src_paths.cert_path.exists()!s:5s}  {src_paths.cert_path}"
        )
        print(
            f"  Private key path exists:{src_paths.private_key_path.exists()!s:5s}  {src_paths.private_key_path}"
        )
        gwcert_command = [
            "gwcert",
            "key",
            "add",
            "--ca-certificate-path",
            str(ca_cert_path),
            "--ca-private-key-path",
            str(ca_private_key_path),
            "--force",
            str(src_paths.cert_path),
        ]
        print(f"Generating keys with command:\n  {' '.join(gwcert_command)}")
        result = subprocess.run(gwcert_command, capture_output=True, check=True)  # noqa: S603
        print(result.stdout.decode("utf-8"))
    dst_paths.mkdirs()
    shutil.copy2(src_paths.ca_cert_path, dst_paths.ca_cert_path)
    shutil.copy2(src_paths.cert_path, dst_paths.cert_path)
    shutil.copy2(src_paths.private_key_path, dst_paths.private_key_path)


def mqtt_client_fields(model: BaseModel | BaseSettings) -> list[tuple[str, MQTTClient]]:
    clients: list[tuple[str, MQTTClient]] = [
        (field_name, getattr(model, field_name))
        for field_name in model.__pydantic_fields__
        if isinstance(getattr(model, field_name), MQTTClient)
    ]
    clients.extend(
        [
            (field_name, field_value)
            for field_name, field_value in getattr(
                model, "mqtt", getattr(model, "mqtt_brokers", {})
            ).items()
            if isinstance(field_value, MQTTClient)
        ]
    )
    return clients


def uses_tls(model: BaseModel | BaseSettings) -> bool:
    """Check whether any MQTTClient in the model have MQTTClient.tls.use_tls == True."""
    return any(
        client_field[1].tls.use_tls for client_field in mqtt_client_fields(model)
    )


def copy_keys(
    model_tag: str,
    model: BaseModel | BaseSettings,
    test_cert_cache_dir: Optional[Path] = None,
) -> None:
    """Copy keys referenced in a model from the cache to locations specified by the model."""
    if test_cert_cache_dir is None:
        test_cert_cache_dir = test_certificate_cache_dir()
    test_cert_cache = Path(test_cert_cache_dir)
    for field_name, client in mqtt_client_fields(model):
        _copy_keys(test_cert_cache / model_tag / field_name, client.tls.paths)


def set_test_certificate_cache_dir(certificate_cache_dir: Path | str) -> None:
    """Set the GWPROACTOR_TEST_CERTIFICATE_CACHE environment variable.

    May be called in conftest.py as:

        set_test_certificate_cache(Path(__file__).parent / '.certificate_cache')

    """
    found_cache_dir = os.getenv(TEST_CERTIFICATE_CACHE_VAR, "")
    if not found_cache_dir:
        os.environ[TEST_CERTIFICATE_CACHE_VAR] = str(
            Path(certificate_cache_dir).absolute()
        )


def test_certificate_cache_dir() -> Path:
    """Get certificate cache directory, or raise an exception if it is not set."""
    found_cache_dir = os.getenv(TEST_CERTIFICATE_CACHE_VAR, "")
    if not found_cache_dir:
        raise ValueError(
            f"ERROR. Environment variable {TEST_CERTIFICATE_CACHE_VAR} is unset or empty.\n"
            f"  To test with TLS, either set {TEST_CERTIFICATE_CACHE_VAR} explicitly, or "
            "set in base conftest.py with, e.g:\n\n"
            "  set_test_certificate_cache(Path(__file__).parent / '.certificate_cache')"
        )
    return Path(found_cache_dir)


def generate_dummy_certs(settings: BaseModel, *, dry_run: bool = False) -> None:
    """Generate certs for dummy proactors."""
    console = rich.console.Console()
    ca_cert_path = test_ca_certificate_path()
    ca_private_key_path = test_ca_private_key_path()
    commands_ = [
        [
            "gwcert",
            "key",
            "add",
            "--ca-certificate-path",
            str(ca_cert_path),
            "--ca-private-key-path",
            str(ca_private_key_path),
            "--force",
            str(client_config.tls.paths.cert_path),
        ]
        for _, client_config in mqtt_client_fields(settings)
    ]
    if dry_run:
        console.print(f"Showing key generation commands for {type(settings)}")
        for command in commands_:
            console.print(f"  {' '.join(command)}", soft_wrap=True)
        console.print("\n\n\n")
    else:
        console.print(f"Generating keys with for {type(settings)}")
        for command in commands_:
            console.print("Generating keys with command:")
            console.print(f"  {' '.join(command)}", soft_wrap=True)
            try:
                result = subprocess.run(command, capture_output=True, check=True)  # noqa: S603
                print(result.stdout.decode("utf-8"))  # noqa
            except subprocess.CalledProcessError as e:
                print(e.stderr.decode("utf-8"))  # noqa
                sys.exit(e.returncode)
        console.print("\n")
