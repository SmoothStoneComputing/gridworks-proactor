"""Generate or copy test certificates for MQTT using TLS."""

import os
import shutil
import subprocess
from pathlib import Path
from typing import Optional

from gwcert import DEFAULT_CA_DIR  # noqa
from pydantic import BaseModel
from pydantic import BaseSettings

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
    src_paths = TLSPaths(
        ca_cert_path=ca_cert_path,
        cert_path=test_cert_dir / dst_paths.cert_path.name,
        private_key_path=test_cert_dir / "private" / dst_paths.private_key_path.name,
    )
    if not src_paths.cert_path.exists() or not src_paths.private_key_path.exists():
        print(
            f"One or more TLS test keys at {test_cert_dir} does not exist. Recreating."
        )
        print(
            f"  Cert path        exists:{str(src_paths.cert_path.exists()):5s}  {src_paths.cert_path}"
        )
        print(
            f"  Private key path exists:{str(src_paths.private_key_path.exists()):5s}  {src_paths.private_key_path}"
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
        result = subprocess.run(gwcert_command, capture_output=True, check=True)
        print(result.stdout.decode("utf-8"))
    dst_paths.mkdirs()
    shutil.copy2(src_paths.ca_cert_path, dst_paths.ca_cert_path)
    shutil.copy2(src_paths.cert_path, dst_paths.cert_path)
    shutil.copy2(src_paths.private_key_path, dst_paths.private_key_path)


def uses_tls(model: BaseModel | BaseSettings) -> bool:
    """Check whether any MQTTClient in the model have MQTTClient.tls.use_tls == True."""
    for k, v in model._iter():  # noqa
        if isinstance(v, MQTTClient):
            if v.tls.use_tls:
                return True
    return False


def copy_keys(
    model_tag: str,
    model: BaseModel | BaseSettings,
    test_cert_cache_dir: Optional[Path] = None,
) -> None:
    """Copy keys referenced in a model from the cache to locations specified by the model."""
    if test_cert_cache_dir is None:
        test_cert_cache_dir = test_certificate_cache_dir()
    test_cert_cache = Path(test_cert_cache_dir)
    for k, v in model._iter():  # noqa
        if isinstance(v, MQTTClient):
            _copy_keys(test_cert_cache / model_tag / k, v.tls.paths)


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
