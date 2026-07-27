"""Parse charon parameter files used by NRRC/MRRC publish tasks."""

from __future__ import annotations

import re
import shutil
from pathlib import Path

from dotenv import dotenv_values

import file

_REGISTRY_SPLIT = re.compile(r"%")


def load_charon_env(path: Path) -> dict[str, str]:
    """Load charon parameters from a dotenv file (``KEY=value`` lines)."""
    if not path.is_file():
        raise FileNotFoundError(f"charon env file not found: {path}")
    values = dotenv_values(path, encoding="utf-8")
    return {key: value for key, value in values.items() if value is not None}


def split_oci_registries(value: str) -> list[str]:
    """Split ``CHARON_OCI_REGISTRY`` on ``%`` into non-empty registry references."""
    return [part.strip() for part in _REGISTRY_SPLIT.split(value) if part.strip()]


def short_sha256_prefix(registry: str) -> str:
    """Return the first six characters of the digest in *registry*."""
    marker = "@sha256:"
    if marker not in registry:
        raise ValueError(f"registry reference missing @sha256: digest: {registry!r}")
    return registry.split(marker, 1)[1][:6]


def source_repo(registry: str) -> str:
    """Return the repository part of an OCI reference (before ``@sha256:``)."""
    return registry.split("@sha256:", 1)[0]


def require_env_keys(env: dict[str, str], *keys: str) -> None:
    """Raise ValueError when any *keys* are missing from *env*."""
    for key in keys:
        if key not in env:
            raise ValueError(f"missing required charon env variable: {key}")


def require_oci_registries(env: dict[str, str]) -> list[str]:
    """Return non-empty ``CHARON_OCI_REGISTRY`` entries from *env*."""
    try:
        value = env["CHARON_OCI_REGISTRY"]
    except KeyError as e:
        raise ValueError("CHARON_OCI_REGISTRY is required in charon env file") from e
    registries = split_oci_registries(value)
    if not registries:
        raise ValueError("CHARON_OCI_REGISTRY must list at least one registry reference")
    return registries


def charon_config_path(*, home: Path | None = None) -> Path:
    """Return the default charon configuration file path under *home* or ``Path.home()``."""
    root = home if home is not None else Path.home()
    return root / ".charon" / "charon.yaml"


def install_charon_config(config_source: Path, *, home: Path | None = None) -> Path:
    """Copy the charon config into ``$HOME/.charon/charon.yaml``."""
    if not config_source.is_file():
        raise FileNotFoundError(f"charon config file not found: {config_source}")
    dest = charon_config_path(home=home)
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(config_source, dest)
    return dest


NRRC_WORK_DIR_DEFAULT = Path("/var/workdir/nrrc")


def nrrc_work_dir() -> Path:
    """Return the NRRC staging directory from ``WORK_DIR`` or ``/var/workdir/nrrc``."""
    return file.path_from_env_variable("WORK_DIR", NRRC_WORK_DIR_DEFAULT)


MRRC_WORK_DIR_DEFAULT = Path("/var/workdir/mrrc")


def mrrc_work_dir() -> Path:
    """Return the MRRC staging directory from ``WORK_DIR`` or ``/var/workdir/mrrc``."""
    return file.path_from_env_variable("WORK_DIR", MRRC_WORK_DIR_DEFAULT)
