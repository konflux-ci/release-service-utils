"""Shared helpers for out-of-tree kernel-module archives and envfiles."""

from __future__ import annotations

import re
import tarfile
from collections.abc import Mapping
from pathlib import Path

from dotenv import dotenv_values

from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.oras_utils import safe_extract_archive

SIGNED_KMODS_DIR = "signed-kmods"
SIGNED_KMODS_ARCHIVE = f"{SIGNED_KMODS_DIR}.tar.gz"
ENVFILE_NAME = "envfile"
_MULTI_PLATFORM = "MULTI_PLATFORM"
_REQUIRED_ENVFILE_KEYS = ("DRIVER_VENDOR", "DRIVER_VERSION", "KERNEL_VERSION")
_KERNEL_ARCH_SUFFIX = re.compile(r"\.(x86_64|amd64|aarch64|arm64|ppc64le|s390x)(\+.*)?$")


def extract_signed_kmods_archive(data_dir: Path, signed_kmods: Path) -> None:
    """Extract ``signed-kmods.tar.gz`` from *data_dir* when present.

    The archive name is fixed: sign-oot-kmods always writes
    ``signed-kmods.tar.gz`` in *data_dir*, regardless of
    ``signedKmodsPath``. Extracting into *data_dir* reconstructs a nested
    path. If the archive is absent, files are assumed to already be
    extracted.
    """
    archive = data_dir / SIGNED_KMODS_ARCHIVE
    if not archive.is_file():
        logger.info(
            "No %s found, assuming files are already extracted",
            archive.name,
        )
        return

    logger.info("Extracting %s", archive.name)
    with tarfile.open(archive, "r:*") as tf:
        safe_extract_archive(tf, data_dir, archive.name)

    ko_count = 0
    if signed_kmods.is_dir():
        ko_count = sum(1 for path in signed_kmods.rglob("*.ko") if path.is_file())
    logger.info("Extracted %d .ko files", ko_count)
    if ko_count == 0:
        logger.warning("No .ko files found after extraction")


def _read_dotenv(path: Path) -> dict[str, str]:
    """Parse *path* as dotenv and drop unset keys."""
    values = dotenv_values(path, encoding="utf-8")
    return {key: value for key, value in values.items() if value is not None}


def load_kmod_envfile(path: Path) -> dict[str, str]:
    """Load a kmod ``envfile`` and require vendor, version, and kernel keys."""
    if not path.is_file():
        raise FileNotFoundError(f"envfile not found in {path.parent}")
    env = _read_dotenv(path)
    missing = [key for key in _REQUIRED_ENVFILE_KEYS if not env.get(key)]
    if missing:
        raise ValueError(f"envfile missing required keys: {', '.join(missing)}")
    return env


def clean_kernel_version(version: str) -> str:
    """Strip a trailing architecture suffix from a kernel version string."""
    return _KERNEL_ARCH_SUFFIX.sub("", version)


def arch_from_env(env: Mapping[str, str], fallback: str) -> str:
    """Return ``ARCH`` from *env* unless it is empty or ``MULTI_PLATFORM``."""
    arch = (env.get("ARCH") or "").strip()
    if arch and arch != _MULTI_PLATFORM:
        logger.info("Using ARCH=%s from envfile (platform: %s)", arch, fallback)
        return arch
    if arch == _MULTI_PLATFORM:
        logger.info(
            "ARCH=MULTI_PLATFORM in envfile, using platform architecture: %s",
            fallback,
        )
        return fallback
    logger.info(
        "No ARCH variable in envfile, using platform architecture: %s",
        fallback,
    )
    return fallback


def resolve_arch_name(dest_dir: Path, platform_arch: str) -> str:
    """Determine the final architecture name from the envfile in *dest_dir*.

    Callers that already loaded the envfile should use ``arch_from_env``
    instead of reading the file again.
    """
    envfile = dest_dir / ENVFILE_NAME
    env = _read_dotenv(envfile) if envfile.is_file() else {}
    return arch_from_env(env, platform_arch)


def destination_prefix(vendor: str, version: str, kernel: str, suffix: str) -> str:
    """Return an object-store key prefix ``vendor/version/kernel/suffix/``."""
    return f"{vendor}/{version}/{kernel}/{suffix}/"


def arch_directories(signed_kmods_path: Path) -> list[Path]:
    """Return immediate architecture subdirectories of *signed_kmods_path*.

    Raises ``RuntimeError`` when none are found.
    """
    dirs = sorted(path for path in signed_kmods_path.iterdir() if path.is_dir())
    if not dirs:
        raise RuntimeError(f"No architecture directories found in {signed_kmods_path}")
    return dirs
