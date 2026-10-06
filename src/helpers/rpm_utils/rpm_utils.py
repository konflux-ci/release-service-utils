"""RPM filename filtering, directory listing, and NEVRA parsing helpers."""

from __future__ import annotations

import dataclasses
import subprocess
from pathlib import Path

from release_service_utils.helpers import subprocess_cmd
from release_service_utils.helpers.logger import logger


@dataclasses.dataclass(frozen=True)
class RpmNevra:
    """Name-Epoch-Version-Release-Architecture for an RPM."""

    name: str
    epoch: str
    version: str
    release: str
    arch: str


def parse_comma_list(value: str) -> list[str]:
    """Split a comma-delimited string into stripped, non-empty items."""
    return [item.strip() for item in value.split(",") if item.strip()]


def should_exclude_file(filename: str, patterns: list[str]) -> bool:
    """Return True if *filename* contains any non-blank *pattern* as a substring."""
    return any(pattern in filename for pattern in patterns if pattern.strip())


def list_rpm_files(files_dir: Path, excludes: list[str]) -> list[Path]:
    """Return regular ``*.rpm`` files in *files_dir*, excluding name patterns.

    Skip directories and non-RPM files. Sort by filename. Log each
    exclusion that matches *excludes*.
    """
    kept: list[Path] = []
    for entry in sorted(files_dir.iterdir(), key=lambda path: path.name):
        if not entry.is_file() or not entry.name.endswith(".rpm"):
            continue
        if should_exclude_file(entry.name, excludes):
            logger.info("Excluding %s (matches pattern)", entry.name)
            continue
        kept.append(entry)
    return kept


def parse_nevra(rpm_path: Path, *, fallback_to_filename: bool = True) -> RpmNevra:
    """Return NEVRA metadata for *rpm_path*.

    Prefer querying the RPM header via ``rpm -qp``. If the header cannot be
    read and *fallback_to_filename* is True, parse
    ``name-version-release.arch.rpm``. Source RPMs (``*.src.rpm``) always
    report ``arch="src"``.

    Raise ``ValueError`` when parsing fails.
    """
    header = _parse_nevra_from_header(rpm_path)
    if header is not None:
        return _force_src_arch(rpm_path, header)
    if not fallback_to_filename:
        raise ValueError(f"Failed to parse NEVRA from header of '{rpm_path.name}'")
    return _force_src_arch(rpm_path, _parse_nevra_from_filename(rpm_path))


def _force_src_arch(rpm_path: Path, nevra: RpmNevra) -> RpmNevra:
    """Force ``arch="src"`` for ``*.src.rpm`` files."""
    if rpm_path.name.endswith(".src.rpm") and nevra.arch != "src":
        return RpmNevra(
            name=nevra.name,
            epoch=nevra.epoch,
            version=nevra.version,
            release=nevra.release,
            arch="src",
        )
    return nevra


def _parse_nevra_from_header(rpm_path: Path) -> RpmNevra | None:
    """Query ``rpm -qp`` for NEVRA fields; return None when the header is unreadable."""
    try:
        result = subprocess_cmd.run_cmd(
            [
                "rpm",
                "-qp",
                "--qf",
                "%{NAME}|%{EPOCH}|%{VERSION}|%{RELEASE}|%{ARCH}\n",
                str(rpm_path),
            ],
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        logger.warning("parse_nevra: exception running rpm -qp: %s", exc)
        return None

    if result.returncode != 0:
        return None

    line = (result.stdout or "").strip().split("\n", 1)[0]
    parts = line.split("|")
    if len(parts) != 5:
        return None

    name, epoch, version, release, arch = parts
    if not epoch or epoch == "(none)":
        epoch = "0"
    if not name or not version or not release or not arch:
        return None
    return RpmNevra(
        name=name,
        epoch=epoch,
        version=version,
        release=release,
        arch=arch,
    )


def _parse_nevra_from_filename(rpm_path: Path) -> RpmNevra:
    """Infer NEVRA from an RPM filename; raise ValueError when parsing fails."""
    filename = rpm_path.name
    if not filename.endswith(".rpm"):
        raise ValueError(f"Failed to parse NEVRA from '{filename}'")
    base = filename[: -len(".rpm")]
    if "." not in base:
        raise ValueError(f"Failed to parse NEVRA from '{filename}'")
    nvra, arch = base.rsplit(".", 1)
    if "-" not in nvra:
        raise ValueError(f"Failed to parse NEVRA from '{filename}'")
    namever, release = nvra.rsplit("-", 1)
    if "-" not in namever:
        raise ValueError(f"Failed to parse NEVRA from '{filename}'")
    name, version_with_epoch = namever.rsplit("-", 1)
    if not name or not version_with_epoch or not release or not arch:
        raise ValueError(f"Failed to parse NEVRA from '{filename}'")

    epoch = "0"
    version = version_with_epoch
    if ":" in version_with_epoch:
        epoch, version = version_with_epoch.split(":", 1)
    if not version:
        raise ValueError(f"Failed to parse NEVRA version from '{filename}'")
    return RpmNevra(
        name=name,
        epoch=epoch,
        version=version,
        release=release,
        arch=arch,
    )
