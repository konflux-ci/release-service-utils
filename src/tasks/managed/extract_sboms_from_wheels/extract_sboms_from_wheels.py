#!/usr/bin/env python3
"""Extract Fromager-generated SBOMs from Python wheels.

Recursively walk ``*.whl`` files under a data directory, pull SBOM files
from each wheel's ``.dist-info/sboms/`` path, and write them into
``<data_dir>/sboms`` for a later Atlas/TPA upload step.
"""

from __future__ import annotations

import hashlib
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import BinaryIO

from release_service_utils.helpers import file, tekton
from release_service_utils.helpers.logger import logger

_SBOM_MARKER = ".dist-info/sboms/"
SBOMS_SUBDIR = "sboms"
_HASH_CHUNK_SIZE = 64 * 1024
_MAX_SBOM_UNCOMPRESSED_BYTES = 16 * 1024 * 1024
_FILENAME_MAX_BYTES = 255
_PATH_DIGEST_HEX = 16
_READABLE_SUFFIX_BYTES = 64


def _is_sbom_zip_member(name: str) -> bool:
    """Return True if *name* is a file under ``.dist-info/sboms/``."""
    if name.endswith("/"):
        return False
    return _SBOM_MARKER in name


def _wheel_identity(wheel: Path, wheels_dir: Path) -> str:
    """Return the posix path of *wheel* relative to *wheels_dir*."""
    return wheel.relative_to(wheels_dir).as_posix()


def _truncate_preserving_json(name: str, limit: int) -> str:
    """Return *name* in at most *limit* UTF-8 bytes, keeping a final ``.json``."""
    raw = name.encode()
    if len(raw) <= limit:
        return name
    suffix = ".json"
    if name.endswith(suffix):
        stem_limit = max(0, limit - len(suffix.encode()))
        stem = name[: -len(suffix)].encode()[:stem_limit]
        return f"{stem.decode('utf-8', errors='ignore')}{suffix}"
    return raw[:limit].decode("utf-8", errors="ignore")


def _bounded_filename_suffix(name: str, limit: int) -> str:
    """Return *name*'s basename in at most *limit* UTF-8 bytes.

    Preserves a final ``.json`` extension so catalog upload still
    recognizes the file. Only the preceding stem is truncated.
    """
    return _truncate_preserving_json(Path(name).name, limit)


def _sbom_output_name(wheel_identity: str, sbom_member: str) -> str:
    """Return a unique filename bounded to ``_FILENAME_MAX_BYTES``.

    Hashes the wheel's relative path and the complete SBOM member path
    so deeply nested wheels cannot exceed the filesystem name limit.
    A short readable suffix is kept from the member basename, including
    its ``.json`` extension.
    """
    wheel_digest = hashlib.sha256(wheel_identity.encode()).hexdigest()[:_PATH_DIGEST_HEX]
    member_digest = hashlib.sha256(sbom_member.encode()).hexdigest()[:_PATH_DIGEST_HEX]
    suffix = _bounded_filename_suffix(sbom_member, _READABLE_SUFFIX_BYTES)
    return _truncate_preserving_json(
        f"{wheel_digest}-{member_digest}-{suffix}",
        _FILENAME_MAX_BYTES,
    )


def _digest_and_size(
    handle: BinaryIO,
    *,
    max_bytes: int | None = None,
) -> tuple[str, int]:
    """Return the SHA-256 hex digest and byte count of *handle*."""
    limit = _MAX_SBOM_UNCOMPRESSED_BYTES if max_bytes is None else max_bytes
    hasher = hashlib.sha256()
    size = 0
    while chunk := handle.read(_HASH_CHUNK_SIZE):
        size += len(chunk)
        if size > limit:
            raise ValueError(f"read data exceeds {limit} bytes")
        hasher.update(chunk)
    return hasher.hexdigest(), size


def _copy_digest_and_size(
    handle: BinaryIO,
    dest: Path,
    *,
    max_bytes: int | None = None,
) -> tuple[str, int]:
    """Copy *handle* to *dest* and return its SHA-256 digest and size."""
    limit = _MAX_SBOM_UNCOMPRESSED_BYTES if max_bytes is None else max_bytes
    hasher = hashlib.sha256()
    size = 0
    try:
        with dest.open("wb") as out:
            while chunk := handle.read(_HASH_CHUNK_SIZE):
                if size + len(chunk) > limit:
                    raise ValueError(f"read data exceeds {limit} bytes")
                hasher.update(chunk)
                out.write(chunk)
                size += len(chunk)
    except (OSError, ValueError):
        dest.unlink(missing_ok=True)
        raise
    return hasher.hexdigest(), size


def extract_sboms_from_wheel(
    wheel: Path,
    sboms_dir: Path,
    *,
    output_prefix: str | None = None,
    written: dict[Path, tuple[str, int]] | None = None,
) -> int:
    """Extract SBOM files from *wheel* into *sboms_dir*.

    *output_prefix* is the wheel identity hashed into each output name.
    When omitted, the wheel filename is used. *written* tracks digest
    and size for each output path so a later wheel cannot overwrite
    different content at the same path. Duplicates are compared by
    streamed hashes, not by retaining decompressed bytes. Members whose
    declared or streamed uncompressed size exceeds
    ``_MAX_SBOM_UNCOMPRESSED_BYTES`` are rejected and any partial
    destination file is removed. Returns the number of new files
    written. Wheels with no SBOMs are skipped (count ``0``) after a
    log message. Archive, member-open, size-limit, and I/O failures
    become ``tekton.CheckStepError``.
    """
    identity = output_prefix if output_prefix is not None else wheel.name
    retained = written if written is not None else {}
    try:
        with zipfile.ZipFile(wheel) as zf:
            members = [n for n in zf.namelist() if _is_sbom_zip_member(n)]
            if not members:
                logger.info("No SBOMs found in %s", wheel)
                return 0

            count = 0
            for sbom_path in members:
                info = zf.getinfo(sbom_path)
                if info.file_size > _MAX_SBOM_UNCOMPRESSED_BYTES:
                    cause = ValueError(
                        f"SBOM member {sbom_path!r} exceeds "
                        f"{_MAX_SBOM_UNCOMPRESSED_BYTES} uncompressed bytes"
                    )
                    raise tekton.CheckStepError(
                        "extracting SBOMs from wheels",
                        cause,
                    ) from cause
                output_name = _sbom_output_name(identity, sbom_path)
                output_path = sboms_dir / output_name
                with zf.open(sbom_path) as src:
                    if output_path in retained or output_path.exists():
                        existing = retained.get(output_path)
                        if existing is None:
                            with output_path.open("rb") as handle:
                                existing = _digest_and_size(handle)
                            retained[output_path] = existing
                        if _digest_and_size(src) == existing:
                            continue
                        cause = ValueError(
                            f"Conflicting SBOM output path {output_name!r} from {wheel}"
                        )
                        raise tekton.CheckStepError(
                            "extracting SBOMs from wheels",
                            cause,
                        ) from cause
                    retained[output_path] = _copy_digest_and_size(src, output_path)
                logger.info("Extracted %s -> %s", sbom_path, output_name)
                count += 1
            return count
    except tekton.CheckStepError:
        raise
    except (
        OSError,
        ValueError,
        RuntimeError,
        zipfile.BadZipFile,
        zipfile.LargeZipFile,
    ) as exc:
        raise tekton.CheckStepError("extracting SBOMs from wheels", exc) from exc


def run(data_dir: Path, files_dir: str) -> int:
    """Extract SBOMs from every wheel under *data_dir* / *files_dir*.

    Writes into a temporary sibling of ``data_dir / sboms`` and replaces
    that directory only after collision checks succeed and at least one
    SBOM is found. A failed run deletes the temporary directory and
    leaves the previous output unchanged. Output names hash each
    wheel's relative path and SBOM member path so names stay unique
    and within the filesystem component-length limit. Raises
    ``tekton.CheckStepError``
    if *files_dir* is not relative to *data_dir*, if it is the sboms
    output directory or sits beneath it, if ``data_dir / sboms`` is a
    symbolic link or a non-directory, if a discovered wheel is a
    symbolic link or resolves outside the files directory, when a
    wheel archive cannot be read, or when no SBOM is found in any
    wheel.
    """
    try:
        wheels_dir = file.resolve_path_under_base(data_dir, files_dir)
    except ValueError as exc:
        raise tekton.CheckStepError("extracting SBOMs from wheels", exc) from exc
    # Do not resolve the final ``sboms`` entry: following an in-tree
    # symlink would make replacement and cleanup delete its target.
    sboms_dir = data_dir.resolve() / SBOMS_SUBDIR
    if sboms_dir.is_symlink() or (sboms_dir.exists() and not sboms_dir.is_dir()):
        cause = ValueError(f"sboms output path must be a directory: {sboms_dir}")
        raise tekton.CheckStepError("extracting SBOMs from wheels", cause) from cause
    if wheels_dir.is_relative_to(sboms_dir):
        cause = ValueError(
            f"files directory {files_dir!r} must not be the sboms output "
            "directory or a path beneath it"
        )
        raise tekton.CheckStepError("extracting SBOMs from wheels", cause) from cause

    parent = sboms_dir.parent
    parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".sboms-", dir=parent))
    committed = False
    try:
        written: dict[Path, tuple[str, int]] = {}
        found = 0
        try:
            wheels = file.contained_regular_files(wheels_dir, "*.whl")
        except ValueError as exc:
            raise tekton.CheckStepError("extracting SBOMs from wheels", exc) from exc
        for wheel in wheels:
            try:
                found += extract_sboms_from_wheel(
                    wheel,
                    staging,
                    output_prefix=_wheel_identity(wheel, wheels_dir),
                    written=written,
                )
            except tekton.CheckStepError:
                raise
            except (OSError, zipfile.BadZipFile, zipfile.LargeZipFile) as exc:
                raise tekton.CheckStepError("extracting SBOMs from wheels", exc) from exc
        if found == 0:
            cause = RuntimeError("No SBOMs found in any wheel")
            raise tekton.CheckStepError("extracting SBOMs from wheels", cause) from cause
        try:
            file.replace_directory(staging, sboms_dir)
        except (ValueError, OSError) as exc:
            raise tekton.CheckStepError("extracting SBOMs from wheels", exc) from exc
        committed = True
    finally:
        if not committed:
            shutil.rmtree(staging, ignore_errors=True)

    logger.info("Extracted %d SBOM(s)", found)
    for path in sorted(path for path in sboms_dir.iterdir() if path.is_file()):
        logger.info("  %s", path.name)
    return found


def main() -> int:
    """Read environment variables and extract SBOMs from wheels."""
    data_dir = Path(tekton.require_env("PARAM_DATA_DIR"))
    files_dir = tekton.require_env("PARAM_FILES_DIR")
    run(data_dir=data_dir, files_dir=files_dir)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
