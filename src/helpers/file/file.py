"""File, path, and temporary-file helpers for task scripts."""

from __future__ import annotations

import base64
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, BinaryIO


def load_json_dict(path: Path) -> dict[str, Any]:
    """Load a JSON file whose root value must be an object."""
    with open(path, encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        msg = f"JSON root must be an object: {path}"
        raise TypeError(msg)
    return data


_GZIP_READ_CHUNK_SIZE = 64 * 1024
_ARCHIVE_TYPE = re.compile(r"(gzip compressed data|POSIX tar archive)")


def sha256(path: Path) -> str:
    """Return the hex SHA-256 digest of the file at *path*."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def path_from_env_variable(
    name: str,
    default: str | Path,
) -> Path:
    """Return a filesystem path from an environment variable, or a default.

    The value of name in `os.environ` (if set and not blank after
    `str.strip`) is interpreted as a path; it is not a path to a file whose
    contents you read, and this function does not open or stat paths.

    If the variable is missing or only whitespace, default is returned (a str
    or an existing `Path`).

    Typical use: a Tekton or pod env var that holds a mount directory path, with
    tests setting the same variable to a temp directory. Existence of the path
    is not checked.
    """
    raw = os.environ.get(name)
    if raw is not None and str(raw).strip() != "":
        return Path(str(raw).strip())
    return default if isinstance(default, Path) else Path(default)


def resolve_path_under_base(base: Path, relative: str | Path) -> Path:
    """Resolve *relative* under *base* and ensure the result stays inside *base*.

    Rejects absolute paths and ``..`` traversal after resolution. Typical use:
    Tekton passes a path relative to a data directory (e.g. charon env/config files).
    """
    text = str(relative).strip()
    if not text:
        raise ValueError(f"path must be relative to {base}: {relative!r}")
    rel = Path(text)
    if rel.is_absolute():
        raise ValueError(f"path must be relative to {base}: {relative!r}")
    root = base.resolve()
    candidate = (root / rel).resolve()
    if not candidate.is_relative_to(root):
        raise ValueError(f"path must stay under {base}: {relative!r}")
    return candidate


def contained_regular_files(root: Path, pattern: str = "*") -> list[Path]:
    """Return regular files under *root* matching *pattern*.

    Rejects symbolic-link entries and any path whose resolved target
    escapes *root* before callers compare, open, or copy the file.
    Non-file matches such as directories are skipped.
    """
    base = root.resolve()
    files: list[Path] = []
    for path in sorted(root.rglob(pattern)):
        if path.is_symlink() or not path.resolve().is_relative_to(base):
            raise ValueError(f"source path must stay under {root}: {path}")
        if path.is_file():
            files.append(path)
    return files


def swap_directory(source: Path, dest: Path) -> Path | None:
    """Replace *dest* with *source* and return the previous *dest* location.

    Moves the previous *dest* aside first. If the swap fails, restore that
    previous directory. The caller owns the returned backup and must
    delete it after a later commit step succeeds. Rejects a symbolic
    link or non-directory *dest* so replacement cannot follow an in-tree
    link and delete unrelated data.
    """
    if dest.is_symlink() or (dest.exists() and not dest.is_dir()):
        raise ValueError(f"destination must be a directory: {dest}")
    outgoing: Path | None = None
    if dest.exists():
        outgoing = Path(tempfile.mkdtemp(prefix=f".{dest.name}-outgoing-", dir=dest.parent))
        outgoing.rmdir()
        dest.rename(outgoing)
    try:
        source.rename(dest)
    except OSError:
        if outgoing is not None and outgoing.exists() and not dest.exists():
            outgoing.rename(dest)
        raise
    return outgoing


def replace_directory(source: Path, dest: Path) -> None:
    """Replace *dest* with *source* and delete the previous directory."""
    outgoing = swap_directory(source, dest)
    if outgoing is not None:
        shutil.rmtree(outgoing, ignore_errors=True)


def restore_directory(backup: Path | None, dest: Path) -> None:
    """Move *dest* aside and put *backup* back at *dest*."""
    if dest.exists():
        failed = Path(tempfile.mkdtemp(prefix=f".{dest.name}-failed-", dir=dest.parent))
        failed.rmdir()
        dest.rename(failed)
        shutil.rmtree(failed, ignore_errors=True)
    if backup is not None:
        backup.rename(dest)


def make_tempfile_path(
    prefix: str,
    data: bytes | None = None,
) -> Path:
    """Create a secure private temp file and return a pathlib.Path to it.

    Uses the standard library `tempfile.mkstemp`, which creates a new file and
    returns a file handle (a safe pattern). We never use the old `mktemp` API
    (unsafe under concurrency; deprecated in Python 3.12). If `data` is given,
    those bytes are written into the new file; otherwise the file is empty. The
    file is closed before returning; the caller is responsible for deleting the
    path when done. `prefix` is the filename prefix in the system temp
    directory, same as the `prefix` argument to `mkstemp`.
    """
    fd, name = tempfile.mkstemp(prefix=prefix)
    try:
        if data is not None:
            os.write(fd, data)
    finally:
        os.close(fd)
    return Path(name)


def encode_json_gzip_b64(value: Any) -> str:
    """Serialize *value* as compact JSON, gzip-compress, and standard-base64 encode."""
    raw = json.dumps(value, separators=(",", ":")).encode("utf-8")
    return base64.standard_b64encode(gzip.compress(raw)).decode("ascii")


def read_bounded(handle: BinaryIO, *, max_bytes: int) -> bytes:
    """Read *handle* in chunks.

    Reads at most *max_bytes* of output; raises `ValueError` if the
    size would exceed that limit.
    """
    output = bytearray()
    while True:
        chunk = handle.read(_GZIP_READ_CHUNK_SIZE)
        if not chunk:
            break
        output.extend(chunk)
        if len(output) > max_bytes:
            msg = f"read data exceeds {max_bytes} bytes"
            raise ValueError(msg)
    return bytes(output)


def decompress_gzip_bounded(data: bytes, *, max_bytes: int) -> bytes:
    """Decompress gzip *data* in chunks.

    Reads at most *max_bytes* of output; raises `ValueError` if the
    decompressed size would exceed that limit (gzip bomb protection).
    """
    output = bytearray()
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz_file:
        while True:
            chunk = gz_file.read(_GZIP_READ_CHUNK_SIZE)
            if not chunk:
                break
            output.extend(chunk)
            if len(output) > max_bytes:
                msg = f"decompressed data exceeds {max_bytes} bytes (possible gzip bomb)"
                raise ValueError(msg)
    return bytes(output)


def is_gzip_or_tar_archive(
    path: Path,
    *,
    file_cmd: Callable[[Sequence[str | Path]], subprocess.CompletedProcess[str]],
) -> bool:
    """Return True when ``file -b`` reports gzip or tar content for *path*."""
    result = file_cmd(["file", "-b", str(path)])
    return _ARCHIVE_TYPE.search(result.stdout) is not None
