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
import zipfile
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, BinaryIO

from release_service_utils.framework import Helper

# 64 KiB reads used by ``read_bounded`` and ``decompress_gzip_bounded``.
_GZIP_READ_CHUNK_SIZE = 64 * 1024
# Uncompressed ZIP-member cap used by wheel SBOM extraction.
MAX_SBOM_UNCOMPRESSED_BYTES = 16 * 1024 * 1024
_ARCHIVE_TYPE = re.compile(r"(gzip compressed data|POSIX tar archive)")


class FileHelper(Helper):
    """File, path, and temporary-file operations."""

    def load_json_dict(self, path: Path) -> dict[str, Any]:
        """Load a JSON file whose root value must be an object.

        Raise ``FileNotFoundError`` when *path* does not exist or is not a file.
        """
        if not path.is_file():
            raise FileNotFoundError(f"JSON file does not exist: {path}")
        with open(path, encoding="utf-8") as handle:
            try:
                data = json.load(handle)
            except json.JSONDecodeError as e:
                raise json.JSONDecodeError(
                    f"File is not valid JSON: {path}", e.doc, e.pos
                ) from e
        if not isinstance(data, dict):
            msg = f"JSON root must be an object: {path}"
            raise TypeError(msg)
        return data

    def sha256(self, path: Path) -> str:
        """Return the hex SHA-256 digest of the file at *path*."""
        h = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()

    def path_from_env_variable(
        self,
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

    def resolve_path_under_base(self, base: Path, relative: str | Path) -> Path:
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

    def make_tempfile_path(
        self,
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

    def encode_json_gzip_b64(self, value: Any) -> str:
        """Serialize *value* as compact JSON, gzip-compress, and standard-base64 encode."""
        raw = json.dumps(value, separators=(",", ":")).encode("utf-8")
        return base64.standard_b64encode(gzip.compress(raw)).decode("ascii")

    def decompress_gzip_bounded(self, data: bytes, *, max_bytes: int) -> bytes:
        """Decompress gzip *data* in chunks.

        Reads at most *max_bytes* of output; raises `ValueError` if the
        decompressed size would exceed that limit (gzip bomb protection).
        """
        _GZIP_READ_CHUNK_SIZE = 64 * 1024
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
        self,
        path: Path,
        *,
        file_cmd: Callable[[Sequence[str | Path]], subprocess.CompletedProcess[str]],
    ) -> bool:
        """Return True when ``file -b`` reports gzip or tar content for *path*."""
        result = file_cmd(["file", "-b", str(path)])
        return _ARCHIVE_TYPE.search(result.stdout) is not None

    def contained_regular_files(self, root: Path, pattern: str = "*") -> list[Path]:
        """Return regular files under *root* matching *pattern*."""
        base = root.resolve()
        files: list[Path] = []
        for path in sorted(root.rglob(pattern)):
            if path.is_symlink() or not path.resolve().is_relative_to(base):
                raise ValueError(f"source path must stay under {root}: {path}")
            if path.is_file():
                files.append(path)
        return files

    def swap_directory(
        self, source: Path | None, dest: Path, *, remove: bool = False
    ) -> Path | None:
        """Replace *dest* with *source*, restoring it if the replacement fails."""
        if dest.is_symlink() or (dest.exists() and not dest.is_dir()):
            raise ValueError(f"destination must be a directory: {dest}")
        outgoing: Path | None = None
        if dest.exists():
            outgoing = Path(
                tempfile.mkdtemp(prefix=f".{dest.name}-outgoing-", dir=dest.parent)
            )
            outgoing.rmdir()
            dest.rename(outgoing)
        try:
            if source is not None:
                source.rename(dest)
        except OSError:
            if outgoing is not None and outgoing.exists() and not dest.exists():
                outgoing.rename(dest)
            raise
        if remove:
            if outgoing is not None:
                shutil.rmtree(outgoing, ignore_errors=True)
            return None
        return outgoing

    def require_zip_member_size(self, info: zipfile.ZipInfo, *, max_bytes: int) -> None:
        """Reject a ZIP member whose declared size exceeds *max_bytes*."""
        if info.file_size > max_bytes:
            raise ValueError(
                f"ZIP member {info.filename!r} exceeds {max_bytes} uncompressed bytes"
            )

    def read_bounded(self, handle: BinaryIO, *, max_bytes: int) -> bytes:
        """Read *handle* and reject output exceeding *max_bytes*."""
        output = bytearray()
        while True:
            chunk = handle.read(_GZIP_READ_CHUNK_SIZE)
            if not chunk:
                break
            output.extend(chunk)
            if len(output) > max_bytes:
                raise ValueError(f"read data exceeds {max_bytes} bytes")
        return bytes(output)


# Singleton instance for backward compatibility
_helper = FileHelper()

# Export instance methods as module-level functions
load_json_dict = _helper.load_json_dict
sha256 = _helper.sha256
path_from_env_variable = _helper.path_from_env_variable
resolve_path_under_base = _helper.resolve_path_under_base
make_tempfile_path = _helper.make_tempfile_path
encode_json_gzip_b64 = _helper.encode_json_gzip_b64
decompress_gzip_bounded = _helper.decompress_gzip_bounded
is_gzip_or_tar_archive = _helper.is_gzip_or_tar_archive
contained_regular_files = _helper.contained_regular_files
swap_directory = _helper.swap_directory
require_zip_member_size = _helper.require_zip_member_size
read_bounded = _helper.read_bounded
