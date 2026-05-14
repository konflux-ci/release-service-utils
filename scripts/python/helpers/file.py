"""File, path, and temporary-file helpers for task scripts."""

from __future__ import annotations

import hashlib
import os
import tempfile
from pathlib import Path


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
    """Return a filesystem path from the named environment variable, or *default*.

    The value of *name* in ``os.environ`` (if set and non-blank after
    ``str.strip``) is interpreted as a path; this function does not open or
    stat paths.

    If the variable is missing or only whitespace, *default* is returned (a
    str or an existing ``Path``).

    Typical use: a Tekton or pod env var that holds a mount directory path, with
    tests setting the same variable to a temp directory. Existence of the path
    is not checked.
    """
    raw = os.environ.get(name)
    if raw is not None and str(raw).strip() != "":
        return Path(str(raw).strip())
    return default if isinstance(default, Path) else Path(default)


def make_tempfile_path(
    prefix: str,
    data: bytes | None = None,
) -> Path:
    """Create a secure private temp file and return a pathlib.Path to it.

    Uses the standard library ``tempfile.mkstemp``, which creates a new file and
    returns a file handle (a safe pattern). We never use the old ``mktemp`` API
    (unsafe under concurrency; deprecated in Python 3.12). If ``data`` is given,
    those bytes are written into the new file; otherwise the file is empty. The
    file is closed before returning; the caller is responsible for deleting the
    path when done. ``prefix`` is the filename prefix in the system temp
    directory, same as the ``prefix`` argument to ``mkstemp``.
    """
    fd, name = tempfile.mkstemp(prefix=prefix)
    try:
        if data is not None:
            os.write(fd, data)
    finally:
        os.close(fd)
    return Path(name)
