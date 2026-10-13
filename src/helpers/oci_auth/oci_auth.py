"""Obtain registry credentials via the ``select-oci-auth`` CLI."""

from __future__ import annotations

import subprocess
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from release_service_utils.helpers import file, subprocess_cmd
from release_service_utils.helpers.logger import logger

EMPTY_AUTH = "{}"


def _log_stderr(
    result: subprocess.CompletedProcess[str] | subprocess.CalledProcessError,
) -> None:
    """Log command stderr without exposing credential-bearing stdout."""
    if result.stderr and result.stderr.strip():
        logger.info(result.stderr.rstrip())


def auth_json(
    reference: str,
    *,
    check: bool = True,
    stderr_path: Path | None = None,
) -> str:
    """Return docker config JSON for *reference* from ``select-oci-auth``.

    Empty stdout becomes ``{}``. Stdout is never logged because it contains
    registry credentials. Stderr is logged on failure. *stderr_path*, when set,
    is passed through to the subprocess helper so callers can capture
    diagnostics.

    When *check* is True, a nonzero exit raises ``CalledProcessError``. When
    False, a nonzero exit returns ``{}``.
    """
    try:
        result = subprocess_cmd.run_cmd(
            ["select-oci-auth", reference],
            check=check,
            stderr_path=stderr_path,
        )
    except subprocess.CalledProcessError as exc:
        _log_stderr(exc)
        raise
    if result.returncode != 0:
        _log_stderr(result)
        return EMPTY_AUTH
    return result.stdout.strip() or EMPTY_AUTH


@contextmanager
def auth_file(
    reference: str,
    *,
    check: bool = True,
    stderr_path: Path | None = None,
) -> Iterator[Path]:
    """Yield a temp file containing auth JSON; delete it on exit."""
    path = file.make_tempfile_path(
        "oci-auth-",
        auth_json(reference, check=check, stderr_path=stderr_path).encode("utf-8"),
    )
    try:
        yield path
    finally:
        path.unlink(missing_ok=True)


@contextmanager
def docker_config(
    reference: str,
    *,
    check: bool = True,
    stderr_path: Path | None = None,
) -> Iterator[Path]:
    """Yield a temp directory containing ``config.json`` for ``DOCKER_CONFIG``."""
    with tempfile.TemporaryDirectory(prefix="docker-config-") as config_dir_name:
        config_dir = Path(config_dir_name)
        (config_dir / "config.json").write_text(
            auth_json(reference, check=check, stderr_path=stderr_path),
            encoding="utf-8",
        )
        yield config_dir
