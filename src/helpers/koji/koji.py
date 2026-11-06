"""Shared Koji client helpers for managed release tasks.

Install tenant koji profiles copied out of ``pipelineImage``, run the koji
CLI with those profiles, and prepare a Kerberos keytab for koji commands.
"""

from __future__ import annotations

import base64
import errno
import os
import re
import shutil
import stat
import subprocess
from pathlib import Path

from release_service_utils.helpers import file as file_helper
from release_service_utils.helpers import tekton
from release_service_utils.helpers.authentication import kinit_with_retry
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.redact import redact_secrets

_KINIT_MAX_SLEEP_SECONDS = 2
_TOKEN_ARG = re.compile(r"--token=\S+")


def install_koji_client_config(source: Path) -> None:
    """Copy staged koji client config into ``~/.koji``.

    *source* is a directory with an optional ``config`` file and ``config.d``
    subdirectory, as produced by the catalog ``copy-koji-config`` step.
    Raise ``ValueError`` when *source* is missing. An empty directory is
    allowed so catalog tests can run against an image that has no profiles.
    Symlinks are followed so tenant profile links are copied as regular files.
    """
    if not source.is_dir():
        raise ValueError(f"koji client config source is missing: {source}")

    dest = Path.home() / ".koji"
    dest_d = dest / "config.d"
    dest_d.mkdir(parents=True, exist_ok=True)

    config_file = source / "config"
    if config_file.is_file():
        shutil.copy2(config_file, dest / "config", follow_symlinks=True)

    config_d = source / "config.d"
    if not config_d.is_dir():
        return
    for entry in config_d.iterdir():
        if entry.is_file():
            shutil.copy2(entry, dest_d / entry.name, follow_symlinks=True)


def _command_secrets(cmd: list[str]) -> list[str]:
    """Return reservation-token values embedded in a Koji command."""
    secrets: list[str] = []
    for arg in cmd:
        if arg.startswith("--token="):
            secret = arg.removeprefix("--token=")
        elif arg.startswith('"') and arg.endswith('"') and arg != '"konflux"':
            secret = arg[1:-1]
        else:
            continue
        if secret:
            secrets.append(secret)
    return secrets


def redact_koji_output(text: str, cmd: list[str]) -> str:
    """Redact reservation tokens and other credentials from Koji output."""
    redacted = redact_secrets(text)
    for secret in _command_secrets(cmd):
        redacted = redacted.replace(secret, "<REDACTED>")
    return _TOKEN_ARG.sub("--token=<REDACTED>", redacted)


def _redact_cmd(cmd: list[str]) -> list[str]:
    """Return a copy of a Koji command with build tokens removed.

    Pipeline logs are a public record. ``import-cg`` takes the token as
    ``--token=``, and ``CGRefundBuild`` takes it as a quoted argument. The
    content-generator name ``"konflux"`` is the only other quoted argument
    these tasks send, so every other quoted argument is treated as a token.
    """
    redacted: list[str] = []
    for arg in cmd:
        if arg.startswith("--token="):
            redacted.append("--token=<REDACTED>")
        elif arg.startswith('"') and arg.endswith('"') and arg != '"konflux"':
            redacted.append('"<REDACTED>"')
        else:
            redacted.append(arg)
    return redacted


def run_koji_cmd(
    profile: str,
    *args: str,
    check: bool = True,
    capture_output: bool = True,
    noauth: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run a koji command with the specified profile.

    Always pass empty ``--plugin-paths=`` so a tenant config copied from
    ``pipelineImage`` cannot load plugins from the shared volume.

    Stdout and stderr are captured so Koji does not write them straight to
    the pipeline log. The logged command has build tokens removed. On
    failure, stderr is logged after reservation tokens and other credentials
    are redacted. The raised ``CalledProcessError`` keeps that redacted
    stderr, and its ``cmd`` is redacted so a traceback cannot reprint the
    token.
    """
    cmd = ["koji", f"--profile={profile}", "--plugin-paths="]
    if noauth:
        cmd.append("--noauth")
    cmd.extend(args)
    logger.info("Running: %s", " ".join(_redact_cmd(cmd)))
    try:
        return subprocess.run(
            cmd,
            capture_output=capture_output,
            text=True,
            check=check,
        )
    except subprocess.CalledProcessError as exc:
        if exc.stderr:
            exc.stderr = redact_koji_output(exc.stderr, cmd)
            if exc.stderr.strip():
                logger.error("Koji stderr: %s", exc.stderr.strip())
        exc.cmd = _redact_cmd(cmd)
        raise


def test_koji_connection(profile: str) -> None:
    """Probe Koji with ``hello`` and log success."""
    logger.info("Testing Koji connection...")
    run_koji_cmd(profile, "hello")
    logger.info("Koji connection successful")


def _reject_escaped_keytab_path(base: Path, relative: str) -> Path:
    """Return *relative* resolved under *base*, or reject an escaping path."""
    try:
        confined = file_helper.resolve_path_under_base(base, relative)
    except ValueError as e:
        raise ValueError(f"keytab path must stay under {base}: {relative!r}") from e
    if confined == base.resolve():
        raise ValueError(f"keytab path must name a file under {base}: {relative!r}")
    return confined


def _confined_keytab_path(base: Path, relative: str) -> Path:
    """Return *relative* under *base* without following dest-side symlinks.

    Absolute paths and ``..`` traversal are rejected. A symlink at any
    destination component is rejected so a later write cannot truncate the
    link target.
    """
    confined = _reject_escaped_keytab_path(base, relative)
    current = base.resolve()
    for part in Path(str(relative).strip()).parts:
        current = current / part
        if current.is_symlink():
            raise ValueError(f"keytab path must not be a symlink: {relative!r}")
    return confined


def _is_regular_file(path: Path) -> bool:
    """Return whether *path* is a regular file, without following symlinks."""
    try:
        mode = path.lstat().st_mode
    except FileNotFoundError:
        return False
    return stat.S_ISREG(mode)


def _read_keytab_bytes(path: Path) -> bytes:
    """Read *path* without following a symlink."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    except OSError as e:
        if e.errno == errno.ELOOP:
            raise ValueError(f"keytab path must not be a symlink: {path}") from e
        raise
    with os.fdopen(fd, "rb") as handle:
        return handle.read()


def _write_keytab_secure(keytab_path: Path, data: bytes) -> None:
    """Write keytab data with mode 0600, without following symlinks.

    ``O_CREAT`` applies the mode only when the file is created. An existing
    destination is forced to 0600 before the keytab bytes are written.
    """
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW
    try:
        fd = os.open(keytab_path, flags, 0o600)
    except OSError as e:
        if e.errno == errno.ELOOP:
            raise ValueError(f"keytab path must not be a symlink: {keytab_path}") from e
        raise
    try:
        os.fchmod(fd, 0o600)
        os.write(fd, data)
    finally:
        os.close(fd)


def prepare_keytab(secret_mount: Path, keytab_file: str, working_dir: Path) -> Path:
    """Copy the keytab into *working_dir*.

    The destination is a regular file under *working_dir* and is never a
    symlink. Source paths may be Secret-volume symlinks, including ``..data``
    links, when they resolve to a regular file inside *secret_mount*. Absolute
    paths, ``..`` traversal, and source links that leave the mount are rejected.
    """
    keytab_path = _confined_keytab_path(working_dir, keytab_file)
    base64_keytab = _reject_escaped_keytab_path(secret_mount, "base64_keytab")
    if _is_regular_file(base64_keytab):
        logger.info("Reading base64-encoded keytab from secret")
        encoded = _read_keytab_bytes(base64_keytab).decode("utf-8").strip()
        _write_keytab_secure(keytab_path, base64.b64decode(encoded))
    else:
        source_keytab = _reject_escaped_keytab_path(secret_mount, keytab_file)
        if not _is_regular_file(source_keytab):
            raise FileNotFoundError(f"Keytab file not found: {source_keytab}")
        logger.info("Copying keytab from secret mount")
        _write_keytab_secure(keytab_path, _read_keytab_bytes(source_keytab))

    return keytab_path


def kinit(principal: str, keytab_path: Path, max_attempts: int = 10) -> str:
    """Perform kinit with retries and return the ccache path.

    Each retry wait is capped at ``_KINIT_MAX_SLEEP_SECONDS``. The helper's
    uncapped backoff grows from 5 seconds, so ten attempts would otherwise
    sleep for about 42 minutes.
    """
    ccache_path = file_helper.make_tempfile_path("ccache-")
    authenticated = False
    try:
        kenv = {"KRB5CCNAME": str(ccache_path)}
        kinit_with_retry(
            principal,
            keytab_path,
            kenv,
            max_attempts=max_attempts,
            max_sleep_seconds=_KINIT_MAX_SLEEP_SECONDS,
        )
        authenticated = True
        return str(ccache_path)
    except subprocess.CalledProcessError as e:
        raise tekton.CheckStepError("logging in with Kerberos (kinit)", e) from e
    finally:
        if not authenticated:
            ccache_path.unlink(missing_ok=True)
