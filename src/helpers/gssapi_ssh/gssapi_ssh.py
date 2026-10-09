"""GSSAPI SSH and SCP helpers for Kerberos-authenticated remote hosts."""

from __future__ import annotations

import logging
import shlex
import shutil
import subprocess
from collections.abc import Sequence
from pathlib import Path

from release_service_utils.helpers import retry

logger = logging.getLogger(__name__)


class SSHConnectionError(Exception):
    """Transient SSH connection failure (exit code 255)."""


def remote_shell_path(*parts: str) -> str:
    """Return a remote path under ``$HOME`` for an ``ssh`` command.

    OpenSSH runs the remote command through a shell, so ``$HOME`` expands.
    Each path component is shell-quoted.
    """
    return '"$HOME"/' + "/".join(shlex.quote(part) for part in parts)


def remote_scp_path(*parts: str) -> str:
    """Return a remote path under the user's home for ``scp``.

    ``scp`` uses SFTP and does not run a login shell, so ``$HOME`` is a
    literal path segment. OpenSSH expands a leading ``~`` to the remote
    home directory. Components are not shell-quoted; quotes would be
    stored as part of the filename.
    """
    return "~/" + "/".join(parts)


def install_known_hosts(fingerprint: Path, ssh_dir: Path) -> Path:
    """Copy *fingerprint* to ``ssh_dir / known_hosts`` and restrict permissions.

    Creates *ssh_dir* when it does not exist. The directory mode is ``0o700``
    and ``known_hosts`` is ``0o600``.
    """
    ssh_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    ssh_dir.chmod(0o700)
    known_hosts = ssh_dir / "known_hosts"
    shutil.copy2(fingerprint, known_hosts)
    known_hosts.chmod(0o600)
    return known_hosts


def ssh_options(known_hosts: Path, *, identities_only: bool = False) -> list[str]:
    """Return OpenSSH flags for GSSAPI authentication with a fixed known_hosts file.

    When *identities_only* is true, also pass ``IdentitiesOnly=yes``.
    """
    options = [
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "GSSAPIAuthentication=yes",
        "-o",
        "GSSAPIDelegateCredentials=yes",
    ]
    if identities_only:
        options.extend(["-o", "IdentitiesOnly=yes"])
    return options


def run_ssh(
    argv: Sequence[str],
    *,
    stdin: str | None = None,
    max_attempts: int = 3,
) -> subprocess.CompletedProcess[str]:
    """Run an SSH or SCP command, retrying transient connection failures.

    Exit code 255 is retried up to *max_attempts* times. Any other non-zero
    exit raises ``CalledProcessError`` immediately. *stdin* is passed to the
    child and is never written to the log.
    """

    def _attempt() -> subprocess.CompletedProcess[str]:
        result = subprocess.run(
            list(argv),
            input=stdin,
            text=stdin is not None,
            check=False,
        )
        if result.returncode == 255:
            logger.warning(
                "SSH connection failed (exit 255), will retry: %s",
                shlex.join(argv),
            )
            raise SSHConnectionError(f"SSH connection failed (exit 255): {shlex.join(argv)}")
        if result.returncode != 0:
            raise subprocess.CalledProcessError(result.returncode, list(argv))
        return result

    return retry.retry_with_exponential_backoff(
        _attempt,
        max_attempts=max_attempts,
        retry_on=SSHConnectionError,
    )
