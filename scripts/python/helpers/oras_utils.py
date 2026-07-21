"""Shared helpers for OCI artifact operations using the oras CLI."""

from __future__ import annotations

import re
import subprocess
import tempfile
from pathlib import Path

import file
import subprocess_cmd
from subprocess_cmd import run_cmd


def oras_resolve(
    reference: str,
    *,
    auth_ref: str | None = None,
    check: bool = True,
) -> str | None:
    """Resolve the digest of an OCI image reference using oras.

    Obtains registry credentials via ``select-oci-auth`` and runs
    ``oras resolve``.

    *auth_ref* overrides the reference passed to ``select-oci-auth`` —
    useful when resolving a tagged reference (``repo:tag``) but the
    auth credentials should be obtained for the bare repository URL.
    Defaults to *reference* when not given.

    When *check* is ``True`` (the default), ``RuntimeError`` is raised on
    a non-zero exit code.  When ``False``, ``None`` is returned instead,
    which is convenient for "try to resolve, treat failure as not-found"
    callers.
    """
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json") as auth_file:
        select_auth = run_cmd(["select-oci-auth", auth_ref or reference], check=False)
        auth_content = select_auth.stdout.strip()
        auth_file.write(auth_content if auth_content else "{}")
        auth_file.flush()

        result = run_cmd(
            ["oras", "resolve", "--registry-config", auth_file.name, reference],
            check=False,
        )

    if result.returncode != 0:
        if check:
            raise RuntimeError(
                f"oras resolve failed for {reference!r}"
                f" (exit {result.returncode}): {result.stderr.strip()}"
            )
        return None
    digest = result.stdout.strip()
    return digest or None


def oras_login(registry: str, username: str, password: str) -> None:
    """Log in to an OCI registry via oras using username/password credentials.

    Credentials are passed via stdin to avoid exposing them in process arguments.
    Raises ``subprocess.CalledProcessError`` if the login fails.
    """
    subprocess.run(
        ["oras", "login", registry, "-u", username, "--password-stdin"],
        input=password,
        text=True,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def oras_pull(
    pull_spec: str,
    download_dir: Path,
    *,
    stderr_path: Path | None = None,
) -> None:
    """Pull an OCI artifact into *download_dir* using select-oci-auth and oras."""
    auth_file = file.make_tempfile_path("oras-auth-")
    try:
        auth_out = subprocess_cmd.run_cmd(
            ["select-oci-auth", str(pull_spec)],
            check=True,
        ).stdout
        auth_file.write_text(auth_out, encoding="utf-8")
        subprocess_cmd.run_cmd(
            [
                "oras",
                "pull",
                "--registry-config",
                str(auth_file),
                str(pull_spec),
            ],
            cwd=download_dir,
            stderr_path=stderr_path,
            check=True,
        )
    finally:
        # Always remove the auth file; subprocess failures still propagate to callers.
        auth_file.unlink(missing_ok=True)


def oras_push(tag: str, directory: Path, subdirectory: str, component_name: str) -> str:
    """Push *subdirectory* inside *directory* to an OCI registry via oras.

    Runs ``oras push --annotation=quay.expires-after=1d <tag> <subdirectory>`` with
    *directory* as the working directory and returns the ``sha256:<hex>`` digest string.

    Raises ``RuntimeError`` if the digest cannot be extracted from the oras output,
    which typically indicates a failed or incomplete push.
    """
    result = subprocess.check_output(
        [
            "oras",
            "push",
            "--annotation=quay.expires-after=1d",
            tag,
            subdirectory,
        ],
        cwd=str(directory),
        stderr=subprocess.STDOUT,
        text=True,
    )
    match = re.search(r"Digest:\s+(\S+)", result)
    if not match:
        raise RuntimeError(
            f"Could not extract digest from oras push output for {component_name}:\n{result}"
        )
    return match.group(1)


def os_arch_dir(
    os_name: str, arch: str, *, mac_windows_base: Path, linux_base: Path
) -> Path | None:
    """Return the OS/arch content directory for *os_name* and *arch*, or ``None``.

    For macOS and Windows, the directory sits under *mac_windows_base* (e.g.
    ``component_dir / "unsigned"`` or ``component_dir / "signed"``); for Linux it sits
    under *linux_base* (typically ``component_dir / "linux"``).  Returns ``None`` for
    unrecognised OS names so callers can skip or raise as appropriate.
    """
    if os_name == "darwin":
        return mac_windows_base / "macos" / arch
    if os_name == "linux":
        return linux_base / arch
    if os_name == "windows":
        return mac_windows_base / "windows" / arch
    return None
