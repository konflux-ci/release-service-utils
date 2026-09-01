"""Skopeo CLI wrapper helpers."""

from __future__ import annotations

import re
import subprocess

_REPO_NOT_FOUND_RE = re.compile(r"name unknown|repository not found", re.IGNORECASE)


def inspect(
    image_ref: str,
    *,
    config: bool = False,
    raw: bool = False,
    no_tags: bool = False,
    override_os: str | None = None,
    override_arch: str | None = None,
    retry_times: int = 3,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run ``skopeo inspect`` on a container image reference."""
    cmd = ["skopeo", "inspect", "--retry-times", str(retry_times)]
    if config:
        cmd.append("--config")
    if raw:
        cmd.append("--raw")
    if no_tags:
        cmd.append("--no-tags")
    if override_os:
        cmd += ["--override-os", override_os]
    if override_arch:
        cmd += ["--override-arch", override_arch]
    cmd.append(f"docker://{image_ref}")
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def list_tags(
    repo: str,
    *,
    retry_times: int = 3,
) -> subprocess.CompletedProcess[str]:
    """Run ``skopeo list-tags`` on a repository reference."""
    cmd = ["skopeo", "list-tags", "--retry-times", str(retry_times), f"docker://{repo}"]
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def is_repo_not_found(result: subprocess.CompletedProcess[str]) -> bool:
    """Return whether a failed skopeo result means the repository doesn't exist.

    Matches the registry API's ``NAME_UNKNOWN`` error (surfaced by skopeo as
    ``name unknown`` or ``repository not found``), as opposed to other
    failures like auth or network errors. Useful for callers that want to
    treat a never-pushed-to repository the same as an existing-but-empty one
    (e.g. tag incrementer logic).
    """
    return bool(_REPO_NOT_FOUND_RE.search(result.stderr or ""))


def copy(
    source: str,
    dest: str,
    *,
    retry_times: int = 3,
    check: bool = False,
) -> subprocess.CompletedProcess[str]:
    """Run ``skopeo copy`` to copy an image between transports."""
    cmd = ["skopeo", "copy", "--retry-times", str(retry_times), source, dest]
    return subprocess.run(cmd, capture_output=True, text=True, check=check)
