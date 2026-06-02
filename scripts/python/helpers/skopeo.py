"""Skopeo CLI wrapper helpers."""

from __future__ import annotations

import subprocess


def inspect(
    image_ref: str,
    *,
    config: bool = False,
    raw: bool = False,
    creds: str | None = None,
    retry_times: int = 3,
) -> subprocess.CompletedProcess[str]:
    """Run ``skopeo inspect`` on a container image reference."""
    cmd = ["skopeo", "inspect", "--retry-times", str(retry_times)]
    if config:
        cmd.append("--config")
    if raw:
        cmd.append("--raw")
    if creds:
        cmd.extend(["--creds", creds])
    cmd.append(f"docker://{image_ref}")
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def copy(
    source: str,
    dest: str,
    *,
    all: bool = False,
    preserve_digests: bool = False,
    src_tls_verify: bool | None = None,
    src_creds: str | None = None,
    dest_creds: str | None = None,
    retry_times: int = 3,
) -> subprocess.CompletedProcess[str]:
    """Run ``skopeo copy`` to copy an image between transports."""
    cmd = ["skopeo", "copy", "--retry-times", str(retry_times)]
    if all:
        cmd.append("--all")
    if preserve_digests:
        cmd.append("--preserve-digests")
    if src_tls_verify is not None:
        cmd.append(f"--src-tls-verify={str(src_tls_verify).lower()}")
    if src_creds:
        cmd.extend(["--src-creds", src_creds])
    if dest_creds:
        cmd.extend(["--dest-creds", dest_creds])
    cmd.extend([source, dest])
    return subprocess.run(cmd, capture_output=True, text=True, check=False)
