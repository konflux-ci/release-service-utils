"""Skopeo CLI wrapper helpers."""

from __future__ import annotations

import subprocess


def inspect(
    image_ref: str,
    *,
    config: bool = False,
    raw: bool = False,
    retry_times: int = 3,
) -> subprocess.CompletedProcess[str]:
    """Run ``skopeo inspect`` on a container image reference."""
    cmd = ["skopeo", "inspect", "--retry-times", str(retry_times)]
    if config:
        cmd.append("--config")
    if raw:
        cmd.append("--raw")
    cmd.append(f"docker://{image_ref}")
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def copy(
    source: str,
    dest: str,
    *,
    retry_times: int = 3,
) -> subprocess.CompletedProcess[str]:
    """Run ``skopeo copy`` to copy an image between transports."""
    cmd = ["skopeo", "copy", "--retry-times", str(retry_times), source, dest]
    return subprocess.run(cmd, capture_output=True, text=True, check=False)
