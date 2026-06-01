"""Helpers for reading Konflux snapshot JSON documents."""

from __future__ import annotations

from pathlib import Path

import file


def first_component(snapshot_path: Path) -> dict[str, str]:
    """Return fields from the first snapshot `components` entry.

    Keys: `revision`, `origin_repo` (`source.git.url` without `.git`),
    and `container_image`.
    """
    snapshot = file.load_json_dict(snapshot_path)
    components = snapshot.get("components")
    if not isinstance(components, list) or not components:
        msg = f"snapshot has no components: {snapshot_path}"
        raise ValueError(msg)
    first = components[0]
    if not isinstance(first, dict):
        msg = f"snapshot component[0] must be an object: {snapshot_path}"
        raise TypeError(msg)
    source = first.get("source")
    if not isinstance(source, dict):
        msg = f"snapshot component[0].source must be an object: {snapshot_path}"
        raise TypeError(msg)
    git_info = source.get("git")
    if not isinstance(git_info, dict):
        msg = f"snapshot component[0].source.git must be an object: {snapshot_path}"
        raise TypeError(msg)
    revision = str(git_info.get("revision", "")).strip()
    origin_repo = str(git_info.get("url", "")).strip().removesuffix(".git")
    container_image = str(first.get("containerImage", "")).strip()
    return {
        "revision": revision,
        "origin_repo": origin_repo,
        "container_image": container_image,
    }
