"""Helpers for reading Konflux snapshot JSON documents."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import file


def _is_truthy(value: Any) -> bool:
    """Return True for boolean ``True`` or the case-insensitive string ``"true"``."""
    return value is True or str(value).lower() == "true"


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


def default_push_source_container(data: dict[str, Any]) -> bool:
    """Return the mapping default for `pushSourceContainer`, defaulting to true."""
    mapping = data.get("mapping")
    if not isinstance(mapping, dict):
        return True
    defaults = mapping.get("defaults")
    if not isinstance(defaults, dict):
        return True
    value = defaults.get("pushSourceContainer")
    if value is None:
        return True
    return bool(value)


def component_push_source_container(
    component: dict[str, Any],
    default_push_source_container: bool,
) -> bool:
    """Return the component level pushSourceContainer value."""
    if component.get("pushSourceContainer") is True:
        return True
    if "pushSourceContainer" not in component and default_push_source_container:
        return True
    return False


def component_public(data: dict[str, Any], component: dict[str, Any]) -> bool:
    """Return whether a component should be made public.

    A component is public when its own `public` field is truthy, or when
    `public` is absent and the mapping-level default (`mapping.defaults.public`)
    is true.
    """
    mapping = data.get("mapping")
    default = False
    if isinstance(mapping, dict):
        defaults = mapping.get("defaults")
        if isinstance(defaults, dict):
            default = _is_truthy(defaults.get("public", False))
    return _is_truthy(component.get("public", default))
