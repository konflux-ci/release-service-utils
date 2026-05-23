"""Helpers for parsing and normalizing container image references."""

from __future__ import annotations


def pyxis_url_for_pull_spec(pyxis_url: str, pull_spec: str) -> str:
    """
    Build the Pyxis repository/tag API URL for `pull_spec`.

    `registry.redhat.io` is rewritten to `registry.access.redhat.com` to
    match Pyxis lookups.
    """
    normalized = pull_spec.replace("registry.redhat.io", "registry.access.redhat.com", 1)
    parts = normalized.split("/", 2)
    if len(parts) < 3:
        raise ValueError(f"invalid pull spec: {pull_spec!r}")
    registry, repo, image_and_tag = parts
    image, sep, tag = image_and_tag.partition(":")
    base = (
        f"{pyxis_url.rstrip('/')}/repositories/registry/{registry}/repository/"
        f"{repo}/{image}"
    )
    if sep and tag:
        return f"{base}/tag/{tag}"
    return base
