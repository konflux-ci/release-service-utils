"""Helpers for parsing and normalizing container image references."""

from .image_ref import (  # noqa: F401
    convert_to_quay,
    convert_to_registry,
    convert_to_registry_access,
    pyxis_url_for_pull_spec,
    registry,
    repository,
    resolve_quay_digest_to_git_sha,
    split_image_ref,
    strip_tag_and_digest,
    translate_delivery_repo,
)
