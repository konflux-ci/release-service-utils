"""Tests for the `image_ref` helper module."""

from __future__ import annotations

import image_ref
import pytest


def test_pyxis_url_for_pull_spec_with_tag_and_registry_rewrite() -> None:
    """Tagged refs map to `.../tag/<tag>` and rewrite registry.redhat.io host."""
    out = image_ref.pyxis_url_for_pull_spec(
        "https://pyxis.engineering.redhat.com/v1",
        "registry.redhat.io/repo/image:1.2",
    )
    assert out.endswith(
        "/repositories/registry/registry.access.redhat.com/repository/repo/image/tag/1.2"
    )


def test_pyxis_url_for_pull_spec_without_tag() -> None:
    """Untyped pull specs omit the trailing `/tag` path segment."""
    out = image_ref.pyxis_url_for_pull_spec("https://pyxis/v1", "r.io/repo/image")
    assert out.endswith("/repositories/registry/r.io/repository/repo/image")
    assert "/tag/" not in out


def test_pyxis_url_for_pull_spec_invalid() -> None:
    """Invalid pull specs raise `ValueError`."""
    with pytest.raises(ValueError, match="invalid pull spec"):
        image_ref.pyxis_url_for_pull_spec("https://pyxis/v1", "not/a-pullspec")
