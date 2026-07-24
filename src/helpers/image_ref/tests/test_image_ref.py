"""Tests for the `image_ref` helper module."""

from __future__ import annotations

import json
import logging
from unittest import mock

from release_service_utils.helpers import image_ref

import pytest
import requests


@pytest.fixture(autouse=True)
def _propagate_release_logger() -> None:
    """Allow caplog to capture records from the 'release' logger."""
    release_logger = logging.getLogger("release")
    release_logger.propagate = True
    yield
    release_logger.propagate = False


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


def test_resolve_quay_digest_skips_non_quay() -> None:
    """Non-quay.io images return `None` without calling the Quay API."""
    assert (
        image_ref.resolve_quay_digest_to_git_sha(
            "sha256:abc",
            "registry.io/org/repo@sha256:abc",
        )
        is None
    )


def test_resolve_quay_digest_finds_sha_tag() -> None:
    """A 40-char hex tag matching the digest is returned from the first API page."""
    digest = "sha256:" + "a" * 64
    sha = "b" * 40
    payload = json.dumps(
        {
            "tags": [{"name": sha, "manifest_digest": digest}],
            "has_additional": False,
        }
    )
    with mock.patch(
        "release_service_utils.helpers.http_client.get_text", return_value=payload
    ):
        out = image_ref.resolve_quay_digest_to_git_sha(
            digest,
            f"quay.io/org/repo@{digest}",
        )
    assert out == sha


def test_resolve_quay_digest_non_200_response() -> None:
    """Quay API errors return `None` instead of raising."""
    digest = "sha256:" + "a" * 64
    response = mock.MagicMock(status_code=503)
    with mock.patch(
        "release_service_utils.helpers.http_client.get_text",
        side_effect=requests.HTTPError(response=response),
    ):
        out = image_ref.resolve_quay_digest_to_git_sha(
            digest,
            f"quay.io/org/repo@{digest}",
        )
    assert out is None


def test_resolve_quay_digest_paginates() -> None:
    """Resolution follows `has_additional` across multiple tag-list pages."""
    digest = "sha256:" + "a" * 64
    sha = "c" * 40
    page_one = json.dumps({"tags": [], "has_additional": True})
    page_two = json.dumps(
        {
            "tags": [{"name": sha, "manifest_digest": digest}],
            "has_additional": False,
        }
    )
    with mock.patch(
        "release_service_utils.helpers.http_client.get_text",
        side_effect=[page_one, page_two],
    ) as get_text:
        out = image_ref.resolve_quay_digest_to_git_sha(
            digest,
            f"quay.io/org/repo@{digest}",
        )
    assert out == sha
    assert get_text.call_count == 2


def test_resolve_quay_digest_no_matching_tag() -> None:
    """Return `None` when no tag has both the digest and a 40-char hex name."""
    digest = "sha256:" + "a" * 64
    payload = json.dumps(
        {
            "tags": [
                {"name": "not-a-sha", "manifest_digest": digest},
                {"name": "b" * 40, "manifest_digest": "sha256:other"},
            ],
            "has_additional": False,
        }
    )
    with mock.patch(
        "release_service_utils.helpers.http_client.get_text", return_value=payload
    ):
        out = image_ref.resolve_quay_digest_to_git_sha(
            digest,
            f"quay.io/org/repo@{digest}",
        )
    assert out is None


def test_resolve_quay_digest_handles_exception() -> None:
    """Unexpected failures are swallowed and return `None`."""
    with mock.patch(
        "release_service_utils.helpers.http_client.get_text",
        side_effect=RuntimeError("network down"),
    ):
        out = image_ref.resolve_quay_digest_to_git_sha(
            "sha256:abc",
            "quay.io/org/repo@sha256:abc",
        )
    assert out is None


def test_translate_delivery_repo_rejects_empty_repo() -> None:
    """Empty repo input raises `ValueError`."""
    with pytest.raises(ValueError, match="Please pass a repo"):
        image_ref.translate_delivery_repo("")


def test_translate_delivery_repo_redhat_prod() -> None:
    """Translate quay.io/redhat-prod delivery repos to public registries."""
    out = image_ref.translate_delivery_repo("quay.io/redhat-prod/product----repo:v1.0")
    assert out == [
        {"repo": "redhat.io", "url": "registry.redhat.io/product/repo:v1.0"},
        {
            "repo": "access.redhat.com",
            "url": "registry.access.redhat.com/product/repo:v1.0",
        },
    ]


def test_translate_delivery_repo_redhat_pending() -> None:
    """Translate quay.io/redhat-pending delivery repos to stage registries."""
    out = image_ref.translate_delivery_repo("quay.io/redhat-pending/product----repo:v1.0")
    assert out == [
        {"repo": "redhat.io", "url": "registry.stage.redhat.io/product/repo:v1.0"},
        {
            "repo": "access.redhat.com",
            "url": "registry.access.stage.redhat.com/product/repo:v1.0",
        },
    ]


def test_translate_delivery_repo_flatpaks_prod() -> None:
    """Translate quay.io/rh-flatpaks-prod delivery repos."""
    out = image_ref.translate_delivery_repo("quay.io/rh-flatpaks-prod/product----repo:v1")
    assert out == [
        {"repo": "redhat.io", "url": "flatpaks.registry.redhat.io/product/repo:v1"},
        {
            "repo": "access.redhat.com",
            "url": "registry.access.redhat.com/product/repo:v1",
        },
    ]


def test_translate_delivery_repo_flatpaks_stage() -> None:
    """Translate quay.io/rh-flatpaks-stage delivery repos."""
    out = image_ref.translate_delivery_repo("quay.io/rh-flatpaks-stage/product----repo:v1")
    assert out == [
        {
            "repo": "redhat.io",
            "url": "flatpaks.registry.stage.redhat.io/product/repo:v1",
        },
        {
            "repo": "access.redhat.com",
            "url": "registry.access.stage.redhat.com/product/repo:v1",
        },
    ]


def test_translate_delivery_repo_index_image() -> None:
    """Translate quay.io/redhat index image repos."""
    out = image_ref.translate_delivery_repo(
        "quay.io/redhat/redhat----fbc-target-index:v4.12",
    )
    assert out == [
        {
            "repo": "redhat.io",
            "url": "registry.redhat.io/redhat/fbc-target-index:v4.12",
        },
        {
            "repo": "access.redhat.com",
            "url": "registry.access.redhat.com/redhat/fbc-target-index:v4.12",
        },
    ]


def test_translate_delivery_repo_unknown_format_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Unknown formats pass through the repo and emit a warning."""
    with caplog.at_level(logging.WARNING, logger="release"):
        out = image_ref.translate_delivery_repo("registry.example.com/org/repo:tag")
    assert out == [
        {"repo": "redhat.io", "url": "registry.example.com/org/repo:tag"},
        {"repo": "access.redhat.com", "url": ""},
    ]
    assert "Repo to translate is not in expected format" in caplog.text
