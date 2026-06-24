"""Tests for `pyxis_api` URL mapping and repository API helpers."""

from __future__ import annotations

from unittest import mock

import pytest
import requests

import pyxis_api


def test_pyxis_api_url_for_production_server() -> None:
    """Map the production server param to the public Pyxis v1 API URL."""
    assert pyxis_api.pyxis_api_url_for_server("production") == (
        "https://pyxis.api.redhat.com/v1"
    )


def test_pyxis_api_url_for_invalid_server_raises() -> None:
    """Reject unknown server param values."""
    with pytest.raises(ValueError, match="Invalid server parameter"):
        pyxis_api.pyxis_api_url_for_server("invalid")


def test_pyxis_registry_for_flatpak_quay_url() -> None:
    """Flatpak Quay repos use the flatpaks Pyxis registry."""
    url = "quay.io/rh-flatpaks-stage/my-product----my-image1"
    assert pyxis_api.pyxis_registry_for_quay_url(url) == ("flatpaks.registry.redhat.io")


def test_pyxis_registry_for_standard_quay_url() -> None:
    """Standard Quay repos use registry.access.redhat.com in Pyxis."""
    url = "quay.io/redhat-prod/my-product----my-image1"
    assert pyxis_api.pyxis_registry_for_quay_url(url) == ("registry.access.redhat.com")


def test_pyxis_repository_from_quay_url_replaces_quadruple_dash() -> None:
    """Convert Quay repo suffix `product----image` to Pyxis `product/image`."""
    assert (
        pyxis_api.pyxis_repository_from_quay_url(
            "quay.io/redhat-prod/my-product----my-image1",
        )
        == "my-product/my-image1"
    )


def test_catalog_base_url_for_prod_and_stage() -> None:
    """Return prod or stage catalog base URLs based on Quay prefix."""
    assert pyxis_api.catalog_base_url_for_quay_url(
        "quay.io/redhat-prod/my-product----my-image1",
    ).startswith("https://catalog.redhat.com/")
    assert pyxis_api.catalog_base_url_for_quay_url(
        "quay.io/redhat-pending/my-product----my-image1",
    ).startswith("https://catalog.stage.redhat.com/")


def test_catalog_base_url_unknown_prefix_raises() -> None:
    """Fail when the Quay repository prefix is not recognized."""
    with pytest.raises(ValueError, match="Unknown repository prefix"):
        pyxis_api.catalog_base_url_for_quay_url("quay.io/unknown/repo")


def test_catalog_url_for_repository_builds_expected_path() -> None:
    """Build a catalog URL from Pyxis repository metadata."""
    url = pyxis_api.catalog_url_for_repository(
        "quay.io/redhat-prod/my-product----my-image1",
        "my-product/my-image1",
        "42",
    )
    assert url == ("https://catalog.redhat.com/software/containers/" "my-product/my-image1/42")


def test_get_repository_json_returns_body() -> None:
    """Parse JSON from a successful Pyxis repository GET."""
    with mock.patch(
        "pyxis_api.http_client.get_text",
        return_value='{"_id": "1", "publish_on_push": true}',
    ) as get_text:
        body = pyxis_api.get_repository_json(
            "https://pyxis/v1",
            "registry.access.redhat.com",
            "my-product/my-image",
            cert=("/tmp/cert", "/tmp/key"),
        )
    assert body["_id"] == "1"
    get_text.assert_called_once_with(
        "https://pyxis/v1/repositories/registry/"
        "registry.access.redhat.com/repository/my-product/my-image",
        cert=("/tmp/cert", "/tmp/key"),
        timeout=120,
        allow_error_status=True,
    )


def test_get_repository_json_invalid_json_raises() -> None:
    """Raise when Pyxis returns a non-JSON GET body."""
    with mock.patch(
        "pyxis_api.http_client.get_text",
        return_value="not-json",
    ):
        with pytest.raises(ValueError, match="invalid JSON from Pyxis GET"):
            pyxis_api.get_repository_json(
                "https://pyxis/v1",
                "registry.access.redhat.com",
                "repo",
                cert=("/tmp/cert", "/tmp/key"),
            )


def test_patch_repository_json_raises_on_http_error() -> None:
    """Surface HTTP errors from Pyxis PATCH calls."""
    response = mock.MagicMock()
    response.status_code = 500
    response.text = "boom"
    response.raise_for_status.side_effect = requests.HTTPError("500")

    session = mock.MagicMock()
    session.patch.return_value = response

    with mock.patch(
        "pyxis_api.http_client.get_retry_session",
        return_value=session,
    ) as get_retry_session:
        with pytest.raises(requests.RequestException, match="Pyxis PATCH failed"):
            pyxis_api.patch_repository_json(
                "https://pyxis/v1",
                "99",
                {"published": True},
                cert=("/tmp/cert", "/tmp/key"),
            )
    get_retry_session.assert_called_once_with(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=5.0,
        allowed_methods=frozenset({"PATCH"}),
    )
