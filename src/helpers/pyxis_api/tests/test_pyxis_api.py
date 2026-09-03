"""Tests for `pyxis_api` URL mapping and repository API helpers."""

from __future__ import annotations

from unittest import mock

import pytest
import requests

from release_service_utils.helpers import pyxis_api


def test_pyxis_api_url_for_production_server() -> None:
    """Map the production server param to the public Pyxis v1 API URL."""
    assert pyxis_api.pyxis_api_url_for_server("production") == (
        "https://pyxis.api.redhat.com/v1"
    )


def test_pyxis_api_url_for_invalid_server_raises() -> None:
    """Reject unknown server param values."""
    with pytest.raises(ValueError, match="Invalid server parameter"):
        pyxis_api.pyxis_api_url_for_server("invalid")


def test_pyxis_graphql_url_for_production_server() -> None:
    """Map the production server param to the public Pyxis GraphQL URL."""
    assert pyxis_api.pyxis_graphql_url_for_server("production") == (
        "https://graphql-pyxis.api.redhat.com/graphql/"
    )


def test_pyxis_graphql_url_for_stage_server() -> None:
    """Map the stage server param to the preprod Pyxis GraphQL URL."""
    assert pyxis_api.pyxis_graphql_url_for_server("stage") == (
        "https://graphql-pyxis.preprod.api.redhat.com/graphql/"
    )


def test_pyxis_graphql_url_for_invalid_server_raises() -> None:
    """Reject unknown server param values for GraphQL."""
    with pytest.raises(ValueError, match="Invalid server parameter"):
        pyxis_api.pyxis_graphql_url_for_server("invalid")


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
        "release_service_utils.helpers.http_client.get_text",
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
        "release_service_utils.helpers.http_client.get_text",
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
        "release_service_utils.helpers.http_client.get_retry_session",
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


def test_get_advisory_success() -> None:
    """GET Pyxis advisory by ID successfully."""
    import json

    response = mock.MagicMock()
    response.status_code = 200
    advisory_data = {"_id": "adv-123", "status": "active"}
    response.json.return_value = advisory_data
    response.text = json.dumps(advisory_data)

    session = mock.MagicMock()
    session.get.return_value = response

    with mock.patch(
        "release_service_utils.helpers.http_client.get_retry_session",
        return_value=session,
    ) as get_retry_session:
        result = pyxis_api.get_advisory(
            "https://pyxis/v1",
            "adv-123",
            cert=("/tmp/cert", "/tmp/key"),
        )
    assert result == advisory_data
    session.get.assert_called_once_with(
        "https://pyxis/v1/id/adv-123", auth=None, headers=None, timeout=120
    )
    assert session.cert == ("/tmp/cert", "/tmp/key")
    get_retry_session.assert_called_once_with(
        total=3,
        connect=3,
        read=3,
        status=2,
        backoff_factor=0.4,
        allowed_methods=frozenset({"GET"}),
    )


def test_get_advisory_not_found() -> None:
    """GET Pyxis advisory returns None if 404 is received."""
    response = mock.MagicMock()
    response.status_code = 404
    response.raise_for_status.side_effect = requests.HTTPError("404 Error", response=response)

    session = mock.MagicMock()
    session.get.return_value = response

    with mock.patch(
        "release_service_utils.helpers.http_client.get_retry_session",
        return_value=session,
    ):
        result = pyxis_api.get_advisory(
            "https://pyxis/v1",
            "adv-123",
            cert=("/tmp/cert", "/tmp/key"),
        )
    assert result is None


def test_get_advisory_http_error_raises() -> None:
    """GET Pyxis advisory raises RequestException on HTTP error."""
    response = mock.MagicMock()
    response.status_code = 500
    response.text = "internal error"
    response.raise_for_status.side_effect = requests.HTTPError("500 Error")

    session = mock.MagicMock()
    session.get.return_value = response

    with mock.patch(
        "release_service_utils.helpers.http_client.get_retry_session",
        return_value=session,
    ):
        with pytest.raises(requests.RequestException, match="Pyxis advisory GET failed"):
            pyxis_api.get_advisory(
                "https://pyxis/v1",
                "adv-123",
                cert=("/tmp/cert", "/tmp/key"),
            )


def test_get_advisory_invalid_json_raises() -> None:
    """GET Pyxis advisory raises ValueError on invalid JSON response."""
    response = mock.MagicMock()
    response.status_code = 200
    response.text = "not json"
    import json

    response.json.side_effect = json.JSONDecodeError("msg", "doc", 0)

    session = mock.MagicMock()
    session.get.return_value = response

    with mock.patch(
        "release_service_utils.helpers.http_client.get_retry_session",
        return_value=session,
    ):
        with pytest.raises(ValueError, match="invalid JSON from Pyxis advisory GET"):
            pyxis_api.get_advisory(
                "https://pyxis/v1",
                "adv-123",
                cert=("/tmp/cert", "/tmp/key"),
            )


def test_create_or_update_advisory_existing_patch() -> None:
    """PATCH existing Pyxis advisory when it exists."""
    existing_advisory = {"_id": "adv-123", "status": "active"}

    session = mock.MagicMock()
    response = mock.MagicMock()
    response.status_code = 200
    session.patch.return_value = response

    payload = {"status": "updated"}

    with (
        mock.patch(
            "release_service_utils.helpers.pyxis_api.pyxis_api.get_advisory",
            return_value=existing_advisory,
        ) as mock_get,
        mock.patch(
            "release_service_utils.helpers.http_client.get_retry_session",
            return_value=session,
        ) as get_retry_session,
    ):
        pyxis_api.create_or_update_advisory(
            "https://pyxis/v1",
            "adv-123",
            payload,
            cert=("/tmp/cert", "/tmp/key"),
        )

    mock_get.assert_called_once_with(
        "https://pyxis/v1", "adv-123", cert=("/tmp/cert", "/tmp/key")
    )
    session.patch.assert_called_once_with(
        "https://pyxis/v1/id/adv-123",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=120,
    )
    assert session.cert == ("/tmp/cert", "/tmp/key")
    get_retry_session.assert_called_once_with(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=2.0,
        allowed_methods=frozenset({"POST", "PATCH"}),
    )


def test_create_or_update_advisory_new_post() -> None:
    """POST a new Pyxis advisory when it does not exist."""
    session = mock.MagicMock()
    response = mock.MagicMock()
    response.status_code = 201
    session.post.return_value = response

    payload = {"_id": "adv-123", "status": "active"}

    with (
        mock.patch(
            "release_service_utils.helpers.pyxis_api.pyxis_api.get_advisory",
            return_value=None,
        ) as mock_get,
        mock.patch(
            "release_service_utils.helpers.http_client.get_retry_session",
            return_value=session,
        ),
    ):
        pyxis_api.create_or_update_advisory(
            "https://pyxis/v1",
            "adv-123",
            payload,
            cert=("/tmp/cert", "/tmp/key"),
        )

    mock_get.assert_called_once_with(
        "https://pyxis/v1", "adv-123", cert=("/tmp/cert", "/tmp/key")
    )
    session.post.assert_called_once_with(
        "https://pyxis/v1",
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=120,
    )
    assert session.cert == ("/tmp/cert", "/tmp/key")


def test_create_or_update_advisory_raises_on_failure() -> None:
    """Raise HTTPError when POST/PATCH fails."""
    session = mock.MagicMock()
    response = mock.MagicMock()
    response.status_code = 500
    response.text = "internal error"
    response.raise_for_status.side_effect = requests.HTTPError("500 error")
    session.post.return_value = response

    payload = {"_id": "adv-123", "status": "active"}

    with (
        mock.patch(
            "release_service_utils.helpers.pyxis_api.pyxis_api.get_advisory",
            return_value=None,
        ) as mock_get,
        mock.patch(
            "release_service_utils.helpers.http_client.get_retry_session",
            return_value=session,
        ),
    ):
        with pytest.raises(requests.HTTPError, match="500 error"):
            pyxis_api.create_or_update_advisory(
                "https://pyxis/v1",
                "adv-123",
                payload,
                cert=("/tmp/cert", "/tmp/key"),
            )

    assert mock_get.call_count == 1
    assert session.post.call_count == 1


def test_get_image_by_digest_success() -> None:
    """GET Pyxis image by digest successfully."""
    import json

    response = mock.MagicMock()
    response.status_code = 200
    image_data = {"_id": "img-123", "repositories": []}
    response.json.return_value = {"data": [image_data]}
    response.text = json.dumps({"data": [image_data]})

    session = mock.MagicMock()
    session.get.return_value = response

    with mock.patch(
        "release_service_utils.helpers.http_client.get_retry_session",
        return_value=session,
    ) as get_retry_session:
        result = pyxis_api.get_image_by_digest(
            "https://pyxis/v1/images",
            "abcdef123456",
            cert=("/tmp/cert", "/tmp/key"),
        )
    assert result == image_data
    expected_url = (
        "https://pyxis/v1/images?page_size=1&"
        "filter=repositories.manifest_schema2_digest%3D%3D%22sha256%3Aabcdef123456%22%3B"
        "not%28deleted%3D%3Dtrue%29"
    )
    session.get.assert_called_once_with(expected_url, auth=None, headers=None, timeout=120)
    assert session.cert == ("/tmp/cert", "/tmp/key")
    get_retry_session.assert_called_once_with(
        total=3,
        connect=3,
        read=3,
        status=2,
        backoff_factor=0.4,
        allowed_methods=frozenset({"GET"}),
    )


def test_get_image_by_digest_http_error_raises() -> None:
    """GET Pyxis image raises RequestException on HTTP error."""
    response = mock.MagicMock()
    response.status_code = 500
    response.text = "internal error"
    response.raise_for_status.side_effect = requests.HTTPError("500 Error")

    session = mock.MagicMock()
    session.get.return_value = response

    with mock.patch(
        "release_service_utils.helpers.http_client.get_retry_session",
        return_value=session,
    ):
        with pytest.raises(requests.RequestException, match="Pyxis image GET failed"):
            pyxis_api.get_image_by_digest(
                "https://pyxis/v1/images",
                "abcdef123456",
                cert=("/tmp/cert", "/tmp/key"),
            )


def test_get_image_by_digest_invalid_json_raises() -> None:
    """GET Pyxis image raises ValueError on invalid JSON response."""
    response = mock.MagicMock()
    response.status_code = 200
    response.text = "not json"
    import json

    response.json.side_effect = json.JSONDecodeError("msg", "doc", 0)

    session = mock.MagicMock()
    session.get.return_value = response

    with mock.patch(
        "release_service_utils.helpers.http_client.get_retry_session",
        return_value=session,
    ):
        with pytest.raises(ValueError, match="invalid JSON from Pyxis image GET"):
            pyxis_api.get_image_by_digest(
                "https://pyxis/v1/images",
                "abcdef123456",
                cert=("/tmp/cert", "/tmp/key"),
            )


def test_get_image_by_digest_no_images_raises() -> None:
    """GET Pyxis image raises ValueError when no image matches the digest."""
    import json

    response = mock.MagicMock()
    response.status_code = 200
    response.json.return_value = {"data": []}
    response.text = json.dumps({"data": []})

    session = mock.MagicMock()
    session.get.return_value = response

    with mock.patch(
        "release_service_utils.helpers.http_client.get_retry_session",
        return_value=session,
    ):
        with pytest.raises(ValueError, match="No Pyxis image found for digest"):
            pyxis_api.get_image_by_digest(
                "https://pyxis/v1/images",
                "abcdef123456",
                cert=("/tmp/cert", "/tmp/key"),
            )


def test_link_image_to_advisory_success() -> None:
    """Successfully PATCH Pyxis image with linked advisory ID."""
    import json

    image_data = {
        "_id": "img-123",
        "repositories": [
            {
                "registry": "registry.access.redhat.com",
                "repository": "my-repo",
                "image_advisory_id": "old-adv",
            },
            {
                "registry": "other.registry",
                "repository": "my-repo",
                "image_advisory_id": "other-adv",
            },
        ],
    }

    get_response = mock.MagicMock()
    get_response.status_code = 200
    get_response.json.return_value = image_data
    get_response.text = json.dumps(image_data)

    patch_response = mock.MagicMock()
    patch_response.status_code = 200

    session = mock.MagicMock()
    session.get.return_value = get_response
    session.patch.return_value = patch_response

    with mock.patch(
        "release_service_utils.helpers.http_client.get_retry_session",
        return_value=session,
    ) as get_retry_session:
        pyxis_api.link_image_to_advisory(
            "https://pyxis/v1/images",
            "img-123",
            "my-repo",
            "new-adv",
            cert=("/tmp/cert", "/tmp/key"),
        )

    expected_url = "https://pyxis/v1/images/id/img-123"
    session.get.assert_called_once_with(expected_url, auth=None, headers=None, timeout=120)

    expected_patch_payload = {
        "_id": "img-123",
        "repositories": [
            {
                "registry": "registry.access.redhat.com",
                "repository": "my-repo",
                "image_advisory_id": "new-adv",
            },
            {
                "registry": "other.registry",
                "repository": "my-repo",
                "image_advisory_id": "other-adv",
            },
        ],
    }
    session.patch.assert_called_once_with(
        expected_url,
        json=expected_patch_payload,
        headers={"Content-Type": "application/json"},
        timeout=120,
    )
    assert get_retry_session.call_count == 2


def test_link_image_to_advisory_get_raises() -> None:
    """GET fails during link_image_to_advisory and raises RequestException."""
    get_response = mock.MagicMock()
    get_response.status_code = 500
    get_response.text = "internal error"
    get_response.raise_for_status.side_effect = requests.HTTPError("500 Error")

    session = mock.MagicMock()
    session.get.return_value = get_response

    with mock.patch(
        "release_service_utils.helpers.http_client.get_retry_session",
        return_value=session,
    ):
        with pytest.raises(requests.RequestException, match="Pyxis image GET failed"):
            pyxis_api.link_image_to_advisory(
                "https://pyxis/v1/images",
                "img-123",
                "my-repo",
                "new-adv",
                cert=("/tmp/cert", "/tmp/key"),
            )


def test_link_image_to_advisory_get_invalid_json_raises() -> None:
    """GET returns invalid JSON and raises ValueError."""
    get_response = mock.MagicMock()
    get_response.status_code = 200
    get_response.text = "not json"
    import json

    get_response.json.side_effect = json.JSONDecodeError("msg", "doc", 0)

    session = mock.MagicMock()
    session.get.return_value = get_response

    with mock.patch(
        "release_service_utils.helpers.http_client.get_retry_session",
        return_value=session,
    ):
        with pytest.raises(ValueError, match="invalid JSON from Pyxis image GET"):
            pyxis_api.link_image_to_advisory(
                "https://pyxis/v1/images",
                "img-123",
                "my-repo",
                "new-adv",
                cert=("/tmp/cert", "/tmp/key"),
            )


def test_link_image_to_advisory_patch_raises() -> None:
    """PATCH fails during link_image_to_advisory and raises RequestException."""
    import json

    get_response = mock.MagicMock()
    get_response.status_code = 200
    get_response.json.return_value = {"_id": "img-123", "repositories": []}
    get_response.text = json.dumps(
        {
            "_id": "img-123",
            "repositories": [
                {
                    "registry": "registry.access.redhat.com",
                    "repository": "my-repo",
                    "image_advisory_id": "new-adv",
                },
            ],
        }
    )

    patch_response = mock.MagicMock()
    patch_response.status_code = 500
    patch_response.text = "internal error"
    patch_response.raise_for_status.side_effect = requests.HTTPError("500 Error")

    session = mock.MagicMock()
    session.get.return_value = get_response
    session.patch.return_value = patch_response

    with mock.patch(
        "release_service_utils.helpers.http_client.get_retry_session",
        return_value=session,
    ):
        with pytest.raises(requests.RequestException, match="Pyxis image PATCH failed"):
            pyxis_api.link_image_to_advisory(
                "https://pyxis/v1/images",
                "img-123",
                "my-repo",
                "new-adv",
                cert=("/tmp/cert", "/tmp/key"),
            )
