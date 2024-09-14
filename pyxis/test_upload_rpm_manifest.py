from unittest.mock import patch, Mock
from urllib.error import HTTPError
import pytest

from upload_rpm_manifest import (
    upload_container_rpm_manifest_with_retry,
    upload_container_rpm_manifest,
    get_rpm_manifest_id,
    create_image_rpm_manifest,
    construct_rpm_items,
)

GRAPHQL_API = "myapiurl"
IMAGE_ID = "123456abcd"
SBOM_PATH = "mypath"
RPM_MANIFEST_ID = "abcd1234"
COMPONENTS = [
    {  # all fields
        "purl": "pkg:rpm/rhel/pkg1@1-2.el8?arch=x86_64&"
        + "upstream=pkg1-1-2.el8.src.rpm&distro=rhel-8.0",
    },
    {  # no version
        "purl": "pkg:rpm/rhel/pkg2?arch=noarch&upstream=pkg2-1-2.el8.src.rpm&distro=rhel-8.0",
    },
    {  # no architecture
        "purl": "pkg:rpm/rhel/pkg3@9-8.el8?upstream=pkg3-9-8.el8.src.rpm&distro=rhel-8.0",
    },
    {  # no upstream
        "purl": "pkg:rpm/rhel/pkg4@1-2.el8?arch=x86_64&distro=rhel-8.0",
    },
    {  # with RH publisher
        "purl": "pkg:rpm/rhel/pkg5?arch=noarch&upstream=pkg5-1-2.el8.src.rpm&distro=rhel-8.0",
        "publisher": "Red Hat, inc.",
    },
    {  # with other publisher
        "purl": "pkg:rpm/rhel/pkg6?arch=noarch&upstream=pkg6-1-2.el8.src.rpm&distro=rhel-8.0",
        "publisher": "Blue Shoe, inc.",
    },
    {  # not an rpm
        "purl": "pkg:golang/./staging/src@(devel)#k8s.io/api",
    },
    {  # no purl
        "bom_ref": "ref",
    },
]


@patch("upload_rpm_manifest.upload_container_rpm_manifest")
def test_upload_container_rpm_manifest_with_retry__success(mock_upload_container_rpm_manifest):
    """upload_container_rpm_manifest succeeds on first attempt"""
    upload_container_rpm_manifest_with_retry(GRAPHQL_API, IMAGE_ID, SBOM_PATH)

    mock_upload_container_rpm_manifest.assert_called_once_with(
        GRAPHQL_API, IMAGE_ID, SBOM_PATH
    )


@patch("upload_rpm_manifest.upload_container_rpm_manifest")
def test_upload_container_rpm_manifest_with_retry__success_after_one_attempt(
    mock_upload_container_rpm_manifest,
):
    """upload_container_rpm_manifest succeeds after one retry"""
    mock_upload_container_rpm_manifest.side_effect = [RuntimeError("error"), None]

    upload_container_rpm_manifest_with_retry(
        GRAPHQL_API, IMAGE_ID, SBOM_PATH, backoff_factor=0
    )

    assert mock_upload_container_rpm_manifest.call_count == 2


@patch("upload_rpm_manifest.upload_container_rpm_manifest")
def test_upload_container_rpm_manifest_with_retry__fails_runtime(
    mock_upload_container_rpm_manifest,
):
    """
    upload_container_rpm_manifest fails constantly with RuntimeError,
    so the retry eventually fails
    """
    mock_upload_container_rpm_manifest.side_effect = RuntimeError("error")

    with pytest.raises(RuntimeError):
        upload_container_rpm_manifest_with_retry(
            GRAPHQL_API, IMAGE_ID, SBOM_PATH, retries=2, backoff_factor=0
        )

    assert mock_upload_container_rpm_manifest.call_count == 2


@patch("upload_rpm_manifest.upload_container_rpm_manifest")
def test_upload_container_rpm_manifest_with_retry__fails_http_504(
    mock_upload_container_rpm_manifest,
):
    """
    upload_container_rpm_manifest fails constantly with HTTPError with code 504,
    so the retry eventually fails
    """
    mock_upload_container_rpm_manifest.side_effect = HTTPError(
        "http://example.com", 504, "Internal Error", {}, None
    )

    with pytest.raises(HTTPError):
        upload_container_rpm_manifest_with_retry(
            GRAPHQL_API, IMAGE_ID, SBOM_PATH, retries=2, backoff_factor=0
        )

    assert mock_upload_container_rpm_manifest.call_count == 2


@patch("upload_rpm_manifest.upload_container_rpm_manifest")
def test_upload_container_rpm_manifest_with_retry__fails_http_other(
    mock_upload_container_rpm_manifest,
):
    """
    upload_container_rpm_manifest fails with HTTPError code other than 504,
    so it fails without retry
    """
    mock_upload_container_rpm_manifest.side_effect = HTTPError(
        "http://example.com", 404, "Internal Error", {}, None
    )

    with pytest.raises(HTTPError):
        upload_container_rpm_manifest_with_retry(
            GRAPHQL_API, IMAGE_ID, SBOM_PATH, retries=2, backoff_factor=0
        )

    assert mock_upload_container_rpm_manifest.call_count == 1


@patch("upload_rpm_manifest.create_image_rpm_manifest")
@patch("upload_rpm_manifest.construct_rpm_items")
@patch("upload_rpm_manifest.load_sbom_components")
@patch("upload_rpm_manifest.get_rpm_manifest_id")
def test_upload_container_rpm_manifest__success(
    mock_get_rpm_manifest_id,
    mock_load_sbom_components,
    mock_construct_rpm_items,
    mock_create_image_rpm_manifest,
):
    """
    Basic use case - RPM Manifest does not exist and is successfully created
    """
    mock_get_rpm_manifest_id.return_value = ""
    mock_load_sbom_components.return_value = COMPONENTS
    mock_construct_rpm_items.return_value = [{"name": "pkg"}]

    upload_container_rpm_manifest(GRAPHQL_API, IMAGE_ID, SBOM_PATH)

    mock_construct_rpm_items.assert_called_once_with(COMPONENTS)
    mock_create_image_rpm_manifest.assert_called_once_with(
        GRAPHQL_API,
        IMAGE_ID,
        [{"name": "pkg"}],
    )


@patch("upload_rpm_manifest.create_image_rpm_manifest")
@patch("upload_rpm_manifest.construct_rpm_items")
@patch("upload_rpm_manifest.load_sbom_components")
@patch("upload_rpm_manifest.get_rpm_manifest_id")
def test_upload_container_rpm_manifest__manifest_already_exists(
    mock_get_rpm_manifest_id,
    mock_load_sbom_components,
    mock_construct_rpm_items,
    mock_create_image_rpm_manifest,
):
    """
    RPM Manifest already exists so the function returns without creating a new one
    """
    mock_get_rpm_manifest_id.return_value = RPM_MANIFEST_ID

    upload_container_rpm_manifest(GRAPHQL_API, IMAGE_ID, SBOM_PATH)

    mock_load_sbom_components.assert_not_called()
    mock_construct_rpm_items.assert_not_called()
    mock_create_image_rpm_manifest.assert_not_called()


def generate_pyxis_response(query_name, data=None, error=False):
    response_json = {
        "data": {
            query_name: {
                "data": data,
                "error": None,
            }
        }
    }
    if error:
        response_json["data"][query_name]["error"] = {"detail": "Major failure!"}
    response = Mock()
    response.json.return_value = response_json

    return response


@patch("pyxis.post")
def test_get_rpm_manifest_id__success(mock_post):
    """The Pyxis query is called and the manifest id is returned"""
    image = {
        "_id": IMAGE_ID,
        "rpm_manifest": {
            "_id": RPM_MANIFEST_ID,
        },
    }
    mock_post.side_effect = [generate_pyxis_response("get_image", image)]

    id = get_rpm_manifest_id(GRAPHQL_API, IMAGE_ID)

    assert id == RPM_MANIFEST_ID
    assert mock_post.call_count == 1


@patch("pyxis.post")
def test_get_rpm_manifest_id__error(mock_post):
    mock_post.return_value = generate_pyxis_response("get_image", error=True)

    with pytest.raises(RuntimeError):
        get_rpm_manifest_id(GRAPHQL_API, IMAGE_ID)

    mock_post.assert_called_once()


@patch("pyxis.post")
def test_create_image_rpm_manifest__success(mock_post):
    mock_post.return_value = generate_pyxis_response(
        "create_image_rpm_manifest", {"_id": RPM_MANIFEST_ID}
    )

    id = create_image_rpm_manifest(GRAPHQL_API, IMAGE_ID, [])

    assert id == RPM_MANIFEST_ID
    mock_post.assert_called_once()


@patch("pyxis.post")
def test_create_image_rpm_manifest__error(mock_post):
    mock_post.return_value = generate_pyxis_response("create_image_rpm_manifest", error=True)

    with pytest.raises(RuntimeError):
        create_image_rpm_manifest(GRAPHQL_API, IMAGE_ID, [])

    mock_post.assert_called_once()


def test_construct_rpm_items__success():
    """Only rpm purls are added, the version, release,
    and architecture fields are added if present"""

    rpms = construct_rpm_items(COMPONENTS)

    assert rpms == [
        {
            "name": "pkg1",
            "summary": "pkg1-1-2.el8.x86_64",
            "nvra": "pkg1-1-2.el8.x86_64",
            "version": "1",
            "release": "2.el8",
            "architecture": "x86_64",
            "srpm_name": "pkg1-1-2.el8.src.rpm",
        },
        {
            "name": "pkg2",
            "summary": "pkg2",
            "architecture": "noarch",
            "srpm_name": "pkg2-1-2.el8.src.rpm",
        },
        {
            "name": "pkg3",
            "summary": "pkg3-9-8.el8.noarch",
            "nvra": "pkg3-9-8.el8.noarch",
            "version": "9",
            "release": "8.el8",
            "architecture": "noarch",
            "srpm_name": "pkg3-9-8.el8.src.rpm",
        },
        {
            "name": "pkg4",
            "summary": "pkg4-1-2.el8.x86_64",
            "nvra": "pkg4-1-2.el8.x86_64",
            "version": "1",
            "release": "2.el8",
            "architecture": "x86_64",
        },
        {
            "name": "pkg5",
            "gpg": "199e2f91fd431d51",
            "summary": "pkg5",
            "architecture": "noarch",
            "srpm_name": "pkg5-1-2.el8.src.rpm",
        },
        {
            "name": "pkg6",
            "summary": "pkg6",
            "architecture": "noarch",
            "srpm_name": "pkg6-1-2.el8.src.rpm",
        },
    ]


def test_construct_rpm_items__no_components_result_in_empty_list():
    """An empty list of components results in an empty list of rpms"""
    rpms = construct_rpm_items([])

    assert rpms == []
