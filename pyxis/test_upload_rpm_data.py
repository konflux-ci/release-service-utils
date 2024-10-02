from unittest.mock import patch, Mock
from urllib.error import HTTPError
import pytest

from upload_rpm_data import (
    upload_container_rpm_data_with_retry,
    upload_container_rpm_data,
    get_image_rpm_data,
    create_image_rpm_manifest,
    update_container_content_sets,
    load_sbom_components,
    check_bom_ref_duplicates,
    construct_rpm_items_and_content_sets,
)

GRAPHQL_API = "myapiurl"
IMAGE_ID = "123456abcd"
SBOM_PATH = "mypath"
RPM_MANIFEST_ID = "abcd1234"
CONTENT_SETS = ["myrepo1", "myrepo2"]
COMPONENTS = [
    {  # all fields
        "purl": "pkg:rpm/rhel/pkg1@1-2.el8?arch=x86_64&"
        + "upstream=pkg1-1-2.el8.src.rpm&distro=rhel-8.0&repository_id=myrepo1",
    },
    {  # no version, same repository_id
        "purl": "pkg:rpm/rhel/pkg2?arch=noarch&upstream=pkg2-1-2.el8.src.rpm&distro=rhel-8.0"
        + "&repository_id=myrepo1",
    },
    {  # no architecture, different repository_id
        "purl": "pkg:rpm/rhel/pkg3@9-8.el8?upstream=pkg3-9-8.el8.src.rpm&distro=rhel-8.0"
        + "&repository_id=myrepo2",
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


@patch("upload_rpm_data.upload_container_rpm_data")
def test_upload_container_rpm_data_with_retry__success(mock_upload_container_rpm_data):
    """upload_container_rpm_data succeeds on first attempt"""
    upload_container_rpm_data_with_retry(GRAPHQL_API, IMAGE_ID, SBOM_PATH)

    mock_upload_container_rpm_data.assert_called_once_with(GRAPHQL_API, IMAGE_ID, SBOM_PATH)


@patch("upload_rpm_data.upload_container_rpm_data")
def test_upload_container_rpm_data_with_retry__success_after_one_attempt(
    mock_upload_container_rpm_data,
):
    """upload_container_rpm_data succeeds after one retry"""
    mock_upload_container_rpm_data.side_effect = [RuntimeError("error"), None]

    upload_container_rpm_data_with_retry(GRAPHQL_API, IMAGE_ID, SBOM_PATH, backoff_factor=0)

    assert mock_upload_container_rpm_data.call_count == 2


@patch("upload_rpm_data.upload_container_rpm_data")
def test_upload_container_rpm_data_with_retry__fails_runtime(
    mock_upload_container_rpm_data,
):
    """
    upload_container_rpm_data fails constantly with RuntimeError,
    so the retry eventually fails
    """
    mock_upload_container_rpm_data.side_effect = RuntimeError("error")

    with pytest.raises(RuntimeError):
        upload_container_rpm_data_with_retry(
            GRAPHQL_API, IMAGE_ID, SBOM_PATH, retries=2, backoff_factor=0
        )

    assert mock_upload_container_rpm_data.call_count == 2


@patch("upload_rpm_data.upload_container_rpm_data")
def test_upload_container_rpm_data_with_retry__fails_http_504(
    mock_upload_container_rpm_data,
):
    """
    upload_container_rpm_data fails constantly with HTTPError with code 504,
    so the retry eventually fails
    """
    mock_upload_container_rpm_data.side_effect = HTTPError(
        "http://example.com", 504, "Internal Error", {}, None
    )

    with pytest.raises(HTTPError):
        upload_container_rpm_data_with_retry(
            GRAPHQL_API, IMAGE_ID, SBOM_PATH, retries=2, backoff_factor=0
        )

    assert mock_upload_container_rpm_data.call_count == 2


@patch("upload_rpm_data.upload_container_rpm_data")
def test_upload_container_rpm_data_with_retry__fails_http_other(
    mock_upload_container_rpm_data,
):
    """
    upload_container_rpm_data fails with HTTPError code other than 504,
    so it fails without retry
    """
    mock_upload_container_rpm_data.side_effect = HTTPError(
        "http://example.com", 404, "Internal Error", {}, None
    )

    with pytest.raises(HTTPError):
        upload_container_rpm_data_with_retry(
            GRAPHQL_API, IMAGE_ID, SBOM_PATH, retries=2, backoff_factor=0
        )

    assert mock_upload_container_rpm_data.call_count == 1


@patch("upload_rpm_data.update_container_content_sets")
@patch("upload_rpm_data.create_image_rpm_manifest")
@patch("upload_rpm_data.construct_rpm_items_and_content_sets")
@patch("upload_rpm_data.load_sbom_components")
@patch("upload_rpm_data.get_image_rpm_data")
def test_upload_container_rpm_data__success(
    mock_get_image_rpm_data,
    mock_load_sbom_components,
    mock_construct_rpm_items_and_content_sets,
    mock_create_image_rpm_manifest,
    mock_update_container_content_sets,
):
    """
    Basic use case:
    RPM Manifest does not exist and is successfully created;
    content_sets are updated as well.
    """
    mock_get_image_rpm_data.return_value = {
        "_id": IMAGE_ID,
        "content_sets": None,
        "rpm_manifest": None,
    }
    mock_load_sbom_components.return_value = COMPONENTS
    mock_construct_rpm_items_and_content_sets.return_value = ([{"name": "pkg"}], ["myrepo1"])

    upload_container_rpm_data(GRAPHQL_API, IMAGE_ID, SBOM_PATH)

    mock_construct_rpm_items_and_content_sets.assert_called_once_with(COMPONENTS)
    mock_create_image_rpm_manifest.assert_called_once_with(
        GRAPHQL_API,
        IMAGE_ID,
        [{"name": "pkg"}],
    )
    mock_update_container_content_sets.assert_called_once_with(
        GRAPHQL_API,
        IMAGE_ID,
        ["myrepo1"],
    )


@patch("upload_rpm_data.update_container_content_sets")
@patch("upload_rpm_data.create_image_rpm_manifest")
@patch("upload_rpm_data.construct_rpm_items_and_content_sets")
@patch("upload_rpm_data.load_sbom_components")
@patch("upload_rpm_data.get_image_rpm_data")
def test_upload_container_rpm_data__data_already_exists(
    mock_get_image_rpm_data,
    mock_load_sbom_components,
    mock_construct_rpm_items_and_content_sets,
    mock_create_image_rpm_manifest,
    mock_update_container_content_sets,
):
    """
    RPM Manifest and content sets already exists so the function
    returns without creating or updating anything
    """
    mock_get_image_rpm_data.return_value = {
        "_id": IMAGE_ID,
        "content_sets": CONTENT_SETS,
        "rpm_manifest": {"_id": RPM_MANIFEST_ID},
    }
    mock_load_sbom_components.return_value = COMPONENTS
    mock_construct_rpm_items_and_content_sets.return_value = ([{"name": "pkg"}], CONTENT_SETS)

    upload_container_rpm_data(GRAPHQL_API, IMAGE_ID, SBOM_PATH)

    mock_load_sbom_components.assert_called_once()
    mock_construct_rpm_items_and_content_sets.assert_called_once_with(COMPONENTS)
    mock_create_image_rpm_manifest.assert_not_called()
    mock_update_container_content_sets.assert_not_called()


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
def test_get_image_rpm_data__success(mock_post):
    """The Pyxis query is called and the image data is returned"""
    image = {
        "_id": IMAGE_ID,
        "content_sets": CONTENT_SETS,
        "rpm_manifest": {
            "_id": RPM_MANIFEST_ID,
        },
    }
    mock_post.side_effect = [generate_pyxis_response("get_image", image)]

    image = get_image_rpm_data(GRAPHQL_API, IMAGE_ID)

    assert image["rpm_manifest"]["_id"] == RPM_MANIFEST_ID
    assert image["content_sets"] == CONTENT_SETS
    assert mock_post.call_count == 1


@patch("pyxis.post")
def test_get_image_rpm_data__error(mock_post):
    mock_post.return_value = generate_pyxis_response("get_image", error=True)

    with pytest.raises(RuntimeError):
        get_image_rpm_data(GRAPHQL_API, IMAGE_ID)

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


@patch("pyxis.post")
def test_update_container_content_sets__success(mock_post):
    mock_post.return_value = generate_pyxis_response("update_image", {"_id": IMAGE_ID})

    id = update_container_content_sets(GRAPHQL_API, IMAGE_ID, CONTENT_SETS)

    assert id == IMAGE_ID
    mock_post.assert_called_once()


@patch("pyxis.post")
def test_update_container_content_sets__error(mock_post):
    mock_post.return_value = generate_pyxis_response("update_image", error=True)

    with pytest.raises(RuntimeError):
        update_container_content_sets(GRAPHQL_API, IMAGE_ID, CONTENT_SETS)

    mock_post.assert_called_once()


@patch("json.load")
@patch("upload_rpm_data.check_bom_ref_duplicates")
@patch("builtins.open")
def test_load_sbom_components__success(mock_open, mock_check_bom_ref_duplicates, mock_load):
    fake_components = [1, 2, 3, 4]
    mock_load.return_value = {"components": fake_components}

    loaded_components = load_sbom_components(SBOM_PATH)

    mock_load.assert_called_once_with(mock_open.return_value.__enter__.return_value)
    mock_check_bom_ref_duplicates.assert_called_once_with(loaded_components)
    assert fake_components == loaded_components


@patch("json.load")
@patch("upload_rpm_data.check_bom_ref_duplicates")
@patch("builtins.open")
def test_load_sbom_components__no_components_key(
    mock_open, mock_check_bom_ref_duplicates, mock_load
):
    mock_load.return_value = {}

    loaded_components = load_sbom_components(SBOM_PATH)

    mock_load.assert_called_once_with(mock_open.return_value.__enter__.return_value)
    mock_check_bom_ref_duplicates.assert_called_once_with(loaded_components)
    assert loaded_components == []


@patch("json.load")
@patch("upload_rpm_data.check_bom_ref_duplicates")
@patch("builtins.open")
def test_load_sbom_components__json_load_fails(
    mock_open, mock_check_bom_ref_duplicates, mock_load
):
    mock_load.side_effect = ValueError

    with pytest.raises(ValueError):
        load_sbom_components(SBOM_PATH)

    mock_load.assert_called_once_with(mock_open.return_value.__enter__.return_value)
    mock_check_bom_ref_duplicates.assert_not_called()


def test_check_bom_ref_duplicates__no_duplicates():
    components = [
        {"bom-ref": "a"},
        {"bom-ref": "b"},
        {},
        {"bom-ref": "c"},
    ]

    check_bom_ref_duplicates(components)


def test_check_bom_ref_duplicates__duplicates_found():
    components = [
        {"bom-ref": "a"},
        {"bom-ref": "b"},
        {},
        {"bom-ref": "a"},
    ]

    with pytest.raises(ValueError):
        check_bom_ref_duplicates(components)


def test_construct_rpm_items_and_content_sets__success():
    """Only rpm purls are added, the version, release,
    and architecture fields are added if present.
    All unique repository_id values are returned."""

    rpms, content_sets = construct_rpm_items_and_content_sets(COMPONENTS)

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

    assert content_sets == ["myrepo1", "myrepo2"]


def test_construct_rpm_items_and_content_sets__no_components_result_in_empty_list():
    """An empty list of components results in an empty list of rpms and content_sets"""
    rpms, content_sets = construct_rpm_items_and_content_sets([])

    assert rpms == []
    assert content_sets == []
