import pytest
from unittest.mock import patch, call

from upload_sbom import (
    upload_sbom,
    get_image,
    create_content_manifest,
    get_existing_components,
    get_existing_bom_refs,
    create_content_manifest_component,
    get_existing_component_count,
    load_sbom_components,
    check_bom_ref_duplicates,
    convert_keys,
    remove_unsupported_fields,
    UNSUPPORTED_FIELDS,
)

GRAPHQL_API = "myapiurl"
IMAGE_ID = "123456abcd"
SBOM_PATH = "mypath"
MANIFEST_ID = "abcd1234"
COMPONENT_ID = "abcd2222"
IMAGE_DICT = {"_id": IMAGE_ID}
COMPONENT_DICT = {"bom_ref": "mybomref"}


@patch("upload_sbom.create_content_manifest_component")
@patch("upload_sbom.get_existing_bom_refs")
@patch("upload_sbom.load_sbom_components")
@patch("upload_sbom.create_content_manifest")
@patch("upload_sbom.get_image")
def test_upload_sbom__success(
    mock_get_image,
    mock_create_content_manifest,
    mock_load_sbom_components,
    mock_get_existing_bom_refs,
    mock_create_content_manifest_component,
):
    """
    Basic use case - nothing exists in Pyxis yet and all components are successfully created
    """
    mock_get_image.return_value = IMAGE_DICT
    mock_create_content_manifest.return_value = MANIFEST_ID
    mock_load_sbom_components.return_value = [
        {"bom-ref": "aaa"},
        {"bom-ref": "bbb"},
        {"type": "library"},
    ]

    upload_sbom(GRAPHQL_API, IMAGE_ID, SBOM_PATH)

    mock_create_content_manifest.assert_called_once_with(GRAPHQL_API, IMAGE_ID)
    mock_get_existing_bom_refs.assert_not_called()
    assert mock_create_content_manifest_component.call_args_list == [
        call(GRAPHQL_API, MANIFEST_ID, {"bom_ref": "aaa"}),
        call(GRAPHQL_API, MANIFEST_ID, {"bom_ref": "bbb"}),
        call(GRAPHQL_API, MANIFEST_ID, {"type": "library"}),
    ]


@patch("upload_sbom.create_content_manifest_component")
@patch("upload_sbom.get_existing_bom_refs")
@patch("upload_sbom.load_sbom_components")
@patch("upload_sbom.create_content_manifest")
@patch("upload_sbom.get_image")
def test_upload_sbom__manifest_and_one_component_exist(
    mock_get_image,
    mock_create_content_manifest,
    mock_load_sbom_components,
    mock_get_existing_bom_refs,
    mock_create_content_manifest_component,
):
    """Creation of manifest and the first component is skipped"""
    mock_get_image.return_value = {
        "_id": IMAGE_ID,
        "content_manifest": {
            "_id": MANIFEST_ID,
        },
        "content_manifest_components": [{"_id": COMPONENT_ID}],
    }
    mock_load_sbom_components.return_value = [
        {"bom-ref": "aaa"},
        {"bom-ref": "bbb"},
    ]
    mock_get_existing_bom_refs.return_value = {"aaa"}

    upload_sbom(GRAPHQL_API, IMAGE_ID, SBOM_PATH)

    mock_get_existing_bom_refs.assert_called_once_with(GRAPHQL_API, MANIFEST_ID)
    mock_create_content_manifest.assert_not_called()
    mock_create_content_manifest_component.assert_called_once_with(
        GRAPHQL_API, MANIFEST_ID, {"bom_ref": "bbb"}
    )


@patch("upload_sbom.create_content_manifest_component")
@patch("upload_sbom.get_existing_bom_refs")
@patch("upload_sbom.load_sbom_components")
@patch("upload_sbom.create_content_manifest")
@patch("upload_sbom.get_image")
def test_upload_sbom__all_components_exist(
    mock_get_image,
    mock_create_content_manifest,
    mock_load_sbom_components,
    mock_get_existing_bom_refs,
    mock_create_content_manifest_component,
):
    """Creation of manifest and all components is skipped"""
    mock_get_image.return_value = {
        "_id": IMAGE_ID,
        "content_manifest": {
            "_id": MANIFEST_ID,
        },
        "content_manifest_components": [{"_id": COMPONENT_ID}, {"_id": "123"}],
    }
    mock_load_sbom_components.return_value = [
        {"bom-ref": "aaa"},
        {"bom-ref": "bbb"},
    ]

    upload_sbom(GRAPHQL_API, IMAGE_ID, SBOM_PATH)

    mock_get_existing_bom_refs.assert_not_called()
    mock_create_content_manifest.assert_not_called()
    mock_create_content_manifest_component.assert_not_called()


def generate_pyxis_response(query_name, data=None, error=False):
    response = {
        "data": {
            query_name: {
                "data": data,
                "error": None,
            }
        }
    }
    if error:
        response["data"][query_name]["error"] = {"detail": "Major failure!"}
    return response


@patch("upload_sbom.create_content_manifest_component")
@patch("upload_sbom.get_existing_bom_refs")
@patch("upload_sbom.load_sbom_components")
@patch("upload_sbom.create_content_manifest")
@patch("upload_sbom.get_image")
def test_upload_sbom__existing_bom_ref_is_skipped(
    mock_get_image,
    mock_create_content_manifest,
    mock_load_sbom_components,
    mock_get_existing_bom_refs,
    mock_create_content_manifest_component,
):
    """One component already exists in Pyxis. Our sbom contains two
    components. So we want to upload only the second one to Pyxis,
    but we notice that bom-ref already exists in Pyxis for this image,
    so we skip it.
    """
    mock_get_image.return_value = {
        "_id": IMAGE_ID,
        "content_manifest": {
            "_id": MANIFEST_ID,
        },
        "content_manifest_components": [{"_id": COMPONENT_ID}],
    }
    mock_load_sbom_components.return_value = [
        {"bom-ref": "bbb"},
        {"bom-ref": "aaa"},
    ]
    mock_get_existing_bom_refs.return_value = {"aaa"}

    upload_sbom(GRAPHQL_API, IMAGE_ID, SBOM_PATH)

    mock_get_existing_bom_refs.assert_called_once_with(GRAPHQL_API, MANIFEST_ID)
    mock_create_content_manifest.assert_not_called()
    mock_create_content_manifest_component.assert_not_called()


@patch("pyxis.post")
def test_get_image__success(mock_post):
    mock_post.return_value = generate_pyxis_response("get_image", IMAGE_DICT)

    image = get_image(GRAPHQL_API, IMAGE_ID)

    assert image == IMAGE_DICT
    mock_post.assert_called_once()


@patch("pyxis.post")
def test_get_image__error(mock_post):
    mock_post.return_value = generate_pyxis_response("get_image", error=True)

    with pytest.raises(RuntimeError):
        get_image(GRAPHQL_API, IMAGE_ID)

    mock_post.assert_called_once()


@patch("pyxis.post")
def test_create_content_manifest__success(mock_post):
    mock_post.return_value = generate_pyxis_response(
        "create_content_manifest", {"_id": MANIFEST_ID}
    )

    id = create_content_manifest(GRAPHQL_API, IMAGE_ID)

    assert id == MANIFEST_ID
    mock_post.assert_called_once()


@patch("pyxis.post")
def test_create_content_manifest__error(mock_post):
    mock_post.return_value = generate_pyxis_response("create_content_manifest", error=True)

    with pytest.raises(RuntimeError):
        create_content_manifest(GRAPHQL_API, IMAGE_ID)

    mock_post.assert_called_once()


@patch("pyxis.post")
def test_get_existing_components__success(mock_post):
    """The Pyxis query is called twice and then the loop stops"""
    data1 = ["a", "b"]
    data2 = ["c"]
    mock_post.side_effect = [
        generate_pyxis_response("find_content_manifest_components", data1),
        generate_pyxis_response("find_content_manifest_components", data2),
    ]

    components = get_existing_components(GRAPHQL_API, MANIFEST_ID, page_size=2)

    assert components == data1 + data2
    assert mock_post.call_count == 2


@patch("pyxis.post")
def test_get_existing_components__no_components(mock_post):
    """Pyxis returns an empty list, so the loop stops and returns that"""
    mock_post.return_value = generate_pyxis_response("find_content_manifest_components", [])

    components = get_existing_components(GRAPHQL_API, MANIFEST_ID)

    assert components == []
    mock_post.assert_called_once()


@patch("pyxis.post")
def test_get_existing_components__error(mock_post):
    """Pyxis returns an error"""
    mock_post.return_value = generate_pyxis_response(
        "find_content_manifest_components", error=True
    )

    with pytest.raises(RuntimeError):
        get_existing_components(GRAPHQL_API, MANIFEST_ID)

    mock_post.assert_called_once()


@patch("upload_sbom.get_existing_components")
def test_get_existing_bom_refs__success(mock_get_existing_components):
    """bom_refs are correctly extracted from components, duplicates are removed"""
    mock_get_existing_components.return_value = [
        {"bom_ref": "a"},
        {"bom_ref": "b"},
        {"bom_ref": "c"},
        {"bom_ref": "a"},
    ]

    bom_refs = get_existing_bom_refs(GRAPHQL_API, MANIFEST_ID)

    assert bom_refs == {"a", "b", "c"}


@patch("upload_sbom.get_existing_components")
def test_get_existing_bom_refs__no_components_result_in_empty_set(
    mock_get_existing_components,
):
    mock_get_existing_components.return_value = []

    bom_refs = get_existing_bom_refs(GRAPHQL_API, MANIFEST_ID)

    assert bom_refs == set()


@patch("pyxis.post")
def test_create_content_manifest_component__success(mock_post):
    mock_post.return_value = generate_pyxis_response(
        "create_content_manifest_component_for_manifest", {"_id": COMPONENT_ID}
    )

    id = create_content_manifest_component(GRAPHQL_API, MANIFEST_ID, COMPONENT_DICT)

    assert id == COMPONENT_ID
    mock_post.assert_called_once()


@patch("pyxis.post")
def test_create_content_manifest_component__error(mock_post):
    mock_post.return_value = generate_pyxis_response(
        "create_content_manifest_component_for_manifest", error=True
    )

    with pytest.raises(RuntimeError):
        create_content_manifest_component(GRAPHQL_API, MANIFEST_ID, COMPONENT_DICT)

    mock_post.assert_called_once()


def test_get_existing_component_count__some_components():
    image = {
        "content_manifest_components": [
            COMPONENT_DICT,
            COMPONENT_DICT,
        ]
    }

    count = get_existing_component_count(image)

    assert count == 2


def test_get_existing_component_count__no_components():
    image = {}

    count = get_existing_component_count(image)

    assert count == 0


@patch("json.load")
@patch("upload_sbom.check_bom_ref_duplicates")
@patch("builtins.open")
def test_load_sbom_components__success(mock_open, mock_check_bom_ref_duplicates, mock_load):
    fake_components = [1, 2, 3, 4]
    mock_load.return_value = {"components": fake_components}

    loaded_components = load_sbom_components(SBOM_PATH)

    mock_load.assert_called_once_with(mock_open.return_value.__enter__.return_value)
    mock_check_bom_ref_duplicates.assert_called_once_with(loaded_components)
    assert fake_components == loaded_components


@patch("json.load")
@patch("upload_sbom.check_bom_ref_duplicates")
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


def test_convert_keys__success():
    external_references = [
        {"url": "myurl1"},
        {"url": "myurl2"},
    ]
    input = {
        "bom-ref": "mybomref",
        "externalReferences": external_references,
        "releaseNotes": {"featuredImage": "myimage"},
    }
    expected_output = {
        "bom_ref": "mybomref",
        "external_references": external_references,
        "release_notes": {"featured_image": "myimage"},
    }

    output = convert_keys(input)

    assert output == expected_output


def test_remove_unsupported_fields__success():
    component_orig = {
        "a": "aa",
        "b": "bb",
    }
    component_new = component_orig.copy()
    component_new[UNSUPPORTED_FIELDS[0]] = "mystr"

    remove_unsupported_fields(component_new)

    assert component_new == component_orig
