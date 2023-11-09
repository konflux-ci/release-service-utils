import pytest
from unittest.mock import patch, call, Mock

from upload_sbom import (
    upload_sbom_with_retry,
    upload_sbom,
    get_image,
    create_content_manifest,
    get_existing_bom_refs,
    create_content_manifest_components,
    get_template,
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
IMAGE_DICT = {
    "_id": IMAGE_ID,
    "content_manifest": None,
    "edges": {"content_manifest_components": {"data": []}},
}
COMPONENT_DICT = {"bom_ref": "mybomref"}


@patch("upload_sbom.upload_sbom")
def test_upload_sbom_with_retry__success(mock_upload_sbom):
    """upload_sbom succeeds on first attempt"""
    upload_sbom_with_retry(GRAPHQL_API, IMAGE_ID, SBOM_PATH)

    mock_upload_sbom.assert_called_once_with(GRAPHQL_API, IMAGE_ID, SBOM_PATH)


@patch("upload_sbom.upload_sbom")
def test_upload_sbom_with_retry__success_after_one_attempt(mock_upload_sbom):
    """upload_sbom succeeds after one retry"""
    mock_upload_sbom.side_effect = [RuntimeError("error"), None]

    upload_sbom_with_retry(GRAPHQL_API, IMAGE_ID, SBOM_PATH, backoff_factor=0)

    assert mock_upload_sbom.call_count == 2


@patch("upload_sbom.upload_sbom")
def test_upload_sbom_with_retry__fails(mock_upload_sbom):
    """upload_sbom fails constantly, so the retry eventually fails"""
    mock_upload_sbom.side_effect = RuntimeError("error")

    with pytest.raises(RuntimeError):
        upload_sbom_with_retry(GRAPHQL_API, IMAGE_ID, SBOM_PATH, retries=2, backoff_factor=0)

    assert mock_upload_sbom.call_count == 2


@patch("upload_sbom.create_content_manifest_components")
@patch("upload_sbom.get_existing_bom_refs")
@patch("upload_sbom.load_sbom_components")
@patch("upload_sbom.create_content_manifest")
@patch("upload_sbom.get_image")
def test_upload_sbom__success(
    mock_get_image,
    mock_create_content_manifest,
    mock_load_sbom_components,
    mock_get_existing_bom_refs,
    mock_create_content_manifest_components,
):
    """
    Basic use case - nothing exists in Pyxis yet and all components are successfully created
    """
    image = IMAGE_DICT.copy()
    image["components"] = []
    mock_get_image.return_value = image
    mock_create_content_manifest.return_value = MANIFEST_ID
    mock_load_sbom_components.return_value = [
        {"bom-ref": "aaa"},
        {"bom-ref": "bbb"},
        {"type": "library"},
    ]

    upload_sbom(GRAPHQL_API, IMAGE_ID, SBOM_PATH)

    mock_create_content_manifest.assert_called_once_with(GRAPHQL_API, IMAGE_ID)
    mock_get_existing_bom_refs.assert_not_called()
    mock_create_content_manifest_components.assert_called_once_with(
        GRAPHQL_API,
        MANIFEST_ID,
        [
            {"bom_ref": "aaa"},
            {"bom_ref": "bbb"},
            {"type": "library"},
        ],
    )


@patch("upload_sbom.create_content_manifest_components")
@patch("upload_sbom.load_sbom_components")
@patch("upload_sbom.create_content_manifest")
@patch("upload_sbom.get_image")
def test_upload_sbom__manifest_and_one_component_exist(
    mock_get_image,
    mock_create_content_manifest,
    mock_load_sbom_components,
    mock_create_content_manifest_components,
):
    """Creation of the manifest and the first component is skipped"""
    mock_get_image.return_value = {
        "_id": IMAGE_ID,
        "content_manifest": {
            "_id": MANIFEST_ID,
        },
        "components": [{"_id": COMPONENT_ID, "bom_ref": "aaa"}],
    }
    mock_load_sbom_components.return_value = [
        {"bom-ref": "aaa"},
        {"bom-ref": "bbb"},
    ]

    upload_sbom(GRAPHQL_API, IMAGE_ID, SBOM_PATH)

    mock_create_content_manifest.assert_not_called()
    mock_create_content_manifest_components.assert_called_once_with(
        GRAPHQL_API, MANIFEST_ID, [{"bom_ref": "bbb"}]
    )


@patch("upload_sbom.create_content_manifest_components")
@patch("upload_sbom.load_sbom_components")
@patch("upload_sbom.create_content_manifest")
@patch("upload_sbom.get_image")
def test_upload_sbom__all_components_exist(
    mock_get_image,
    mock_create_content_manifest,
    mock_load_sbom_components,
    mock_create_content_manifest_components,
):
    """Creation of manifest and all components is skipped"""
    mock_get_image.return_value = {
        "_id": IMAGE_ID,
        "content_manifest": {
            "_id": MANIFEST_ID,
        },
        "components": [{"_id": COMPONENT_ID}, {"_id": "123"}],
    }
    mock_load_sbom_components.return_value = [
        {"bom-ref": "aaa"},
        {"bom-ref": "bbb"},
    ]

    upload_sbom(GRAPHQL_API, IMAGE_ID, SBOM_PATH)

    mock_create_content_manifest.assert_not_called()
    mock_create_content_manifest_components.assert_not_called()


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


@patch("upload_sbom.create_content_manifest_components")
@patch("upload_sbom.load_sbom_components")
@patch("upload_sbom.create_content_manifest")
@patch("upload_sbom.get_image")
def test_upload_sbom__existing_bom_ref_is_skipped(
    mock_get_image,
    mock_create_content_manifest,
    mock_load_sbom_components,
    mock_create_content_manifest_components,
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
        "components": [{"_id": COMPONENT_ID, "bom_ref": "aaa"}],
    }
    mock_load_sbom_components.return_value = [
        {"bom-ref": "bbb"},
        {"bom-ref": "aaa"},
    ]

    upload_sbom(GRAPHQL_API, IMAGE_ID, SBOM_PATH)

    mock_create_content_manifest.assert_not_called()
    mock_create_content_manifest_components.assert_called_with(GRAPHQL_API, MANIFEST_ID, [])


@patch("pyxis.post")
def test_get_image__success(mock_post):
    """The Pyxis query is called twice and then the loop stops"""
    image1 = {
        "_id": IMAGE_ID,
        "content_manifest": {
            "_id": MANIFEST_ID,
        },
        "edges": {
            "content_manifest_components": {
                "data": [
                    {"_id": "aa"},
                    {"_id": "bb"},
                ]
            }
        },
    }
    image2 = {
        "_id": IMAGE_ID,
        "content_manifest": {
            "_id": MANIFEST_ID,
        },
        "edges": {
            "content_manifest_components": {
                "data": [
                    {"_id": "cc"},
                ]
            }
        },
    }
    mock_post.side_effect = [
        generate_pyxis_response("get_image", image1),
        generate_pyxis_response("get_image", image2),
    ]

    image = get_image(GRAPHQL_API, IMAGE_ID, page_size=2)

    assert image["components"] == [
        {"_id": "aa"},
        {"_id": "bb"},
        {"_id": "cc"},
    ]
    assert image["_id"] == IMAGE_ID
    assert image["content_manifest"] == {"_id": MANIFEST_ID}
    assert mock_post.call_count == 2


@patch("pyxis.post")
def test_get_image__no_manifest_and_no_components(mock_post):
    """There are no components, so the query is called once"""
    mock_post.return_value = generate_pyxis_response("get_image", IMAGE_DICT)

    image = get_image(GRAPHQL_API, IMAGE_ID, page_size=2)

    assert image["components"] == []
    assert image["_id"] == IMAGE_ID
    assert image["content_manifest"] is None
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


def test_get_existing_bom_refs__success():
    """bom_refs are correctly extracted from components, duplicates are removed"""
    components = [
        {"bom_ref": "a"},
        {"bom_ref": "b"},
        {"bom_ref": "c"},
        {},  # Component with no bom_ref
        {"bom_ref": "a"},
    ]

    bom_refs = get_existing_bom_refs(components)

    assert bom_refs == {"a", "b", "c"}


def test_get_existing_bom_refs__no_components_result_in_empty_set():
    bom_refs = get_existing_bom_refs([])

    assert bom_refs == set()


@patch("pyxis.graphql_query")
@patch("upload_sbom.get_template")
def test_create_content_manifest_components__success(mock_get_template, mock_graphql_query):
    create_content_manifest_components(
        GRAPHQL_API, MANIFEST_ID, [COMPONENT_DICT], batch_size=2
    )

    mock_get_template.assert_called_once_with()
    mock_get_template.return_value.render.assert_called_once_with(components=[COMPONENT_DICT])
    mock_graphql_query.assert_called_once()


@patch("pyxis.graphql_query")
@patch("upload_sbom.get_template")
def test_create_content_manifest_component__multiple_batches(
    mock_get_template, mock_graphql_query
):
    comp1 = {"bom_ref": "aaa"}
    comp2 = {"bom_ref": "bbb"}
    comp3 = {"type": "library"}
    components = [comp1, comp2, comp3]

    create_content_manifest_components(GRAPHQL_API, MANIFEST_ID, components, batch_size=2)

    mock_get_template.assert_called_once_with()
    assert mock_get_template.return_value.render.call_args_list == [
        call(components=[comp1, comp2]),
        call(components=[comp3]),
    ]
    assert mock_graphql_query.call_count == 2
    assert mock_graphql_query.call_args_list == [
        call(
            GRAPHQL_API,
            {
                "query": mock_get_template.return_value.render.return_value,
                "variables": {
                    "id": MANIFEST_ID,
                    "input0": comp1,
                    "input1": comp2,
                },
            },
        ),
        call(
            GRAPHQL_API,
            {
                "query": mock_get_template.return_value.render.return_value,
                "variables": {
                    "id": MANIFEST_ID,
                    "input0": comp3,
                },
            },
        ),
    ]


@patch("pyxis.graphql_query")
@patch("upload_sbom.get_template")
def test_create_content_manifest_component__no_components(
    mock_get_template, mock_graphql_query
):
    create_content_manifest_components(GRAPHQL_API, MANIFEST_ID, [])

    mock_get_template.assert_not_called()
    mock_graphql_query.assert_not_called()


@patch("upload_sbom.Template")
@patch("builtins.open")
@patch("upload_sbom.os")
def test_get_template(mock_os, mock_open, mock_template):
    template_path = mock_os.path.join.return_value

    template = get_template()

    mock_open.assert_called_with(template_path)
    assert template == mock_template.return_value
    mock_open.return_value.__enter__.return_value.read.assert_called_once_with()


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
