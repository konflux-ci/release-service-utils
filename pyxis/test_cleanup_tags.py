import pytest
from unittest.mock import patch, call

from cleanup_tags import (
    cleanup_tags_with_retry,
    cleanup_tags,
    get_image,
    get_rh_registry_image_properties,
    get_candidates_for_cleanup,
    update_images,
    update_image,
    remove_none_values,
)

GRAPHQL_API = "myapiurl"
IMAGE_ID = "1111"
REGISTRY = "registry.access.redhat.com"
REPOSITORY = "myproduct/myimage"


@patch("cleanup_tags.cleanup_tags")
def test_cleanup_tags_with_retry__success(mock_cleanup_tags):
    """cleanup_tags succeeds on first attempt"""
    cleanup_tags_with_retry(GRAPHQL_API, IMAGE_ID)

    mock_cleanup_tags.assert_called_once_with(GRAPHQL_API, IMAGE_ID)


@patch("cleanup_tags.cleanup_tags")
def test_cleanup_tags_with_retry__success_after_one_attempt(mock_cleanup_tags):
    """cleanup_tags succeeds after one retry"""
    mock_cleanup_tags.side_effect = [RuntimeError("error"), None]

    cleanup_tags_with_retry(GRAPHQL_API, IMAGE_ID, backoff_factor=0)

    assert mock_cleanup_tags.call_count == 2


@patch("cleanup_tags.cleanup_tags")
def test_cleanup_tags_with_retry__fails(mock_cleanup_tags):
    """cleanup_tags fails constantly, so the retry eventually fails"""
    mock_cleanup_tags.side_effect = RuntimeError("error")

    with pytest.raises(RuntimeError):
        cleanup_tags_with_retry(GRAPHQL_API, IMAGE_ID, retries=2, backoff_factor=0)

    assert mock_cleanup_tags.call_count == 2


@patch("cleanup_tags.update_images")
@patch("cleanup_tags.get_candidates_for_cleanup")
@patch("cleanup_tags.get_image")
def test_cleanup_tags__success(
    mock_get_image,
    mock_get_candidates_for_cleanup,
    mock_update_images,
):
    """
    Most common use case - there are 4 candidates for cleanup in total:
    - 1 is the very same image we use as input, so it's skipped
    - 1 has tags to be removed
    - 2 are a different arch, so they are skipped
    """
    image1 = generate_image("1111", "amd64", ["latest", "9.4", "9.4-1111"])
    image2 = generate_image("2222", "amd64", ["latest", "9.4", "9.4-2222"])
    image3 = generate_image("3333", "arm64", ["latest", "9.4", "9.4-1111"])
    image4 = generate_image("4444", "arm64", ["latest", "9.4", "9.4-2222"])
    mock_get_image.return_value = image1
    mock_get_candidates_for_cleanup.side_effect = [
        [image1, image2, image3, image4],
        [image1, image2, image3, image4],
        [image1, image3],
    ]

    cleanup_tags(GRAPHQL_API, "1111")

    mock_get_image.assert_called_once_with(GRAPHQL_API, "1111")
    assert mock_get_candidates_for_cleanup.call_args_list == [
        call(GRAPHQL_API, REGISTRY, REPOSITORY, "latest"),
        call(GRAPHQL_API, REGISTRY, REPOSITORY, "9.4"),
        call(GRAPHQL_API, REGISTRY, REPOSITORY, "9.4-1111"),
    ]
    mock_update_images.assert_called_once_with(
        GRAPHQL_API, ["latest", "9.4", "9.4-1111"], {image2["_id"]: image2}
    )


@patch("cleanup_tags.update_images")
@patch("cleanup_tags.get_candidates_for_cleanup")
@patch("cleanup_tags.get_image")
def test_cleanup_tags__nothing_to_cleanup(
    mock_get_image,
    mock_get_candidates_for_cleanup,
    mock_update_images,
):
    """
    A case when no other image (with the same architecture) has any tags in common
    - 1 is the very same image we use as input, so it's skipped
    - 1 has tags to be removed
    - 2 are a different arch, so they are skipped
    """
    image1 = generate_image("1111", "amd64", ["latest", "9.4", "9.4-1111"])
    image2 = generate_image("2222", "arm64", ["latest", "9.4", "9.4-1111"])
    mock_get_image.return_value = image1
    mock_get_candidates_for_cleanup.side_effect = [
        [image1, image2],
        [image1, image2],
        [image1, image2],
    ]

    cleanup_tags(GRAPHQL_API, "1111")

    mock_get_image.assert_called_once_with(GRAPHQL_API, "1111")
    assert mock_get_candidates_for_cleanup.call_args_list == [
        call(GRAPHQL_API, REGISTRY, REPOSITORY, "latest"),
        call(GRAPHQL_API, REGISTRY, REPOSITORY, "9.4"),
        call(GRAPHQL_API, REGISTRY, REPOSITORY, "9.4-1111"),
    ]
    mock_update_images.assert_called_once_with(GRAPHQL_API, ["latest", "9.4", "9.4-1111"], {})


@patch("pyxis.graphql_query")
def test_get_image__success(mock_graphql_query):
    """The Pyxis query is called once"""
    image1 = generate_image("1111", "amd64", ["latest", "9.4", "9.4-1111"])
    mock_graphql_query.return_value = generate_pyxis_response("get_image", image1)

    image = get_image(GRAPHQL_API, "1111")

    assert image == image1
    mock_graphql_query.assert_called_once()


def test_get_rh_registry_image_properties__success():
    """Basic scenario where the function parses the image and returns
    the expected values
    """
    image = generate_image("1111", "amd64", ["latest"])

    registry, repository, tags = get_rh_registry_image_properties(image)

    assert registry == REGISTRY
    assert repository == REPOSITORY
    assert tags == ["latest"]


def test_get_rh_registry_image_properties__no_tags():
    """Scenario where the access.rh.c repository in the image has no tags,
    so the returned tags should be empty
    """
    image = generate_image("1111", "amd64", [])
    image["repositories"][0]["tags"] = None
    image["repositories"][1]["tags"] = None

    registry, repository, tags = get_rh_registry_image_properties(image)

    assert registry == REGISTRY
    assert repository == REPOSITORY
    assert tags == []


def test_get_rh_registry_image_properties__failure():
    """The Red Hat registry repository is not found in the image,
    so an exception is raised
    """
    image = generate_image("1111", "amd64", ["latest"])
    image["repositories"] = [
        {
            "registry": "quay.io",
            "repository": "myrepo",
            "tags": [
                {"name": "latest"},
                {"name": "9.4"},
            ],
        }
    ]

    with pytest.raises(RuntimeError):
        get_rh_registry_image_properties(image)


@patch("pyxis.graphql_query")
def test_get_candidates_for_cleanup__success(mock_graphql_query):
    """Basic happy path scenario. The pyxis result has two pages, so there are two calls"""
    image1 = generate_image("1111", "amd64", ["latest", "9.4", "9.4-1111"])
    image2 = generate_image("2222", "amd64", ["latest", "9.4", "9.4-2222"])
    image3 = generate_image("3333", "amd64", ["9.4-3333"])
    mock_graphql_query.side_effect = [
        generate_pyxis_response(
            "find_repository_images_by_registry_path_tag", [image1, image2]
        ),
        generate_pyxis_response("find_repository_images_by_registry_path_tag", [image3]),
    ]

    images = get_candidates_for_cleanup(GRAPHQL_API, REGISTRY, REPOSITORY, "latest", 2)

    assert images == [image1, image2, image3]
    assert mock_graphql_query.call_count == 2


@patch("cleanup_tags.update_image")
def test_update_images__success(mock_update_image):
    """Happy path scenario:
    There are 2 images on input and both have the correct tags removed
    """
    image1 = generate_image("1111", "amd64", ["latest", "9.4", "9.4-1111"])
    image1_new = generate_image("1111", "amd64", ["9.4-1111"])
    image2 = generate_image("2222", "amd64", ["9.4", "9.4-2222"])
    image2_new = generate_image("2222", "amd64", ["9.4-2222"])
    images = {
        image1["_id"]: image1,
        image2["_id"]: image2,
    }

    update_images(GRAPHQL_API, ["latest", "9.4", "9.4-0000"], images)

    assert mock_update_image.call_args_list == [
        call(GRAPHQL_API, image1_new),
        call(GRAPHQL_API, image2_new),
    ]


@patch("pyxis.graphql_query")
def test_update_image__success(mock_graphql_query):
    """The Pyxis query is called once"""
    image1 = generate_image("1111", "amd64", ["latest", "9.4", "9.4-1111"])
    mock_graphql_query.return_value = generate_pyxis_response("update_image", image1)

    image = update_image(GRAPHQL_API, image1)

    assert image == image1
    mock_graphql_query.assert_called_once()


def test_remove_none_values__success():
    """This test will verify that None values are removed correctly"""
    data = {
        "a": 1,
        "b": None,
        "c": {"d": 2, "e": None, "f": {"g": 3, "h": None}},
        "i": [None, 4, 5, None],
        "j": ["text", None, {"k": None, "l": 6}],
    }

    expected_result = {
        "a": 1,
        "c": {"d": 2, "f": {"g": 3}},
        "i": [4, 5],
        "j": ["text", {"l": 6}],
    }

    assert remove_none_values(data) == expected_result


def generate_image(id, architecture, tags):
    image = {
        "_id": id,
        "architecture": architecture,
        "repositories": [
            {
                "registry": "quay.io",
                "repository": "redhat-prod/myproduct----myimage",
                "tags": [{"name": tag} for tag in tags],
            },
            {
                "registry": REGISTRY,
                "repository": REPOSITORY,
                "tags": [{"name": tag} for tag in tags],
            },
        ],
    }
    return image


def generate_pyxis_response(query_name, data):
    response_json = {
        query_name: {
            "data": data,
            "error": None,
        }
    }

    return response_json
