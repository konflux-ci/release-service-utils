import pytest
from datetime import datetime
import json
from unittest.mock import patch, MagicMock

from create_container_image import (
    proxymap,
    find_image,
    repo_in_image,
    prepare_parsed_data,
    pyxis_tags,
    repository_digest_values,
    create_container_image,
    add_container_image_repository,
    construct_repositories,
)


PYXIS_URL = "https://catalog.redhat.com/api/containers/"


def test_proxymap():
    repository = "quay.io/redhat-pending/foo----bar"

    mapped = proxymap(repository)

    assert mapped == "foo/bar"


@patch("create_container_image.pyxis.get")
def test_find_image__image_does_exist(mock_get):
    # Arrange
    mock_rsp = MagicMock()
    mock_get.return_value = mock_rsp
    architecture_digest = "some_digest"
    mock_image = {"_id": 1}

    # Image already exists
    mock_rsp.json.return_value = {"data": [mock_image]}

    # Act
    image = find_image(PYXIS_URL, architecture_digest)

    # Assert
    assert image == mock_image
    mock_get.assert_called_once_with(
        PYXIS_URL
        + "v1/images?page_size=1&filter="
        + "repositories.manifest_schema2_digest%3D%3D%22some_digest%22"
        + "%3Bnot%28deleted%3D%3Dtrue%29"
    )


@patch("create_container_image.pyxis.get")
def test_find_image__image_does_not_exist(mock_get):
    # Arrange
    mock_rsp = MagicMock()
    mock_get.return_value = mock_rsp
    digest = "some_digest"

    # Image doesn't exist
    mock_rsp.json.return_value = {"data": []}

    # Act
    image = find_image(PYXIS_URL, digest)

    # Assert
    assert image is None


@patch("create_container_image.pyxis.get")
def test_find_image__no_id_in_image(mock_get):
    # Arrange
    mock_rsp = MagicMock()
    mock_get.return_value = mock_rsp
    digest = "some_digest"

    # Image exists, but has no id
    mock_rsp.json.return_value = {"data": [{"some_key": "some_value"}]}

    # Act
    with pytest.raises(RuntimeError):
        find_image(PYXIS_URL, digest)


# scenario where repo is present in the image
def test_repo_in_image__true():
    image = {"repositories": [{"repository": "my/repo"}, {"repository": "foo/bar"}]}

    result = repo_in_image("foo/bar", image)

    assert result


# scenario where repo is not present in the image
def test_repo_in_image__false():
    image = {"repositories": [{"repository": "my/repo"}, {"repository": "foo/bar"}]}

    result = repo_in_image("something/missing", image)

    assert not result


@patch("create_container_image.pyxis.post")
@patch("create_container_image.datetime")
def test_create_container_image(mock_datetime, mock_post):
    # Mock an _id in the response for logger check
    mock_post.return_value.json.return_value = {"_id": 0}

    # mock date
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))

    args = MagicMock()
    args.pyxis_url = PYXIS_URL
    args.tags = "some_version"
    args.certified = "false"
    args.rh_push = "false"
    args.architecture_digest = "arch specific digest"
    args.digest = "some_digest"
    args.media_type = "single architecture"
    args.name = "quay.io/some_repo"

    # Act
    create_container_image(
        args,
        {"architecture": "ok"},
    )

    # Assert
    mock_post.assert_called_with(
        PYXIS_URL + "v1/images",
        {
            "repositories": [
                {
                    "published": False,
                    "registry": "quay.io",
                    "repository": "some_repo",
                    "push_date": "1970-10-10T10:10:10.000000+00:00",
                    "tags": [
                        {
                            "added_date": "1970-10-10T10:10:10.000000+00:00",
                            "name": "some_version",
                        }
                    ],
                    # Note, no manifest_list_digest here. Single arch.
                    "manifest_schema2_digest": "arch specific digest",
                }
            ],
            "certified": False,
            "image_id": "arch specific digest",
            "architecture": "ok",
            "parsed_data": {"architecture": "ok"},
            "sum_layer_size_bytes": 0,
        },
    )


@patch("create_container_image.pyxis.patch")
@patch("create_container_image.datetime")
def test_add_container_image_repository(mock_datetime, mock_patch):
    # Mock an _id in the response for logger check
    mock_patch.return_value.json.return_value = {"_id": 0}

    # mock date
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))

    args = MagicMock()
    args.pyxis_url = PYXIS_URL
    args.tags = "some_version"
    args.rh_push = "true"
    args.architecture_digest = "arch specific digest"
    args.media_type = "single architecture"
    args.name = "quay.io/redhat-pending/some_product----some_repo"

    # Act
    add_container_image_repository(
        args,
        {"_id": "some_id", "repositories": []},
    )

    # Assert
    mock_patch.assert_called_with(
        PYXIS_URL + "v1/images/id/some_id",
        {
            "repositories": [
                {
                    "published": False,
                    "registry": "quay.io",
                    "repository": "redhat-pending/some_product----some_repo",
                    "push_date": "1970-10-10T10:10:10.000000+00:00",
                    "tags": [
                        {
                            "added_date": "1970-10-10T10:10:10.000000+00:00",
                            "name": "some_version",
                        }
                    ],
                    # Note, no manifest_list_digest here. Single arch.
                    "manifest_schema2_digest": "arch specific digest",
                },
                {
                    "published": True,
                    "registry": "registry.access.redhat.com",
                    "repository": "some_product/some_repo",
                    "push_date": "1970-10-10T10:10:10.000000+00:00",
                    "tags": [
                        {
                            "added_date": "1970-10-10T10:10:10.000000+00:00",
                            "name": "some_version",
                        }
                    ],
                    # Note, no manifest_list_digest here. Single arch.
                    "manifest_schema2_digest": "arch specific digest",
                },
            ],
        },
    )


@patch("create_container_image.pyxis.post")
@patch("create_container_image.datetime")
def test_create_container_image_latest(mock_datetime, mock_post):
    # Mock an _id in the response for logger check
    mock_post.return_value.json.return_value = {"_id": 0}

    # mock date
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))

    args = MagicMock()
    args.pyxis_url = PYXIS_URL
    args.tags = "some_version"
    args.certified = "false"
    args.is_latest = "true"
    args.rh_push = "false"
    args.digest = "some_digest"
    args.architecture_digest = "arch specific digest"
    args.media_type = "application/vnd.oci.image.index.v1+json"
    args.digest = "some_digest"
    args.name = "redhat.com/some_repo/foobar"

    # Act
    create_container_image(
        args,
        {
            "architecture": "ok",
        },
    )

    # Assert
    mock_post.assert_called_with(
        PYXIS_URL + "v1/images",
        {
            "repositories": [
                {
                    "published": False,
                    "registry": "redhat.com",
                    "repository": "some_repo/foobar",
                    "push_date": "1970-10-10T10:10:10.000000+00:00",
                    "tags": [
                        {
                            "added_date": "1970-10-10T10:10:10.000000+00:00",
                            "name": "some_version",
                        },
                        {
                            "added_date": "1970-10-10T10:10:10.000000+00:00",
                            "name": "latest",
                        },
                    ],
                    "manifest_list_digest": "some_digest",
                    "manifest_schema2_digest": "arch specific digest",
                }
            ],
            "certified": False,
            "image_id": "arch specific digest",
            "architecture": "ok",
            "parsed_data": {"architecture": "ok"},
            "sum_layer_size_bytes": 0,
        },
    )


@patch("create_container_image.pyxis.post")
@patch("create_container_image.datetime")
def test_create_container_image_rh_push_multiple_tags(mock_datetime, mock_post):
    # Mock an _id in the response for logger check
    mock_post.return_value.json.return_value = {"_id": 0}

    # mock date
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))

    args = MagicMock()
    args.pyxis_url = PYXIS_URL
    args.tags = "tagprefix tagprefix-timestamp"
    args.certified = "false"
    args.rh_push = "true"
    args.digest = "some_digest"
    args.architecture_digest = "arch specific digest"
    args.media_type = "application/vnd.oci.image.index.v1+json"
    args.digest = "some_digest"
    args.name = "quay.io/redhat-pending/some-product----some-image"

    # Act
    create_container_image(
        args,
        {
            "architecture": "ok",
        },
    )

    # Assert
    mock_post.assert_called_with(
        PYXIS_URL + "v1/images",
        {
            "repositories": [
                {
                    "published": False,
                    "registry": "quay.io",
                    "repository": "redhat-pending/some-product----some-image",
                    "push_date": "1970-10-10T10:10:10.000000+00:00",
                    "tags": [
                        {
                            "added_date": "1970-10-10T10:10:10.000000+00:00",
                            "name": "tagprefix",
                        },
                        {
                            "added_date": "1970-10-10T10:10:10.000000+00:00",
                            "name": "tagprefix-timestamp",
                        },
                    ],
                    "manifest_list_digest": "some_digest",
                    "manifest_schema2_digest": "arch specific digest",
                },
                {
                    "published": True,
                    "registry": "registry.access.redhat.com",
                    "repository": "some-product/some-image",
                    "push_date": "1970-10-10T10:10:10.000000+00:00",
                    "tags": [
                        {
                            "added_date": "1970-10-10T10:10:10.000000+00:00",
                            "name": "tagprefix",
                        },
                        {
                            "added_date": "1970-10-10T10:10:10.000000+00:00",
                            "name": "tagprefix-timestamp",
                        },
                    ],
                    "manifest_list_digest": "some_digest",
                    "manifest_schema2_digest": "arch specific digest",
                },
            ],
            "certified": False,
            "image_id": "arch specific digest",
            "architecture": "ok",
            "parsed_data": {"architecture": "ok"},
            "sum_layer_size_bytes": 0,
        },
    )


def test_create_container_image_no_digest():
    args = MagicMock()

    with pytest.raises(Exception):
        create_container_image(
            args,
            {
                "architecture": "ok",
                "name": "redhat.com/some_repo/foobar",
            },
        )


def test_create_container_image_no_name():
    args = MagicMock()

    with pytest.raises(Exception):
        create_container_image(
            args,
            {
                "architecture": "ok",
                "digest": "some_digest",
            },
        )


@patch("builtins.open")
def test_prepare_parsed_data__success(mock_open):
    args = MagicMock()
    args.architecture = "test"
    args.dockerfile = "mydockerfile"
    manifest_content = json.dumps(
        {
            "layers": [{"digest": "1"}, {"digest": "2"}],
        }
    )
    dockerfile_content = """FROM myimage\n\nRUN command\n"""
    mock_open1 = MagicMock()
    mock_open2 = MagicMock()
    mock_open.side_effect = [mock_open1, mock_open2]
    file1 = mock_open1.__enter__.return_value
    file2 = mock_open2.__enter__.return_value
    file1.read.return_value = manifest_content
    file2.read.return_value = dockerfile_content

    parsed_data = prepare_parsed_data(args)

    assert parsed_data == {
        "architecture": "test",
        "layers": ["2", "1"],
        "files": [
            {"key": "buildfile", "filename": "Dockerfile", "content": dockerfile_content}
        ],
        "sum_layer_size_bytes": 0,
        "uncompressed_layer_sizes": [],
        "uncompressed_size_bytes": 0,
        "top_layer_id": "2",
        "uncompressed_top_layer_id": None,
    }


@patch("builtins.open")
def test_prepare_parsed_data__success_no_dockerfile(mock_open):
    args = MagicMock()
    args.architecture = "test"
    args.dockerfile = ""
    manifest_content = json.dumps(
        {
            "layers": [{"digest": "1"}, {"digest": "2"}],
        }
    )
    file = mock_open.return_value.__enter__.return_value
    file.read.return_value = manifest_content

    parsed_data = prepare_parsed_data(args)

    assert parsed_data == {
        "architecture": "test",
        "layers": ["2", "1"],
        "sum_layer_size_bytes": 0,
        "uncompressed_layer_sizes": [],
        "uncompressed_size_bytes": 0,
        "top_layer_id": "2",
        "uncompressed_top_layer_id": None,
    }


@patch("builtins.open")
def test_prepare_parsed_data__with_layer_sizes(mock_open):
    args = MagicMock()
    args.architecture = "test"
    args.dockerfile = "mydockerfile"
    manifest_content = json.dumps(
        {
            "layers": [{"digest": "1", "size": 4}, {"digest": "2", "size": 3}],
            "uncompressed_layers": [
                {"digest": "3", "size": 5},
                {"digest": "4", "size": 4},
            ],
        }
    )
    dockerfile_content = """FROM myimage\n\nRUN command\n"""
    mock_open1 = MagicMock()
    mock_open2 = MagicMock()
    mock_open.side_effect = [mock_open1, mock_open2]
    file1 = mock_open1.__enter__.return_value
    file2 = mock_open2.__enter__.return_value
    file1.read.return_value = manifest_content
    file2.read.return_value = dockerfile_content

    parsed_data = prepare_parsed_data(args)

    assert parsed_data == {
        "architecture": "test",
        "layers": ["2", "1"],
        "files": [
            {"key": "buildfile", "filename": "Dockerfile", "content": dockerfile_content}
        ],
        "sum_layer_size_bytes": 7,
        "uncompressed_layer_sizes": [
            {"layer_id": "4", "size_bytes": 4},
            {"layer_id": "3", "size_bytes": 5},
        ],
        "uncompressed_size_bytes": 9,
        "top_layer_id": "2",
        "uncompressed_top_layer_id": "4",
    }


def test_pyxis_tags__with_latest():
    args = MagicMock()
    args.tags = "tag1 tag2"
    args.is_latest = "true"
    now = "now"

    tags = pyxis_tags(args, now)

    assert tags == [
        {"added_date": "now", "name": "tag1"},
        {"added_date": "now", "name": "tag2"},
        {"added_date": "now", "name": "latest"},
    ]


def test_pyxis_tags__without_latest():
    args = MagicMock()
    args.tags = "tag1 tag2"
    args.is_latest = "false"
    now = "now"

    tags = pyxis_tags(args, now)

    assert tags == [
        {"added_date": "now", "name": "tag1"},
        {"added_date": "now", "name": "tag2"},
    ]


def test_repository_digest_values__single_arch():
    args = MagicMock()
    args.media_type = "application/vnd.docker.distribution.manifest.v2+json"
    args.architecture_digest = "mydigest"

    result = repository_digest_values(args)

    assert result == {"manifest_schema2_digest": "mydigest"}


def test_repository_digest_values__multi_arch():
    args = MagicMock()
    args.media_type = "application/vnd.docker.distribution.manifest.list.v2+json"
    args.architecture_digest = "mydigest"
    args.digest = "mytopdigest"

    result = repository_digest_values(args)

    assert result == {
        "manifest_schema2_digest": "mydigest",
        "manifest_list_digest": "mytopdigest",
    }


@patch("create_container_image.datetime")
def test_construct_repositories__rh_push_true(mock_datetime):
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))
    args = MagicMock()
    args.media_type = "application/vnd.docker.distribution.manifest.list.v2+json"
    args.architecture_digest = "arch specific digest"
    args.digest = "some_digest"
    args.tags = "tagprefix tagprefix-timestamp"
    args.is_latest = "false"
    args.rh_push = "true"
    args.name = "quay.io/redhat-pending/some-product----some-image"

    repos = construct_repositories(args)

    assert repos == [
        {
            "published": False,
            "registry": "quay.io",
            "repository": "redhat-pending/some-product----some-image",
            "push_date": "1970-10-10T10:10:10.000000+00:00",
            "tags": [
                {
                    "added_date": "1970-10-10T10:10:10.000000+00:00",
                    "name": "tagprefix",
                },
                {
                    "added_date": "1970-10-10T10:10:10.000000+00:00",
                    "name": "tagprefix-timestamp",
                },
            ],
            "manifest_list_digest": "some_digest",
            "manifest_schema2_digest": "arch specific digest",
        },
        {
            "published": True,
            "registry": "registry.access.redhat.com",
            "repository": "some-product/some-image",
            "push_date": "1970-10-10T10:10:10.000000+00:00",
            "tags": [
                {
                    "added_date": "1970-10-10T10:10:10.000000+00:00",
                    "name": "tagprefix",
                },
                {
                    "added_date": "1970-10-10T10:10:10.000000+00:00",
                    "name": "tagprefix-timestamp",
                },
            ],
            "manifest_list_digest": "some_digest",
            "manifest_schema2_digest": "arch specific digest",
        },
    ]


@patch("create_container_image.datetime")
def test_construct_repositories__rh_push_false(mock_datetime):
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))
    args = MagicMock()
    args.media_type = "application/vnd.docker.distribution.manifest.list.v2+json"
    args.architecture_digest = "arch specific digest"
    args.digest = "some_digest"
    args.tags = "tagprefix tagprefix-timestamp"
    args.is_latest = "true"
    args.rh_push = "false"
    args.name = "quay.io/some-org/some-image"

    repos = construct_repositories(args)

    assert repos == [
        {
            "published": False,
            "registry": "quay.io",
            "repository": "some-org/some-image",
            "push_date": "1970-10-10T10:10:10.000000+00:00",
            "tags": [
                {
                    "added_date": "1970-10-10T10:10:10.000000+00:00",
                    "name": "tagprefix",
                },
                {
                    "added_date": "1970-10-10T10:10:10.000000+00:00",
                    "name": "tagprefix-timestamp",
                },
                {
                    "added_date": "1970-10-10T10:10:10.000000+00:00",
                    "name": "latest",
                },
            ],
            "manifest_list_digest": "some_digest",
            "manifest_schema2_digest": "arch specific digest",
        },
    ]
