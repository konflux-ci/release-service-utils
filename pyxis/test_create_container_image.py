import pytest
from datetime import datetime
import json
from unittest.mock import patch, MagicMock

from create_container_image import (
    proxymap,
    find_image,
    find_repo_in_image,
    prepare_parsed_data,
    pyxis_tags,
    repository_digest_values,
    create_container_image,
    update_container_image_repositories,
    construct_repository,
    _rh_push_registry,
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
def test_find_repo_in_image__found():
    image = {"repositories": [{"repository": "my/repo"}, {"repository": "foo/bar"}]}

    result = find_repo_in_image("foo/bar", image)

    assert result == 1


# scenario where repo is not present in the image
def test_find_repo_in_image__not_found():
    image = {"repositories": [{"repository": "my/repo"}, {"repository": "foo/bar"}]}

    result = find_repo_in_image("something/missing", image)

    assert result is None


@patch("create_container_image.pyxis.post")
@patch("create_container_image.datetime")
def test_create_container_image(mock_datetime, mock_post):
    # Mock an _id in the response for logger check
    mock_post.return_value.json.return_value = {"_id": 0}

    # mock date
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))

    args = MagicMock()
    args.pyxis_url = PYXIS_URL
    args.certified = "false"
    args.rh_push = "false"
    args.architecture_digest = "arch specific digest"
    args.digest = "some_digest"
    args.media_type = "single architecture"
    args.name = "quay.io/some_repo"
    tags = ["some_version"]

    # Act
    create_container_image(
        args,
        {"architecture": "ok"},
        tags,
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


@patch("create_container_image.pyxis.post")
@patch("create_container_image.datetime")
def test_create_container_image__top_layer_id(mock_datetime, mock_post):
    """Scenario where top_layer_id and uncompressed_top_layer_id are defined in parsed_data"""
    # Mock an _id in the response for logger check
    mock_post.return_value.json.return_value = {"_id": 0}

    # mock date
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))

    args = MagicMock()
    args.pyxis_url = PYXIS_URL
    args.certified = "false"
    args.rh_push = "false"
    args.digest = "some_digest"
    args.architecture_digest = "arch specific digest"
    args.media_type = "application/vnd.oci.image.index.v1+json"
    args.digest = "some_digest"
    args.name = "redhat.com/some_repo/foobar"
    tags = ["some_version", "latest"]

    # Act
    create_container_image(
        args,
        {
            "architecture": "ok",
            "top_layer_id": "some_top_id",
            "uncompressed_top_layer_id": "other_top_id",
        },
        tags,
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
            "top_layer_id": "some_top_id",
            "uncompressed_top_layer_id": "other_top_id",
        },
    )


@patch("create_container_image.pyxis.post")
@patch("create_container_image.datetime")
def test_create_container_image__rh_push_multiple_tags(mock_datetime, mock_post):
    # Mock an _id in the response for logger check
    mock_post.return_value.json.return_value = {"_id": 0}

    # mock date
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))

    args = MagicMock()
    args.pyxis_url = PYXIS_URL
    args.certified = "false"
    args.rh_push = "true"
    args.digest = "some_digest"
    args.architecture_digest = "arch specific digest"
    args.media_type = "application/vnd.oci.image.index.v1+json"
    args.digest = "some_digest"
    args.name = "quay.io/redhat-pending/some-product----some-image"
    tags = ["tagprefix", "tagprefix-timestamp"]

    # Act
    create_container_image(
        args,
        {
            "architecture": "ok",
        },
        tags,
    )

    # Assert
    mock_post.assert_called_with(
        PYXIS_URL + "v1/images",
        {
            "repositories": [
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


def test_create_container_image__no_digest():
    args = MagicMock()

    with pytest.raises(Exception):
        create_container_image(
            args,
            {
                "architecture": "ok",
                "name": "redhat.com/some_repo/foobar",
            },
            [],
        )


def test_create_container_image__no_name():
    args = MagicMock()

    with pytest.raises(Exception):
        create_container_image(
            args,
            {
                "architecture": "ok",
                "digest": "some_digest",
            },
            [],
        )


@patch("create_container_image.pyxis.patch")
def test_update_container_image_repositories(mock_patch):
    image_id = "0000"
    # Mock an _id in the response for logger check
    mock_patch.return_value.json.return_value = {"_id": image_id}
    repositories = [
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
        }
    ]

    # Act
    update_container_image_repositories(
        PYXIS_URL,
        image_id,
        repositories,
    )

    # Assert
    mock_patch.assert_called_with(
        PYXIS_URL + "v1/images/id/0000",
        {"repositories": repositories},
    )


@patch("builtins.open")
def test_prepare_parsed_data__success(mock_open):
    args = MagicMock()
    args.architecture = "test"
    args.dockerfile = "mydockerfile"
    args.metadata = "test_metadata.json"
    manifest_content = json.dumps(
        {
            "layers": [{"digest": "1"}, {"digest": "2"}],
        }
    )
    metadata_json = json.dumps(
        {
            "env_variables": [
                "ENV_VAR_1=VALUE_1",
                "ENV_VAR_2=VALUE_2",
            ],
            "labels": [
                {"key": "LABEL_1", "value": "VALUE_1"},
                {"key": "LABEL_2", "value": "VALUE_2"},
            ],
        }
    )
    dockerfile_content = """FROM myimage\n\nRUN command\n"""
    mock_open1 = MagicMock()
    mock_open2 = MagicMock()
    mock_open3 = MagicMock()
    mock_open.side_effect = [mock_open1, mock_open2, mock_open3]
    file1 = mock_open1.__enter__.return_value
    file2 = mock_open2.__enter__.return_value
    file3 = mock_open3.__enter__.return_value
    file1.read.return_value = manifest_content
    file2.read.return_value = dockerfile_content
    file3.read.return_value = metadata_json

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
        "env_variables": [
            "ENV_VAR_1=VALUE_1",
            "ENV_VAR_2=VALUE_2",
        ],
        "labels": [
            {"key": "LABEL_1", "value": "VALUE_1"},
            {"key": "LABEL_2", "value": "VALUE_2"},
        ],
    }


@patch("builtins.open")
def test_prepare_parsed_data__success_no_dockerfile(mock_open):
    args = MagicMock()
    args.architecture = "test"
    args.dockerfile = ""
    args.metadata = ""
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
    args.metadata = ""
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


@patch("builtins.open")
def test_prepare_parsed_data__metadata_empty_json(mock_open):
    args = MagicMock()
    args.architecture = "test"
    args.dockerfile = ""
    args.metadata = ""
    manifest_content = json.dumps(
        {
            "layers": [{"digest": "1"}, {"digest": "2"}],
        }
    )
    file = mock_open.return_value.__enter__.return_value
    file.read.return_value = manifest_content

    parsed_data = prepare_parsed_data(args)

    assert "env_variables" not in parsed_data
    assert "labels" not in parsed_data


@patch("builtins.open")
def test_prepare_parsed_data__metadata_empty_object(mock_open):
    args = MagicMock()
    args.architecture = "test"
    args.dockerfile = ""
    args.metadata = "empty_object_metadata.json"
    manifest_content = json.dumps(
        {
            "layers": [{"digest": "1"}, {"digest": "2"}],
        }
    )
    metadata_json = json.dumps({})
    mock_open1 = MagicMock()
    mock_open2 = MagicMock()
    mock_open.side_effect = [mock_open1, mock_open2]
    file1 = mock_open1.__enter__.return_value
    file2 = mock_open2.__enter__.return_value
    file1.read.return_value = manifest_content
    file2.read.return_value = metadata_json

    parsed_data = prepare_parsed_data(args)

    assert "env_variables" not in parsed_data
    assert "labels" not in parsed_data


@patch("builtins.open")
def test_prepare_parsed_data__metadata_env_only(mock_open):
    args = MagicMock()
    args.architecture = "test"
    args.dockerfile = ""
    args.metadata = "env_only_metadata.json"
    manifest_content = json.dumps({"layers": [{"digest": "1"}]})
    metadata_json = json.dumps({"env_variables": ["ENV_VAR_1=VALUE_1"]})

    mock_open1 = MagicMock()
    mock_open2 = MagicMock()
    mock_open.side_effect = [mock_open1, mock_open2]
    file1 = mock_open1.__enter__.return_value
    file2 = mock_open2.__enter__.return_value
    file1.read.return_value = manifest_content
    file2.read.return_value = metadata_json

    parsed_data = prepare_parsed_data(args)

    assert parsed_data["env_variables"] == ["ENV_VAR_1=VALUE_1"]
    assert "labels" not in parsed_data


@patch("builtins.open")
def test_prepare_parsed_data__metadata_labels_only(mock_open):
    args = MagicMock()
    args.architecture = "test"
    args.dockerfile = ""
    args.metadata = "labels_only_metadata.json"
    manifest_content = json.dumps({"layers": [{"digest": "1"}]})
    metadata_json = json.dumps({"labels": [{"key": "LABEL_1", "value": "VALUE_1"}]})

    mock_open1 = MagicMock()
    mock_open2 = MagicMock()
    mock_open.side_effect = [mock_open1, mock_open2]
    file1 = mock_open1.__enter__.return_value
    file2 = mock_open2.__enter__.return_value
    file1.read.return_value = manifest_content
    file2.read.return_value = metadata_json

    parsed_data = prepare_parsed_data(args)

    assert parsed_data["labels"] == [{"key": "LABEL_1", "value": "VALUE_1"}]
    assert "env_variables" not in parsed_data


def test_pyxis_tags():
    tags = ["tag1", "tag2"]
    now = "now"

    generated_tags = pyxis_tags(tags, now)

    assert generated_tags == [
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


def test_rh_push_registry():
    """Flatpak namespaces use flatpaks.registry.redhat.io;
    others use registry.access.redhat.com.
    """
    assert (
        _rh_push_registry("quay.io/rh-flatpaks-prod/foo/bar") == "flatpaks.registry.redhat.io"
    )
    assert (
        _rh_push_registry("quay.io/rh-flatpaks-stage/foo/bar") == "flatpaks.registry.redhat.io"
    )
    assert (
        _rh_push_registry("quay.io/redhat-prod/product----image")
        == "registry.access.redhat.com"
    )
    assert (
        _rh_push_registry("quay.io/redhat-pending/product----image")
        == "registry.access.redhat.com"
    )
    assert _rh_push_registry("quay.io/some-org/repo") == "registry.access.redhat.com"


@patch("create_container_image.datetime")
def test_construct_repository__rh_push_true(mock_datetime):
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))
    args = MagicMock()
    args.media_type = "application/vnd.docker.distribution.manifest.list.v2+json"
    args.architecture_digest = "arch specific digest"
    args.digest = "some_digest"
    args.rh_push = "true"
    args.name = "quay.io/redhat-pending/some-product----some-image"
    tags = ["tagprefix", "tagprefix-timestamp"]

    repo = construct_repository(args, tags)

    assert repo == {
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
    }


@patch("create_container_image.datetime")
def test_construct_repository__rh_push_true_flatpak_prod(mock_datetime):
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))
    args = MagicMock()
    args.media_type = "application/vnd.oci.image.manifest.v1+json"
    args.architecture_digest = "sha256:abc"
    args.digest = "sha256:top"
    args.rh_push = "true"
    args.name = "quay.io/rh-flatpaks-prod/myapp----myflatpak"
    tags = ["latest"]

    repo = construct_repository(args, tags)

    assert repo["registry"] == "flatpaks.registry.redhat.io"
    assert repo["repository"] == "myapp/myflatpak"
    assert repo["published"] is True


@patch("create_container_image.datetime")
def test_construct_repository__rh_push_true_flatpak_stage(mock_datetime):
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))
    args = MagicMock()
    args.media_type = "application/vnd.oci.image.manifest.v1+json"
    args.architecture_digest = "sha256:def"
    args.digest = "sha256:top2"
    args.rh_push = "true"
    args.name = "quay.io/rh-flatpaks-stage/namespace----another-flatpak"
    tags = ["1.0"]

    repo = construct_repository(args, tags)

    assert repo["registry"] == "flatpaks.registry.redhat.io"
    assert repo["repository"] == "namespace/another-flatpak"  # proxymap: ---- -> /
    assert repo["published"] is True


@patch("create_container_image.datetime")
def test_construct_repository__rh_push_false(mock_datetime):
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))
    args = MagicMock()
    args.media_type = "application/vnd.docker.distribution.manifest.list.v2+json"
    args.architecture_digest = "arch specific digest"
    args.digest = "some_digest"
    args.rh_push = "false"
    args.name = "quay.io/some-org/some-image"
    tags = ["tagprefix", "tagprefix-timestamp", "latest"]

    repo = construct_repository(args, tags)

    assert repo == {
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
    }
