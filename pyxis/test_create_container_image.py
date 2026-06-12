"""Tests for create_container_image module."""

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
    construct_tags,
    repository_digest_values,
    create_container_image,
    update_container_image_repositories,
    construct_repository,
    _rh_push_registry,
    main,
)

PYXIS_URL = "https://catalog.redhat.com/api/containers/"


def test_proxymap():
    """Test proxymap function."""
    repository = "quay.io/redhat-pending/foo----bar"

    mapped = proxymap(repository)

    assert mapped == "foo/bar"


@patch("create_container_image.pyxis.get")
def test_find_image__image_does_exist(mock_get):
    """Test find image  image does exist."""
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
    """Test find image  image does not exist."""
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
    """Test find image  no id in image."""
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
    """Test find repo in image  found."""
    image = {"repositories": [{"repository": "my/repo"}, {"repository": "foo/bar"}]}

    result = find_repo_in_image("foo/bar", image)

    assert result == 1


# scenario where repo is not present in the image
def test_find_repo_in_image__not_found():
    """Test find repo in image  not found."""
    image = {"repositories": [{"repository": "my/repo"}, {"repository": "foo/bar"}]}

    result = find_repo_in_image("something/missing", image)

    assert result is None


@patch("create_container_image.pyxis.post")
@patch("create_container_image.datetime")
def test_create_container_image(mock_datetime, mock_post):
    """Test create container image."""
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
    """Scenario where top_layer_id and uncompressed_top_layer_id are defined in parsed_data."""
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
    """Test create container image  rh push multiple tags."""
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
    """Test create container image  no digest."""
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
    """Test create container image  no name."""
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
    """Test update container image repositories."""
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
    """Test prepare parsed data  success."""
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
    """Test prepare parsed data  success no dockerfile."""
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
    """Test prepare parsed data  with layer sizes."""
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
    """Test prepare parsed data  metadata empty json."""
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
    """Test prepare parsed data  metadata empty object."""
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
    """Test prepare parsed data  metadata env only."""
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
    """Test prepare parsed data  metadata labels only."""
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
    """Test pyxis tags."""
    tags = ["tag1", "tag2"]
    now = "now"

    generated_tags = pyxis_tags(tags, now)

    assert generated_tags == [
        {"added_date": "now", "name": "tag1"},
        {"added_date": "now", "name": "tag2"},
    ]


def test_repository_digest_values__single_arch():
    """Test repository digest values  single arch."""
    args = MagicMock()
    args.media_type = "application/vnd.docker.distribution.manifest.v2+json"
    args.architecture_digest = "mydigest"

    result = repository_digest_values(args)

    assert result == {"manifest_schema2_digest": "mydigest"}


def test_repository_digest_values__multi_arch():
    """Test repository digest values  multi arch."""
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
    """Test registry selection for flatpak vs standard namespaces."""
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
    """Test construct repository  rh push true."""
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))
    args = MagicMock()
    args.media_type = "application/vnd.docker.distribution.manifest.list.v2+json"
    args.architecture_digest = "arch specific digest"
    args.digest = "some_digest"
    args.rh_push = "true"
    args.name = "quay.io/redhat-pending/some-product----some-image"
    tag_names = ["tagprefix", "tagprefix-timestamp"]
    tag_dicts = construct_tags(tag_names)

    repo = construct_repository(args, tag_dicts)

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
    """Test construct repository  rh push true flatpak prod."""
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))
    args = MagicMock()
    args.media_type = "application/vnd.oci.image.manifest.v1+json"
    args.architecture_digest = "sha256:abc"
    args.digest = "sha256:top"
    args.rh_push = "true"
    args.name = "quay.io/rh-flatpaks-prod/myapp----myflatpak"
    tag_names = ["latest"]
    tag_dicts = construct_tags(tag_names)

    repo = construct_repository(args, tag_dicts)

    assert repo["registry"] == "flatpaks.registry.redhat.io"
    assert repo["repository"] == "myapp/myflatpak"
    assert repo["published"] is True


@patch("create_container_image.datetime")
def test_construct_repository__rh_push_true_flatpak_stage(mock_datetime):
    """Test construct repository  rh push true flatpak stage."""
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))
    args = MagicMock()
    args.media_type = "application/vnd.oci.image.manifest.v1+json"
    args.architecture_digest = "sha256:def"
    args.digest = "sha256:top2"
    args.rh_push = "true"
    args.name = "quay.io/rh-flatpaks-stage/namespace----another-flatpak"
    tag_names = ["1.0"]
    tag_dicts = construct_tags(tag_names)

    repo = construct_repository(args, tag_dicts)

    assert repo["registry"] == "flatpaks.registry.redhat.io"
    assert repo["repository"] == "namespace/another-flatpak"  # proxymap: ---- -> /
    assert repo["published"] is True


@patch("create_container_image.datetime")
def test_construct_repository__rh_push_false(mock_datetime):
    """Test construct repository  rh push false."""
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))
    args = MagicMock()
    args.media_type = "application/vnd.docker.distribution.manifest.list.v2+json"
    args.architecture_digest = "arch specific digest"
    args.digest = "some_digest"
    args.rh_push = "false"
    args.name = "quay.io/some-org/some-image"
    tag_names = ["tagprefix", "tagprefix-timestamp", "latest"]
    tag_dicts = construct_tags(tag_names)

    repo = construct_repository(args, tag_dicts)

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


@patch("create_container_image.datetime")
def test_construct_tags__normal_mode(mock_datetime):
    """Test construct_tags without existing tags (normal mode)."""
    mock_datetime.now = MagicMock(return_value=datetime(2026, 5, 21, 10, 10, 10))
    tag_names = ["v1.0", "latest"]

    result = construct_tags(tag_names)

    assert result == [
        {"added_date": "2026-05-21T10:10:10.000000+00:00", "name": "v1.0"},
        {"added_date": "2026-05-21T10:10:10.000000+00:00", "name": "latest"},
    ]


@patch("create_container_image.datetime")
def test_construct_tags__append_mode_all_new(mock_datetime):
    """Test construct_tags with existing tags, but all new tags are different."""
    mock_datetime.now = MagicMock(return_value=datetime(2026, 5, 21, 12, 0, 0))
    tag_names = ["v2.0", "v2.1"]
    existing_tags = [
        {"added_date": "2026-05-20T10:00:00.000000+00:00", "name": "v1.0"},
        {"added_date": "2026-05-20T11:00:00.000000+00:00", "name": "v1.1"},
    ]

    result = construct_tags(tag_names, existing_tags)

    assert result == [
        {"added_date": "2026-05-21T12:00:00.000000+00:00", "name": "v2.0"},
        {"added_date": "2026-05-21T12:00:00.000000+00:00", "name": "v2.1"},
        {"added_date": "2026-05-20T10:00:00.000000+00:00", "name": "v1.0"},
        {"added_date": "2026-05-20T11:00:00.000000+00:00", "name": "v1.1"},
    ]


@patch("create_container_image.datetime")
def test_construct_tags__append_mode_overlapping(mock_datetime):
    """Test construct_tags with existing tags where some tags overlap."""
    mock_datetime.now = MagicMock(return_value=datetime(2026, 5, 21, 12, 0, 0))
    tag_names = ["v1.0", "v2.0"]
    existing_tags = [
        {"added_date": "2026-05-20T10:00:00.000000+00:00", "name": "v1.0"},
        {"added_date": "2026-05-20T11:00:00.000000+00:00", "name": "latest"},
    ]

    result = construct_tags(tag_names, existing_tags)

    # v1.0 gets updated date, v2.0 is new, latest is preserved
    assert result == [
        {"added_date": "2026-05-21T12:00:00.000000+00:00", "name": "v1.0"},
        {"added_date": "2026-05-21T12:00:00.000000+00:00", "name": "v2.0"},
        {"added_date": "2026-05-20T11:00:00.000000+00:00", "name": "latest"},
    ]


@patch("create_container_image.datetime")
def test_construct_tags__append_mode_empty_existing(mock_datetime):
    """Test construct_tags with empty existing tags list."""
    mock_datetime.now = MagicMock(return_value=datetime(2026, 5, 21, 12, 0, 0))
    tag_names = ["v1.0"]
    existing_tags = []

    result = construct_tags(tag_names, existing_tags)

    assert result == [
        {"added_date": "2026-05-21T12:00:00.000000+00:00", "name": "v1.0"},
    ]


@patch("create_container_image.update_container_image_repositories")
@patch("create_container_image.find_image")
@patch("create_container_image.prepare_parsed_data")
@patch("create_container_image.pyxis.setup_logger")
@patch("create_container_image.datetime")
def test_main__append_tags_false_replaces_tags(
    mock_datetime,
    mock_setup_logger,
    mock_prepare_parsed_data,
    mock_find_image,
    mock_update,
):
    """Test that without --append-tags, tags are replaced (existing behavior)."""
    mock_datetime.now = MagicMock(return_value=datetime(2026, 5, 21, 12, 0, 0))
    mock_prepare_parsed_data.return_value = {"architecture": "amd64"}

    # Existing image with v1.0 and v1.1 tags
    existing_image = {
        "_id": "12345",
        "repositories": [
            {
                "registry": "quay.io",
                "repository": "myorg/myimage",
                "tags": [
                    {"added_date": "2026-05-20T10:00:00.000000+00:00", "name": "v1.0"},
                    {"added_date": "2026-05-20T11:00:00.000000+00:00", "name": "v1.1"},
                ],
            }
        ],
    }
    mock_find_image.return_value = existing_image

    # Simulate running with new tags v2.0
    test_args = [
        "create_container_image.py",
        "--pyxis-url",
        "https://pyxis.test.com",
        "--certified",
        "false",
        "--tags",
        "v2.0",
        "--oras-manifest-fetch",
        "/dev/null",
        "--is-latest",
        "false",
        "--name",
        "quay.io/myorg/myimage",
        "--digest",
        "sha256:abc",
        "--architecture-digest",
        "sha256:def",
        "--architecture",
        "amd64",
        "--media-type",
        "application/vnd.docker.distribution.manifest.v2+json",
        "--append-tags",
        "false",
    ]

    with patch("sys.argv", test_args), patch("builtins.open", create=True):
        main()

    # Verify update was called with only the new tag (old tags replaced)
    assert mock_update.called
    updated_repos = mock_update.call_args[0][2]
    updated_tags = updated_repos[0]["tags"]
    tag_names = [tag["name"] for tag in updated_tags]

    assert tag_names == ["v2.0"]
    assert len(updated_tags) == 1


@patch("create_container_image.update_container_image_repositories")
@patch("create_container_image.find_image")
@patch("create_container_image.prepare_parsed_data")
@patch("create_container_image.pyxis.setup_logger")
@patch("create_container_image.datetime")
def test_main__append_tags_true_preserves_tags(
    mock_datetime,
    mock_setup_logger,
    mock_prepare_parsed_data,
    mock_find_image,
    mock_update,
):
    """Test that with --append-tags true, existing tags are preserved."""
    mock_datetime.now = MagicMock(return_value=datetime(2026, 5, 21, 12, 0, 0))
    mock_prepare_parsed_data.return_value = {"architecture": "amd64"}

    # Existing image with v1.0 and v1.1 tags
    existing_image = {
        "_id": "12345",
        "repositories": [
            {
                "registry": "quay.io",
                "repository": "myorg/myimage",
                "tags": [
                    {"added_date": "2026-05-20T10:00:00.000000+00:00", "name": "v1.0"},
                    {"added_date": "2026-05-20T11:00:00.000000+00:00", "name": "v1.1"},
                ],
            }
        ],
    }
    mock_find_image.return_value = existing_image

    # Simulate running with new tags v2.0
    test_args = [
        "create_container_image.py",
        "--pyxis-url",
        "https://pyxis.test.com",
        "--certified",
        "false",
        "--tags",
        "v2.0",
        "--oras-manifest-fetch",
        "/dev/null",
        "--is-latest",
        "false",
        "--name",
        "quay.io/myorg/myimage",
        "--digest",
        "sha256:abc",
        "--architecture-digest",
        "sha256:def",
        "--architecture",
        "amd64",
        "--media-type",
        "application/vnd.docker.distribution.manifest.v2+json",
        "--append-tags",
        "true",
    ]

    with patch("sys.argv", test_args), patch("builtins.open", create=True):
        main()

    # Verify update was called with all tags (new + existing)
    assert mock_update.called
    updated_repos = mock_update.call_args[0][2]
    updated_tags = updated_repos[0]["tags"]
    tag_names = [tag["name"] for tag in updated_tags]

    # Should have v2.0 (new) and v1.0, v1.1 (existing preserved)
    assert set(tag_names) == {"v2.0", "v1.0", "v1.1"}
    assert len(updated_tags) == 3

    # Check that existing tags kept their original dates
    for tag in updated_tags:
        if tag["name"] == "v1.0":
            assert tag["added_date"] == "2026-05-20T10:00:00.000000+00:00"
        elif tag["name"] == "v1.1":
            assert tag["added_date"] == "2026-05-20T11:00:00.000000+00:00"
        elif tag["name"] == "v2.0":
            # New tag gets current date
            assert tag["added_date"] == "2026-05-21T12:00:00.000000+00:00"


@patch("create_container_image.update_container_image_repositories")
@patch("create_container_image.find_image")
@patch("create_container_image.prepare_parsed_data")
@patch("create_container_image.pyxis.setup_logger")
@patch("create_container_image.datetime")
def test_main__append_tags_true_updates_reapplied_tag_date(
    mock_datetime,
    mock_setup_logger,
    mock_prepare_parsed_data,
    mock_find_image,
    mock_update,
):
    """Test that with --append-tags true, re-applied tags get updated dates."""
    mock_datetime.now = MagicMock(return_value=datetime(2026, 5, 21, 12, 0, 0))
    mock_prepare_parsed_data.return_value = {"architecture": "amd64"}

    # Existing image with v1.0 and latest tags
    existing_image = {
        "_id": "12345",
        "repositories": [
            {
                "registry": "quay.io",
                "repository": "myorg/myimage",
                "tags": [
                    {"added_date": "2026-05-20T10:00:00.000000+00:00", "name": "v1.0"},
                    {"added_date": "2026-05-20T11:00:00.000000+00:00", "name": "latest"},
                ],
            }
        ],
    }
    mock_find_image.return_value = existing_image

    # Simulate running with v1.0 and v2.0 (v1.0 is re-applied)
    test_args = [
        "create_container_image.py",
        "--pyxis-url",
        "https://pyxis.test.com",
        "--certified",
        "false",
        "--tags",
        "v1.0 v2.0",
        "--oras-manifest-fetch",
        "/dev/null",
        "--is-latest",
        "false",
        "--name",
        "quay.io/myorg/myimage",
        "--digest",
        "sha256:abc",
        "--architecture-digest",
        "sha256:def",
        "--architecture",
        "amd64",
        "--media-type",
        "application/vnd.docker.distribution.manifest.v2+json",
        "--append-tags",
        "true",
    ]

    with patch("sys.argv", test_args), patch("builtins.open", create=True):
        main()

    # Verify update was called
    assert mock_update.called
    updated_repos = mock_update.call_args[0][2]
    updated_tags = updated_repos[0]["tags"]

    # Should have v1.0 (updated date), v2.0 (new), latest (preserved)
    assert len(updated_tags) == 3
    tag_dict = {tag["name"]: tag for tag in updated_tags}

    # v1.0 was re-applied, should have updated date
    assert tag_dict["v1.0"]["added_date"] == "2026-05-21T12:00:00.000000+00:00"

    # v2.0 is new, should have current date
    assert tag_dict["v2.0"]["added_date"] == "2026-05-21T12:00:00.000000+00:00"

    # latest was not re-applied, should have original date
    assert tag_dict["latest"]["added_date"] == "2026-05-20T11:00:00.000000+00:00"
