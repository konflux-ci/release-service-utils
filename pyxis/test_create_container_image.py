import pytest
from datetime import datetime
import json
from unittest.mock import patch, MagicMock

from create_container_image import (
    image_already_exists,
    create_container_image,
    prepare_parsed_data,
)


mock_pyxis_url = "https://catalog.redhat.com/api/containers/"


@patch("create_container_image.pyxis.get")
def test_image_already_exists__image_does_exist(mock_get):
    # Arrange
    mock_rsp = MagicMock()
    mock_get.return_value = mock_rsp
    args = MagicMock()
    args.pyxis_url = mock_pyxis_url
    args.architecture_digest = "some_digest"

    # Image already exists
    mock_rsp.json.return_value = {"data": [{"_id": 0}]}

    # Act
    exists = image_already_exists(args, args.architecture_digest)

    # Assert
    assert exists
    mock_get.assert_called_once_with(
        mock_pyxis_url
        + "v1/images?page_size=1&filter="
        + "repositories.manifest_schema2_digest%3D%3D%22some_digest%22"
        + "%3Bnot%28deleted%3D%3Dtrue%29"
    )


@patch("create_container_image.pyxis.get")
def test_image_already_exists__image_does_not_exist(mock_get):
    # Arrange
    mock_rsp = MagicMock()
    mock_get.return_value = mock_rsp
    args = MagicMock()
    args.pyxis_url = mock_pyxis_url
    digest = "some_digest"

    # Image doesn't exist
    mock_rsp.json.return_value = {"data": []}

    # Act
    exists = image_already_exists(args, digest)

    # Assert
    assert not exists


@patch("create_container_image.pyxis.post")
@patch("create_container_image.datetime")
def test_create_container_image(mock_datetime, mock_post):
    # Mock an _id in the response for logger check
    mock_post.return_value.json.return_value = {"_id": 0}

    # mock date
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))

    args = MagicMock()
    args.pyxis_url = mock_pyxis_url
    args.tags = "some_version"
    args.certified = "false"
    args.rh_push = "false"
    args.architecture_digest = "arch specific digest"
    args.media_type = "single architecture"

    # Act
    create_container_image(
        args,
        {"architecture": "ok", "digest": "some_digest", "name": "quay.io/some_repo"},
    )

    # Assert
    mock_post.assert_called_with(
        mock_pyxis_url + "v1/images",
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
    args.pyxis_url = mock_pyxis_url
    args.tags = "some_version"
    args.certified = "false"
    args.is_latest = "true"
    args.rh_push = "false"
    args.architecture_digest = "arch specific digest"
    args.media_type = "application/vnd.oci.image.index.v1+json"

    # Act
    create_container_image(
        args,
        {
            "architecture": "ok",
            "digest": "some_digest",
            "name": "redhat.com/some_repo/foobar",
        },
    )

    # Assert
    mock_post.assert_called_with(
        mock_pyxis_url + "v1/images",
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
    args.pyxis_url = mock_pyxis_url
    args.tags = "tagprefix tagprefix-timestamp"
    args.certified = "false"
    args.rh_push = "true"
    args.architecture_digest = "arch specific digest"
    args.media_type = "application/vnd.oci.image.index.v1+json"

    # Act
    create_container_image(
        args,
        {
            "architecture": "ok",
            "digest": "some_digest",
            "name": "quay.io/redhat-pending/some-product----some-image",
        },
    )

    # Assert
    mock_post.assert_called_with(
        mock_pyxis_url + "v1/images",
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
    args.architecture_digest = "sha:abc"
    args.name = "quay.io/hacbs-release/release-service-utils"
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
        "digest": "sha:abc",
        "layers": ["1", "2"],
        "name": "quay.io/hacbs-release/release-service-utils",
        "files": [
            {"key": "buildfile", "filename": "Dockerfile", "content": dockerfile_content}
        ],
    }


@patch("builtins.open")
def test_prepare_parsed_data__success_no_dockerfile(mock_open):
    args = MagicMock()
    args.architecture = "test"
    args.architecture_digest = "sha:abc"
    args.name = "quay.io/hacbs-release/release-service-utils"
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
        "digest": "sha:abc",
        "layers": ["1", "2"],
        "name": "quay.io/hacbs-release/release-service-utils",
    }
