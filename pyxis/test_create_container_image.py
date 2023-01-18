import json
import pytest
from datetime import datetime
from unittest import mock
from unittest.mock import patch, MagicMock

from create_container_image import (
    image_already_exists,
    create_container_image,
    prepare_parsed_data,
)


mock_pyxis_url = "https://catalog.redhat.com/api/containers/"


@patch("create_container_image.pyxis.get")
def test_image_already_exists(mock_get: MagicMock):
    # Arrange
    mock_rsp = MagicMock()
    mock_get.return_value = mock_rsp

    args = MagicMock()
    args.pyxis_url = mock_pyxis_url
    digest = "some_digest"

    # Image already exist
    mock_rsp.json.return_value = {"data": [{}]}

    # Act
    exists = image_already_exists(args, digest)
    # Assert
    assert exists
    mock_get.assert_called_with(
        mock_pyxis_url
        + "v1/images?page_size=1&filter=docker_image_digest%3D%3D%22some_digest%22%3Bnot%28deleted%3D%3Dtrue%29"
    )

    # Image doesn't exist
    mock_rsp.json.return_value = {"data": []}

    # Act
    exists = image_already_exists(args, digest)
    # Assert
    assert not exists


@patch("create_container_image.pyxis.post")
@patch("create_container_image.datetime")
def test_create_container_image(mock_datetime: MagicMock, mock_post: MagicMock):
    # Arrange
    mock_post.return_value = "ok"

    # mock date
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))

    args = MagicMock()
    args.pyxis_url = mock_pyxis_url
    args.tag = "some_version"
    args.certified = "false"

    # Act
    rsp = create_container_image(
        args,
        {"architecture": "ok", "digest": "some_digest", "name": "quay.io/some_repo"},
    )

    # Assert
    assert rsp == "ok"
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
                }
            ],
            "certified": False,
            "docker_image_digest": "some_digest",
            "image_id": "some_digest",
            "architecture": "ok",
            "parsed_data": {"architecture": "ok"},
        },
    )


@patch("create_container_image.pyxis.post")
@patch("create_container_image.datetime")
def test_create_container_image_latest(mock_datetime: MagicMock, mock_post: MagicMock):
    # Arrange
    mock_post.return_value = "ok"

    # mock date
    mock_datetime.now = MagicMock(return_value=datetime(1970, 10, 10, 10, 10, 10))

    args = MagicMock()
    args.pyxis_url = mock_pyxis_url
    args.tag = "some_version"
    args.certified = "false"
    args.is_latest = "true"

    # Act
    rsp = create_container_image(
        args,
        {
            "architecture": "ok",
            "digest": "some_digest",
            "name": "redhat.com/some_repo/foobar",
        },
    )

    # Assert
    assert rsp == "ok"
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
                }
            ],
            "certified": False,
            "docker_image_digest": "some_digest",
            "image_id": "some_digest",
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


def test_prepare_parsed_data():
    # Arrange
    file_content = {
        "Digest": "sha:abc",
        "DockerVersion": "1",
        "Layers": ["1", "2"],
        "Name": "quay.io/hacbs-release/release-utils",
        "Architecture": "test",
        "Env": ["a=test"],
    }

    # Act
    parsed_data = prepare_parsed_data(file_content)

    # Assert
    assert parsed_data == {
        "architecture": "test",
        "digest": "sha:abc",
        "docker_version": "1",
        "env_variables": ["a=test"],
        "layers": ["1", "2"],
        "name": "quay.io/hacbs-release/release-utils",
    }
