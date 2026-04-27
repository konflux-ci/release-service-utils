import logging
import os
import sys
import tempfile
from unittest.mock import ANY, patch

import pytest

import developer_portal_wrapper


@pytest.fixture()
def mock_gw_env_vars():
    with patch.dict(
        os.environ, {k: "test" for k in developer_portal_wrapper.CGW_ENV_VARS_STRICT}
    ):
        yield


def test_no_args(capsys):
    with pytest.raises(SystemExit):
        developer_portal_wrapper.main()

    _, err = capsys.readouterr()
    assert (
        "developer_portal_wrapper: error: the following "
        "arguments are required: --product-name, --product-code," in err
    )


def test_dry_run(caplog, mock_gw_env_vars):

    temp_dir = tempfile.TemporaryDirectory()
    text_file_path = f"{temp_dir.name}/test-component-1.1.txt"
    qcow_file_path = f"{temp_dir.name}/test-component-1.1.qcow"

    with open(text_file_path, "w") as file:
        file.write("This is a text file.")
    with open(qcow_file_path, "w") as file:
        file.write("This is a qcow file.")

    args = [
        "",
        "--dry-run",
        "--product-name",
        "TEST",
        "--product-code",
        "2",
        "--product-version-name",
        "TEST NAME",
        "--cgw-hostname",
        "localhost",
        "--content-directory",
        temp_dir.name,
        "--file-prefix",
        "test-component",
    ]

    with patch.object(sys, "argv", args):
        with caplog.at_level(logging.INFO):
            developer_portal_wrapper.main()
            assert "This is a dry-run!" in caplog.messages
            assert "2 files will be published to CGW" in caplog.messages
            assert (
                "Would idempotently publish 2 file(s) to CGW host "
                "localhost for product 2/TEST NAME"
            ) in caplog.messages
            assert "CGW publish summary: created=2 updated=0 skipped=0" in caplog.messages

    temp_dir.cleanup()


@patch("developer_portal_wrapper.cgw_idempotency.create_files")
@patch("developer_portal_wrapper.cgw_idempotency.get_version_id")
@patch("developer_portal_wrapper.cgw_idempotency.get_product_id")
def test_disk_image_style_publish_is_idempotent(
    mock_get_product, mock_get_version, mock_create_files, mock_gw_env_vars
):
    """Validate disk-image style flow uses idempotent CGW publish."""
    mock_get_product.return_value = 101
    mock_get_version.return_value = 202
    mock_create_files.return_value = ([1001], [1002], [1003])

    temp_dir = tempfile.TemporaryDirectory()
    matching_files = [
        "amd-1.3-x86_64-kvm.qcow2",
        "amd-1.3-x86_64-boot.iso.gz",
    ]
    non_matching_files = ["sha256sum.txt", "sha256sum.txt.sig", "notes.txt"]

    for file_name in matching_files + non_matching_files:
        with open(f"{temp_dir.name}/{file_name}", "w") as file:
            file.write("dummy content")

    args = [
        "",
        "--product-name",
        "Disk Image for Linux",
        "--product-code",
        "DISK",
        "--product-version-name",
        "1.3-staging",
        "--cgw-hostname",
        "https://content-gateway.example.com",
        "--content-directory",
        temp_dir.name,
        "--file-prefix",
        "amd-1.3",
    ]

    with patch.object(sys, "argv", args):
        developer_portal_wrapper.main()

    mock_get_product.assert_called_once_with(
        host="https://content-gateway.example.com",
        session=ANY,
        product_name="Disk Image for Linux",
        product_code="DISK",
    )
    mock_get_version.assert_called_once_with(
        host="https://content-gateway.example.com",
        session=ANY,
        product_id=101,
        version_name="1.3-staging",
    )

    create_kwargs = mock_create_files.call_args.kwargs
    assert create_kwargs["host"] == "https://content-gateway.example.com"
    assert create_kwargs["product_id"] == 101
    assert create_kwargs["version_id"] == 202

    metadata = create_kwargs["metadata"]
    labels = {item["label"] for item in metadata}
    assert labels == set(matching_files)
    for item in metadata:
        assert item["productVersionId"] == 202
        assert item["shortURL"].startswith("/cgw/DISK/1.3-staging/")
        assert "/content/origin/files/sha256/" in item["downloadURL"]

    temp_dir.cleanup()
