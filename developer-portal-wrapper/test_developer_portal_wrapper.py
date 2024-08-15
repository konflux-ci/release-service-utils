import logging
import os
import sys
import tempfile
from unittest.mock import patch

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
                "Would have run: push-cgw-metadata "
                "--CGW_filepath /tmp/cgw_metadata.yaml "
                "--CGW_hostname localhost"
            ) in caplog.messages

    temp_dir.cleanup()
