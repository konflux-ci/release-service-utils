import logging
import os
import sys
from unittest.mock import patch

import pytest

import marketplacesvm_push_wrapper


@pytest.fixture()
def mock_mkt_env_vars():
    with patch.dict(
        os.environ, {k: "test" for k in marketplacesvm_push_wrapper.CLOUD_MKTS_ENV_VARS_STRICT}
    ):
        yield


def test_no_args(capsys):
    with pytest.raises(SystemExit):
        marketplacesvm_push_wrapper.main()

    _, err = capsys.readouterr()
    assert (
        "marketplacesvm_push_wrapper: error: the following arguments are required:"
        " --source, --starmap-file"
    ) in err


def test_dry_run(caplog, mock_mkt_env_vars):
    args = [
        "",
        "--dry-run",
        "--source",
        "/test1/starmap",
        "--source",
        "/test2/starmap",
        "--starmap-file",
        "mapping.yaml",
    ]

    with patch.object(sys, "argv", args):
        with caplog.at_level(logging.INFO):
            marketplacesvm_push_wrapper.main()
            assert "This is a dry-run!" in caplog.messages
            assert (
                "Would have run: pubtools-marketplacesvm-marketplace-push "
                "--offline --repo-file mapping.yaml "
                "staged:/test1/starmap,/test2/starmap"
            ) in caplog.messages


@patch("subprocess.run")
def test_basic_command(mock_run, caplog, mock_mkt_env_vars):
    args = [
        "",
        "--source",
        "/test1/starmap",
        "--source",
        "/test2/starmap",
        "--starmap-file",
        "mapping.yaml",
    ]

    with patch.object(sys, "argv", args):
        with caplog.at_level(logging.INFO):
            marketplacesvm_push_wrapper.main()
            assert "This is a dry-run!" not in caplog.messages
            assert (
                "Running pubtools-marketplacesvm-marketplace-push "
                "--offline --repo-file mapping.yaml "
                "staged:/test1/starmap,/test2/starmap"
            ) in caplog.messages

    mock_run.assert_called_once_with(
        [
            "pubtools-marketplacesvm-marketplace-push",
            "--offline",
            "--repo-file",
            "mapping.yaml",
            "staged:/test1/starmap,/test2/starmap",
        ],
        check=True,
    )


@patch("subprocess.run")
def test_basic_command_nochannel(mock_run, caplog, mock_mkt_env_vars):
    args = [
        "",
        "--nochannel",
        "--source",
        "/test1/starmap",
        "--source",
        "/test2/starmap",
        "--starmap-file",
        "mapping.yaml",
    ]

    with patch.object(sys, "argv", args):
        with caplog.at_level(logging.INFO):
            marketplacesvm_push_wrapper.main()
            assert "This is a dry-run!" not in caplog.messages
            assert (
                "Running pubtools-marketplacesvm-marketplace-push "
                "--offline --pre-push --repo-file mapping.yaml "
                "staged:/test1/starmap,/test2/starmap"
            ) in caplog.messages

    mock_run.assert_called_once_with(
        [
            "pubtools-marketplacesvm-marketplace-push",
            "--offline",
            "--pre-push",
            "--repo-file",
            "mapping.yaml",
            "staged:/test1/starmap,/test2/starmap",
        ],
        check=True,
    )


@pytest.mark.parametrize(
    "stageddirs",
    [
        ["/foo/bar/"],
        ["/a", "/tmp/foo/"],
        ["/a/b/c/d/e/f/g/h/i", "/a1/a2/a3/", "/f"],
        ["/tmp/afASFu.fas", "/tmp/A.fs_.", "/tmp/fas12414fas"],
    ],
)
def test_get_source_url(stageddirs):
    res = marketplacesvm_push_wrapper.get_source_url(stageddirs)

    assert res == f"staged:{','.join(stageddirs)}"


@pytest.mark.parametrize(
    "stageddirs",
    [["foo"], ["foo/bar"], [r"/////////////"]],
)
def test_get_source_url_invalid(stageddirs):
    err = "Not a valid staging directory:"
    with pytest.raises(ValueError, match=err):
        marketplacesvm_push_wrapper.get_source_url(stageddirs)
