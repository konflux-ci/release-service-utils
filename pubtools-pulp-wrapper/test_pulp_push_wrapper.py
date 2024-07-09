import logging
import os
import sys
from unittest.mock import patch

import pytest

import pulp_push_wrapper


@pytest.fixture()
def mock_gw_env_vars():
    with patch.dict(os.environ, {k: "test" for k in pulp_push_wrapper.EXODUS_ENV_VARS_STRICT}):
        yield


def test_no_args(capsys):
    with pytest.raises(SystemExit):
        pulp_push_wrapper.main()

    _, err = capsys.readouterr()
    assert (
        "pulp_push_wrapper: error: the following arguments are required: --source, --pulp-url"
        in err
    )


def test_dry_run(caplog, mock_gw_env_vars):
    args = [
        "",
        "--dry-run",
        "--source",
        "/test/1",
        "--source",
        "/test/2",
        "--pulp-url",
        "https://pulp-test.dev",
    ]

    with patch.object(sys, "argv", args):
        with caplog.at_level(logging.INFO):
            pulp_push_wrapper.main()
            assert "This is a dry-run!" in caplog.messages
            assert (
                "Would have run: pubtools-pulp-push --pulp-url https://pulp-test.dev"
                " --source staged:/test/1,/test/2"
            ) in caplog.messages


@patch("subprocess.run")
def test_basic_command(mock_run, caplog, mock_gw_env_vars):
    args = [
        "",
        "--source",
        "/test/1",
        "--source",
        "/test/2",
        "--pulp-url",
        "https://pulp-test.dev",
    ]

    with patch.object(sys, "argv", args):
        with caplog.at_level(logging.INFO):
            pulp_push_wrapper.main()
            assert "This is a dry-run!" not in caplog.messages
            assert (
                "Running pubtools-pulp-push --pulp-url https://pulp-test.dev"
                " --source staged:/test/1,/test/2"
            ) in caplog.messages

    mock_run.assert_called_once_with(
        [
            "pubtools-pulp-push",
            "--pulp-url",
            "https://pulp-test.dev",
            "--source",
            "staged:/test/1,/test/2",
        ],
        check=True,
    )
