"""Tests for the ``oci_auth`` helper module."""

from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest
from release_service_utils.helpers import oci_auth

_RUN_CMD = "release_service_utils.helpers.oci_auth.oci_auth.subprocess_cmd.run_cmd"
_IMAGE = "quay.io/test/img@sha256:abc"


def _completed(
    cmd: list[str],
    stdout: str = "",
    returncode: int = 0,
    stderr: str = "",
) -> subprocess.CompletedProcess[str]:
    """Build a CompletedProcess for command mocks."""
    return subprocess.CompletedProcess(
        args=cmd, returncode=returncode, stdout=stdout, stderr=stderr
    )


def _called_process_error(
    cmd: list[str],
    *,
    stderr: str = "",
    stdout: str = "",
) -> subprocess.CalledProcessError:
    """Build a CalledProcessError as raised by subprocess_cmd.run_cmd."""
    return subprocess.CalledProcessError(1, cmd, output=stdout, stderr=stderr)


def test_auth_json_returns_stripped_stdout() -> None:
    """Return docker config JSON from select-oci-auth stdout."""
    with patch(
        _RUN_CMD,
        return_value=_completed(["select-oci-auth", _IMAGE], stdout='{"auths": {}}\n'),
    ) as mock_run:
        result = oci_auth.auth_json(_IMAGE)

    assert result == '{"auths": {}}'
    mock_run.assert_called_once_with(["select-oci-auth", _IMAGE], check=True, stderr_path=None)


def test_auth_json_empty_stdout_becomes_empty_object() -> None:
    """Write an empty auths object when select-oci-auth prints nothing."""
    with patch(_RUN_CMD, return_value=_completed(["select-oci-auth", _IMAGE], stdout="  \n")):
        assert oci_auth.auth_json(_IMAGE) == "{}"


def test_auth_json_failure_raises_and_logs_stderr() -> None:
    """Raise when select-oci-auth fails and log stderr only."""
    with patch(
        _RUN_CMD,
        side_effect=_called_process_error(
            ["select-oci-auth", _IMAGE], stderr="no credentials\n"
        ),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            oci_auth.auth_json(_IMAGE)


def test_auth_json_check_false_returns_empty_object_on_failure() -> None:
    """Return {} when check=False and select-oci-auth exits nonzero."""
    with patch(
        _RUN_CMD,
        return_value=_completed(
            ["select-oci-auth", _IMAGE], returncode=1, stderr="no creds\n"
        ),
    ):
        assert oci_auth.auth_json(_IMAGE, check=False) == "{}"


def test_auth_json_passes_stderr_path(tmp_path: Path) -> None:
    """Forward stderr_path to the subprocess helper."""
    stderr_path = tmp_path / "stderr.txt"
    with patch(
        _RUN_CMD, return_value=_completed(["select-oci-auth", _IMAGE], stdout="{}")
    ) as mock_run:
        oci_auth.auth_json(_IMAGE, stderr_path=stderr_path)

    mock_run.assert_called_once_with(
        ["select-oci-auth", _IMAGE], check=True, stderr_path=stderr_path
    )


def test_auth_file_writes_json_and_removes_file() -> None:
    """Write auth JSON into a temp file and delete it on exit."""
    with patch(
        _RUN_CMD, return_value=_completed(["select-oci-auth", _IMAGE], stdout='{"a":1}')
    ):
        with oci_auth.auth_file(_IMAGE) as path:
            assert path.read_text(encoding="utf-8") == '{"a":1}'
            kept = path
        assert not kept.exists()


def test_docker_config_writes_auth_and_removes_dir() -> None:
    """Write select-oci-auth JSON into a temp dir and delete it on exit."""
    with patch(
        _RUN_CMD,
        return_value=_completed(["select-oci-auth", _IMAGE], stdout='{"auths": {}}\n'),
    ):
        with oci_auth.docker_config(_IMAGE) as config_dir:
            assert (config_dir / "config.json").read_text(encoding="utf-8") == '{"auths": {}}'
            kept = config_dir
        assert not kept.exists()
