"""Unit tests for inspect_target_index."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from inspect_target_index import inspect_image, main, read_credential

SKOPEO_INSPECT_OUTPUT = json.dumps(
    {
        "Digest": "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
    }
)

SKOPEO_RAW_OUTPUT = json.dumps(
    {
        "manifests": [
            {"digest": "sha256:amd64digest1234567890abcdef1234567890abcdef1234567890abcdef"},
            {"digest": "sha256:arm64digest1234567890abcdef1234567890abcdef1234567890abcdef"},
        ],
    }
)


# --- read_credential ---


def test_read_credential(tmp_path) -> None:
    """Reads and strips the credential file."""
    cred_file = tmp_path / "cred"
    cred_file.write_text("user:pass\n")
    assert read_credential(cred_file) == "user:pass"


def test_read_credential_missing_file(tmp_path) -> None:
    """Raises when credential file does not exist."""
    with pytest.raises(FileNotFoundError):
        read_credential(tmp_path / "nonexistent")


# --- inspect_image ---


@patch("inspect_target_index.run_cmd")
def test_inspect_image_happy_path(mock_run) -> None:
    """Returns sha and digests from skopeo output."""
    mock_run.side_effect = [
        MagicMock(stdout=SKOPEO_INSPECT_OUTPUT),
        MagicMock(stdout=SKOPEO_RAW_OUTPUT),
    ]

    result = inspect_image("quay.io/redhat/index:v4.13", "user:pass")

    assert result["sha"] == (
        "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
    )
    assert len(result["digests"]) == 2
    assert result["digests"][0].startswith("sha256:")
    assert mock_run.call_count == 2


@patch("inspect_target_index.run_cmd")
def test_inspect_image_calls_skopeo_with_correct_args(mock_run) -> None:
    """Verifies the exact skopeo commands issued."""
    mock_run.side_effect = [
        MagicMock(stdout=SKOPEO_INSPECT_OUTPUT),
        MagicMock(stdout=SKOPEO_RAW_OUTPUT),
    ]

    inspect_image("quay.io/redhat/index:v4.13", "user:pass")

    calls = mock_run.call_args_list
    assert calls[0].args[0] == [
        "skopeo",
        "inspect",
        "--retry-times",
        "3",
        "--creds",
        "user:pass",
        "docker://quay.io/redhat/index:v4.13",
    ]
    assert calls[1].args[0] == [
        "skopeo",
        "inspect",
        "--retry-times",
        "3",
        "--raw",
        "--creds",
        "user:pass",
        "docker://quay.io/redhat/index:v4.13",
    ]


@patch("inspect_target_index.run_cmd")
def test_inspect_image_skopeo_failure_raises(mock_run) -> None:
    """Propagates exceptions from skopeo."""
    mock_run.side_effect = RuntimeError("skopeo failed")

    with pytest.raises(RuntimeError, match="skopeo failed"):
        inspect_image("quay.io/redhat/index:v4.13", "user:pass")


# --- main ---


@patch("inspect_target_index.run_cmd")
def test_main_happy_path(mock_run, tmp_path, monkeypatch) -> None:
    """Writes JSON result on success."""
    mock_run.side_effect = [
        MagicMock(stdout=SKOPEO_INSPECT_OUTPUT),
        MagicMock(stdout=SKOPEO_RAW_OUTPUT),
    ]

    cred_file = tmp_path / "cred"
    cred_file.write_text("user:pass")
    result_file = tmp_path / "result"

    monkeypatch.setenv("PARAM_SOURCE_INDEX", "quay.io/redhat/index:v4.13")
    monkeypatch.setenv("PARAM_INSPECT_CREDENTIALS_PATH", str(cred_file))
    monkeypatch.setenv("RESULT_REQUEST_MESSAGE_PATH", str(result_file))

    exit_code = main()

    assert exit_code == 0
    result = json.loads(result_file.read_text())
    assert result["sha"].startswith("sha256:")
    assert len(result["digests"]) == 2


@patch("inspect_target_index.run_cmd")
def test_main_error_writes_error_string(mock_run, tmp_path, monkeypatch) -> None:
    """Writes error message and exits 0 on failure."""
    mock_run.side_effect = RuntimeError("connection refused")

    cred_file = tmp_path / "cred"
    cred_file.write_text("user:pass")
    result_file = tmp_path / "result"

    monkeypatch.setenv("PARAM_SOURCE_INDEX", "quay.io/redhat/index:v4.13")
    monkeypatch.setenv("PARAM_INSPECT_CREDENTIALS_PATH", str(cred_file))
    monkeypatch.setenv("RESULT_REQUEST_MESSAGE_PATH", str(result_file))

    exit_code = main()

    assert exit_code == 0
    assert result_file.read_text() == "Error: Failed to inspect target index"


def test_main_missing_credential_writes_error(tmp_path, monkeypatch) -> None:
    """Writes error when credential file is missing."""
    result_file = tmp_path / "result"

    monkeypatch.setenv("PARAM_SOURCE_INDEX", "quay.io/redhat/index:v4.13")
    monkeypatch.setenv("PARAM_INSPECT_CREDENTIALS_PATH", str(tmp_path / "nonexistent"))
    monkeypatch.setenv("RESULT_REQUEST_MESSAGE_PATH", str(result_file))

    exit_code = main()

    assert exit_code == 0
    assert result_file.read_text() == "Error: Failed to inspect target index"
