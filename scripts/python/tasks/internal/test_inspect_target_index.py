"""Unit tests for inspect_target_index."""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from inspect_target_index import inspect_image, main

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


# --- inspect_image ---


@patch("inspect_target_index.run_cmd")
def test_inspect_image_happy_path(mock_run, tmp_path) -> None:
    """Return sha and digests from skopeo output."""
    mock_run.side_effect = [
        MagicMock(stdout=SKOPEO_INSPECT_OUTPUT),
        MagicMock(stdout=SKOPEO_RAW_OUTPUT),
    ]
    auth_file = tmp_path / "auth.json"
    auth_file.write_text("{}")

    result = inspect_image("quay.io/redhat/index:v4.13", auth_file)

    assert result["sha"] == (
        "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
    )
    assert len(result["digests"]) == 2
    assert result["digests"][0].startswith("sha256:")
    assert mock_run.call_count == 2


@patch("inspect_target_index.run_cmd")
def test_inspect_image_calls_skopeo_with_authfile(mock_run, tmp_path) -> None:
    """Verify skopeo is called with --authfile instead of --creds."""
    mock_run.side_effect = [
        MagicMock(stdout=SKOPEO_INSPECT_OUTPUT),
        MagicMock(stdout=SKOPEO_RAW_OUTPUT),
    ]
    auth_file = tmp_path / "auth.json"
    auth_file.write_text("{}")

    inspect_image("quay.io/redhat/index:v4.13", auth_file)

    calls = mock_run.call_args_list
    assert calls[0].args[0] == [
        "skopeo",
        "inspect",
        "--retry-times",
        "3",
        "--authfile",
        str(auth_file),
        "docker://quay.io/redhat/index:v4.13",
    ]
    assert calls[1].args[0] == [
        "skopeo",
        "inspect",
        "--retry-times",
        "3",
        "--raw",
        "--authfile",
        str(auth_file),
        "docker://quay.io/redhat/index:v4.13",
    ]


@patch("inspect_target_index.run_cmd")
def test_inspect_image_skopeo_failure_raises(mock_run, tmp_path) -> None:
    """Propagate exceptions from skopeo."""
    mock_run.side_effect = RuntimeError("skopeo failed")
    auth_file = tmp_path / "auth.json"
    auth_file.write_text("{}")

    with pytest.raises(RuntimeError, match="skopeo failed"):
        inspect_image("quay.io/redhat/index:v4.13", auth_file)


# --- main ---


@patch("inspect_target_index.subprocess.check_output", return_value=b'{"auths":{}}')
@patch("inspect_target_index.run_cmd")
def test_main_happy_path(mock_run, mock_check_output, tmp_path, monkeypatch) -> None:
    """Write JSON result on success."""
    mock_run.side_effect = [
        MagicMock(stdout=SKOPEO_INSPECT_OUTPUT),
        MagicMock(stdout=SKOPEO_RAW_OUTPUT),
    ]

    result_file = tmp_path / "result"

    monkeypatch.setenv("PARAM_SOURCE_INDEX", "quay.io/redhat/index:v4.13")
    monkeypatch.setenv("RESULT_REQUEST_MESSAGE_PATH", str(result_file))

    exit_code = main()

    assert exit_code == 0
    result = json.loads(result_file.read_text())
    assert result["sha"].startswith("sha256:")
    assert len(result["digests"]) == 2
    mock_check_output.assert_called_once_with(
        ["select-oci-auth", "quay.io/redhat/index:v4.13"],
        stderr=mock_check_output.call_args.kwargs["stderr"],
    )


@patch(
    "inspect_target_index.subprocess.check_output",
    side_effect=RuntimeError("select-oci-auth failed"),
)
def test_main_auth_error_writes_error_string(mock_check_output, tmp_path, monkeypatch) -> None:
    """Write error message and exit 0 when select-oci-auth fails."""
    result_file = tmp_path / "result"

    monkeypatch.setenv("PARAM_SOURCE_INDEX", "quay.io/redhat/index:v4.13")
    monkeypatch.setenv("RESULT_REQUEST_MESSAGE_PATH", str(result_file))

    exit_code = main()

    assert exit_code == 0
    assert result_file.read_text() == "Error: Failed to inspect target index"


@patch("inspect_target_index.subprocess.check_output", return_value=b'{"auths":{}}')
@patch("inspect_target_index.run_cmd")
def test_main_skopeo_error_writes_error_string(
    mock_run, mock_check_output, tmp_path, monkeypatch
) -> None:
    """Write error message and exit 0 on skopeo failure."""
    mock_run.side_effect = RuntimeError("connection refused")

    result_file = tmp_path / "result"

    monkeypatch.setenv("PARAM_SOURCE_INDEX", "quay.io/redhat/index:v4.13")
    monkeypatch.setenv("RESULT_REQUEST_MESSAGE_PATH", str(result_file))

    exit_code = main()

    assert exit_code == 0
    assert result_file.read_text() == "Error: Failed to inspect target index"


@patch("inspect_target_index.subprocess.check_output", return_value=b'{"auths":{}}')
@patch("inspect_target_index.run_cmd")
def test_main_cleans_up_auth_file(mock_run, mock_check_output, tmp_path, monkeypatch) -> None:
    """Verify the temporary auth file is removed after main completes."""
    mock_run.side_effect = [
        MagicMock(stdout=SKOPEO_INSPECT_OUTPUT),
        MagicMock(stdout=SKOPEO_RAW_OUTPUT),
    ]

    result_file = tmp_path / "result"

    monkeypatch.setenv("PARAM_SOURCE_INDEX", "quay.io/redhat/index:v4.13")
    monkeypatch.setenv("RESULT_REQUEST_MESSAGE_PATH", str(result_file))

    created_paths: list[Path] = []
    original_named = tempfile.NamedTemporaryFile

    def tracking_named(*args, **kwargs):
        kwargs["delete"] = False
        f = original_named(*args, **kwargs)
        created_paths.append(Path(f.name))
        return f

    with patch(
        "inspect_target_index.tempfile.NamedTemporaryFile",
        side_effect=tracking_named,
    ):
        main()

    for p in created_paths:
        assert not p.exists(), f"Auth file was not cleaned up: {p}"
