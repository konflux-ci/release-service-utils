"""Unit tests for update_index_image."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from update_index_image import (
    UPDATED_SNAPSHOT_FILENAME,
    is_floating_tag,
    main,
    run_inspect_internal_request,
    run_update_index_image,
    update_index_images,
    validate_inspect_result,
)

MOCK_SHA = "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
MOCK_DIGESTS = [
    "sha256:amd64digest1234567890abcdef1234567890abcdef1234567890abcdef",
    "sha256:arm64digest1234567890abcdef1234567890abcdef1234567890abcdef",
]

FLOATING_SNAPSHOT = {
    "application": "test-app",
    "components": [
        {
            "name": "floating-component",
            "containerImage": "quay.io/redhat/index@sha256:olddigest",
            "repositories": [
                {"url": "quay.io/redhat/preview-operator-index", "tags": ["v4.13"]}
            ],
            "imageDigests": ["sha256:olddigest"],
        }
    ],
}

TIMESTAMPED_SNAPSHOT = {
    "application": "test-app",
    "components": [
        {
            "name": "timestamped-component",
            "containerImage": "quay.io/redhat/index@sha256:olddigest",
            "repositories": [
                {
                    "url": "quay.io/redhat/preview-operator-index",
                    "tags": ["v4.13-1783440062"],
                }
            ],
            "imageDigests": ["sha256:olddigest"],
        }
    ],
}

MIXED_SNAPSHOT = {
    "application": "test-app",
    "components": [
        {
            "name": "floating",
            "containerImage": "quay.io/redhat/index@sha256:olddigest1",
            "repositories": [
                {"url": "quay.io/redhat/preview-operator-index", "tags": ["v4.13"]}
            ],
            "imageDigests": ["sha256:olddigest1"],
        },
        {
            "name": "timestamped",
            "containerImage": "quay.io/redhat/index@sha256:olddigest2",
            "repositories": [
                {
                    "url": "quay.io/redhat/preview-operator-index",
                    "tags": ["v4.13-1783440062"],
                }
            ],
            "imageDigests": ["sha256:olddigest2"],
        },
    ],
}

DATA = {"fbc": {"publishingCredentials": "fbc-publishing-credentials"}}


# --- is_floating_tag ---


def test_is_floating_tag_bare_ocp_version() -> None:
    """Bare OCP version tags are floating."""
    assert is_floating_tag("v4.13") is True
    assert is_floating_tag("v4.9") is True


def test_is_floating_tag_timestamped() -> None:
    """Timestamped tags are not floating."""
    assert is_floating_tag("v4.13-1783440062") is False


def test_is_floating_tag_patch_version() -> None:
    """Patch version tags are not floating."""
    assert is_floating_tag("v4.13.1") is False


def test_is_floating_tag_pre_ga() -> None:
    """Pre-GA tags are not floating."""
    assert is_floating_tag("v4.13-pre-ga") is False


def test_is_floating_tag_empty() -> None:
    """Empty string is not a floating tag."""
    assert is_floating_tag("") is False


# --- validate_inspect_result ---


def test_validate_inspect_result_valid() -> None:
    """Returns sha and digests for valid input."""
    result = {"sha": MOCK_SHA, "digests": MOCK_DIGESTS}
    sha, digests = validate_inspect_result(result, "ir-test")
    assert sha == MOCK_SHA
    assert digests == MOCK_DIGESTS


def test_validate_inspect_result_missing_sha() -> None:
    """Raises on missing sha."""
    with pytest.raises(ValueError, match="missing or null 'sha'"):
        validate_inspect_result({"digests": MOCK_DIGESTS}, "ir-test")


def test_validate_inspect_result_null_sha() -> None:
    """Raises on null sha."""
    with pytest.raises(ValueError, match="missing or null 'sha'"):
        validate_inspect_result({"sha": None, "digests": MOCK_DIGESTS}, "ir-test")


def test_validate_inspect_result_invalid_sha_prefix() -> None:
    """Raises when sha doesn't start with sha256:."""
    with pytest.raises(ValueError, match="invalid sha"):
        validate_inspect_result(
            {"sha": "md5:abcdef", "digests": MOCK_DIGESTS},
            "ir-test",
        )


def test_validate_inspect_result_non_array_digests() -> None:
    """Raises when digests is not a list."""
    with pytest.raises(ValueError, match="non-array 'digests'"):
        validate_inspect_result(
            {"sha": MOCK_SHA, "digests": "not-a-list"},
            "ir-test",
        )


def test_validate_inspect_result_empty_digests() -> None:
    """Raises on empty digests array."""
    with pytest.raises(ValueError, match="empty 'digests' array"):
        validate_inspect_result({"sha": MOCK_SHA, "digests": []}, "ir-test")


def test_validate_inspect_result_invalid_digest_prefix() -> None:
    """Raises when a digest doesn't start with sha256:."""
    with pytest.raises(ValueError, match="invalid digests"):
        validate_inspect_result(
            {"sha": MOCK_SHA, "digests": ["sha256:valid", "md5:invalid"]},
            "ir-test",
        )


# --- run_inspect_internal_request ---


@patch("update_index_image.run_cmd")
def test_run_inspect_internal_request_happy_path(mock_run) -> None:
    """Returns parsed requestMessage from InternalRequest."""
    ir_output = "InternalRequest 'ir-abc-123' created\n"
    kubectl_output = json.dumps(
        {"requestMessage": json.dumps({"sha": MOCK_SHA, "digests": MOCK_DIGESTS})}
    )
    mock_run.side_effect = [
        MagicMock(stdout=ir_output),
        MagicMock(stdout=kubectl_output),
    ]

    result = run_inspect_internal_request(
        "quay.io/redhat/index:v4.13",
        "creds-secret",
        "http://localhost",
        "main",
        "task-1",
        "pr-1",
    )

    assert result["sha"] == MOCK_SHA
    assert result["digests"] == MOCK_DIGESTS
    assert mock_run.call_count == 2


@patch("update_index_image.run_cmd")
def test_run_inspect_internal_request_no_ir_name(mock_run) -> None:
    """Raises when IR name cannot be extracted from output."""
    mock_run.return_value = MagicMock(stdout="some unexpected output\n")

    with pytest.raises(RuntimeError, match="Failed to extract InternalRequest name"):
        run_inspect_internal_request(
            "quay.io/redhat/index:v4.13",
            "creds-secret",
            "http://localhost",
            "main",
            "task-1",
            "pr-1",
        )


@patch("update_index_image.run_cmd")
def test_run_inspect_internal_request_empty_request_message(mock_run) -> None:
    """Raises when requestMessage is empty."""
    ir_output = "InternalRequest 'ir-abc-123' created\n"
    kubectl_output = json.dumps({"requestMessage": ""})
    mock_run.side_effect = [
        MagicMock(stdout=ir_output),
        MagicMock(stdout=kubectl_output),
    ]

    with pytest.raises(RuntimeError, match="empty requestMessage"):
        run_inspect_internal_request(
            "quay.io/redhat/index:v4.13",
            "creds-secret",
            "http://localhost",
            "main",
            "task-1",
            "pr-1",
        )


# --- update_index_images ---


@patch("update_index_image.run_inspect_internal_request")
def test_update_index_images_floating_tag(mock_inspect) -> None:
    """Floating tag component is inspected and updated."""
    mock_inspect.return_value = {"sha": MOCK_SHA, "digests": MOCK_DIGESTS}

    result = update_index_images(
        FLOATING_SNAPSHOT,
        "creds",
        "git-url",
        "main",
        "task-1",
        "pr-1",
    )

    assert result["components"][0]["containerImage"] == (
        f"quay.io/redhat/preview-operator-index@{MOCK_SHA}"
    )
    assert result["components"][0]["imageDigests"] == MOCK_DIGESTS
    mock_inspect.assert_called_once()


@patch("update_index_image.run_inspect_internal_request")
def test_update_index_images_timestamped_tag(mock_inspect) -> None:
    """Timestamped tag component is not inspected."""
    result = update_index_images(
        TIMESTAMPED_SNAPSHOT,
        "creds",
        "git-url",
        "main",
        "task-1",
        "pr-1",
    )

    assert result["components"][0]["containerImage"] == (
        "quay.io/redhat/index@sha256:olddigest"
    )
    assert result["components"][0]["imageDigests"] == ["sha256:olddigest"]
    mock_inspect.assert_not_called()


@patch("update_index_image.run_inspect_internal_request")
def test_update_index_images_mixed_tags(mock_inspect) -> None:
    """Only floating tag components are updated in a mixed snapshot."""
    mock_inspect.return_value = {"sha": MOCK_SHA, "digests": MOCK_DIGESTS}

    result = update_index_images(
        MIXED_SNAPSHOT,
        "creds",
        "git-url",
        "main",
        "task-1",
        "pr-1",
    )

    assert result["components"][0]["containerImage"] == (
        f"quay.io/redhat/preview-operator-index@{MOCK_SHA}"
    )
    assert result["components"][0]["imageDigests"] == MOCK_DIGESTS

    assert result["components"][1]["containerImage"] == (
        "quay.io/redhat/index@sha256:olddigest2"
    )
    assert result["components"][1]["imageDigests"] == ["sha256:olddigest2"]
    mock_inspect.assert_called_once()


@patch("update_index_image.run_inspect_internal_request")
def test_update_index_images_no_repositories(mock_inspect) -> None:
    """Component without repositories is skipped."""
    snapshot = {
        "components": [
            {
                "name": "no-repos",
                "containerImage": "quay.io/redhat/index@sha256:old",
            }
        ]
    }
    result = update_index_images(snapshot, "creds", "git-url", "main", "task-1", "pr-1")

    assert result["components"][0]["containerImage"] == ("quay.io/redhat/index@sha256:old")
    mock_inspect.assert_not_called()


@patch("update_index_image.run_inspect_internal_request")
def test_update_index_images_empty_tags_list(mock_inspect) -> None:
    """Component with empty tags list is skipped without IndexError."""
    snapshot = {
        "components": [
            {
                "name": "empty-tags",
                "containerImage": "quay.io/redhat/index@sha256:old",
                "repositories": [{"url": "quay.io/redhat/preview-operator-index", "tags": []}],
            }
        ]
    }
    result = update_index_images(snapshot, "creds", "git-url", "main", "task-1", "pr-1")

    assert result["components"][0]["containerImage"] == ("quay.io/redhat/index@sha256:old")
    mock_inspect.assert_not_called()


@patch("update_index_image.run_inspect_internal_request")
def test_update_index_images_empty_components(mock_inspect) -> None:
    """Empty components list produces no changes."""
    snapshot = {"components": []}
    result = update_index_images(
        snapshot,
        "creds",
        "git-url",
        "main",
        "task-1",
        "pr-1",
    )

    assert result["components"] == []
    mock_inspect.assert_not_called()


@patch("update_index_image.run_inspect_internal_request")
def test_update_index_images_does_not_mutate_input(mock_inspect) -> None:
    """Input snapshot is not mutated."""
    mock_inspect.return_value = {"sha": MOCK_SHA, "digests": MOCK_DIGESTS}
    original_image = FLOATING_SNAPSHOT["components"][0]["containerImage"]

    update_index_images(
        FLOATING_SNAPSHOT,
        "creds",
        "git-url",
        "main",
        "task-1",
        "pr-1",
    )

    assert FLOATING_SNAPSHOT["components"][0]["containerImage"] == original_image


# --- run_update_index_image ---


def _write_fixtures(
    tmp_path: Path,
    snapshot: dict | None = None,
    data: dict | None = None,
) -> tuple[Path, str, str, Path]:
    """Write snapshot and data fixtures.

    Returns (data_dir, snapshot_path, data_path, result_path).
    """
    uid_dir = tmp_path / "uid123"
    uid_dir.mkdir()

    snap = uid_dir / "index_image_snapshot.json"
    snap.write_text(json.dumps(snapshot or FLOATING_SNAPSHOT))

    data_file = uid_dir / "data.json"
    data_file.write_text(json.dumps(data or DATA))

    result_path = tmp_path / "result"
    return tmp_path, "uid123/index_image_snapshot.json", "uid123/data.json", result_path


@patch("update_index_image.run_inspect_internal_request")
def test_run_update_index_image_writes_output(mock_inspect, tmp_path) -> None:
    """Writes updated snapshot and result file."""
    mock_inspect.return_value = {"sha": MOCK_SHA, "digests": MOCK_DIGESTS}
    data_dir, snap_path, data_path, result_path = _write_fixtures(tmp_path)

    run_update_index_image(
        data_dir=data_dir,
        snapshot_path=snap_path,
        data_path=data_path,
        task_git_url="http://localhost",
        task_git_revision="main",
        task_id="task-1",
        pipelinerun_uid="pr-1",
        index_image_snapshot_result_path=result_path,
    )

    output_file = data_dir / "uid123" / UPDATED_SNAPSHOT_FILENAME
    assert output_file.is_file()
    updated = json.loads(output_file.read_text())
    assert updated["components"][0]["containerImage"].endswith(f"@{MOCK_SHA}")
    assert result_path.read_text() == UPDATED_SNAPSHOT_FILENAME


def test_run_update_index_image_missing_snapshot(tmp_path) -> None:
    """Raises when snapshot file does not exist."""
    result_path = tmp_path / "result"
    with pytest.raises(FileNotFoundError, match="snapshot"):
        run_update_index_image(
            data_dir=tmp_path,
            snapshot_path="nonexistent.json",
            data_path="data.json",
            task_git_url="http://localhost",
            task_git_revision="main",
            task_id="task-1",
            pipelinerun_uid="pr-1",
            index_image_snapshot_result_path=result_path,
        )


def test_run_update_index_image_missing_data(tmp_path) -> None:
    """Raises when data file does not exist."""
    snap = tmp_path / "snap.json"
    snap.write_text(json.dumps(FLOATING_SNAPSHOT))
    result_path = tmp_path / "result"

    with pytest.raises(FileNotFoundError, match="data JSON"):
        run_update_index_image(
            data_dir=tmp_path,
            snapshot_path="snap.json",
            data_path="nonexistent.json",
            task_git_url="http://localhost",
            task_git_revision="main",
            task_id="task-1",
            pipelinerun_uid="pr-1",
            index_image_snapshot_result_path=result_path,
        )


def test_run_update_index_image_missing_credentials(tmp_path) -> None:
    """Raises when publishingCredentials is missing from data."""
    data_dir, snap_path, data_path, result_path = _write_fixtures(
        tmp_path,
        data={"fbc": {}},
    )

    with pytest.raises(ValueError, match="publishingCredentials"):
        run_update_index_image(
            data_dir=data_dir,
            snapshot_path=snap_path,
            data_path=data_path,
            task_git_url="http://localhost",
            task_git_revision="main",
            task_id="task-1",
            pipelinerun_uid="pr-1",
            index_image_snapshot_result_path=result_path,
        )


# --- main ---


@patch("update_index_image.run_inspect_internal_request")
def test_main_success(mock_inspect, tmp_path, monkeypatch) -> None:
    """Main returns 0 on success."""
    mock_inspect.return_value = {"sha": MOCK_SHA, "digests": MOCK_DIGESTS}
    data_dir, snap_path, data_path, result_path = _write_fixtures(tmp_path)

    monkeypatch.setenv("PARAM_DATA_DIR", str(data_dir))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", snap_path)
    monkeypatch.setenv("PARAM_DATA_PATH", data_path)
    monkeypatch.setenv("PARAM_TASK_GIT_URL", "http://localhost")
    monkeypatch.setenv("PARAM_TASK_GIT_REVISION", "main")
    monkeypatch.setenv("PARAM_TASK_ID", "task-1")
    monkeypatch.setenv("PARAM_PIPELINERUN_UID", "pr-1")
    monkeypatch.setenv("RESULT_INDEX_IMAGE_SNAPSHOT_PATH", str(result_path))

    assert main() == 0
    assert result_path.read_text() == UPDATED_SNAPSHOT_FILENAME
