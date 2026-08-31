"""Tests for ``update_trusted_tasks``."""

from __future__ import annotations

import json
import logging
import runpy
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.tasks.managed.update_trusted_tasks import update_trusted_tasks

TASK = "release_service_utils.tasks.managed.update_trusted_tasks.update_trusted_tasks"


@pytest.fixture(autouse=True)
def _propagate_release_logger() -> None:
    """Allow caplog to capture records from the ``release`` logger."""
    release_logger = logging.getLogger("release")
    release_logger.propagate = True
    yield
    release_logger.propagate = False


def _write_snapshot(path: Path, data: dict[str, Any]) -> None:
    """Write *data* as JSON to *path*, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _make_snapshot(
    components: list[dict[str, Any]],
    component_group: str = "test-group",
) -> dict[str, Any]:
    """Build a snapshot dict with the given components."""
    return {"componentGroup": component_group, "components": components}


def _single_component(
    image: str = "quay.io/org/echo@sha256:abcde",
    repo_url: str = "quay.io/org/task-echo",
    tags: list[str] | None = None,
) -> dict[str, Any]:
    """Build a component dict with a single repository and tags."""
    return {
        "containerImage": image,
        "name": "echo",
        "repositories": [{"url": repo_url, "tags": tags or ["0.1"]}],
    }


# ---------------------------------------------------------------------------
# derive_acceptable_bundles_repo
# ---------------------------------------------------------------------------


def test_derive_acceptable_bundles_repo() -> None:
    """Replace last segment with ``data-acceptable-bundles``."""
    result = update_trusted_tasks.derive_acceptable_bundles_repo("quay.io/myorg/myrepo")
    assert result == "quay.io/myorg/data-acceptable-bundles"


def test_derive_acceptable_bundles_repo_nested() -> None:
    """Handle deeper paths correctly."""
    result = update_trusted_tasks.derive_acceptable_bundles_repo(
        "registry.example.com/a/b/repo"
    )
    assert result == "registry.example.com/a/b/data-acceptable-bundles"


# ---------------------------------------------------------------------------
# check_latest_exists
# ---------------------------------------------------------------------------


@patch(f"{TASK}.skopeo.inspect")
def test_check_latest_exists_true(mock_inspect: MagicMock) -> None:
    """:latest exists when skopeo returns 0."""
    mock_inspect.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
    assert update_trusted_tasks.check_latest_exists("quay.io/org/bundles") is True
    mock_inspect.assert_called_once_with(
        "quay.io/org/bundles:latest",
        raw=True,
        no_tags=True,
        check=False,
    )


@patch(f"{TASK}.skopeo.inspect")
def test_check_latest_exists_false(mock_inspect: MagicMock) -> None:
    """:latest does not exist when skopeo returns non-zero."""
    mock_inspect.return_value = subprocess.CompletedProcess([], 1, stdout="", stderr="")
    assert update_trusted_tasks.check_latest_exists("quay.io/org/bundles") is False


# ---------------------------------------------------------------------------
# _ec_track_bundle
# ---------------------------------------------------------------------------


@patch(f"{TASK}.run_cmd_text")
def test_ec_track_bundle_without_input(mock_run: MagicMock) -> None:
    """Create mode: no --input flag."""
    update_trusted_tasks._ec_track_bundle(
        "quay.io/org/task:0.1@sha256:abc",
        "quay.io/org/data:1234",
    )
    mock_run.assert_called_once_with(
        [
            "ec",
            "track",
            "bundle",
            "--bundle",
            "quay.io/org/task:0.1@sha256:abc",
            "--output",
            "oci:quay.io/org/data:1234",
        ]
    )


@patch(f"{TASK}.run_cmd_text")
def test_ec_track_bundle_with_input(mock_run: MagicMock) -> None:
    """Append mode: --input flag present."""
    update_trusted_tasks._ec_track_bundle(
        "quay.io/org/task:0.1@sha256:abc",
        "quay.io/org/data:1234",
        input_ref="quay.io/org/data:latest",
    )
    mock_run.assert_called_once_with(
        [
            "ec",
            "track",
            "bundle",
            "--bundle",
            "quay.io/org/task:0.1@sha256:abc",
            "--input",
            "oci:quay.io/org/data:latest",
            "--output",
            "oci:quay.io/org/data:1234",
        ]
    )


@patch(f"{TASK}.run_cmd_text")
def test_ec_track_bundle_failure(mock_run: MagicMock) -> None:
    """CalledProcessError from ec propagates."""
    mock_run.side_effect = subprocess.CalledProcessError(1, "ec")
    with pytest.raises(subprocess.CalledProcessError):
        update_trusted_tasks._ec_track_bundle(
            "quay.io/org/task:0.1@sha256:abc",
            "quay.io/org/data:1234",
        )


# ---------------------------------------------------------------------------
# _tag_as_latest
# ---------------------------------------------------------------------------


@patch(f"{TASK}.skopeo.copy")
def test_tag_as_latest(mock_copy: MagicMock) -> None:
    """Copy timestamp tag to :latest on a successful skopeo exit."""
    mock_copy.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
    update_trusted_tasks._tag_as_latest("quay.io/org/data", "9999")
    mock_copy.assert_called_once_with(
        "docker://quay.io/org/data:9999",
        "docker://quay.io/org/data:latest",
        retry_times=3,
    )


@patch(f"{TASK}.skopeo.copy")
def test_tag_as_latest_failure_logs_stderr(
    mock_copy: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Non-zero skopeo exit logs stderr and raises CalledProcessError."""
    mock_copy.return_value = subprocess.CompletedProcess(
        ["skopeo", "copy"],
        1,
        stdout="",
        stderr="unauthorized: access denied",
    )
    with caplog.at_level(logging.ERROR, logger="release"):
        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            update_trusted_tasks._tag_as_latest("quay.io/org/data", "9999")
    assert "unauthorized: access denied" in caplog.text
    assert exc_info.value.stderr == "unauthorized: access denied"


# ---------------------------------------------------------------------------
# run — integration-style tests
# ---------------------------------------------------------------------------


@patch(f"{TASK}.skopeo.copy")
@patch(f"{TASK}.run_cmd_text")
@patch(f"{TASK}.skopeo.inspect")
@patch(f"{TASK}.time")
def test_run_no_latest(
    mock_time: MagicMock,
    mock_inspect: MagicMock,
    mock_run: MagicMock,
    mock_copy: MagicMock,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """No :latest exists -- create new bundle."""
    mock_time.time.return_value = 1234567890.0
    mock_inspect.return_value = subprocess.CompletedProcess([], 1, stdout="", stderr="")
    mock_copy.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")

    snapshot = _make_snapshot([_single_component()])
    snapshot_file = tmp_path / "snapshot.json"
    _write_snapshot(snapshot_file, snapshot)

    with caplog.at_level(logging.INFO, logger="release"):
        update_trusted_tasks.run(snapshot_file=snapshot_file)

    mock_run.assert_called_once_with(
        [
            "ec",
            "track",
            "bundle",
            "--bundle",
            "quay.io/org/task-echo:0.1@sha256:abcde",
            "--output",
            "oci:quay.io/org/data-acceptable-bundles:1234567890",
        ]
    )
    mock_copy.assert_called_once()
    assert "does not exist" in caplog.text


@patch(f"{TASK}.skopeo.copy")
@patch(f"{TASK}.run_cmd_text")
@patch(f"{TASK}.skopeo.inspect")
@patch(f"{TASK}.time")
def test_run_with_latest(
    mock_time: MagicMock,
    mock_inspect: MagicMock,
    mock_run: MagicMock,
    mock_copy: MagicMock,
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """:latest exists -- append with --input."""
    mock_time.time.return_value = 1234567890.0
    mock_inspect.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")
    mock_copy.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")

    snapshot = _make_snapshot([_single_component()])
    snapshot_file = tmp_path / "snapshot.json"
    _write_snapshot(snapshot_file, snapshot)

    with caplog.at_level(logging.INFO, logger="release"):
        update_trusted_tasks.run(snapshot_file=snapshot_file)

    mock_run.assert_called_once_with(
        [
            "ec",
            "track",
            "bundle",
            "--bundle",
            "quay.io/org/task-echo:0.1@sha256:abcde",
            "--input",
            "oci:quay.io/org/data-acceptable-bundles:latest",
            "--output",
            "oci:quay.io/org/data-acceptable-bundles:1234567890",
        ]
    )
    assert "using it as an input" in caplog.text


@patch(f"{TASK}.skopeo.copy")
@patch(f"{TASK}.run_cmd_text")
@patch(f"{TASK}.skopeo.inspect")
@patch(f"{TASK}.time")
def test_run_multiple_tags(
    mock_time: MagicMock,
    mock_inspect: MagicMock,
    mock_run: MagicMock,
    mock_copy: MagicMock,
    tmp_path: Path,
) -> None:
    """First tag creates, subsequent tags use :latest as input."""
    mock_time.time.return_value = 1234567890.0
    mock_inspect.return_value = subprocess.CompletedProcess([], 1, stdout="", stderr="")
    mock_copy.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")

    component = _single_component(tags=["0.1", "0.2", "latest"])
    snapshot = _make_snapshot([component])
    snapshot_file = tmp_path / "snapshot.json"
    _write_snapshot(snapshot_file, snapshot)

    update_trusted_tasks.run(snapshot_file=snapshot_file)

    assert mock_run.call_count == 3
    first_call_args = mock_run.call_args_list[0][0][0]
    assert "--input" not in first_call_args
    for call in mock_run.call_args_list[1:]:
        assert "--input" in call[0][0]
    assert mock_copy.call_count == 3


@patch(f"{TASK}.skopeo.copy")
@patch(f"{TASK}.run_cmd_text")
@patch(f"{TASK}.skopeo.inspect")
@patch(f"{TASK}.time")
def test_run_multiple_components(
    mock_time: MagicMock,
    mock_inspect: MagicMock,
    mock_run: MagicMock,
    mock_copy: MagicMock,
    tmp_path: Path,
) -> None:
    """Multiple components with different repos are all processed."""
    mock_time.time.return_value = 1234567890.0
    mock_inspect.return_value = subprocess.CompletedProcess([], 1, stdout="", stderr="")
    mock_copy.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")

    snapshot = _make_snapshot(
        [
            _single_component(
                image="quay.io/a/img1@sha256:aaa",
                repo_url="quay.io/a/task1",
            ),
            _single_component(
                image="quay.io/b/img2@sha256:bbb",
                repo_url="quay.io/b/task2",
            ),
        ]
    )
    snapshot_file = tmp_path / "snapshot.json"
    _write_snapshot(snapshot_file, snapshot)

    update_trusted_tasks.run(snapshot_file=snapshot_file)

    assert mock_run.call_count == 2
    assert mock_inspect.call_count == 2
    assert mock_copy.call_count == 2


@patch(f"{TASK}.skopeo.copy")
@patch(f"{TASK}.run_cmd_text")
@patch(f"{TASK}.skopeo.inspect")
@patch(f"{TASK}.time")
def test_run_single_component_two_repositories(
    mock_time: MagicMock,
    mock_inspect: MagicMock,
    mock_run: MagicMock,
    mock_copy: MagicMock,
    tmp_path: Path,
) -> None:
    """One component with two repositories processes each repo independently."""
    mock_time.time.return_value = 1234567890.0
    mock_inspect.return_value = subprocess.CompletedProcess([], 1, stdout="", stderr="")
    mock_copy.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")

    snapshot = _make_snapshot(
        [
            {
                "containerImage": "quay.io/org/echo@sha256:abcde",
                "name": "echo",
                "repositories": [
                    {"url": "quay.io/org-a/task-echo", "tags": ["0.1"]},
                    {"url": "quay.io/org-b/task-echo", "tags": ["0.1"]},
                ],
            }
        ]
    )
    snapshot_file = tmp_path / "snapshot.json"
    _write_snapshot(snapshot_file, snapshot)

    update_trusted_tasks.run(snapshot_file=snapshot_file)

    assert mock_inspect.call_count == 2
    assert mock_run.call_count == 2
    assert mock_copy.call_count == 2
    outputs = [
        call[0][0][call[0][0].index("--output") + 1] for call in mock_run.call_args_list
    ]
    assert outputs == [
        "oci:quay.io/org-a/data-acceptable-bundles:1234567890",
        "oci:quay.io/org-b/data-acceptable-bundles:1234567890",
    ]


@patch(f"{TASK}.skopeo.copy")
@patch(f"{TASK}.run_cmd_text")
@patch(f"{TASK}.skopeo.inspect")
@patch(f"{TASK}.time")
def test_run_empty_components(
    mock_time: MagicMock,
    mock_inspect: MagicMock,
    mock_run: MagicMock,
    mock_copy: MagicMock,
    tmp_path: Path,
) -> None:
    """Snapshot with no components completes without calling any CLI."""
    mock_time.time.return_value = 1234567890.0
    snapshot = _make_snapshot([])
    snapshot_file = tmp_path / "snapshot.json"
    _write_snapshot(snapshot_file, snapshot)

    update_trusted_tasks.run(snapshot_file=snapshot_file)

    mock_inspect.assert_not_called()
    mock_run.assert_not_called()
    mock_copy.assert_not_called()


def test_run_missing_digest_in_container_image(tmp_path: Path) -> None:
    """ContainerImage without '@' digest raises ValueError."""
    snapshot = _make_snapshot([_single_component(image="quay.io/org/echo:latest")])
    snapshot_file = tmp_path / "snapshot.json"
    _write_snapshot(snapshot_file, snapshot)

    with pytest.raises(ValueError, match="missing digest"):
        update_trusted_tasks.run(snapshot_file=snapshot_file)


def test_run_missing_snapshot(tmp_path: Path) -> None:
    """Missing snapshot file raises FileNotFoundError."""
    missing = tmp_path / "no_such_file.json"
    with pytest.raises(FileNotFoundError):
        update_trusted_tasks.run(snapshot_file=missing)


# ---------------------------------------------------------------------------
# main — env var wiring
# ---------------------------------------------------------------------------


@patch(f"{TASK}.run")
def test_main_env_wiring(
    mock_run: MagicMock,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main() reads env vars and calls run() with the resolved path."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    snapshot = _make_snapshot([])
    snapshot_file = data_dir / "snapshot.json"
    _write_snapshot(snapshot_file, snapshot)

    monkeypatch.setenv("PARAM_DATA_DIR", str(data_dir))
    monkeypatch.setenv("PARAM_SNAPSHOT_PATH", "snapshot.json")

    result = update_trusted_tasks.main()

    assert result == 0
    mock_run.assert_called_once()
    call_kwargs = mock_run.call_args[1]
    assert call_kwargs["snapshot_file"] == snapshot_file


def test_main_missing_snapshot_path_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """main() exits 1 when PARAM_SNAPSHOT_PATH is unset."""
    monkeypatch.delenv("PARAM_SNAPSHOT_PATH", raising=False)
    monkeypatch.setenv("PARAM_DATA_DIR", "/tmp")
    with pytest.raises(SystemExit) as exc_info:
        update_trusted_tasks.main()
    assert exc_info.value.code == 1


def test_main_entrypoint(monkeypatch: pytest.MonkeyPatch) -> None:
    """Executing the file as __main__ triggers raise SystemExit(main())."""
    monkeypatch.delenv("PARAM_SNAPSHOT_PATH", raising=False)
    monkeypatch.setenv("PARAM_DATA_DIR", "/tmp")
    monkeypatch.setattr(sys, "argv", ["update_trusted_tasks.py"])
    with pytest.raises(SystemExit) as exc_info:
        runpy.run_path(
            str(Path(update_trusted_tasks.__file__)),
            run_name="__main__",
        )
    assert exc_info.value.code == 1
