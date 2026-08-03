"""Tests for ``push_artifacts_to_storage``."""

from __future__ import annotations

import json
import logging
import subprocess
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import push_artifacts_to_storage
import pytest
import tekton


@pytest.fixture
def release_log_caplog(
    caplog: pytest.LogCaptureFixture,
) -> pytest.LogCaptureFixture:
    """Allow caplog to capture records from the ``release`` logger."""
    release_logger = logging.getLogger("release")
    release_logger.propagate = True
    yield caplog
    release_logger.propagate = False


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")


def _make_snapshot(components: list[dict[str, str]]) -> dict[str, Any]:
    return {"components": components, "componentGroup": "test-group"}


def _make_data(koji_import_draft: bool | None = None) -> dict[str, Any]:
    data: dict[str, Any] = {}
    if koji_import_draft is not None:
        data["pushOptions"] = {"koji_import_draft": koji_import_draft}
    return data


def _setup_files(
    tmp_path: Path,
    *,
    snapshot: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
    create_cli_toml: bool = False,
) -> tuple[Path, Path, Path, Path]:
    """Create data_dir, rok_access_path, results_dir and write fixture files."""
    data_dir = tmp_path / "release"
    rok_access_path = tmp_path / "rok-access"
    results_dir = tmp_path / "results"
    rok_access_path.mkdir(parents=True)

    if snapshot is not None:
        _write_json(data_dir / "snapshot.json", snapshot)
    if data is not None:
        _write_json(data_dir / "data.json", data)
    if create_cli_toml:
        (rok_access_path / "cli.toml").write_text("[config]", encoding="utf-8")

    return data_dir, rok_access_path, results_dir, data_dir


@patch("push_artifacts_to_storage.run_cmd")
@patch("push_artifacts_to_storage.oras_utils.oras_pull")
def test_happy_path(
    mock_oras_pull: MagicMock, mock_run_cmd: MagicMock, tmp_path: Path
) -> None:
    """Pull all components, push to storage when cli.toml exists and draft is true."""
    snapshot = _make_snapshot(
        [
            {"containerImage": "quay.io/app1@sha256:aaa"},
            {"containerImage": "quay.io/app2@sha256:bbb"},
        ]
    )
    data = _make_data(True)
    data_dir, rok_access_path, results_dir, _ = _setup_files(
        tmp_path, snapshot=snapshot, data=data, create_cli_toml=True
    )

    def fake_oras_pull(pull_spec: str, download_dir: Path, **kw: object) -> None:
        del kw
        download_dir.mkdir(parents=True, exist_ok=True)

    mock_oras_pull.side_effect = fake_oras_pull
    mock_run_cmd.return_value = subprocess.CompletedProcess([], 0, stdout="", stderr="")

    push_artifacts_to_storage.run(
        data_dir=data_dir,
        snapshot_path="snapshot.json",
        data_path="data.json",
        snapshot_build_id="build-123",
        snapshot_namespace="test-ns",
        rok_access_path=rok_access_path,
        results_dir=results_dir,
    )

    assert mock_oras_pull.call_count == 2
    mock_oras_pull.assert_any_call("quay.io/app1@sha256:aaa", results_dir)
    mock_oras_pull.assert_any_call("quay.io/app2@sha256:bbb", results_dir)
    mock_run_cmd.assert_called_once_with(
        [
            "pulp-tool",
            "--config",
            str(rok_access_path / "cli.toml"),
            "--build-id",
            "build-123",
            "--namespace",
            "test-ns",
            "upload",
            "--rpm-path",
            str(results_dir),
        ]
    )


@patch("push_artifacts_to_storage.run_cmd")
@patch("push_artifacts_to_storage.oras_utils.oras_pull")
def test_missing_cli_toml_skips_push(
    mock_oras_pull: MagicMock,
    mock_run_cmd: MagicMock,
    tmp_path: Path,
    release_log_caplog: pytest.LogCaptureFixture,
) -> None:
    """Pull still happens but push is skipped when cli.toml is absent."""
    snapshot = _make_snapshot(
        [
            {"containerImage": "quay.io/app@sha256:aaa"},
        ]
    )
    data = _make_data(True)
    data_dir, rok_access_path, results_dir, _ = _setup_files(
        tmp_path, snapshot=snapshot, data=data, create_cli_toml=False
    )

    def fake_oras_pull(pull_spec: str, download_dir: Path, **kw: object) -> None:
        del kw
        download_dir.mkdir(parents=True, exist_ok=True)

    mock_oras_pull.side_effect = fake_oras_pull

    with release_log_caplog.at_level(logging.INFO, logger="release"):
        push_artifacts_to_storage.run(
            data_dir=data_dir,
            snapshot_path="snapshot.json",
            data_path="data.json",
            snapshot_build_id="build-123",
            snapshot_namespace="test-ns",
            rok_access_path=rok_access_path,
            results_dir=results_dir,
        )

    mock_oras_pull.assert_called_once()
    mock_run_cmd.assert_not_called()
    assert "skipping" in release_log_caplog.text.lower()


@patch("push_artifacts_to_storage.run_cmd")
@patch("push_artifacts_to_storage.oras_utils.oras_pull")
def test_koji_import_draft_false_skips_push(
    mock_oras_pull: MagicMock, mock_run_cmd: MagicMock, tmp_path: Path
) -> None:
    """Push is skipped when koji_import_draft is explicitly 'false'."""
    snapshot = _make_snapshot(
        [
            {"containerImage": "quay.io/app@sha256:aaa"},
        ]
    )
    data = _make_data(False)
    data_dir, rok_access_path, results_dir, _ = _setup_files(
        tmp_path, snapshot=snapshot, data=data, create_cli_toml=True
    )

    def fake_oras_pull(pull_spec: str, download_dir: Path, **kw: object) -> None:
        del pull_spec, kw
        download_dir.mkdir(parents=True, exist_ok=True)

    mock_oras_pull.side_effect = fake_oras_pull

    push_artifacts_to_storage.run(
        data_dir=data_dir,
        snapshot_path="snapshot.json",
        data_path="data.json",
        snapshot_build_id="build-123",
        snapshot_namespace="test-ns",
        rok_access_path=rok_access_path,
        results_dir=results_dir,
    )

    mock_run_cmd.assert_not_called()


@patch("push_artifacts_to_storage.run_cmd")
@patch("push_artifacts_to_storage.oras_utils.oras_pull")
def test_koji_import_draft_missing_skips_push(
    mock_oras_pull: MagicMock, mock_run_cmd: MagicMock, tmp_path: Path
) -> None:
    """Push is skipped when koji_import_draft key is absent (defaults to 'false')."""
    snapshot = _make_snapshot(
        [
            {"containerImage": "quay.io/app@sha256:aaa"},
        ]
    )
    data: dict[str, Any] = {}
    data_dir, rok_access_path, results_dir, _ = _setup_files(
        tmp_path, snapshot=snapshot, data=data, create_cli_toml=True
    )

    def fake_oras_pull(pull_spec: str, download_dir: Path, **kw: object) -> None:
        del pull_spec, kw
        download_dir.mkdir(parents=True, exist_ok=True)

    mock_oras_pull.side_effect = fake_oras_pull

    push_artifacts_to_storage.run(
        data_dir=data_dir,
        snapshot_path="snapshot.json",
        data_path="data.json",
        snapshot_build_id="build-123",
        snapshot_namespace="test-ns",
        rok_access_path=rok_access_path,
        results_dir=results_dir,
    )

    mock_run_cmd.assert_not_called()


def test_missing_snapshot_file(tmp_path: Path) -> None:
    """FileNotFoundError raised when snapshot file does not exist."""
    data_dir, rok_access_path, results_dir, _ = _setup_files(
        tmp_path, data={"pushOptions": {}}, create_cli_toml=True
    )

    with pytest.raises(tekton.CheckStepError, match="snapshot"):
        push_artifacts_to_storage.run(
            data_dir=data_dir,
            snapshot_path="snapshot.json",
            data_path="data.json",
            snapshot_build_id="build-123",
            snapshot_namespace="test-ns",
            rok_access_path=rok_access_path,
            results_dir=results_dir,
        )


def test_missing_data_file(tmp_path: Path) -> None:
    """FileNotFoundError raised when data file does not exist."""
    snapshot = _make_snapshot([{"containerImage": "quay.io/app@sha256:aaa"}])
    data_dir, rok_access_path, results_dir, _ = _setup_files(
        tmp_path, snapshot=snapshot, create_cli_toml=True
    )

    with pytest.raises(tekton.CheckStepError, match="data"):
        push_artifacts_to_storage.run(
            data_dir=data_dir,
            snapshot_path="snapshot.json",
            data_path="data.json",
            snapshot_build_id="build-123",
            snapshot_namespace="test-ns",
            rok_access_path=rok_access_path,
            results_dir=results_dir,
        )
