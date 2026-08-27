"""Test OCP version collection and validation across FBC fragment components."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest
from release_service_utils.tasks.managed.get_ocp_version import get_ocp_version

TASK = "release_service_utils.tasks.managed.get_ocp_version.get_ocp_version"


def _snapshot(components: list[dict]) -> dict:
    """Build a minimal snapshot dict."""
    return {"application": "test", "components": components}


def _component(image: str) -> dict:
    """Build a component dict with a containerImage."""
    return {"name": "comp", "containerImage": image}


class TestValidateOcpVersions:
    """Test validate_ocp_versions across multiple components."""

    def test_single_component_happy_path(self) -> None:
        """A single valid component returns its version."""
        snapshot = _snapshot([_component("quay.io/fbc/test-fbc-component@sha256:manifest")])
        with patch(f"{TASK}.resolve_ocp_version", return_value="v4.12"):
            assert get_ocp_version.validate_ocp_versions(snapshot) == "v4.12"

    def test_matching_versions_across_components(self) -> None:
        """Multiple components reporting the same version validate successfully."""
        snapshot = _snapshot(
            [
                _component("quay.io/fbc/comp-a@sha256:a"),
                _component("quay.io/fbc/comp-b@sha256:b"),
            ]
        )
        with patch(f"{TASK}.resolve_ocp_version", return_value="v4.14"):
            assert get_ocp_version.validate_ocp_versions(snapshot) == "v4.14"

    def test_version_mismatch_raises(self) -> None:
        """A version mismatch between components raises ValueError."""
        snapshot = _snapshot(
            [
                _component("quay.io/fbc/comp-a@sha256:a"),
                _component("quay.io/fbc/comp-b@sha256:b"),
            ]
        )
        with patch(
            f"{TASK}.resolve_ocp_version",
            side_effect=["v4.12", "v4.13"],
        ):
            with pytest.raises(ValueError, match="OCP version mismatch"):
                get_ocp_version.validate_ocp_versions(snapshot)

    def test_invalid_version_format_raises(self) -> None:
        """A malformed version string raises ValueError."""
        snapshot = _snapshot([_component("quay.io/fbc/comp-a@sha256:a")])
        with patch(f"{TASK}.resolve_ocp_version", return_value="not-a-version"):
            with pytest.raises(ValueError, match="Invalid OCP version format"):
                get_ocp_version.validate_ocp_versions(snapshot)

    def test_empty_components_raises(self) -> None:
        """An empty components list raises ValueError."""
        with pytest.raises(ValueError, match="No components found"):
            get_ocp_version.validate_ocp_versions(_snapshot([]))

    def test_missing_components_key_raises(self) -> None:
        """A snapshot without a components key raises ValueError."""
        with pytest.raises(ValueError, match="No components found"):
            get_ocp_version.validate_ocp_versions({"application": "test"})

    def test_missing_container_image_raises(self) -> None:
        """A component without a containerImage raises KeyError."""
        snapshot = _snapshot([{"name": "comp"}])
        with pytest.raises(KeyError, match="containerImage"):
            get_ocp_version.validate_ocp_versions(snapshot)


class TestRun:
    """Test the run() orchestration: file I/O plus validation."""

    def test_writes_version_to_result_file(self, tmp_path: Path) -> None:
        """run() writes the validated version to the result path, no trailing newline."""
        snapshot_path = tmp_path / "snapshot.json"
        snapshot_path.write_text(
            json.dumps(_snapshot([_component("quay.io/fbc/comp-a@sha256:a")])),
            encoding="utf-8",
        )
        result_path = tmp_path / "stored-version"
        with patch(f"{TASK}.validate_ocp_versions", return_value="v4.12"):
            get_ocp_version.run(snapshot_path, result_path)
        assert result_path.read_text(encoding="utf-8") == "v4.12"

    def test_missing_snapshot_raises(self, tmp_path: Path) -> None:
        """A missing snapshot file raises naturally (FileNotFoundError)."""
        with pytest.raises(FileNotFoundError):
            get_ocp_version.run(tmp_path / "missing.json", tmp_path / "stored-version")

    def test_validation_error_propagates(self, tmp_path: Path) -> None:
        """A ValueError from validate_ocp_versions propagates out of run()."""
        snapshot_path = tmp_path / "snapshot.json"
        snapshot_path.write_text(json.dumps(_snapshot([])), encoding="utf-8")
        with pytest.raises(ValueError, match="No components found"):
            get_ocp_version.run(snapshot_path, tmp_path / "stored-version")


class TestParseArgs:
    """Test CLI argument parsing."""

    def test_snapshot_path_required(self) -> None:
        """--snapshot-path is required."""
        with pytest.raises(SystemExit):
            get_ocp_version._parse_args([])

    def test_parses_snapshot_path(self) -> None:
        """--snapshot-path is correctly parsed."""
        args = get_ocp_version._parse_args(["--snapshot-path", "/data/snapshot.json"])
        assert args.snapshot_path == "/data/snapshot.json"


class TestMain:
    """Test the main() entry point."""

    def test_success(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        """Return 0 on successful run."""
        snapshot_path = tmp_path / "snapshot.json"
        snapshot_path.write_text(json.dumps(_snapshot([])), encoding="utf-8")
        result_path = tmp_path / "stored-version"
        monkeypatch.setenv("RESULT_STORED_VERSION", str(result_path))
        with patch(f"{TASK}.run") as mock_run:
            assert get_ocp_version.main(["--snapshot-path", str(snapshot_path)]) == 0
        mock_run.assert_called_once_with(snapshot_path, result_path)

    def test_missing_env_var_exits(self) -> None:
        """SystemExit when RESULT_STORED_VERSION is not set."""
        with pytest.raises(SystemExit):
            get_ocp_version.main(["--snapshot-path", "/tmp/snapshot.json"])

    def test_value_error_propagates(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """ValueError from run() propagates out of main()."""
        result_path = tmp_path / "stored-version"
        monkeypatch.setenv("RESULT_STORED_VERSION", str(result_path))
        with patch(
            f"{TASK}.run",
            side_effect=ValueError("No components found in snapshot"),
        ):
            with pytest.raises(ValueError, match="No components found"):
                get_ocp_version.main(["--snapshot-path", "/tmp/snapshot.json"])
