"""Tests for push_artifacts_to_cdn module."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.tasks.managed.push_artifacts_to_cdn import (
    extract_artifact_files,
    get_release_author,
    get_signing_key_name,
    main,
    prepare_snapshot,
    resolve_quay_url,
    run,
    write_results_file,
)

TASK = "release_service_utils.tasks.managed.push_artifacts_to_cdn.push_artifacts_to_cdn"


class TestExtractArtifactFiles:
    """Tests for the extract_artifact_files function."""

    def test_extracts_filenames(self) -> None:
        """Test extracting filenames from staged files."""
        snapshot = {
            "components": [
                {
                    "name": "comp1",
                    "staged": {
                        "files": [
                            {"filename": "artifact-1.tar.gz"},
                            {"filename": "artifact-2.zip"},
                        ]
                    },
                }
            ]
        }
        result = extract_artifact_files(snapshot)
        assert result == ["artifact-1.tar.gz", "artifact-2.zip"]

    def test_multiple_components(self) -> None:
        """Test extracting from multiple components."""
        snapshot = {
            "components": [
                {"name": "comp1", "staged": {"files": [{"filename": "a.tar.gz"}]}},
                {"name": "comp2", "staged": {"files": [{"filename": "b.zip"}]}},
            ]
        }
        result = extract_artifact_files(snapshot)
        assert result == ["a.tar.gz", "b.zip"]

    def test_no_staged(self) -> None:
        """Test component without staged field."""
        snapshot = {"components": [{"name": "comp1"}]}
        assert extract_artifact_files(snapshot) == []

    def test_staged_none(self) -> None:
        """Test component with staged set to None."""
        snapshot = {"components": [{"name": "comp1", "staged": None}]}
        assert extract_artifact_files(snapshot) == []

    def test_no_files_in_staged(self) -> None:
        """Test staged without files key."""
        snapshot = {"components": [{"name": "comp1", "staged": {"destination": "dest"}}]}
        assert extract_artifact_files(snapshot) == []

    def test_empty_components(self) -> None:
        """Test empty components list."""
        assert extract_artifact_files({"components": []}) == []

    def test_no_components_key(self) -> None:
        """Test snapshot without components key."""
        assert extract_artifact_files({}) == []

    def test_file_entry_without_filename(self) -> None:
        """Test file entry missing filename key is skipped."""
        snapshot = {
            "components": [{"name": "comp1", "staged": {"files": [{"source": "disk.raw"}]}}]
        }
        assert extract_artifact_files(snapshot) == []

    def test_file_entry_non_dict(self) -> None:
        """Test non-dict file entry is skipped."""
        snapshot = {"components": [{"name": "comp1", "staged": {"files": ["not-a-dict"]}}]}
        assert extract_artifact_files(snapshot) == []


class TestPrepareSnapshot:
    """Tests for the prepare_snapshot function."""

    def test_strips_metadata(self, tmp_path: Path) -> None:
        """Test that .metadata is removed from each component."""
        snapshot = {
            "application": "test-app",
            "components": [
                {
                    "name": "comp1",
                    "containerImage": "quay.io/test@sha256:abc",
                    "metadata": {"env_variables": {"FOO": "BAR"}},
                },
            ],
        }
        snapshot_file = tmp_path / "snapshot.json"
        snapshot_file.write_text(json.dumps(snapshot))

        result = prepare_snapshot(snapshot_file)

        assert "metadata" not in result["components"][0]
        assert result["application"] == "test-app"

    def test_no_metadata(self, tmp_path: Path) -> None:
        """Test components without metadata are unchanged."""
        snapshot = {"components": [{"name": "comp1", "containerImage": "img@sha256:abc"}]}
        snapshot_file = tmp_path / "snapshot.json"
        snapshot_file.write_text(json.dumps(snapshot))

        result = prepare_snapshot(snapshot_file)

        assert result["components"][0]["name"] == "comp1"


class TestWriteResultsFile:
    """Tests for the write_results_file function."""

    def test_writes_json(self, tmp_path: Path) -> None:
        """Test results file is written with correct content."""
        results_dir = tmp_path / "results"
        write_results_file(results_dir, ["a.tar.gz", "b.zip"])

        results_file = results_dir / "push-artifacts-results.json"
        data = json.loads(results_file.read_text())
        assert data == {"artifacts": ["a.tar.gz", "b.zip"]}

    def test_empty_filenames(self, tmp_path: Path) -> None:
        """Test results file with no filenames."""
        results_dir = tmp_path / "results"
        write_results_file(results_dir, [])

        results_file = results_dir / "push-artifacts-results.json"
        assert json.loads(results_file.read_text()) == {"artifacts": []}

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        """Test that parent directories are created."""
        results_dir = tmp_path / "deep" / "nested" / "results"
        write_results_file(results_dir, ["file.tar.gz"])
        assert (results_dir / "push-artifacts-results.json").exists()

    def test_compact_json(self, tmp_path: Path) -> None:
        """Test output uses compact JSON format."""
        results_dir = tmp_path / "results"
        write_results_file(results_dir, ["f.tar.gz"])
        content = (results_dir / "push-artifacts-results.json").read_text()
        assert " " not in content


class TestResolveQuayUrl:
    """Tests for the resolve_quay_url function."""

    def test_staging_intention(self) -> None:
        """Staging intention resolves to the nonprod Quay URL."""
        assert resolve_quay_url("staging") == "quay.io/konflux-artifacts/nonprod"

    def test_production_intention(self) -> None:
        """A production intention resolves to the prod Quay URL."""
        assert resolve_quay_url("production") == "quay.io/konflux-artifacts/prod"

    def test_empty_intention(self) -> None:
        """An empty (default) intention resolves to the prod Quay URL."""
        assert resolve_quay_url("") == "quay.io/konflux-artifacts/prod"


class TestGetReleaseAuthor:
    """Tests for the get_release_author function."""

    def test_returns_author(self) -> None:
        """Return the author when present."""
        release = {"status": {"attribution": {"author": "JohnDoe"}}}
        assert get_release_author(release) == "JohnDoe"

    def test_missing_status_raises(self) -> None:
        """Raise ValueError when status is absent."""
        with pytest.raises(ValueError, match="No author found"):
            get_release_author({})

    def test_missing_author_raises(self) -> None:
        """Raise ValueError when author is absent."""
        with pytest.raises(ValueError, match="No author found"):
            get_release_author({"status": {"attribution": {}}})

    def test_empty_author_raises(self) -> None:
        """Raise ValueError when author is an empty string."""
        with pytest.raises(ValueError, match="No author found"):
            get_release_author({"status": {"attribution": {"author": ""}}})


class TestGetSigningKeyName:
    """Tests for the get_signing_key_name function."""

    @patch(f"{TASK}.kubectl.get_configmap")
    def test_prefers_singular_key(self, mock_get_configmap: MagicMock) -> None:
        """SIG_KEY_NAME is preferred when both fields are present."""
        mock_get_configmap.return_value = {
            "data": {"SIG_KEY_NAME": "singleKey", "SIG_KEY_NAMES": "a,b"}
        }
        assert get_signing_key_name("test-config-map") == "singleKey"

    @patch(f"{TASK}.kubectl.get_configmap")
    def test_falls_back_to_plural_first_entry(self, mock_get_configmap: MagicMock) -> None:
        """The first entry of SIG_KEY_NAMES is used when SIG_KEY_NAME is absent."""
        mock_get_configmap.return_value = {"data": {"SIG_KEY_NAMES": "e2eTestKey, anotherKey"}}
        assert get_signing_key_name("test-config-map") == "e2eTestKey"

    @patch(f"{TASK}.kubectl.get_configmap")
    def test_raises_when_neither_key_present(self, mock_get_configmap: MagicMock) -> None:
        """Raise ValueError when neither SIG_KEY_NAME nor SIG_KEY_NAMES is set."""
        mock_get_configmap.return_value = {"data": {}}
        with pytest.raises(ValueError, match="No SIG_KEY_NAME or SIG_KEY_NAMES"):
            get_signing_key_name("test-config-map")

    @patch(f"{TASK}.kubectl.get_configmap")
    def test_no_data_key_raises(self, mock_get_configmap: MagicMock) -> None:
        """Raise ValueError when the configmap has no data key at all."""
        mock_get_configmap.return_value = {}
        with pytest.raises(ValueError, match="No SIG_KEY_NAME or SIG_KEY_NAMES"):
            get_signing_key_name("test-config-map")


class TestRun:
    """Tests for the run function."""

    def _snapshot_data(self) -> dict:
        return {
            "application": "artifacts",
            "components": [
                {
                    "name": "test-component",
                    "containerImage": "quay.io/test@sha256:abc",
                    "metadata": {"env_variables": {"K": "V"}},
                    "staged": {"files": [{"filename": "artifact.tar.gz"}]},
                }
            ],
        }

    def _data_json(self, env: str = "stage", intention: str = "") -> dict:
        data: dict = {
            "contentGateway": {"productName": "Test"},
            "cdn": {"env": env},
            "sign": {"configMapName": "test-config-map"},
        }
        if intention:
            data["intention"] = intention
        return data

    def _setup_files(
        self,
        tmp_path: Path,
        *,
        env: str = "stage",
        intention: str = "",
        author: str | None = "JohnDoe",
    ) -> tuple[Path, str, str, str, str]:
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        sub = "uid123"
        (data_dir / sub).mkdir()
        (data_dir / sub / "results").mkdir()

        snapshot_file = data_dir / sub / "snapshot.json"
        snapshot_file.write_text(json.dumps(self._snapshot_data()))

        data_file = data_dir / sub / "data.json"
        data_file.write_text(json.dumps(self._data_json(env, intention)))

        release: dict = {"status": {"attribution": {}}}
        if author is not None:
            release["status"]["attribution"]["author"] = author
        release_file = data_dir / sub / "release.json"
        release_file.write_text(json.dumps(release))

        return (
            data_dir,
            f"{sub}/release.json",
            f"{sub}/snapshot.json",
            f"{sub}/data.json",
            sub,
        )

    @staticmethod
    def _configure_mocks(
        mock_ir: MagicMock,
        mock_get_configmap: MagicMock,
        *,
        create_return: str = "test-ir-name",
        fetch_return: dict | None = None,
        signing_key: str = "testKey",
    ) -> None:
        """Set the common attributes every test needs on the mocked modules."""
        mock_ir.SPAWN_OVERHEAD_SECONDS = 300
        mock_ir.InternalRequestWaitError = RuntimeError
        mock_ir.create.return_value = create_return
        mock_ir.fetch_results.return_value = (
            fetch_return
            if fetch_return is not None
            else {"result": "Success", "checksum_map": "quay.io/checksum-map@sha256:abc"}
        )
        mock_get_configmap.return_value = {"data": {"SIG_KEY_NAME": signing_key}}

    @patch(f"{TASK}.kubectl.get_configmap")
    @patch(f"{TASK}.internal_request")
    def test_run_success(
        self, mock_ir: MagicMock, mock_get_configmap: MagicMock, tmp_path: Path
    ) -> None:
        """Test successful run creates IR, writes results, and writes checksum map."""
        data_dir, release_path, snap_path, data_path, sub = self._setup_files(tmp_path)
        self._configure_mocks(mock_ir, mock_get_configmap)
        checksum_map_path = data_dir / sub / "checksum_map"

        run(
            data_dir=data_dir,
            release_path=release_path,
            snapshot_path=snap_path,
            data_path=data_path,
            pipeline_run_uid="uid-123",
            results_dir_path=f"{sub}/results",
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
            checksum_map_path=checksum_map_path,
        )

        mock_ir.create.assert_called_once()
        params = mock_ir.create.call_args.kwargs["params"]
        assert params["author"] == "JohnDoe"
        assert params["signingKeyName"] == "testKey"
        assert params["exodusGwSecret"] == "exodus-prod-secret"
        assert params["exodusGwEnv"] == "pre"
        assert params["quayURL"] == "quay.io/konflux-artifacts/prod"
        assert params["taskGitUrl"] == "https://github.com/test/repo"
        assert "metadata" not in json.loads(params["snapshot_json"])["components"][0]

        results_file = data_dir / sub / "results" / "push-artifacts-results.json"
        assert json.loads(results_file.read_text())["artifacts"] == ["artifact.tar.gz"]
        assert checksum_map_path.read_text() == "quay.io/checksum-map@sha256:abc"

    @patch(f"{TASK}.kubectl.get_configmap")
    @patch(f"{TASK}.internal_request")
    def test_run_staging_intention_uses_nonprod_quay_url(
        self, mock_ir: MagicMock, mock_get_configmap: MagicMock, tmp_path: Path
    ) -> None:
        """A staging intention resolves quayURL to the nonprod path."""
        data_dir, release_path, snap_path, data_path, sub = self._setup_files(
            tmp_path, intention="staging"
        )
        self._configure_mocks(mock_ir, mock_get_configmap)

        run(
            data_dir=data_dir,
            release_path=release_path,
            snapshot_path=snap_path,
            data_path=data_path,
            pipeline_run_uid="uid-staging",
            results_dir_path=f"{sub}/results",
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
            checksum_map_path=data_dir / sub / "checksum_map",
        )

        params = mock_ir.create.call_args.kwargs["params"]
        assert params["quayURL"] == "quay.io/konflux-artifacts/nonprod"

    @patch(f"{TASK}.kubectl.get_configmap")
    @patch(f"{TASK}.internal_request")
    def test_run_production_env(
        self, mock_ir: MagicMock, mock_get_configmap: MagicMock, tmp_path: Path
    ) -> None:
        """Test run with production cdn environment."""
        data_dir, release_path, snap_path, data_path, sub = self._setup_files(
            tmp_path, env="production"
        )
        self._configure_mocks(mock_ir, mock_get_configmap, create_return="prod-ir")

        run(
            data_dir=data_dir,
            release_path=release_path,
            snapshot_path=snap_path,
            data_path=data_path,
            pipeline_run_uid="uid-456",
            results_dir_path=f"{sub}/results",
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
            checksum_map_path=data_dir / sub / "checksum_map",
        )

        params = mock_ir.create.call_args.kwargs["params"]
        assert params["exodusGwSecret"] == "exodus-prod-secret"
        assert params["exodusGwEnv"] == "live"
        assert params["pulpSecret"] == "rhsm-pulp-prod-secret"

    @patch(f"{TASK}.kubectl.get_configmap")
    @patch(f"{TASK}.internal_request")
    def test_run_qa_env(
        self, mock_ir: MagicMock, mock_get_configmap: MagicMock, tmp_path: Path
    ) -> None:
        """Test run with qa cdn environment."""
        data_dir, release_path, snap_path, data_path, sub = self._setup_files(
            tmp_path, env="qa"
        )
        self._configure_mocks(mock_ir, mock_get_configmap, create_return="qa-ir")

        run(
            data_dir=data_dir,
            release_path=release_path,
            snapshot_path=snap_path,
            data_path=data_path,
            pipeline_run_uid="uid-789",
            results_dir_path=f"{sub}/results",
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
            checksum_map_path=data_dir / sub / "checksum_map",
        )

        params = mock_ir.create.call_args.kwargs["params"]
        assert params["exodusGwSecret"] == "exodus-stage-secret"
        assert params["exodusGwEnv"] == "live"
        assert params["pulpSecret"] == "rhsm-pulp-qa-secret"

    @patch(f"{TASK}.kubectl.get_configmap")
    @patch(f"{TASK}.internal_request")
    def test_run_signing_key_names_fallback(
        self, mock_ir: MagicMock, mock_get_configmap: MagicMock, tmp_path: Path
    ) -> None:
        """The first entry of a plural SIG_KEY_NAMES list is used as the signing key."""
        data_dir, release_path, snap_path, data_path, sub = self._setup_files(tmp_path)
        self._configure_mocks(mock_ir, mock_get_configmap)
        mock_get_configmap.return_value = {"data": {"SIG_KEY_NAMES": "e2eTestKey, anotherKey"}}

        run(
            data_dir=data_dir,
            release_path=release_path,
            snapshot_path=snap_path,
            data_path=data_path,
            pipeline_run_uid="uid-fallback",
            results_dir_path=f"{sub}/results",
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
            checksum_map_path=data_dir / sub / "checksum_map",
        )

        params = mock_ir.create.call_args.kwargs["params"]
        assert params["signingKeyName"] == "e2eTestKey"

    @patch(f"{TASK}.kubectl.get_configmap")
    @patch(f"{TASK}.internal_request")
    def test_run_ir_failure_writes_checksum_map_and_raises(
        self, mock_ir: MagicMock, mock_get_configmap: MagicMock, tmp_path: Path
    ) -> None:
        """A failed InternalRequest result still writes checksum_map, then raises."""
        data_dir, release_path, snap_path, data_path, sub = self._setup_files(tmp_path)
        self._configure_mocks(
            mock_ir,
            mock_get_configmap,
            create_return="fail-ir",
            fetch_return={"result": "Failure", "checksum_map": "partial-ref"},
        )
        checksum_map_path = data_dir / sub / "checksum_map"

        with pytest.raises(RuntimeError, match="Artifact push failed"):
            run(
                data_dir=data_dir,
                release_path=release_path,
                snapshot_path=snap_path,
                data_path=data_path,
                pipeline_run_uid="uid-fail",
                results_dir_path=f"{sub}/results",
                task_git_url="https://github.com/test/repo",
                task_git_revision="main",
                checksum_map_path=checksum_map_path,
            )

        assert checksum_map_path.read_text() == "partial-ref"

    @patch(f"{TASK}.kubectl.get_configmap")
    @patch(f"{TASK}.internal_request")
    def test_run_ir_wait_error(
        self, mock_ir: MagicMock, mock_get_configmap: MagicMock, tmp_path: Path
    ) -> None:
        """Test run raises RuntimeError when the IR wait fails."""
        data_dir, release_path, snap_path, data_path, sub = self._setup_files(tmp_path)

        class MockWaitError(RuntimeError):
            pass

        mock_ir.SPAWN_OVERHEAD_SECONDS = 300
        mock_ir.InternalRequestWaitError = MockWaitError
        mock_ir.create.side_effect = MockWaitError("timeout")
        mock_get_configmap.return_value = {"data": {"SIG_KEY_NAME": "testKey"}}

        with pytest.raises(RuntimeError, match="timeout"):
            run(
                data_dir=data_dir,
                release_path=release_path,
                snapshot_path=snap_path,
                data_path=data_path,
                pipeline_run_uid="uid-timeout",
                results_dir_path=f"{sub}/results",
                task_git_url="https://github.com/test/repo",
                task_git_revision="main",
                checksum_map_path=data_dir / sub / "checksum_map",
            )

    @patch(f"{TASK}.kubectl.get_configmap")
    @patch(f"{TASK}.internal_request")
    def test_run_invalid_cdn_env(
        self, mock_ir: MagicMock, mock_get_configmap: MagicMock, tmp_path: Path
    ) -> None:
        """Test run raises when cdn.env is invalid."""
        data_dir, release_path, snap_path, _, sub = self._setup_files(tmp_path)
        data_file = data_dir / sub / "data.json"
        data_file.write_text(
            json.dumps({"cdn": {"env": "invalid"}, "sign": {"configMapName": "cm"}})
        )
        self._configure_mocks(mock_ir, mock_get_configmap)

        with pytest.raises(ValueError, match="cdn.env"):
            run(
                data_dir=data_dir,
                release_path=release_path,
                snapshot_path=snap_path,
                data_path=f"{sub}/data.json",
                pipeline_run_uid="uid-bad",
                results_dir_path=f"{sub}/results",
                task_git_url="https://github.com/test/repo",
                task_git_revision="main",
                checksum_map_path=data_dir / sub / "checksum_map",
            )

    @patch(f"{TASK}.kubectl.get_configmap")
    @patch(f"{TASK}.internal_request")
    def test_run_no_author_raises(
        self, mock_ir: MagicMock, mock_get_configmap: MagicMock, tmp_path: Path
    ) -> None:
        """Test run raises when the release has no author."""
        data_dir, release_path, snap_path, data_path, sub = self._setup_files(
            tmp_path, author=None
        )
        self._configure_mocks(mock_ir, mock_get_configmap)

        with pytest.raises(ValueError, match="No author found"):
            run(
                data_dir=data_dir,
                release_path=release_path,
                snapshot_path=snap_path,
                data_path=data_path,
                pipeline_run_uid="uid-no-author",
                results_dir_path=f"{sub}/results",
                task_git_url="https://github.com/test/repo",
                task_git_revision="main",
                checksum_map_path=data_dir / sub / "checksum_map",
            )

    @patch(f"{TASK}.kubectl.get_configmap")
    @patch(f"{TASK}.internal_request")
    def test_run_missing_snapshot_raises(
        self, mock_ir: MagicMock, mock_get_configmap: MagicMock, tmp_path: Path
    ) -> None:
        """Test run raises FileNotFoundError when the snapshot file is missing."""
        data_dir, release_path, _, data_path, sub = self._setup_files(tmp_path)
        self._configure_mocks(mock_ir, mock_get_configmap)

        with pytest.raises(FileNotFoundError):
            run(
                data_dir=data_dir,
                release_path=release_path,
                snapshot_path=f"{sub}/missing_snapshot.json",
                data_path=data_path,
                pipeline_run_uid="uid-no-snapshot",
                results_dir_path=f"{sub}/results",
                task_git_url="https://github.com/test/repo",
                task_git_revision="main",
                checksum_map_path=data_dir / sub / "checksum_map",
            )

    @patch(f"{TASK}.kubectl.get_configmap")
    @patch(f"{TASK}.internal_request")
    def test_run_service_account_and_timeouts(
        self, mock_ir: MagicMock, mock_get_configmap: MagicMock, tmp_path: Path
    ) -> None:
        """Test that service_account, sync, and timeouts are set correctly."""
        data_dir, release_path, snap_path, data_path, sub = self._setup_files(tmp_path)
        self._configure_mocks(mock_ir, mock_get_configmap, create_return="sa-ir")

        run(
            data_dir=data_dir,
            release_path=release_path,
            snapshot_path=snap_path,
            data_path=data_path,
            pipeline_run_uid="uid-sa",
            results_dir_path=f"{sub}/results",
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
            checksum_map_path=data_dir / sub / "checksum_map",
        )

        call_kwargs = mock_ir.create.call_args.kwargs
        assert call_kwargs["service_account"] == "release-service-account"
        assert call_kwargs["sync"] is True
        assert call_kwargs["pipeline_timeout"] == "24h0m0s"
        assert call_kwargs["task_timeout"] == "23h50m0s"
        assert call_kwargs["finally_timeout"] == "0h10m0s"
        assert call_kwargs["timeout"] == 86400 + 300


class TestMain:
    """Tests for the main function."""

    @staticmethod
    def _set_env(monkeypatch: pytest.MonkeyPatch, env: dict[str, str]) -> None:
        """Clear all env vars and set *env* via monkeypatch."""
        for key in list(os.environ):
            monkeypatch.delenv(key)
        for key, value in env.items():
            monkeypatch.setenv(key, value)

    @staticmethod
    def _env_vars(tmp_path: Path) -> dict[str, str]:
        data_dir = tmp_path / "data"
        data_dir.mkdir(exist_ok=True)
        return {
            "PARAM_DATA_DIR": str(data_dir),
            "PARAM_RELEASE_PATH": "release.json",
            "PARAM_SNAPSHOT_PATH": "snap.json",
            "PARAM_DATA_PATH": "data.json",
            "PARAM_PIPELINE_RUN_UID": "uid-main",
            "PARAM_RESULTS_DIR_PATH": "results",
            "PARAM_TASK_GIT_URL": "https://github.com/test/repo",
            "PARAM_TASK_GIT_REVISION": "main",
            "RESULT_CHECKSUM_MAP": str(tmp_path / "checksum_map"),
        }

    @patch(f"{TASK}.run")
    def test_main_calls_run(
        self,
        mock_run: MagicMock,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Test main reads env vars and calls run."""
        self._set_env(monkeypatch, self._env_vars(tmp_path))
        result = main()
        assert result == 0
        mock_run.assert_called_once()
        call_kwargs = mock_run.call_args.kwargs
        assert call_kwargs["release_path"] == "release.json"
        assert call_kwargs["snapshot_path"] == "snap.json"
        assert call_kwargs["data_path"] == "data.json"
        assert call_kwargs["pipeline_run_uid"] == "uid-main"
        assert call_kwargs["task_git_url"] == "https://github.com/test/repo"
        assert call_kwargs["task_git_revision"] == "main"
        assert call_kwargs["data_dir"] == Path(str(tmp_path / "data"))
        assert call_kwargs["checksum_map_path"] == Path(str(tmp_path / "checksum_map"))

    def test_main_missing_env_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test main raises when required env vars are missing."""
        self._set_env(monkeypatch, {})
        with pytest.raises(SystemExit):
            main()

    def test_dunder_main_block(
        self,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Exercise the ``if __name__ == "__main__"`` block."""
        env = self._env_vars(tmp_path)
        data_dir = Path(env["PARAM_DATA_DIR"])
        (data_dir / env["PARAM_SNAPSHOT_PATH"]).write_text('{"components": []}')
        (data_dir / env["PARAM_DATA_PATH"]).write_text(
            json.dumps({"cdn": {"env": "stage"}, "sign": {"configMapName": "cm"}})
        )
        (data_dir / env["PARAM_RELEASE_PATH"]).write_text(
            json.dumps({"status": {"attribution": {"author": "JohnDoe"}}})
        )
        self._set_env(monkeypatch, env)
        with (
            patch(
                f"{TASK}.kubectl.get_configmap", return_value={"data": {"SIG_KEY_NAME": "k"}}
            ),
            patch(f"{TASK}.internal_request.create", return_value="ir"),
            patch(
                f"{TASK}.internal_request.fetch_results",
                return_value={"result": "Success", "checksum_map": "ref"},
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            import runpy

            runpy.run_module(TASK, run_name="__main__")
        assert exc_info.value.code == 0
