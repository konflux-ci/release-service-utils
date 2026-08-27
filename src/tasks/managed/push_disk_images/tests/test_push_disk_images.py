"""Tests for push_disk_images module."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from release_service_utils.tasks.managed.push_disk_images import (
    extract_disk_image_files,
    main,
    prepare_snapshot,
    resolve_cdn_env_config,
    run,
    write_results_file,
)

TASK = "release_service_utils.tasks.managed.push_disk_images.push_disk_images"


class TestResolveCdnEnvConfig:
    """Tests for the resolve_cdn_env_config function."""

    def test_production(self) -> None:
        """Test production environment returns correct config."""
        config = resolve_cdn_env_config("production")
        assert config["exodusGwSecret"] == "exodus-prod-secret"
        assert config["exodusGwEnv"] == "live"
        assert config["pulpSecret"] == "rhsm-pulp-prod-secret"
        assert config["udcacheSecret"] == "udcache-prod-secret"
        assert "developers.redhat.com" in config["cgwHostname"]
        assert config["cgwSecret"] == "cgw-service-account-prod-secret"

    def test_stage(self) -> None:
        """Test stage environment returns correct config."""
        config = resolve_cdn_env_config("stage")
        assert config["exodusGwSecret"] == "exodus-prod-secret"
        assert config["exodusGwEnv"] == "pre"
        assert config["pulpSecret"] == "rhsm-pulp-stage-secret"
        assert config["udcacheSecret"] == "udcache-stage-secret"
        assert "developers.qa.redhat.com" in config["cgwHostname"]
        assert config["cgwSecret"] == "cgw-service-account-stage-secret"

    def test_qa(self) -> None:
        """Test qa environment returns correct config."""
        config = resolve_cdn_env_config("qa")
        assert config["exodusGwSecret"] == "exodus-stage-secret"
        assert config["exodusGwEnv"] == "live"
        assert config["pulpSecret"] == "rhsm-pulp-qa-secret"
        assert config["udcacheSecret"] == "udcache-qa-secret"
        assert "developers.qa.redhat.com" in config["cgwHostname"]
        assert config["cgwSecret"] == "cgw-service-account-stage-secret"

    def test_invalid_env_raises(self) -> None:
        """Test invalid environment raises ValueError."""
        with pytest.raises(ValueError, match="cdn.env.*must be one of"):
            resolve_cdn_env_config("invalid")

    def test_empty_env_raises(self) -> None:
        """Test empty string raises ValueError."""
        with pytest.raises(ValueError, match="cdn.env.*must be one of"):
            resolve_cdn_env_config("")

    def test_returns_copy(self) -> None:
        """Test that returned config is a copy, not the original dict."""
        config1 = resolve_cdn_env_config("production")
        config2 = resolve_cdn_env_config("production")
        config1["exodusGwSecret"] = "modified"
        assert config2["exodusGwSecret"] == "exodus-prod-secret"


class TestExtractDiskImageFiles:
    """Tests for the extract_disk_image_files function."""

    def test_extracts_filenames(self) -> None:
        """Test extracting filenames from staged files."""
        snapshot = {
            "components": [
                {
                    "name": "comp1",
                    "staged": {
                        "files": [
                            {"filename": "image-1.ami"},
                            {"filename": "image-2.qcow2"},
                        ]
                    },
                }
            ]
        }
        result = extract_disk_image_files(snapshot)
        assert result == ["image-1.ami", "image-2.qcow2"]

    def test_multiple_components(self) -> None:
        """Test extracting from multiple components."""
        snapshot = {
            "components": [
                {
                    "name": "comp1",
                    "staged": {"files": [{"filename": "a.ami"}]},
                },
                {
                    "name": "comp2",
                    "staged": {"files": [{"filename": "b.qcow2"}]},
                },
            ]
        }
        result = extract_disk_image_files(snapshot)
        assert result == ["a.ami", "b.qcow2"]

    def test_no_staged(self) -> None:
        """Test component without staged field."""
        snapshot = {"components": [{"name": "comp1"}]}
        result = extract_disk_image_files(snapshot)
        assert result == []

    def test_staged_none(self) -> None:
        """Test component with staged set to None."""
        snapshot = {"components": [{"name": "comp1", "staged": None}]}
        result = extract_disk_image_files(snapshot)
        assert result == []

    def test_no_files_in_staged(self) -> None:
        """Test staged without files key."""
        snapshot = {"components": [{"name": "comp1", "staged": {"destination": "dest"}}]}
        result = extract_disk_image_files(snapshot)
        assert result == []

    def test_empty_components(self) -> None:
        """Test empty components list."""
        snapshot = {"components": []}
        result = extract_disk_image_files(snapshot)
        assert result == []

    def test_no_components_key(self) -> None:
        """Test snapshot without components key."""
        result = extract_disk_image_files({})
        assert result == []

    def test_file_entry_without_filename(self) -> None:
        """Test file entry missing filename key is skipped."""
        snapshot = {
            "components": [
                {
                    "name": "comp1",
                    "staged": {"files": [{"source": "disk.raw"}]},
                }
            ]
        }
        result = extract_disk_image_files(snapshot)
        assert result == []

    def test_file_entry_non_dict(self) -> None:
        """Test non-dict file entry is skipped."""
        snapshot = {
            "components": [
                {
                    "name": "comp1",
                    "staged": {"files": ["not-a-dict"]},
                }
            ]
        }
        result = extract_disk_image_files(snapshot)
        assert result == []


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
                {
                    "name": "comp2",
                    "containerImage": "quay.io/test@sha256:def",
                    "metadata": {"labels": {"key": "val"}},
                },
            ],
        }
        snapshot_file = tmp_path / "snapshot.json"
        snapshot_file.write_text(json.dumps(snapshot))

        result = prepare_snapshot(snapshot_file)

        for comp in result["components"]:
            assert "metadata" not in comp
        assert result["application"] == "test-app"
        assert result["components"][0]["name"] == "comp1"

    def test_no_metadata(self, tmp_path: Path) -> None:
        """Test components without metadata are unchanged."""
        snapshot = {"components": [{"name": "comp1", "containerImage": "img@sha256:abc"}]}
        snapshot_file = tmp_path / "snapshot.json"
        snapshot_file.write_text(json.dumps(snapshot))

        result = prepare_snapshot(snapshot_file)

        assert result["components"][0]["name"] == "comp1"
        assert "metadata" not in result["components"][0]

    def test_empty_components(self, tmp_path: Path) -> None:
        """Test snapshot with empty components list."""
        snapshot_file = tmp_path / "snapshot.json"
        snapshot_file.write_text(json.dumps({"components": []}))

        result = prepare_snapshot(snapshot_file)

        assert result["components"] == []


class TestWriteResultsFile:
    """Tests for the write_results_file function."""

    def test_writes_json(self, tmp_path: Path) -> None:
        """Test results file is written with correct content."""
        results_dir = tmp_path / "results"
        write_results_file(results_dir, ["a.ami", "b.qcow2"])

        results_file = results_dir / "push-disk-images-results.json"
        assert results_file.exists()
        data = json.loads(results_file.read_text())
        assert data == {"disk-image-files": ["a.ami", "b.qcow2"]}

    def test_empty_filenames(self, tmp_path: Path) -> None:
        """Test results file with no filenames."""
        results_dir = tmp_path / "results"
        write_results_file(results_dir, [])

        results_file = results_dir / "push-disk-images-results.json"
        data = json.loads(results_file.read_text())
        assert data == {"disk-image-files": []}

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        """Test that parent directories are created."""
        results_dir = tmp_path / "deep" / "nested" / "results"
        write_results_file(results_dir, ["file.ami"])

        assert (results_dir / "push-disk-images-results.json").exists()

    def test_compact_json(self, tmp_path: Path) -> None:
        """Test output uses compact JSON format."""
        results_dir = tmp_path / "results"
        write_results_file(results_dir, ["f.ami"])

        content = (results_dir / "push-disk-images-results.json").read_text()
        assert " " not in content


class TestRun:
    """Tests for the run function."""

    def _snapshot_data(self) -> dict:
        return {
            "application": "disk-images",
            "components": [
                {
                    "name": "nvidia-disk-image",
                    "containerImage": "quay.io/test@sha256:abc",
                    "metadata": {"env_variables": {"K": "V"}},
                    "staged": {"files": [{"filename": "image.ami"}]},
                }
            ],
        }

    def _data_json(self, env: str = "stage") -> dict:
        return {
            "contentGateway": {"productName": "Test"},
            "cdn": {"env": env},
        }

    def _setup_files(self, tmp_path: Path, env: str = "stage") -> tuple[Path, str, str, str]:
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        sub = "uid123"
        (data_dir / sub).mkdir()
        (data_dir / sub / "results").mkdir()

        snapshot_file = data_dir / sub / "snapshot.json"
        snapshot_file.write_text(json.dumps(self._snapshot_data()))

        data_file = data_dir / sub / "data.json"
        data_file.write_text(json.dumps(self._data_json(env)))

        return data_dir, f"{sub}/snapshot.json", f"{sub}/data.json", sub

    @staticmethod
    def _configure_mock_ir(
        mock_ir: MagicMock,
        *,
        create_return: str = "test-ir-name",
        fetch_return: dict | None = None,
    ) -> None:
        """Set the common attributes every test needs on the mock_ir module."""
        mock_ir.SPAWN_OVERHEAD_SECONDS = 300
        mock_ir.InternalRequestWaitError = RuntimeError
        mock_ir.create.return_value = create_return
        mock_ir.fetch_results.return_value = (
            fetch_return if fetch_return is not None else {"result": "Success"}
        )

    @patch(f"{TASK}.internal_request")
    def test_run_success(self, mock_ir: MagicMock, tmp_path: Path) -> None:
        """Test successful run creates IR and checks results."""
        data_dir, snap_path, data_path, sub = self._setup_files(tmp_path)
        self._configure_mock_ir(mock_ir)

        run(
            data_dir=data_dir,
            snapshot_path=snap_path,
            data_path=data_path,
            pipeline_run_uid="uid-123",
            results_dir_path=f"{sub}/results",
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
        )

        mock_ir.create.assert_called_once()
        call_kwargs = mock_ir.create.call_args
        params = call_kwargs.kwargs["params"]
        assert params["exodusGwSecret"] == "exodus-prod-secret"
        assert params["exodusGwEnv"] == "pre"
        assert params["taskGitUrl"] == "https://github.com/test/repo"
        assert "metadata" not in json.loads(params["snapshot_json"])["components"][0]

        results_file = data_dir / sub / "results" / "push-disk-images-results.json"
        assert results_file.exists()
        results_data = json.loads(results_file.read_text())
        assert results_data["disk-image-files"] == ["image.ami"]

    @patch(f"{TASK}.internal_request")
    def test_run_production_env(self, mock_ir: MagicMock, tmp_path: Path) -> None:
        """Test run with production environment."""
        data_dir, snap_path, data_path, sub = self._setup_files(tmp_path, env="production")
        self._configure_mock_ir(mock_ir, create_return="prod-ir")

        run(
            data_dir=data_dir,
            snapshot_path=snap_path,
            data_path=data_path,
            pipeline_run_uid="uid-456",
            results_dir_path=f"{sub}/results",
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
        )

        params = mock_ir.create.call_args.kwargs["params"]
        assert params["exodusGwSecret"] == "exodus-prod-secret"
        assert params["exodusGwEnv"] == "live"
        assert params["pulpSecret"] == "rhsm-pulp-prod-secret"

    @patch(f"{TASK}.internal_request")
    def test_run_qa_env(self, mock_ir: MagicMock, tmp_path: Path) -> None:
        """Test run with qa environment."""
        data_dir, snap_path, data_path, sub = self._setup_files(tmp_path, env="qa")
        self._configure_mock_ir(mock_ir, create_return="qa-ir")

        run(
            data_dir=data_dir,
            snapshot_path=snap_path,
            data_path=data_path,
            pipeline_run_uid="uid-789",
            results_dir_path=f"{sub}/results",
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
        )

        params = mock_ir.create.call_args.kwargs["params"]
        assert params["exodusGwSecret"] == "exodus-stage-secret"
        assert params["exodusGwEnv"] == "live"
        assert params["pulpSecret"] == "rhsm-pulp-qa-secret"

    @patch(f"{TASK}.internal_request")
    def test_run_ir_failure(self, mock_ir: MagicMock, tmp_path: Path) -> None:
        """Test run raises when IR results indicate failure."""
        data_dir, snap_path, data_path, sub = self._setup_files(tmp_path)
        self._configure_mock_ir(
            mock_ir, create_return="fail-ir", fetch_return={"result": "Failure"}
        )

        with pytest.raises(RuntimeError, match="Disk image push failed"):
            run(
                data_dir=data_dir,
                snapshot_path=snap_path,
                data_path=data_path,
                pipeline_run_uid="uid-fail",
                results_dir_path=f"{sub}/results",
                task_git_url="https://github.com/test/repo",
                task_git_revision="main",
            )

    @patch(f"{TASK}.internal_request")
    def test_run_ir_wait_error(self, mock_ir: MagicMock, tmp_path: Path) -> None:
        """Test run raises RuntimeError when IR wait fails."""
        data_dir, snap_path, data_path, sub = self._setup_files(tmp_path)

        class MockWaitError(RuntimeError):
            pass

        mock_ir.SPAWN_OVERHEAD_SECONDS = 300
        mock_ir.InternalRequestWaitError = MockWaitError
        mock_ir.create.side_effect = MockWaitError("timeout")

        with pytest.raises(RuntimeError, match="timeout"):
            run(
                data_dir=data_dir,
                snapshot_path=snap_path,
                data_path=data_path,
                pipeline_run_uid="uid-timeout",
                results_dir_path=f"{sub}/results",
                task_git_url="https://github.com/test/repo",
                task_git_revision="main",
            )

    @patch(f"{TASK}.internal_request")
    def test_run_invalid_env(self, mock_ir: MagicMock, tmp_path: Path) -> None:
        """Test run raises when cdn.env is invalid."""
        data_dir, snap_path, _, sub = self._setup_files(tmp_path)
        data_file = data_dir / sub / "data.json"
        data_file.write_text(json.dumps({"cdn": {"env": "invalid"}}))

        self._configure_mock_ir(mock_ir)

        with pytest.raises(ValueError, match="cdn.env"):
            run(
                data_dir=data_dir,
                snapshot_path=snap_path,
                data_path=f"{sub}/data.json",
                pipeline_run_uid="uid-bad",
                results_dir_path=f"{sub}/results",
                task_git_url="https://github.com/test/repo",
                task_git_revision="main",
            )

    @patch(f"{TASK}.internal_request")
    def test_run_service_account(self, mock_ir: MagicMock, tmp_path: Path) -> None:
        """Test that service_account is set to release-service-account."""
        data_dir, snap_path, data_path, sub = self._setup_files(tmp_path)
        self._configure_mock_ir(mock_ir, create_return="sa-ir")

        run(
            data_dir=data_dir,
            snapshot_path=snap_path,
            data_path=data_path,
            pipeline_run_uid="uid-sa",
            results_dir_path=f"{sub}/results",
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
        )

        call_kwargs = mock_ir.create.call_args.kwargs
        assert call_kwargs["service_account"] == "release-service-account"
        assert call_kwargs["sync"] is True

    @patch(f"{TASK}.internal_request")
    def test_run_timeouts(self, mock_ir: MagicMock, tmp_path: Path) -> None:
        """Test that correct timeouts are passed to create."""
        data_dir, snap_path, data_path, sub = self._setup_files(tmp_path)
        self._configure_mock_ir(mock_ir, create_return="timeout-ir")

        run(
            data_dir=data_dir,
            snapshot_path=snap_path,
            data_path=data_path,
            pipeline_run_uid="uid-t",
            results_dir_path=f"{sub}/results",
            task_git_url="https://github.com/test/repo",
            task_git_revision="main",
        )

        call_kwargs = mock_ir.create.call_args.kwargs
        assert call_kwargs["pipeline_timeout"] == "24h0m0s"
        assert call_kwargs["task_timeout"] == "23h50m0s"
        assert call_kwargs["finally_timeout"] == "0h10m0s"
        assert call_kwargs["timeout"] == 86400 + 300


class TestMain:
    """Tests for the main function."""

    @staticmethod
    def _set_env(
        monkeypatch: pytest.MonkeyPatch,
        env: dict[str, str],
    ) -> None:
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
            "PARAM_SNAPSHOT_PATH": "snap.json",
            "PARAM_DATA_PATH": "data.json",
            "PARAM_PIPELINE_RUN_UID": "uid-main",
            "PARAM_RESULTS_DIR_PATH": "results",
            "PARAM_TASK_GIT_URL": "https://github.com/test/repo",
            "PARAM_TASK_GIT_REVISION": "main",
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
        assert call_kwargs["snapshot_path"] == "snap.json"
        assert call_kwargs["data_path"] == "data.json"
        assert call_kwargs["pipeline_run_uid"] == "uid-main"
        assert call_kwargs["task_git_url"] == ("https://github.com/test/repo")
        assert call_kwargs["task_git_revision"] == "main"
        assert call_kwargs["data_dir"] == Path(str(tmp_path / "data"))

    def test_main_missing_env_raises(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
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
        snap = data_dir / env["PARAM_SNAPSHOT_PATH"]
        snap.write_text('{"components": []}')
        data = data_dir / env["PARAM_DATA_PATH"]
        data.write_text('{"cdn": {"env": "stage"}}')
        self._set_env(monkeypatch, env)
        with (
            patch(f"{TASK}.internal_request.create", return_value="ir"),
            patch(
                f"{TASK}.internal_request.fetch_results",
                return_value={"result": "Success"},
            ),
            pytest.raises(SystemExit) as exc_info,
        ):
            import runpy

            runpy.run_module(
                "release_service_utils.tasks.managed.push_disk_images.push_disk_images",
                run_name="__main__",
            )
        assert exc_info.value.code == 0
