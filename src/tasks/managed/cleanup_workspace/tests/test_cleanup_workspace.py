"""Test cleanup of workspace directories and InternalRequest CRs."""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import cleanup_workspace
import pytest


class TestCleanupDirectory:
    """Test directory removal logic."""

    def test_removes_existing_directory(self, tmp_path: Path) -> None:
        """Existing subdirectory is removed."""
        target = tmp_path / "subdir"
        target.mkdir()
        (target / "file.txt").write_text("content", encoding="utf-8")

        cleanup_workspace.cleanup_directory(str(tmp_path), "subdir")

        assert not target.exists()

    def test_noop_when_directory_does_not_exist(self, tmp_path: Path) -> None:
        """No error when the subdirectory does not exist."""
        cleanup_workspace.cleanup_directory(str(tmp_path), "nonexistent")

    def test_preserves_sibling_directories(self, tmp_path: Path) -> None:
        """Only the target subdirectory is removed."""
        target = tmp_path / "remove-me"
        target.mkdir()
        sibling = tmp_path / "keep-me"
        sibling.mkdir()
        (sibling / "important.txt").write_text("keep", encoding="utf-8")

        cleanup_workspace.cleanup_directory(str(tmp_path), "remove-me")

        assert not target.exists()
        assert sibling.exists()
        assert (sibling / "important.txt").read_text(encoding="utf-8") == "keep"


class TestRun:
    """Test the run() orchestration."""

    def test_full_flow(self, tmp_path: Path) -> None:
        """Directory is removed after IR cleanup and delay."""
        target = tmp_path / "cleanup-dir"
        target.mkdir()
        (target / "file.txt").write_text("delete me", encoding="utf-8")

        with (
            mock.patch(
                "cleanup_workspace.cleanup_workspace.cleanup_internal_requests.run"
            ) as mock_ir,
            mock.patch("cleanup_workspace.cleanup_workspace.time.sleep") as mock_sleep,
        ):
            cleanup_workspace.run("cleanup-dir", 5, "uid-123", str(tmp_path))

        mock_ir.assert_called_once_with("uid-123")
        mock_sleep.assert_called_once_with(5)
        assert not target.exists()

    def test_empty_subdirectory_skips_after_ir_cleanup(self) -> None:
        """Empty subdirectory string skips delay and directory removal."""
        with (
            mock.patch(
                "cleanup_workspace.cleanup_workspace.cleanup_internal_requests.run"
            ) as mock_ir,
            mock.patch("cleanup_workspace.cleanup_workspace.time.sleep") as mock_sleep,
        ):
            cleanup_workspace.run("", 60, "uid-456", "/fake/path")

        mock_ir.assert_called_once_with("uid-456")
        mock_sleep.assert_not_called()

    def test_delay_is_applied(self, tmp_path: Path) -> None:
        """Delay value is passed to time.sleep."""
        with (
            mock.patch("cleanup_workspace.cleanup_workspace.cleanup_internal_requests.run"),
            mock.patch("cleanup_workspace.cleanup_workspace.time.sleep") as mock_sleep,
        ):
            cleanup_workspace.run("somedir", 42, "", str(tmp_path))

        mock_sleep.assert_called_once_with(42)

    def test_ir_cleanup_called_with_uid(self, tmp_path: Path) -> None:
        """IR cleanup receives the pipeline run UID."""
        with (
            mock.patch(
                "cleanup_workspace.cleanup_workspace.cleanup_internal_requests.run"
            ) as mock_ir,
            mock.patch("cleanup_workspace.cleanup_workspace.time.sleep"),
        ):
            cleanup_workspace.run("dir", 0, "my-uid", str(tmp_path))

        mock_ir.assert_called_once_with("my-uid")

    def test_ir_cleanup_called_with_empty_uid(self, tmp_path: Path) -> None:
        """IR cleanup is called even with empty UID (it handles the no-op)."""
        with (
            mock.patch(
                "cleanup_workspace.cleanup_workspace.cleanup_internal_requests.run"
            ) as mock_ir,
            mock.patch("cleanup_workspace.cleanup_workspace.time.sleep"),
        ):
            cleanup_workspace.run("dir", 0, "", str(tmp_path))

        mock_ir.assert_called_once_with("")

    def test_ir_cleanup_error_propagates(self) -> None:
        """RuntimeError from IR cleanup propagates to the caller."""
        with mock.patch(
            "cleanup_workspace.cleanup_workspace.cleanup_internal_requests.run",
            side_effect=RuntimeError("kubectl failed"),
        ):
            with pytest.raises(RuntimeError, match="kubectl failed"):
                cleanup_workspace.run("dir", 0, "uid", "/fake")


class TestParseArgs:
    """Test argument parsing."""

    def test_all_args(self) -> None:
        """All arguments are parsed correctly."""
        args = cleanup_workspace.cleanup_workspace._parse_args(
            [
                "--subdirectory",
                "mydir",
                "--delay",
                "30",
                "--pipeline-run-uid",
                "uid-789",
                "--workspace-path",
                "/workspace",
            ]
        )
        assert args.subdirectory == "mydir"
        assert args.delay == 30
        assert args.pipeline_run_uid == "uid-789"
        assert args.workspace_path == "/workspace"

    def test_defaults(self) -> None:
        """Default values for optional arguments."""
        args = cleanup_workspace.cleanup_workspace._parse_args(
            ["--workspace-path", "/workspace"]
        )
        assert args.subdirectory == ""
        assert args.delay == 60
        assert args.pipeline_run_uid == ""

    def test_workspace_path_required(self) -> None:
        """Missing --workspace-path causes a SystemExit."""
        with pytest.raises(SystemExit):
            cleanup_workspace.cleanup_workspace._parse_args([])


class TestMain:
    """Test the CLI entry point."""

    def test_success(self) -> None:
        """Return 0 on successful run."""
        with mock.patch("cleanup_workspace.cleanup_workspace.run") as mock_run:
            result = cleanup_workspace.main(
                [
                    "--subdirectory",
                    "dir",
                    "--delay",
                    "0",
                    "--pipeline-run-uid",
                    "uid",
                    "--workspace-path",
                    "/ws",
                ]
            )
        assert result == 0
        mock_run.assert_called_once_with("dir", 0, "uid", "/ws")

    def test_default_values(self) -> None:
        """Default values are forwarded correctly."""
        with mock.patch("cleanup_workspace.cleanup_workspace.run") as mock_run:
            result = cleanup_workspace.main(["--workspace-path", "/ws"])
        assert result == 0
        mock_run.assert_called_once_with("", 60, "", "/ws")

    def test_runtime_error_propagates(self) -> None:
        """RuntimeError from run() propagates as unhandled exception."""
        with mock.patch(
            "cleanup_workspace.cleanup_workspace.run",
            side_effect=RuntimeError("boom"),
        ):
            with pytest.raises(RuntimeError, match="boom"):
                cleanup_workspace.main(["--workspace-path", "/ws"])
