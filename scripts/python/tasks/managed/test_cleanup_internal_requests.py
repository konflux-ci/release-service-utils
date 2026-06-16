"""Test cleanup of InternalRequest CRs."""

from __future__ import annotations

import subprocess
from unittest import mock
from unittest.mock import MagicMock

import cleanup_internal_requests
import pytest


def _completed(
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
) -> MagicMock:
    """Build a mock ``subprocess.CompletedProcess``."""
    proc = MagicMock(spec=subprocess.CompletedProcess)
    proc.returncode = returncode
    proc.stdout = stdout
    proc.stderr = stderr
    return proc


class TestCleanup:
    """Test kubectl delete of InternalRequests."""

    def test_successful_deletion(self) -> None:
        """Successful kubectl delete does not raise."""
        proc = _completed()
        with mock.patch(
            "cleanup_internal_requests.subprocess.run",
            return_value=proc,
        ):
            cleanup_internal_requests.cleanup("test-uid-123")

    def test_calls_kubectl_with_correct_label(self) -> None:
        """Verify kubectl is called with the correct label selector."""
        proc = _completed()
        with mock.patch(
            "cleanup_internal_requests.subprocess.run",
            return_value=proc,
        ) as mock_run:
            cleanup_internal_requests.cleanup("my-uid")
        expected_label = "internal-services.appstudio.openshift.io/pipelinerun-uid=my-uid"
        mock_run.assert_called_once_with(
            [
                "kubectl",
                "delete",
                "internalrequest",
                "-l",
                expected_label,
            ],
            capture_output=True,
            text=True,
            check=False,
        )

    def test_failure_raises_runtime_error(self) -> None:
        """Failed kubectl delete raises RuntimeError with stderr."""
        proc = _completed(returncode=1, stderr="resource not found")
        with mock.patch(
            "cleanup_internal_requests.subprocess.run",
            return_value=proc,
        ):
            with pytest.raises(RuntimeError, match="resource not found"):
                cleanup_internal_requests.cleanup("test-uid")


class TestRun:
    """Test the run() orchestration."""

    def test_skips_on_empty_uid(self) -> None:
        """No kubectl calls when UID is empty."""
        with mock.patch("cleanup_internal_requests.cleanup") as mock_cleanup:
            cleanup_internal_requests.run("")
        mock_cleanup.assert_not_called()

    def test_calls_cleanup_with_uid(self) -> None:
        """Call cleanup when UID is non-empty."""
        with mock.patch("cleanup_internal_requests.cleanup") as mock_cleanup:
            cleanup_internal_requests.run("some-uid")
        mock_cleanup.assert_called_once_with("some-uid")

    def test_propagates_cleanup_error(self) -> None:
        """RuntimeError from cleanup() propagates to the caller."""
        with mock.patch(
            "cleanup_internal_requests.cleanup",
            side_effect=RuntimeError("delete failed"),
        ):
            with pytest.raises(RuntimeError, match="delete failed"):
                cleanup_internal_requests.run("some-uid")


class TestMain:
    """Test the CLI entry point."""

    def test_success(self) -> None:
        """Return 0 on successful run."""
        with mock.patch("cleanup_internal_requests.run"):
            assert cleanup_internal_requests.main(["--pipeline-run-uid", "test-uid"]) == 0

    def test_empty_uid_is_valid(self) -> None:
        """Return 0 when UID is empty (no-op)."""
        with mock.patch("cleanup_internal_requests.run") as mock_run:
            assert cleanup_internal_requests.main(["--pipeline-run-uid", ""]) == 0
        mock_run.assert_called_once_with("")

    def test_missing_arg_treated_as_empty(self) -> None:
        """Return 0 when --pipeline-run-uid is omitted (defaults to empty)."""
        with mock.patch("cleanup_internal_requests.run") as mock_run:
            assert cleanup_internal_requests.main([]) == 0
        mock_run.assert_called_once_with("")

    def test_runtime_error_propagates(self) -> None:
        """RuntimeError from run() propagates as unhandled exception."""
        with mock.patch(
            "cleanup_internal_requests.run",
            side_effect=RuntimeError("boom"),
        ):
            with pytest.raises(RuntimeError, match="boom"):
                cleanup_internal_requests.main(["--pipeline-run-uid", "test-uid"])
