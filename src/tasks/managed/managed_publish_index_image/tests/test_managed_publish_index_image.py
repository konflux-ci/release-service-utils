"""Tests for managed_publish_index_image.py."""

from __future__ import annotations

from concurrent.futures import Future
import json
import re
import sys

import pytest
from unittest import mock

from release_service_utils.tasks.managed import managed_publish_index_image
from release_service_utils.tasks.managed.managed_publish_index_image import (
    managed_publish_index_image as mpi_module,
)

_create_ir_wrapper = mpi_module._create_ir_wrapper
format_seconds = mpi_module.format_seconds
make_parser = mpi_module.make_parser
main = mpi_module.main

TASK = "release_service_utils.tasks.managed.managed_publish_index_image"
MOD = TASK + ".managed_publish_index_image"


@pytest.fixture
def data_file(tmp_path):
    """Create a temporary data file with publishing credentials."""
    file_path = tmp_path / "data.json"
    file_path.write_text(
        json.dumps({"fbc": {"publishingCredentials": "fbc-publishing-credentials"}})
    )
    return file_path


def test_format_seconds() -> None:
    """Test format_seconds function with various inputs."""
    assert format_seconds(0) == "00h00m00s"
    assert format_seconds(59) == "00h00m59s"
    assert format_seconds(60) == "00h01m00s"
    assert format_seconds(61) == "00h01m01s"
    assert format_seconds(3600) == "01h00m00s"
    assert format_seconds(3661) == "01h01m01s"


@pytest.fixture
def results_file(tmp_path):
    """Create a temporary results file for testing."""
    file_path = tmp_path / "results.json"
    file_path.write_text(
        json.dumps(
            {
                "components": [
                    {
                        "target_index": "index1",
                        "target_index_with_timestamp": "index1-20240601",
                        "index_image": "image1",
                        "index_image_resolved": "image1@sha256:aaa",
                        "ocp_version": "4.12",
                    },
                    {
                        "target_index": "index2",
                        "target_index_with_timestamp": "index2-20240601",
                        "index_image": "image2",
                        "index_image_resolved": "image2@sha256:bbb",
                        "ocp_version": "4.12",
                    },
                ]
            }
        )
    )
    return file_path


def test_make_parser() -> None:
    """Test make_parser function with sample arguments."""
    parser = make_parser()
    args = parser.parse_args(
        [
            "--data-file",
            "/tmp/data.json",
            "--ir-results-file",
            "results.json",
            "--task-git-url",
            "github.com/example/repo.git",
            "--task-git-revision",
            "main",
            "--pipeline-run-id",
            "run-123",
            "--request-timeout",
            "120",
            "--retries",
            "3",
            "--log-level",
            "INFO",
        ]
    )
    assert args.data_file == "/tmp/data.json"
    assert args.ir_results_file == "results.json"
    assert args.task_git_url == "github.com/example/repo.git"
    assert args.task_git_revision == "main"
    assert args.pipeline_run_id == "run-123"
    assert args.request_timeout == 120
    assert args.retries == 3
    assert args.log_level == "INFO"


@pytest.fixture
def fix_spawn_internal_request():
    """Mock create_internal_request to return a Future with the IR name."""
    with mock.patch.object(
        managed_publish_index_image.managed_publish_index_image,
        "create_internal_request",
    ) as mock_spawn:

        def create_mock_future(*args, **kwargs):
            future = Future()
            future.set_result("publish-index-image-pipeline-ok")
            return future

        mock_spawn.side_effect = create_mock_future
        yield mock_spawn


@pytest.fixture
def fix_spawn_internal_request_failed():
    """Mock create_internal_request to return a Future with failed IR name."""
    with mock.patch.object(
        managed_publish_index_image.managed_publish_index_image,
        "create_internal_request",
    ) as mock_spawn:

        def create_mock_future(*args, **kwargs):
            future = Future()
            future.set_result("publish-index-image-pipeline-failed")
            return future

        mock_spawn.side_effect = create_mock_future
        yield mock_spawn


@pytest.fixture
def fix_wait_ok():
    """Mock wait_for_internal_request to succeed."""
    with mock.patch.object(
        managed_publish_index_image.managed_publish_index_image,
        "wait_for_internal_request",
    ) as mock_wait:

        def create_mock_future(*args, **kwargs):
            future = Future()
            future.set_result(None)
            return future

        mock_wait.side_effect = create_mock_future
        yield mock_wait


@pytest.fixture
def fix_fetch_results_ok():
    """Mock _fetch_results to return success."""
    with mock.patch.object(
        managed_publish_index_image.managed_publish_index_image, "_fetch_results"
    ) as mock_fetch:
        mock_fetch.return_value = {"requestMessage": "success"}
        yield mock_fetch


@pytest.fixture
def fix_fetch_results_error():
    """Mock _fetch_results to return error."""
    with mock.patch.object(
        managed_publish_index_image.managed_publish_index_image, "_fetch_results"
    ) as mock_fetch:
        mock_fetch.return_value = {"requestMessage": "error"}
        yield mock_fetch


def test_main(
    data_file,
    results_file,
    fix_spawn_internal_request,
    fix_wait_ok,
    fix_fetch_results_ok,
    monkeypatch,
) -> None:
    """Test main function with successful pipeline execution."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "managed_publish_index_image.py",
            "--data-file",
            str(data_file),
            "--ir-results-file",
            str(results_file),
            "--task-git-url",
            "github.com/example/repo.git",
            "--task-git-revision",
            "main",
            "--pipeline-run-id",
            "run-123",
            "--request-timeout",
            "120",
            "--retries",
            "3",
            "--log-level",
            "INFO",
        ],
    )
    assert main() == 0


def test_main_failed(
    data_file,
    results_file,
    fix_spawn_internal_request_failed,
    fix_wait_ok,
    fix_fetch_results_error,
    monkeypatch,
) -> None:
    """Test main function when pipeline execution fails."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "managed_publish_index_image.py",
            "--data-file",
            str(data_file),
            "--ir-results-file",
            str(results_file),
            "--task-git-url",
            "github.com/example/repo.git",
            "--task-git-revision",
            "main",
            "--pipeline-run-id",
            "run-123",
            "--request-timeout",
            "120",
            "--retries",
            "3",
            "--log-level",
            "INFO",
        ],
    )
    assert main() == 1


def test_main_exception(
    data_file, results_file, fix_spawn_internal_request, monkeypatch
) -> None:
    """Test main function when create_internal_request raises an exception."""
    with mock.patch.object(
        managed_publish_index_image.managed_publish_index_image,
        "create_internal_request",
    ) as mock_spawn:

        def create_failing_future(*args, **kwargs):
            future = Future()
            future.set_exception(RuntimeError("Failed to get internal request"))
            return future

        mock_spawn.side_effect = create_failing_future

        monkeypatch.setattr(
            sys,
            "argv",
            [
                "managed_publish_index_image.py",
                "--data-file",
                str(data_file),
                "--ir-results-file",
                str(results_file),
                "--task-git-url",
                "github.com/example/repo.git",
                "--task-git-revision",
                "main",
                "--pipeline-run-id",
                "run-123",
                "--request-timeout",
                "120",
                "--retries",
                "3",
                "--log-level",
                "INFO",
            ],
        )
        with pytest.raises(
            RuntimeError,
            match=re.escape("Failed to get internal request"),
        ):
            main()


class TestCreateIrWrapper:
    """Tests for _create_ir_wrapper function."""

    def test_create_ir_wrapper_with_labels(self) -> None:
        """Test _create_ir_wrapper converts params and labels to dicts."""
        with mock.patch.object(
            managed_publish_index_image.managed_publish_index_image, "_create_ir"
        ) as mock_create_ir:
            mock_create_ir.return_value = "test-request-id"

            params = [("key1", "value1"), ("key2", "value2")]
            labels = [("label1", "val1"), ("label2", "val2")]

            result = _create_ir_wrapper(
                pipeline="test-pipeline",
                params=params,
                labels=labels,
                task_timeout="1h",
                pipeline_timeout="2h",
            )

            assert result == "test-request-id"
            mock_create_ir.assert_called_once_with(
                pipeline="test-pipeline",
                params={"key1": "value1", "key2": "value2"},
                labels={"label1": "val1", "label2": "val2"},
                sync=False,
                task_timeout="1h",
                pipeline_timeout="2h",
                finally_timeout="0h5m0s",
            )

    def test_create_ir_wrapper_without_labels(self) -> None:
        """Test _create_ir_wrapper handles None labels."""
        with mock.patch.object(
            managed_publish_index_image.managed_publish_index_image, "_create_ir"
        ) as mock_create_ir:
            mock_create_ir.return_value = "test-request-id"

            params = [("key1", "value1")]
            labels = None

            result = _create_ir_wrapper(
                pipeline="test-pipeline",
                params=params,
                labels=labels,
                task_timeout="1h",
                pipeline_timeout="2h",
            )

            assert result == "test-request-id"
            mock_create_ir.assert_called_once_with(
                pipeline="test-pipeline",
                params={"key1": "value1"},
                labels=None,
                sync=False,
                task_timeout="1h",
                pipeline_timeout="2h",
                finally_timeout="0h5m0s",
            )

    def test_create_ir_wrapper_with_empty_labels(self) -> None:
        """Test _create_ir_wrapper handles empty labels list."""
        with mock.patch.object(
            managed_publish_index_image.managed_publish_index_image, "_create_ir"
        ) as mock_create_ir:
            mock_create_ir.return_value = "test-request-id"

            params = [("key1", "value1")]
            labels = []

            result = _create_ir_wrapper(
                pipeline="test-pipeline",
                params=params,
                labels=labels,
                task_timeout="1h",
                pipeline_timeout="2h",
            )

            assert result == "test-request-id"
            mock_create_ir.assert_called_once_with(
                pipeline="test-pipeline",
                params={"key1": "value1"},
                labels=None,
                sync=False,
                task_timeout="1h",
                pipeline_timeout="2h",
                finally_timeout="0h5m0s",
            )


class TestMainEdgeCases:
    """Tests for main function edge cases."""

    @pytest.fixture
    def results_file_no_status(self, tmp_path):
        """Create a results file for testing IR without status."""
        file_path = tmp_path / "results_no_status.json"
        file_path.write_text(
            json.dumps(
                {
                    "components": [
                        {
                            "target_index": "quay.io/test/index:v1.0",
                            "target_index_with_timestamp": (
                                "quay.io/test/index:v1.0-20240601"
                            ),
                            "index_image": "quay.io/test/source@sha256:abc123",
                            "index_image_resolved": ("quay.io/test/source@sha256:abc123"),
                            "ocp_version": "4.12",
                        }
                    ]
                }
            )
        )
        return file_path

    def test_main_ir_without_status(
        self,
        data_file,
        results_file_no_status,
        fix_spawn_internal_request,
        fix_wait_ok,
        monkeypatch,
    ) -> None:
        """Test main when IR has no request message."""
        with mock.patch.object(
            managed_publish_index_image.managed_publish_index_image, "_fetch_results"
        ) as mock_fetch:
            mock_fetch.return_value = {}
            monkeypatch.setattr(
                sys,
                "argv",
                [
                    "managed_publish_index_image.py",
                    "--data-file",
                    str(data_file),
                    "--ir-results-file",
                    str(results_file_no_status),
                    "--task-git-url",
                    "github.com/example/repo.git",
                    "--task-git-revision",
                    "main",
                    "--pipeline-run-id",
                    "run-123",
                    "--request-timeout",
                    "120",
                    "--retries",
                    "3",
                    "--log-level",
                    "INFO",
                ],
            )
            assert main() == 0

    @pytest.fixture
    def results_file_with_timestamp(self, tmp_path):
        """Create results file where target already includes timestamp."""
        target = "quay.io/test/index:v1.0-20240601"
        file_path = tmp_path / "results_with_timestamp.json"
        file_path.write_text(
            json.dumps(
                {
                    "components": [
                        {
                            "target_index": target,
                            "target_index_with_timestamp": target,
                            "index_image": "quay.io/test/source@sha256:abc123",
                            "index_image_resolved": ("quay.io/test/source@sha256:abc123"),
                            "ocp_version": "4.12",
                        }
                    ]
                }
            )
        )
        return file_path

    def test_main_target_already_has_timestamp(
        self,
        data_file,
        results_file_with_timestamp,
        fix_spawn_internal_request,
        fix_wait_ok,
        fix_fetch_results_ok,
        monkeypatch,
    ) -> None:
        """Test main when target index already includes build timestamp."""
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "managed_publish_index_image.py",
                "--data-file",
                str(data_file),
                "--ir-results-file",
                str(results_file_with_timestamp),
                "--task-git-url",
                "github.com/example/repo.git",
                "--task-git-revision",
                "main",
                "--pipeline-run-id",
                "run-123",
                "--request-timeout",
                "120",
                "--retries",
                "3",
                "--log-level",
                "INFO",
            ],
        )
        result = main()
        assert result == 0
        assert fix_spawn_internal_request.call_count == 1


class TestMakeParserDefaults:
    """Additional tests for make_parser function."""

    def test_parser_with_defaults(self) -> None:
        """Test parser uses correct default values."""
        parser = make_parser()
        args = parser.parse_args(["--data-file", "/tmp/data.json"])

        assert args.data_file == "/tmp/data.json"
        assert args.request_timeout == 360
        assert args.retries == 3
        assert args.log_level == "DEBUG"
        assert args.task_git_url == "https://github.com/example/repo.git"
        assert args.task_git_revision == "main"
        assert args.pipeline_run_id == "default-run"
        assert args.ir_results_file == "ir_results.json"
