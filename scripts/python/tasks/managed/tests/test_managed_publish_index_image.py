"""Tests for managed_publish_index_image.py."""

from concurrent.futures import Future
import json
import re
import sys

import pytest
from unittest import mock


from .. import managed_publish_index_image  # noqa: E402
from ..managed_publish_index_image import (  # noqa: E402
    _create_ir_wrapper,
    format_seconds,
    make_parser,
    main,
)  # noqa: E402
from rsmodels.internal_request_models import InternalRequest  # noqa: E402
from .irs import IRS  # noqa: E402


@pytest.fixture
def temp_file(tmp_path):
    """Create a temporary file for testing."""
    file_path = tmp_path / "test_file.txt"
    file_path.write_text("This is a test file.")
    return file_path


@pytest.fixture
def tmp_file_content(temp_file, request):
    """Write request.param content to a temporary file."""
    with open(temp_file, "w") as f:
        f.write(json.dumps(request.param))
    return temp_file


def test_format_seconds():
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
                        "index_image": "image1",
                        "completion_time": "2024-06-01T12:00:00Z",
                    },
                    {
                        "target_index": "index2",
                        "index_image": "image2",
                        "completion_time": "2024-06-01T12:05:00Z",
                    },
                ]
            }
        )
    )
    return file_path


def test_make_parser():
    """Test make_parser function with sample arguments."""
    parser = make_parser()
    args = parser.parse_args(
        [
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
    assert args.ir_results_file == "results.json"
    assert args.task_git_url == "github.com/example/repo.git"
    assert args.task_git_revision == "main"
    assert args.pipeline_run_id == "run-123"
    assert args.request_timeout == 120
    assert args.retries == 3
    assert args.log_level == "INFO"


@pytest.fixture
def fix_spawn_internal_request(request):
    """Mock create_internal_request to return a Future with request.param as result."""
    with mock.patch.object(
        managed_publish_index_image, "create_internal_request"
    ) as mock_spawn:

        def create_mock_future(*args, **kwargs):
            future = Future()
            future.set_result(request.param)
            return future

        mock_spawn.side_effect = create_mock_future
        yield mock_spawn


@pytest.fixture
def fix_get_internal_request(request):
    """Mock _get_internal_request to return an InternalRequest based on request.param."""
    with mock.patch.object(managed_publish_index_image, "_get_internal_request") as mock_get:
        mock_get.side_effect = lambda request_id: InternalRequest(**IRS[request.param])
        yield mock_get


@pytest.fixture
def fix_get_internal_request_exception():
    """Mock _get_internal_request to raise an exception."""
    with mock.patch.object(managed_publish_index_image, "_get_internal_request") as mock_get:
        mock_get.side_effect = RuntimeError("Failed to get internal request")
        yield mock_get


@pytest.mark.parametrize(
    "fix_spawn_internal_request", ["publish-index-image-pipeline-ok"], indirect=True
)
@pytest.mark.parametrize(
    "fix_get_internal_request", ["publish-index-image-pipeline-ok"], indirect=True
)
def test_main(results_file, fix_get_internal_request, fix_spawn_internal_request, monkeypatch):
    """Test main function with successful pipeline execution."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "managed_publish_index_image.py",
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
            "--publishing-credentials",
            "dummy_credentials",
            "--retries",
            "3",
            "--log-level",
            "INFO",
        ],
    )
    assert main() == 0


@pytest.mark.parametrize(
    "fix_spawn_internal_request", ["publish-index-image-pipeline-failed"], indirect=True
)
@pytest.mark.parametrize(
    "fix_get_internal_request", ["publish-index-image-pipeline-failed"], indirect=True
)
def test_main_failed(
    results_file, fix_get_internal_request, fix_spawn_internal_request, monkeypatch
):
    """Test main function when pipeline execution fails."""
    monkeypatch.setenv("PUBLISHING_CREDENTIALS", "dummy_credentials")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "managed_publish_index_image.py",
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


@pytest.mark.parametrize(
    "fix_spawn_internal_request", ["publish-index-image-pipeline-ok"], indirect=True
)
def test_main_exception(
    results_file, fix_get_internal_request_exception, fix_spawn_internal_request, monkeypatch
):
    """Test main function when _get_internal_request raises an exception."""
    monkeypatch.setenv("PUBLISHING_CREDENTIALS", "dummy_credentials")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "managed_publish_index_image.py",
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
    with pytest.raises(RuntimeError, match=re.escape("Failed to get internal request")):
        main()


class TestCreateIrWrapper:
    """Tests for _create_ir_wrapper function."""

    def test_create_ir_wrapper_with_labels(self) -> None:
        """Test _create_ir_wrapper converts params and labels to dicts."""
        with mock.patch.object(
            managed_publish_index_image, "_create_internal_request"
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
                task_timeout="1h",
                pipeline_timeout="2h",
                finally_timeout="0h5m0s",
            )

    def test_create_ir_wrapper_without_labels(self) -> None:
        """Test _create_ir_wrapper handles None labels."""
        with mock.patch.object(
            managed_publish_index_image, "_create_internal_request"
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
                task_timeout="1h",
                pipeline_timeout="2h",
                finally_timeout="0h5m0s",
            )

    def test_create_ir_wrapper_with_empty_labels(self) -> None:
        """Test _create_ir_wrapper handles empty labels list."""
        with mock.patch.object(
            managed_publish_index_image, "_create_internal_request"
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
                            "index_image": "quay.io/test/source@sha256:abc123",
                            "completion_time": "2024-06-01T12:00:00Z",
                        }
                    ]
                }
            )
        )
        return file_path

    @pytest.fixture
    def fix_spawn_internal_request_no_status(self):
        """Mock create_internal_request for testing."""
        with mock.patch.object(
            managed_publish_index_image, "create_internal_request"
        ) as mock_spawn:

            def create_mock_future(*args, **kwargs):
                future = Future()
                future.set_result("test-ir-no-status")
                return future

            mock_spawn.side_effect = create_mock_future
            yield mock_spawn

    @pytest.fixture
    def fix_get_internal_request_no_status(self):
        """Mock _get_internal_request to return IR without status."""
        ir_no_status = {
            "apiVersion": "appstudio.redhat.com/v1alpha1",
            "kind": "InternalRequest",
            "metadata": {
                "creationTimestamp": "2026-05-19T13:45:44Z",
                "name": "test-ir-no-status",
                "namespace": "testing",
            },
            "spec": {
                "params": {
                    "sourceIndex": "quay.io/test/source@sha256:abc",
                    "targetIndex": "quay.io/test/target:v1.0",
                },
                "pipeline": {
                    "pipelineRef": {
                        "resolver": "git",
                        "params": [
                            {"name": "url", "value": "https://github.com/example/repo.git"},
                            {"name": "revision", "value": "main"},
                            {"name": "pathInRepo", "value": "pipeline.yaml"},
                        ],
                    }
                },
            },
        }

        with mock.patch.object(
            managed_publish_index_image, "_get_internal_request"
        ) as mock_get:
            mock_get.return_value = InternalRequest(**ir_no_status)
            yield mock_get

    def test_main_ir_without_status(
        self,
        results_file_no_status,
        fix_spawn_internal_request_no_status,
        fix_get_internal_request_no_status,
        monkeypatch,
    ) -> None:
        """Test main when InternalRequest has no status (line 207)."""
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "managed_publish_index_image.py",
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
        file_path = tmp_path / "results_with_timestamp.json"
        timestamp = "2024-06-01T12:00:00Z"
        file_path.write_text(
            json.dumps(
                {
                    "components": [
                        {
                            "target_index": f"quay.io/test/index:v1.0-{timestamp}",
                            "index_image": "quay.io/test/source@sha256:abc123",
                            "completion_time": timestamp,
                        }
                    ]
                }
            )
        )
        return file_path

    @pytest.mark.parametrize(
        "fix_spawn_internal_request", ["publish-index-image-pipeline-ok"], indirect=True
    )
    @pytest.mark.parametrize(
        "fix_get_internal_request", ["publish-index-image-pipeline-ok"], indirect=True
    )
    def test_main_target_already_has_timestamp(
        self,
        results_file_with_timestamp,
        fix_spawn_internal_request,
        fix_get_internal_request,
        monkeypatch,
    ) -> None:
        """Test main when target index already includes build timestamp."""
        monkeypatch.setattr(
            sys,
            "argv",
            [
                "managed_publish_index_image.py",
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
        args = parser.parse_args([])

        assert args.publishing_credentials == "/mnt/publishingCredentials/credential"
        assert args.request_timeout == 360
        assert args.retries == 3
        assert args.log_level == "DEBUG"
        assert args.task_git_url == "https://github.com/example/repo.git"
        assert args.task_git_revision == "main"
        assert args.pipeline_run_id == "default-run"
        assert args.ir_results_file == "ir_results.json"
