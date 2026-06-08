"""Unit tests for internal_request module."""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

from internal_request import (
    InternalRequestError,
    InternalRequestFailedError,
    InternalRequestTimeoutError,
    TimeoutValidationError,
    _build_internal_request_payload,
    _convert_to_seconds,
    _get_namespace,
    _load_kube_config,
    _parse_labels,
    _parse_params,
    _validate_timeout_format,
    _validate_timeouts,
    _write_output_file,
    create_internal_request,
    get_internal_request,
    parse_arguments,
    wait_for_internal_request,
    main,
    write_result_paths,
)


def test_write_result_paths(tmp_path: Path) -> None:
    """Test that write_result_paths correctly writes pipeline and task run names to files."""
    pr_path = tmp_path / "pr"
    tr_path = tmp_path / "tr"
    write_result_paths(
        {"internal_pr_name": pr_path, "internal_task_run_name": tr_path},
        pipeline_run_name="pr-123",
        task_run_name="tr-456",
    )
    assert pr_path.read_text(encoding="utf-8") == "pr-123"
    assert tr_path.read_text(encoding="utf-8") == "tr-456"


class TestTimeoutValidation:
    """Tests for timeout validation functions."""

    def test_validate_timeout_format_valid(self):
        """Test valid timeout formats."""
        _validate_timeout_format("1h0m0s")
        _validate_timeout_format("0h5m30s")
        _validate_timeout_format("12h34m56s")

    def test_validate_timeout_format_invalid(self):
        """Test invalid timeout formats."""
        with pytest.raises(TimeoutValidationError, match="XhYmZs format"):
            _validate_timeout_format("90m")

        with pytest.raises(TimeoutValidationError, match="XhYmZs format"):
            _validate_timeout_format("1h30m")

        with pytest.raises(TimeoutValidationError, match="XhYmZs format"):
            _validate_timeout_format("invalid")

    def test_convert_to_seconds(self):
        """Test timeout conversion to seconds."""
        assert _convert_to_seconds("1h0m0s") == 3600
        assert _convert_to_seconds("0h5m30s") == 330
        assert _convert_to_seconds("2h30m45s") == 9045
        assert _convert_to_seconds("0h0m0s") == 0

    def test_convert_to_seconds_invalid(self):
        """Test conversion with invalid format."""
        with pytest.raises(TimeoutValidationError):
            _convert_to_seconds("invalid")

    def test_validate_timeouts_valid(self):
        """Test valid timeout relationships."""
        _validate_timeouts("1h0m0s", "0h55m0s", "0h5m0s")
        _validate_timeouts("2h0m0s", "1h30m0s", "0h30m0s")

    def test_validate_timeouts_pipeline_too_small(self):
        """Test pipeline timeout smaller than task + finally."""
        with pytest.raises(TimeoutValidationError, match="cannot exceed"):
            _validate_timeouts("1h0m0s", "0h55m0s", "0h10m0s")

    def test_validate_timeouts_invalid_format(self):
        """Test validation with invalid formats."""
        with pytest.raises(TimeoutValidationError):
            _validate_timeouts("invalid", "0h55m0s", "0h5m0s")


class TestParamsParsing:
    """Tests for parameter parsing functions."""

    def test_parse_params_simple(self):
        """Test parsing simple parameters."""
        result = _parse_params(["key1=value1", "key2=value2"])
        assert result == {"key1": "value1", "key2": "value2"}

    def test_parse_params_with_equals_in_value(self):
        """Test parsing params where value contains '='."""
        result = _parse_params(["url=http://example.com?a=b"])
        assert result == {"url": "http://example.com?a=b"}

    def test_parse_params_json_value(self):
        """Test parsing params with JSON-like values (stored as strings)."""
        result = _parse_params(['json={"key": "value"}'])
        assert result == {"json": '{"key": "value"}'}

    def test_parse_params_empty(self):
        """Test parsing empty param list."""
        result = _parse_params([])
        assert result == {}

    def test_parse_params_invalid_format(self):
        """Test parsing invalid parameter format."""
        with pytest.raises(ValueError, match="key=value format"):
            _parse_params(["invalid"])

    def test_parse_labels_simple(self):
        """Test parsing simple labels."""
        result = _parse_labels(["app=myapp", "env=prod"])
        assert result == {"app": "myapp", "env": "prod"}

    def test_parse_labels_empty(self):
        """Test parsing empty label list."""
        result = _parse_labels([])
        assert result == {}

    def test_parse_labels_invalid(self):
        """Test parsing invalid label format."""
        with pytest.raises(ValueError, match="key=value format"):
            _parse_labels(["invalid"])


class TestPayloadBuilding:
    """Tests for building InternalRequest payload."""

    def test_build_payload_minimal(self):
        """Test building payload with minimal required fields."""
        payload = _build_internal_request_payload(
            pipeline="test-pipeline",
            task_git_url="https://github.com/example/repo",
            task_git_revision="main",
            params={"key": "value"},
            labels=None,
            service_account=None,
            pipeline_timeout="1h0m0s",
            task_timeout="0h55m0s",
            finally_timeout="0h5m0s",
        )

        assert payload["apiVersion"] == "appstudio.redhat.com/v1alpha1"
        assert payload["kind"] == "InternalRequest"
        assert payload["metadata"]["generateName"] == "test-pipeline-"
        assert payload["spec"]["params"] == {"key": "value"}
        assert payload["spec"]["timeouts"]["pipeline"] == "1h0m0s"
        assert "labels" not in payload["metadata"]
        assert "serviceAccount" not in payload["spec"]

    def test_build_payload_with_labels(self):
        """Test building payload with labels."""
        payload = _build_internal_request_payload(
            pipeline="test-pipeline",
            task_git_url="https://github.com/example/repo",
            task_git_revision="main",
            params={},
            labels={"app": "test"},
            service_account=None,
            pipeline_timeout="1h0m0s",
            task_timeout="0h55m0s",
            finally_timeout="0h5m0s",
        )

        assert payload["metadata"]["labels"] == {"app": "test"}

    def test_build_payload_with_service_account(self):
        """Test building payload with service account."""
        payload = _build_internal_request_payload(
            pipeline="test-pipeline",
            task_git_url="https://github.com/example/repo",
            task_git_revision="main",
            params={},
            labels=None,
            service_account="my-sa",
            pipeline_timeout="1h0m0s",
            task_timeout="0h55m0s",
            finally_timeout="0h5m0s",
        )

        assert payload["spec"]["serviceAccount"] == "my-sa"

    def test_build_payload_pipeline_ref(self):
        """Test payload pipeline reference structure."""
        payload = _build_internal_request_payload(
            pipeline="my-pipeline",
            task_git_url="https://github.com/example/repo",
            task_git_revision="v1.0",
            params={},
            labels=None,
            service_account=None,
            pipeline_timeout="1h0m0s",
            task_timeout="0h55m0s",
            finally_timeout="0h5m0s",
        )

        pipeline_ref = payload["spec"]["pipeline"]["pipelineRef"]
        assert pipeline_ref["resolver"] == "git"
        assert len(pipeline_ref["params"]) == 3
        assert pipeline_ref["params"][0] == {
            "name": "url",
            "value": "https://github.com/example/repo",
        }
        assert pipeline_ref["params"][1] == {"name": "revision", "value": "v1.0"}
        assert pipeline_ref["params"][2]["name"] == "pathInRepo"
        assert "my-pipeline" in pipeline_ref["params"][2]["value"]


class TestCreateInternalRequest:
    """Tests for create_internal_request function."""

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    def test_create_success(self, mock_api_class, mock_contexts, mock_load_config):
        """Test successful InternalRequest creation."""
        # Mock namespace lookup
        mock_contexts.return_value = (
            [],
            {"context": {"namespace": "test-ns"}},
        )

        # Mock API response
        mock_api = mock.Mock()
        mock_api.create_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-pipeline-abc123"}
        }
        mock_api_class.return_value = mock_api

        result = create_internal_request(
            pipeline="test-pipeline",
            params={
                "taskGitUrl": "https://github.com/example/repo",
                "taskGitRevision": "main",
                "key": "value",
            },
        )

        assert result == "test-pipeline-abc123"
        mock_api.create_namespaced_custom_object.assert_called_once()

        # Verify the call
        call_args = mock_api.create_namespaced_custom_object.call_args
        assert call_args[1]["group"] == "appstudio.redhat.com"
        assert call_args[1]["version"] == "v1alpha1"
        assert call_args[1]["namespace"] == "test-ns"
        assert call_args[1]["plural"] == "internalrequests"

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    def test_create_missing_task_git_url(
        self, mock_api_class, mock_contexts, mock_load_config
    ):
        """Test creation fails without taskGitUrl."""
        with pytest.raises(ValueError, match="taskGitUrl"):
            create_internal_request(
                pipeline="test",
                params={"taskGitRevision": "main"},
            )

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    def test_create_missing_task_git_revision(
        self, mock_api_class, mock_contexts, mock_load_config
    ):
        """Test creation fails without taskGitRevision."""
        with pytest.raises(ValueError, match="taskGitRevision"):
            create_internal_request(
                pipeline="test",
                params={"taskGitUrl": "https://github.com/example/repo"},
            )

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    def test_create_with_labels(self, mock_api_class, mock_contexts, mock_load_config):
        """Test creation with labels."""
        mock_contexts.return_value = ([], {"context": {"namespace": "test-ns"}})
        mock_api = mock.Mock()
        mock_api.create_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-abc"}
        }
        mock_api_class.return_value = mock_api

        create_internal_request(
            pipeline="test",
            params={
                "taskGitUrl": "https://github.com/example/repo",
                "taskGitRevision": "main",
            },
            labels={"app": "test"},
        )

        call_body = mock_api.create_namespaced_custom_object.call_args[1]["body"]
        assert call_body["metadata"]["labels"] == {"app": "test"}

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    def test_create_invalid_timeout(self, mock_api_class, mock_contexts, mock_load_config):
        """Test creation with invalid timeout."""
        with pytest.raises(TimeoutValidationError):
            create_internal_request(
                pipeline="test",
                params={
                    "taskGitUrl": "https://github.com/example/repo",
                    "taskGitRevision": "main",
                },
                pipeline_timeout="invalid",
            )


class TestGetInternalRequest:
    """Tests for get_internal_request function."""

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    def test_get_success(self, mock_api_class, mock_contexts, mock_load_config):
        """Test successfully getting an InternalRequest."""
        mock_contexts.return_value = ([], {"context": {"namespace": "test-ns"}})

        mock_api = mock.Mock()
        mock_api.get_namespaced_custom_object.return_value = {
            "apiVersion": "appstudio.redhat.com/v1alpha1",
            "kind": "InternalRequest",
            "metadata": {"name": "test-ir", "namespace": "test-ns"},
            "spec": {
                "pipeline": {
                    "pipelineRef": {
                        "resolver": "git",
                        "params": [
                            {"name": "url", "value": "https://github.com/example/repo"},
                            {"name": "revision", "value": "main"},
                            {"name": "pathInRepo", "value": "pipelines/test.yaml"},
                        ],
                    }
                },
                "params": {"key": "value"},
            },
            "status": {
                "conditions": [{"reason": "Succeeded", "type": "Succeeded", "status": "True"}],
                "pipelineRun": "test-ns/test-pr",
            },
        }
        mock_api_class.return_value = mock_api

        result = get_internal_request("test-ir")

        assert result.metadata.name == "test-ir"
        assert result.status.pipelineRun == "test-ns/test-pr"
        assert result.status.conditions[0].reason == "Succeeded"

        mock_api.get_namespaced_custom_object.assert_called_once_with(
            group="appstudio.redhat.com",
            version="v1alpha1",
            namespace="test-ns",
            plural="internalrequests",
            name="test-ir",
        )

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    def test_get_not_found(self, mock_api_class, mock_contexts, mock_load_config):
        """Test getting a non-existent InternalRequest."""
        mock_contexts.return_value = ([], {"context": {"namespace": "test-ns"}})

        from kubernetes.client.rest import ApiException

        mock_api = mock.Mock()
        mock_api.get_namespaced_custom_object.side_effect = ApiException(status=404)
        mock_api_class.return_value = mock_api

        with pytest.raises(InternalRequestError, match="not found"):
            get_internal_request("nonexistent-ir")

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    def test_get_api_error(self, mock_api_class, mock_contexts, mock_load_config):
        """Test getting IR with API error."""
        mock_contexts.return_value = ([], {"context": {"namespace": "test-ns"}})

        from kubernetes.client.rest import ApiException

        mock_api = mock.Mock()
        mock_api.get_namespaced_custom_object.side_effect = ApiException(status=500)
        mock_api_class.return_value = mock_api

        with pytest.raises(InternalRequestError, match="Failed to get"):
            get_internal_request("test-ir")


class TestWaitForInternalRequest:
    """Tests for wait_for_internal_request function."""

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    @mock.patch("internal_request.time.sleep")
    def test_wait_by_name_success(
        self, mock_sleep, mock_api_class, mock_contexts, mock_load_config
    ):
        """Test waiting for a specific IR that succeeds."""
        mock_contexts.return_value = ([], {"context": {"namespace": "test-ns"}})

        # Mock IR that is already succeeded
        mock_api = mock.Mock()
        mock_api.get_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-ir"},
            "status": {
                "conditions": [{"reason": "Succeeded"}],
                "pipelineRun": "test-ns/test-pr",
            },
        }
        mock_api_class.return_value = mock_api

        result = wait_for_internal_request(
            name="test-ir", timeout=60, write_output_files=False
        )

        assert result is True
        mock_api.get_namespaced_custom_object.assert_called()

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    @mock.patch("internal_request.time.sleep")
    def test_wait_by_name_failed(
        self, mock_sleep, mock_api_class, mock_contexts, mock_load_config
    ):
        """Test waiting for IR that fails."""
        mock_contexts.return_value = ([], {"context": {"namespace": "test-ns"}})

        mock_api = mock.Mock()
        mock_api.get_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-ir"},
            "status": {
                "conditions": [{"reason": "Failed"}],
                "pipelineRun": "test-ns/test-pr",
            },
        }
        mock_api_class.return_value = mock_api

        with pytest.raises(InternalRequestFailedError, match="failed or was rejected"):
            wait_for_internal_request(name="test-ir", timeout=60, write_output_files=False)

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    @mock.patch("internal_request.time.time")
    @mock.patch("internal_request.time.sleep")
    def test_wait_timeout(
        self, mock_sleep, mock_time, mock_api_class, mock_contexts, mock_load_config
    ):
        """Test waiting times out."""
        mock_contexts.return_value = ([], {"context": {"namespace": "test-ns"}})

        # Mock time to trigger timeout
        mock_time.side_effect = [1000, 1700]  # Start, then past timeout

        mock_api = mock.Mock()
        mock_api.get_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-ir"},
            "status": {"conditions": [{"reason": "Running"}]},
        }
        mock_api_class.return_value = mock_api

        with pytest.raises(InternalRequestTimeoutError, match="Timeout"):
            wait_for_internal_request(name="test-ir", timeout=600, write_output_files=False)

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    @mock.patch("internal_request.time.sleep")
    def test_wait_by_labels(self, mock_sleep, mock_api_class, mock_contexts, mock_load_config):
        """Test waiting for IRs by label selector."""
        mock_contexts.return_value = ([], {"context": {"namespace": "test-ns"}})

        mock_api = mock.Mock()
        mock_api.list_namespaced_custom_object.return_value = {
            "items": [
                {
                    "metadata": {"name": "test-ir-1"},
                    "status": {
                        "conditions": [{"reason": "Succeeded"}],
                        "pipelineRun": "test-ns/test-pr-1",
                    },
                },
                {
                    "metadata": {"name": "test-ir-2"},
                    "status": {
                        "conditions": [{"reason": "Succeeded"}],
                        "pipelineRun": "test-ns/test-pr-2",
                    },
                },
            ]
        }
        mock_api_class.return_value = mock_api

        result = wait_for_internal_request(
            labels="app=test", timeout=60, write_output_files=False
        )

        assert result is True
        mock_api.list_namespaced_custom_object.assert_called()

    def test_wait_no_name_or_labels(self):
        """Test waiting fails without name or labels."""
        with pytest.raises(ValueError, match="name or labels must be specified"):
            wait_for_internal_request(timeout=60)

    def test_wait_both_name_and_labels(self):
        """Test waiting fails with both name and labels."""
        with pytest.raises(ValueError, match="Cannot specify both"):
            wait_for_internal_request(name="test", labels="app=test", timeout=60)

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    @mock.patch("internal_request.time.sleep")
    @mock.patch("pathlib.Path.open", new_callable=mock.mock_open)
    def test_wait_writes_output_file(
        self, mock_open, mock_sleep, mock_api_class, mock_contexts, mock_load_config
    ):
        """Test that output file is written when IR completes."""
        mock_contexts.return_value = ([], {"context": {"namespace": "test-ns"}})

        mock_api = mock.Mock()
        mock_api.get_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-ir"},
            "status": {
                "conditions": [{"reason": "Succeeded"}],
                "pipelineRun": "test-ns/test-pr",
            },
        }
        mock_api_class.return_value = mock_api

        wait_for_internal_request(name="test-ir", timeout=60, write_output_files=True)

        # Verify file was opened for writing
        mock_open.assert_called_with("w")


class TestLoadKubeConfig:
    """Tests for _load_kube_config function."""

    @mock.patch("internal_request.config.load_incluster_config")
    def test_load_incluster_config_success(self, mock_incluster) -> None:
        """Test successful in-cluster config loading."""
        mock_incluster.return_value = None
        result = _load_kube_config()
        assert result is True
        mock_incluster.assert_called_once()

    @mock.patch("internal_request.config.load_incluster_config")
    @mock.patch("internal_request.config.load_kube_config")
    def test_load_kube_config_fallback(self, mock_kube, mock_incluster) -> None:
        """Test fallback to kube_config when in-cluster fails."""
        mock_incluster.side_effect = Exception("Not in cluster")
        mock_kube.return_value = None
        result = _load_kube_config()
        assert result is False
        mock_kube.assert_called_once()


class TestGetNamespace:
    """Tests for _get_namespace function."""

    @mock.patch("internal_request._load_kube_config")
    @mock.patch("pathlib.Path.exists")
    @mock.patch("pathlib.Path.read_text")
    def test_get_namespace_incluster_with_file(
        self, mock_read, mock_exists, mock_load
    ) -> None:
        """Test getting namespace from service account file in-cluster."""
        mock_load.return_value = True
        mock_exists.return_value = True
        mock_read.return_value = "my-namespace\n"

        result = _get_namespace()

        assert result == "my-namespace"

    @mock.patch("internal_request._load_kube_config")
    @mock.patch("pathlib.Path.exists")
    def test_get_namespace_incluster_no_file(self, mock_exists, mock_load) -> None:
        """Test getting namespace in-cluster when file doesn't exist."""
        mock_load.return_value = True
        mock_exists.return_value = False

        result = _get_namespace()

        assert result == "default"

    @mock.patch("internal_request._load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    def test_get_namespace_from_kubeconfig(self, mock_contexts, mock_load) -> None:
        """Test getting namespace from kubeconfig context."""
        mock_load.return_value = False
        mock_contexts.return_value = ([], {"context": {"namespace": "test-ns"}})

        result = _get_namespace()

        assert result == "test-ns"

    @mock.patch("internal_request._load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    def test_get_namespace_kubeconfig_exception(self, mock_contexts, mock_load) -> None:
        """Test getting namespace with exception from kubeconfig."""
        mock_load.return_value = False
        mock_contexts.side_effect = Exception("Config error")

        result = _get_namespace()

        assert result == "default"


class TestGetInternalRequestWithNamespace:
    """Tests for get_internal_request with namespace override."""

    @mock.patch("internal_request._load_kube_config")
    @mock.patch("internal_request.client.CustomObjectsApi")
    def test_get_with_namespace_override(self, mock_api_class, mock_load) -> None:
        """Test get_internal_request with explicit namespace parameter."""
        mock_load.return_value = None

        mock_api = mock.Mock()
        mock_api.get_namespaced_custom_object.return_value = {
            "apiVersion": "appstudio.redhat.com/v1alpha1",
            "kind": "InternalRequest",
            "metadata": {"name": "test-ir", "namespace": "custom-ns"},
            "spec": {
                "pipeline": {
                    "pipelineRef": {
                        "resolver": "git",
                        "params": [
                            {"name": "url", "value": "https://github.com/example/repo"},
                            {"name": "revision", "value": "main"},
                            {"name": "pathInRepo", "value": "pipelines/test.yaml"},
                        ],
                    }
                },
                "params": {},
            },
        }
        mock_api_class.return_value = mock_api

        result = get_internal_request("test-ir", namespace="custom-ns")

        assert result.metadata.name == "test-ir"
        mock_api.get_namespaced_custom_object.assert_called_once_with(
            group="appstudio.redhat.com",
            version="v1alpha1",
            namespace="custom-ns",
            plural="internalrequests",
            name="test-ir",
        )


class TestCreateInternalRequestError:
    """Tests for create_internal_request error handling."""

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    def test_create_api_exception(
        self, mock_api_class, mock_contexts, mock_load_config
    ) -> None:
        """Test create_internal_request with ApiException."""
        from kubernetes.client.rest import ApiException

        mock_contexts.return_value = ([], {"context": {"namespace": "test-ns"}})

        mock_api = mock.Mock()
        mock_api.create_namespaced_custom_object.side_effect = ApiException(status=500)
        mock_api_class.return_value = mock_api

        with pytest.raises(InternalRequestError, match="Failed to create"):
            create_internal_request(
                pipeline="test",
                params={
                    "taskGitUrl": "https://github.com/example/repo",
                    "taskGitRevision": "main",
                },
            )


class TestWaitForInternalRequestEdgeCases:
    """Tests for wait_for_internal_request edge cases."""

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    @mock.patch("internal_request.time.sleep")
    def test_wait_no_condition_reason(
        self, mock_sleep, mock_api_class, mock_contexts, mock_load_config
    ) -> None:
        """Test waiting for IR with no condition reason yet."""
        mock_contexts.return_value = ([], {"context": {"namespace": "test-ns"}})

        # First call: no condition, second call: succeeded
        mock_api = mock.Mock()
        mock_api.get_namespaced_custom_object.side_effect = [
            {
                "metadata": {"name": "test-ir"},
                "status": {"conditions": []},
            },
            {
                "metadata": {"name": "test-ir"},
                "status": {
                    "conditions": [{"reason": "Succeeded"}],
                    "pipelineRun": "test-ns/test-pr",
                },
            },
        ]
        mock_api_class.return_value = mock_api

        result = wait_for_internal_request(
            name="test-ir", timeout=60, write_output_files=False
        )

        assert result is True
        assert mock_sleep.called

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    @mock.patch("internal_request.time.sleep")
    @mock.patch("pathlib.Path.open", new_callable=mock.mock_open)
    def test_wait_writes_output_file_on_failure(
        self, mock_open, mock_sleep, mock_api_class, mock_contexts, mock_load_config
    ) -> None:
        """Test that output file is written when IR fails."""
        mock_contexts.return_value = ([], {"context": {"namespace": "test-ns"}})

        mock_api = mock.Mock()
        mock_api.get_namespaced_custom_object.return_value = {
            "metadata": {"name": "test-ir"},
            "status": {
                "conditions": [{"reason": "Failed"}],
                "pipelineRun": "test-ns/test-pr",
            },
        }
        mock_api_class.return_value = mock_api

        with pytest.raises(InternalRequestFailedError):
            wait_for_internal_request(name="test-ir", timeout=60, write_output_files=True)

        # Verify file was opened for writing even on failure
        mock_open.assert_called_with("w")

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    @mock.patch("internal_request.time.time")
    @mock.patch("internal_request.time.sleep")
    def test_wait_404_then_timeout(
        self, mock_sleep, mock_time, mock_api_class, mock_contexts, mock_load_config
    ) -> None:
        """Test waiting with 404 errors that lead to timeout."""
        from kubernetes.client.rest import ApiException

        mock_contexts.return_value = ([], {"context": {"namespace": "test-ns"}})

        # Mock time to trigger timeout
        mock_time.side_effect = [1000, 1000, 1700]  # Start, check, timeout

        mock_api = mock.Mock()
        mock_api.get_namespaced_custom_object.side_effect = ApiException(status=404)
        mock_api_class.return_value = mock_api

        with pytest.raises(InternalRequestTimeoutError, match="Timeout"):
            wait_for_internal_request(name="test-ir", timeout=600, write_output_files=False)

        assert mock_sleep.called

    @mock.patch("internal_request.config.load_kube_config")
    @mock.patch("internal_request.config.list_kube_config_contexts")
    @mock.patch("internal_request.client.CustomObjectsApi")
    @mock.patch("internal_request.time.sleep")
    def test_wait_non_404_api_exception(
        self, mock_sleep, mock_api_class, mock_contexts, mock_load_config
    ) -> None:
        """Test waiting with non-404 ApiException raises error."""
        from kubernetes.client.rest import ApiException

        mock_contexts.return_value = ([], {"context": {"namespace": "test-ns"}})

        mock_api = mock.Mock()
        mock_api.get_namespaced_custom_object.side_effect = ApiException(status=500)
        mock_api_class.return_value = mock_api

        with pytest.raises(InternalRequestError, match="Failed to get"):
            wait_for_internal_request(name="test-ir", timeout=60, write_output_files=False)


class TestWriteOutputFile:
    """Tests for _write_output_file function."""

    @mock.patch("pathlib.Path.open", new_callable=mock.mock_open)
    def test_write_output_file(self, mock_open) -> None:
        """Test writing output file with IR name and pipeline run."""
        _write_output_file("test-ir-abc123", "test-ns/test-pr-xyz")

        mock_open.assert_called_once_with("w")
        # Verify json.dump was called with the file handle
        handle = mock_open()
        handle.write.assert_called()


class TestParseArguments:
    """Tests for parse_arguments function."""

    def test_parse_minimal_args(self) -> None:
        """Test parsing with minimal required arguments."""
        test_args = [
            "--pipeline",
            "test-pipeline",
            "-p",
            "taskGitUrl=https://github.com/example/repo",
            "-p",
            "taskGitRevision=main",
        ]
        with mock.patch("sys.argv", ["internal_request.py"] + test_args):
            args = parse_arguments()
            assert args.pipeline == "test-pipeline"
            assert len(args.params) == 2
            assert args.sync is False
            assert args.timeout == 3600

    def test_parse_all_args(self) -> None:
        """Test parsing with all arguments."""
        test_args = [
            "--pipeline",
            "my-pipeline",
            "-p",
            "taskGitUrl=https://github.com/example/repo",
            "-p",
            "taskGitRevision=v1.0",
            "-p",
            "key=value",
            "-l",
            "app=test",
            "-l",
            "env=prod",
            "-t",
            "7200",
            "--service-account",
            "my-sa",
            "--pipeline-timeout",
            "2h0m0s",
            "--task-timeout",
            "1h55m0s",
            "--finally-timeout",
            "0h10m0s",
            "--verbose",
        ]
        with mock.patch("sys.argv", ["internal_request.py"] + test_args):
            args = parse_arguments()
            assert args.pipeline == "my-pipeline"
            assert len(args.params) == 3
            assert len(args.labels) == 2
            assert args.timeout == 7200
            assert args.service_account == "my-sa"
            assert args.pipeline_timeout == "2h0m0s"
            assert args.task_timeout == "1h55m0s"
            assert args.finally_timeout == "0h10m0s"
            assert args.verbose is True

    def test_parse_missing_pipeline(self) -> None:
        """Test parsing fails without --pipeline."""
        with mock.patch("sys.argv", ["internal_request.py"]):
            with pytest.raises(SystemExit):
                parse_arguments()


class TestMain:
    """Tests for main function."""

    @mock.patch("internal_request.parse_arguments")
    @mock.patch("internal_request.setup_logger")
    @mock.patch("internal_request.create_internal_request")
    @mock.patch("internal_request.wait_for_internal_request")
    def test_main_success_with_sync(
        self, mock_wait, mock_create, mock_logger, mock_parse
    ) -> None:
        """Test main function with successful sync execution."""
        mock_args = mock.Mock()
        mock_args.pipeline = "test-pipeline"
        mock_args.params = [
            "taskGitUrl=https://github.com/example/repo",
            "taskGitRevision=main",
        ]
        mock_args.labels = []
        mock_args.sync = True
        mock_args.timeout = 3600
        mock_args.service_account = None
        mock_args.pipeline_timeout = "1h0m0s"
        mock_args.task_timeout = "0h55m0s"
        mock_args.finally_timeout = "0h5m0s"
        mock_args.verbose = False
        mock_parse.return_value = mock_args

        mock_create.return_value = "test-ir-abc123"
        mock_wait.return_value = True

        result = main()

        assert result == 0
        mock_create.assert_called_once()
        mock_wait.assert_called_once_with(name="test-ir-abc123", timeout=3600)

    @mock.patch("internal_request.parse_arguments")
    @mock.patch("internal_request.setup_logger")
    @mock.patch("internal_request.create_internal_request")
    def test_main_success_no_sync(self, mock_create, mock_logger, mock_parse) -> None:
        """Test main function without sync mode."""
        mock_args = mock.Mock()
        mock_args.pipeline = "test-pipeline"
        mock_args.params = [
            "taskGitUrl=https://github.com/example/repo",
            "taskGitRevision=main",
        ]
        mock_args.labels = []
        mock_args.sync = False
        mock_args.timeout = 3600
        mock_args.service_account = None
        mock_args.pipeline_timeout = "1h0m0s"
        mock_args.task_timeout = "0h55m0s"
        mock_args.finally_timeout = "0h5m0s"
        mock_args.verbose = False
        mock_parse.return_value = mock_args

        mock_create.return_value = "test-ir-abc123"

        result = main()

        assert result == 0
        mock_create.assert_called_once()

    @mock.patch("internal_request.parse_arguments")
    @mock.patch("internal_request.setup_logger")
    def test_main_missing_required_params(self, mock_logger, mock_parse, capsys) -> None:
        """Test main function with missing required parameters."""
        mock_args = mock.Mock()
        mock_args.pipeline = "test-pipeline"
        mock_args.params = []
        mock_args.labels = []
        mock_args.verbose = False
        mock_parse.return_value = mock_args

        result = main()

        assert result == 1
        captured = capsys.readouterr()
        assert "taskGitUrl" in captured.err
        assert "taskGitRevision" in captured.err

    @mock.patch("internal_request.parse_arguments")
    @mock.patch("internal_request.setup_logger")
    @mock.patch("internal_request.create_internal_request")
    def test_main_timeout_validation_error(
        self, mock_create, mock_logger, mock_parse, capsys
    ) -> None:
        """Test main function with timeout validation error."""
        from internal_request import EXIT_ERROR

        mock_args = mock.Mock()
        mock_args.pipeline = "test-pipeline"
        mock_args.params = [
            "taskGitUrl=https://github.com/example/repo",
            "taskGitRevision=main",
        ]
        mock_args.labels = []
        mock_args.service_account = None
        mock_args.pipeline_timeout = "invalid"
        mock_args.task_timeout = "0h55m0s"
        mock_args.finally_timeout = "0h5m0s"
        mock_args.verbose = False
        mock_parse.return_value = mock_args

        result = main()

        assert result == EXIT_ERROR
        captured = capsys.readouterr()
        assert "Error" in captured.err

    @mock.patch("internal_request.parse_arguments")
    @mock.patch("internal_request.setup_logger")
    @mock.patch("internal_request.create_internal_request")
    @mock.patch("internal_request.wait_for_internal_request")
    def test_main_timeout_error(self, mock_wait, mock_create, mock_logger, mock_parse) -> None:
        """Test main function with InternalRequestTimeoutError."""
        from internal_request import EXIT_TIMEOUT

        mock_args = mock.Mock()
        mock_args.pipeline = "test-pipeline"
        mock_args.params = [
            "taskGitUrl=https://github.com/example/repo",
            "taskGitRevision=main",
        ]
        mock_args.labels = []
        mock_args.sync = True
        mock_args.timeout = 60
        mock_args.service_account = None
        mock_args.pipeline_timeout = "1h0m0s"
        mock_args.task_timeout = "0h55m0s"
        mock_args.finally_timeout = "0h5m0s"
        mock_args.verbose = False
        mock_parse.return_value = mock_args

        mock_create.return_value = "test-ir"
        mock_wait.side_effect = InternalRequestTimeoutError("Timeout")

        result = main()

        assert result == EXIT_TIMEOUT

    @mock.patch("internal_request.parse_arguments")
    @mock.patch("internal_request.setup_logger")
    @mock.patch("internal_request.create_internal_request")
    @mock.patch("internal_request.wait_for_internal_request")
    def test_main_failed_error(self, mock_wait, mock_create, mock_logger, mock_parse) -> None:
        """Test main function with InternalRequestFailedError."""
        from internal_request import EXIT_FAILED

        mock_args = mock.Mock()
        mock_args.pipeline = "test-pipeline"
        mock_args.params = [
            "taskGitUrl=https://github.com/example/repo",
            "taskGitRevision=main",
        ]
        mock_args.labels = []
        mock_args.sync = True
        mock_args.timeout = 60
        mock_args.service_account = None
        mock_args.pipeline_timeout = "1h0m0s"
        mock_args.task_timeout = "0h55m0s"
        mock_args.finally_timeout = "0h5m0s"
        mock_args.verbose = False
        mock_parse.return_value = mock_args

        mock_create.return_value = "test-ir"
        mock_wait.side_effect = InternalRequestFailedError("Failed")

        result = main()

        assert result == EXIT_FAILED

    @mock.patch("internal_request.parse_arguments")
    @mock.patch("internal_request.setup_logger")
    @mock.patch("internal_request.create_internal_request")
    def test_main_config_exception(self, mock_create, mock_logger, mock_parse) -> None:
        """Test main function with ConfigException."""
        from kubernetes.config.config_exception import ConfigException
        from internal_request import EXIT_ERROR

        mock_args = mock.Mock()
        mock_args.pipeline = "test-pipeline"
        mock_args.params = [
            "taskGitUrl=https://github.com/example/repo",
            "taskGitRevision=main",
        ]
        mock_args.labels = []
        mock_args.timeout = 3600
        mock_args.service_account = None
        mock_args.pipeline_timeout = "1h0m0s"
        mock_args.task_timeout = "0h55m0s"
        mock_args.finally_timeout = "0h5m0s"
        mock_args.verbose = False
        mock_parse.return_value = mock_args

        mock_create.side_effect = ConfigException("Kube config error")

        result = main()

        assert result == EXIT_ERROR

    @mock.patch("internal_request.parse_arguments")
    @mock.patch("internal_request.setup_logger")
    @mock.patch("internal_request._convert_to_seconds")
    @mock.patch("internal_request.create_internal_request")
    def test_main_pipeline_timeout_warning(
        self, mock_create, mock_convert, mock_logger, mock_parse, capsys
    ) -> None:
        """Test main function warns when pipeline timeout exceeds script timeout."""
        mock_args = mock.Mock()
        mock_args.pipeline = "test-pipeline"
        mock_args.params = [
            "taskGitUrl=https://github.com/example/repo",
            "taskGitRevision=main",
        ]
        mock_args.labels = []
        mock_args.sync = False
        mock_args.timeout = 60
        mock_args.service_account = None
        mock_args.pipeline_timeout = "2h0m0s"
        mock_args.task_timeout = "1h55m0s"
        mock_args.finally_timeout = "0h5m0s"
        mock_args.verbose = False
        mock_parse.return_value = mock_args

        mock_convert.return_value = 7200  # 2 hours > 60 seconds
        mock_create.return_value = "test-ir"

        result = main()

        assert result == 0
        captured = capsys.readouterr()
        assert "WARNING" in captured.out
