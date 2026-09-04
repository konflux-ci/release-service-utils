"""Test internal_request helpers."""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

from release_service_utils.helpers.internal_request import internal_request as ir_module
from release_service_utils.helpers import retry
from release_service_utils.helpers.internal_request import (
    EXIT_FAILED,
    EXIT_TIMEOUT,
    InternalRequestWaitError,
    wait_for_completion,
)


@pytest.fixture()
def k8s_api():
    """Provide a mock Kubernetes CustomObjects API client."""
    api = mock.MagicMock()
    with mock.patch.object(ir_module, "_get_namespace", return_value="test-ns"):
        yield api


def test_duration_to_seconds_parses_hms() -> None:
    """Convert XhYmZs durations to seconds."""
    assert ir_module.duration_to_seconds("1h0m0s") == 3600
    assert ir_module.duration_to_seconds("0h55m0s") == 3300


def test_duration_to_seconds_rejects_invalid_format() -> None:
    """Reject durations that do not use XhYmZs format."""
    with pytest.raises(ValueError, match="XhYmZs"):
        ir_module.duration_to_seconds("60m")


def test_validate_timeouts_rejects_task_plus_finally_exceeding_pipeline() -> None:
    """Reject when task and finally timeouts exceed the pipeline timeout."""
    with pytest.raises(ValueError, match="cannot exceed the pipeline timeout"):
        ir_module.validate_timeouts(
            timeout=3600,
            pipeline_timeout="0h10m0s",
            task_timeout="0h8m0s",
            finally_timeout="0h5m0s",
        )


def test_validate_timeouts_rejects_invalid_format() -> None:
    """Reject timeout values that do not use XhYmZs format."""
    with pytest.raises(ValueError, match="task_timeout must use XhYmZs"):
        ir_module.validate_timeouts(
            timeout=3600,
            pipeline_timeout="1h0m0s",
            task_timeout="55m",
            finally_timeout="0h5m0s",
        )


def test_validate_timeouts_warns_when_pipeline_exceeds_script_timeout() -> None:
    """Log a warning when the pipeline timeout exceeds the script timeout."""
    with mock.patch.object(ir_module.logger, "warning") as warning:
        ir_module.validate_timeouts(
            timeout=60,
            pipeline_timeout="1h0m0s",
            task_timeout="0h55m0s",
            finally_timeout="0h5m0s",
        )

    warning.assert_called_once()
    assert "pipeline timeout is greater than the script timeout" in warning.call_args[0][0]


def test_build_payload_includes_required_fields() -> None:
    """Build an InternalRequest manifest with git resolver metadata."""
    payload = ir_module.build_payload(
        pipeline="create-advisory",
        params={
            "taskGitUrl": "https://example.test/catalog",
            "taskGitRevision": "main",
            "advisory_json": "abc",
        },
        labels={"foo": "bar"},
        pipeline_git_url="https://example.test/catalog",
        pipeline_git_revision="main",
        pipeline_timeout="1h0m0s",
        task_timeout="0h55m0s",
        finally_timeout="0h5m0s",
        service_account="ir-sa",
    )

    assert payload["metadata"]["generateName"] == "create-advisory-"
    assert payload["metadata"]["labels"]["foo"] == "bar"
    assert payload["metadata"]["labels"][ir_module.PIPELINE_NAME_LABEL] == "create-advisory"
    assert payload["spec"]["serviceAccount"] == "ir-sa"
    assert payload["spec"]["params"]["advisory_json"] == "abc"
    assert (
        payload["spec"]["pipeline"]["pipelineRef"]["params"][2]["value"]
        == "pipelines/internal/create-advisory/create-advisory.yaml"
    )


def test_cleanup_existing_requests_deletes_matching_irs(k8s_api: mock.MagicMock) -> None:
    """Delete existing InternalRequests before creating a new one."""
    k8s_api.list_namespaced_custom_object.return_value = {
        "items": [{"metadata": {"name": "old-ir-1"}}],
    }

    with mock.patch.object(ir_module.time, "sleep"):
        ir_module.cleanup_existing_requests(
            pipeline="create-advisory",
            labels={ir_module.PIPELINERUN_UID_LABEL: "uid-123"},
            k8s_api=k8s_api,
        )

    list_call = k8s_api.list_namespaced_custom_object.call_args
    assert list_call.kwargs["label_selector"] == (
        f"{ir_module.PIPELINERUN_UID_LABEL}=uid-123,"
        f"{ir_module.PIPELINE_NAME_LABEL}=create-advisory"
    )

    del_call = k8s_api.delete_namespaced_custom_object.call_args
    assert del_call.kwargs["name"] == "old-ir-1"
    assert del_call.kwargs["namespace"] == "test-ns"


def test_cleanup_existing_requests_skips_without_pipelinerun_uid(
    k8s_api: mock.MagicMock,
) -> None:
    """Skip cleanup when the pipelinerun-uid label is absent."""
    ir_module.cleanup_existing_requests(
        pipeline="create-advisory",
        labels={"other": "value"},
        k8s_api=k8s_api,
    )
    k8s_api.list_namespaced_custom_object.assert_not_called()


def test_cleanup_existing_requests_skips_when_no_matching_items(
    k8s_api: mock.MagicMock,
) -> None:
    """Skip deletion when no existing InternalRequests are found."""
    k8s_api.list_namespaced_custom_object.return_value = {"items": []}

    ir_module.cleanup_existing_requests(
        pipeline="create-advisory",
        labels={ir_module.PIPELINERUN_UID_LABEL: "uid-123"},
        k8s_api=k8s_api,
    )

    k8s_api.list_namespaced_custom_object.assert_called_once()
    k8s_api.delete_namespaced_custom_object.assert_not_called()


def test_cleanup_existing_requests_skips_non_dict_items(k8s_api: mock.MagicMock) -> None:
    """Ignore list entries that are not InternalRequest objects."""
    k8s_api.list_namespaced_custom_object.return_value = {
        "items": ["not-a-dict", {"metadata": {"name": "old-ir-1"}}],
    }

    with mock.patch.object(ir_module.time, "sleep"):
        ir_module.cleanup_existing_requests(
            pipeline="create-advisory",
            labels={ir_module.PIPELINERUN_UID_LABEL: "uid-123"},
            k8s_api=k8s_api,
        )

    del_call = k8s_api.delete_namespaced_custom_object.call_args
    assert del_call.kwargs["name"] == "old-ir-1"


def test_cleanup_existing_requests_skips_invalid_ir_name(k8s_api: mock.MagicMock) -> None:
    """Ignore InternalRequests whose metadata name is missing or not a string."""
    k8s_api.list_namespaced_custom_object.return_value = {
        "items": [
            {"metadata": {}},
            {"metadata": {"name": ""}},
            {"metadata": {"name": 123}},
        ],
    }

    with mock.patch.object(ir_module.time, "sleep"):
        ir_module.cleanup_existing_requests(
            pipeline="create-advisory",
            labels={ir_module.PIPELINERUN_UID_LABEL: "uid-123"},
            k8s_api=k8s_api,
        )

    k8s_api.delete_namespaced_custom_object.assert_not_called()


def test_create_creates_internal_request_without_waiting(k8s_api: mock.MagicMock) -> None:
    """Create an InternalRequest and return its name when sync is false."""
    k8s_api.list_namespaced_custom_object.return_value = {"items": []}
    k8s_api.create_namespaced_custom_object.return_value = {
        "metadata": {"name": "create-advisory-abc"},
    }

    name = ir_module.create(
        "create-advisory",
        params={
            "taskGitUrl": "https://example.test/catalog",
            "taskGitRevision": "main",
        },
        sync=False,
        k8s_api=k8s_api,
    )

    assert name == "create-advisory-abc"
    k8s_api.create_namespaced_custom_object.assert_called_once()


def test_create_requires_task_git_params(k8s_api: mock.MagicMock) -> None:
    """Reject creation when git resolver params are missing."""
    with pytest.raises(ValueError, match="taskGitUrl and taskGitRevision"):
        ir_module.create(
            "create-advisory",
            params={"componentGroup": "myapp"},
            sync=False,
            k8s_api=k8s_api,
        )


def test_create_requires_pipeline(k8s_api: mock.MagicMock) -> None:
    """Reject creation when the pipeline name is empty."""
    with pytest.raises(ValueError, match="pipeline is required"):
        ir_module.create(
            "",
            params={
                "taskGitUrl": "https://example.test/catalog",
                "taskGitRevision": "main",
            },
            sync=False,
            k8s_api=k8s_api,
        )


def test_create_internal_request_raises_when_name_missing(k8s_api: mock.MagicMock) -> None:
    """Raise when the API does not return an InternalRequest name."""
    k8s_api.create_namespaced_custom_object.return_value = {"metadata": {}}

    with pytest.raises(RuntimeError, match="did not return an InternalRequest name"):
        ir_module.create_internal_request({"kind": "InternalRequest"}, k8s_api=k8s_api)


def test_create_waits_when_sync_is_true(k8s_api: mock.MagicMock) -> None:
    """Wait for completion after creating the InternalRequest."""
    with (
        mock.patch.object(ir_module, "cleanup_existing_requests"),
        mock.patch.object(
            ir_module,
            "create_internal_request",
            return_value="create-advisory-abc",
        ),
        mock.patch.object(ir_module, "wait_for_completion") as wait,
    ):
        name = ir_module.create(
            "create-advisory",
            params={
                "taskGitUrl": "https://example.test/catalog",
                "taskGitRevision": "main",
            },
            sync=True,
            k8s_api=k8s_api,
        )

    assert name == "create-advisory-abc"
    wait.assert_called_once_with(name="create-advisory-abc", timeout=3600, k8s_api=k8s_api)


def test_create_skips_cleanup_when_cleanup_is_false(k8s_api: mock.MagicMock) -> None:
    """Do not delete prior InternalRequests when cleanup is False."""
    with (
        mock.patch.object(ir_module, "cleanup_existing_requests") as cleanup,
        mock.patch.object(
            ir_module,
            "create_internal_request",
            return_value="create-advisory-abc",
        ),
        mock.patch.object(ir_module, "wait_for_completion"),
    ):
        ir_module.create(
            "create-advisory",
            params={
                "taskGitUrl": "https://example.test/catalog",
                "taskGitRevision": "main",
            },
            sync=True,
            cleanup=False,
            k8s_api=k8s_api,
        )

    cleanup.assert_not_called()


def test_wait_for_completion_requires_exactly_one_selector(
    k8s_api: mock.MagicMock,
) -> None:
    """Reject calls that provide both or neither selector."""
    with pytest.raises(ValueError, match="exactly one"):
        wait_for_completion(k8s_api=k8s_api)

    with pytest.raises(ValueError, match="exactly one"):
        wait_for_completion(name="ir-1", label_selector="foo=bar", k8s_api=k8s_api)


def _patch_ir_output_path(tmp_path: Path, ir_name: str = "ir-1") -> tuple[mock._patch, Path]:
    """Patch ``_ir_output_path`` to write under *tmp_path* for isolated tests."""
    output_path = tmp_path / f"{ir_name}-output.json"
    patch = mock.patch.object(ir_module, "_ir_output_path", return_value=output_path)
    return patch, output_path


def test_wait_for_completion_handles_running_before_success(
    tmp_path: Path, k8s_api: mock.MagicMock
) -> None:
    """Poll again when an InternalRequest is still running."""
    running_body = {
        "metadata": {"name": "ir-1"},
        "status": {
            "conditions": [{"reason": "Running"}],
            "pipelineRun": "pr-running",
        },
    }
    succeeded_body = {
        "metadata": {"name": "ir-1"},
        "status": {
            "conditions": [{"reason": "Succeeded"}],
            "pipelineRun": "pr-1",
        },
    }
    k8s_api.get_namespaced_custom_object.side_effect = [running_body, succeeded_body]
    output_patch, output_path = _patch_ir_output_path(tmp_path)

    with (
        output_patch,
        mock.patch.object(retry.retry.time, "sleep") as sleep,
        mock.patch.object(ir_module.time, "time", side_effect=[0, 1]),
    ):
        wait_for_completion(name="ir-1", timeout=600, k8s_api=k8s_api)

    sleep.assert_called_once_with(5)
    assert output_path.read_text(encoding="utf-8") == (
        '{"name": "ir-1", "pipelineRun": "pr-1"}\n'
    )


def test_wait_for_completion_writes_output_json_on_success(
    tmp_path: Path, k8s_api: mock.MagicMock
) -> None:
    """Write name and pipelineRun to the IR output file on success."""
    ir_body = {
        "metadata": {"name": "ir-1"},
        "status": {
            "conditions": [{"reason": "Succeeded"}],
            "pipelineRun": "pr-1",
        },
    }
    k8s_api.get_namespaced_custom_object.return_value = ir_body
    output_patch, output_path = _patch_ir_output_path(tmp_path)

    with (
        output_patch,
        mock.patch.object(retry.retry.time, "sleep"),
    ):
        wait_for_completion(name="ir-1", timeout=600, k8s_api=k8s_api)

    assert output_path.read_text(encoding="utf-8") == (
        '{"name": "ir-1", "pipelineRun": "pr-1"}\n'
    )


def test_wait_for_completion_raises_on_failure(
    tmp_path: Path, k8s_api: mock.MagicMock
) -> None:
    """Raise InternalRequestWaitError when an IR completes unsuccessfully."""
    ir_body = {
        "metadata": {"name": "ir-1"},
        "status": {
            "conditions": [{"reason": "Failed"}],
            "pipelineRun": "pr-1",
        },
    }
    k8s_api.get_namespaced_custom_object.return_value = ir_body
    output_patch, output_path = _patch_ir_output_path(tmp_path)

    with (
        output_patch,
        mock.patch.object(retry.retry.time, "sleep"),
        pytest.raises(InternalRequestWaitError) as exc_info,
    ):
        wait_for_completion(name="ir-1", timeout=600, k8s_api=k8s_api)

    assert exc_info.value.exit_code == EXIT_FAILED
    assert output_path.read_text(encoding="utf-8") == (
        '{"name": "ir-1", "pipelineRun": "pr-1"}\n'
    )


def test_wait_for_completion_raises_on_timeout(k8s_api: mock.MagicMock) -> None:
    """Raise InternalRequestWaitError when the wait timeout elapses."""
    ir_body = {
        "metadata": {"name": "ir-1"},
        "status": {"conditions": []},
    }
    k8s_api.get_namespaced_custom_object.return_value = ir_body

    with (
        mock.patch.object(retry.retry.time, "sleep"),
        mock.patch.object(ir_module.time, "time", side_effect=[0, 601]),
        pytest.raises(InternalRequestWaitError) as exc_info,
    ):
        wait_for_completion(name="ir-1", timeout=600, k8s_api=k8s_api)

    assert exc_info.value.exit_code == EXIT_TIMEOUT


def test_wait_for_completion_keeps_polling_when_label_selector_matches_nothing(
    k8s_api: mock.MagicMock,
) -> None:
    """Keep polling until timeout when a label selector matches no InternalRequests."""
    k8s_api.list_namespaced_custom_object.return_value = {"items": []}

    with (
        mock.patch.object(retry.retry.time, "sleep"),
        mock.patch.object(ir_module.time, "time", side_effect=[0, 601]),
        pytest.raises(InternalRequestWaitError) as exc_info,
    ):
        wait_for_completion(label_selector="foo=bar", timeout=600, k8s_api=k8s_api)

    assert exc_info.value.exit_code == EXIT_TIMEOUT


def test_fetch_results_returns_empty_when_no_results(k8s_api: mock.MagicMock) -> None:
    """Return an empty dict when the InternalRequest has no results."""
    k8s_api.get_namespaced_custom_object.return_value = {
        "metadata": {"name": "ir-1"},
        "status": {},
    }

    assert ir_module.fetch_results("ir-1", k8s_api=k8s_api) == {}


def test_fetch_results_parses_results(k8s_api: mock.MagicMock) -> None:
    """Return the status.results dict from the InternalRequest."""
    results = {"result": "Success", "advisory_url": "url"}
    k8s_api.get_namespaced_custom_object.return_value = {
        "metadata": {"name": "ir-1"},
        "status": {"results": results},
    }

    assert ir_module.fetch_results("ir-1", k8s_api=k8s_api) == results


def test_fetch_results_ignores_non_dict_results(k8s_api: mock.MagicMock) -> None:
    """Return an empty dict when status.results is not a dict."""
    k8s_api.get_namespaced_custom_object.return_value = {
        "metadata": {"name": "ir-1"},
        "status": {"results": ["not", "dict"]},
    }

    assert ir_module.fetch_results("ir-1", k8s_api=k8s_api) == {}


def test_create_internal_request_logs_created_name(k8s_api: mock.MagicMock) -> None:
    """Log the name of the created InternalRequest."""
    k8s_api.create_namespaced_custom_object.return_value = {
        "metadata": {"name": "ir-abc"},
    }

    with mock.patch.object(ir_module.logger, "info") as log_info:
        name = ir_module.create_internal_request({"kind": "InternalRequest"}, k8s_api=k8s_api)

    assert name == "ir-abc"
    logged = any("ir-abc" in str(call) for call in log_info.call_args_list)
    assert logged


def test_get_namespace_reads_from_service_account_file(tmp_path: Path) -> None:
    """Read namespace from the in-cluster service account file."""
    ns_file = tmp_path / "namespace"
    ns_file.write_text("my-ns\n")
    with mock.patch.object(ir_module, "_NAMESPACE_FILE", ns_file):
        assert ir_module._get_namespace() == "my-ns"


def test_get_namespace_falls_back_to_kubeconfig(tmp_path: Path) -> None:
    """Fall back to kubeconfig context when the SA file is missing."""
    ns_file = tmp_path / "namespace"
    context = {"context": {"namespace": "dev-ns"}}
    with (
        mock.patch.object(ir_module, "_NAMESPACE_FILE", ns_file),
        mock.patch.object(
            ir_module.k8s_config, "list_kube_config_contexts", return_value=([], context)
        ),
    ):
        assert ir_module._get_namespace() == "dev-ns"


def test_default_k8s_api_loads_incluster_config() -> None:
    """Load in-cluster config and return a CustomObjectsApi."""
    with (
        mock.patch.object(ir_module.k8s_config, "load_incluster_config") as incluster,
        mock.patch.object(ir_module.k8s_client, "CustomObjectsApi") as api_cls,
    ):
        result = ir_module._default_k8s_api()

    incluster.assert_called_once()
    api_cls.assert_called_once()
    assert result is api_cls.return_value


def test_default_k8s_api_falls_back_to_kubeconfig() -> None:
    """Fall back to kubeconfig when in-cluster config is unavailable."""
    from kubernetes.config import ConfigException

    with (
        mock.patch.object(
            ir_module.k8s_config,
            "load_incluster_config",
            side_effect=ConfigException,
        ),
        mock.patch.object(ir_module.k8s_config, "load_kube_config") as kubeconfig,
        mock.patch.object(ir_module.k8s_client, "CustomObjectsApi") as api_cls,
    ):
        result = ir_module._default_k8s_api()

    kubeconfig.assert_called_once()
    api_cls.assert_called_once()
    assert result is api_cls.return_value
