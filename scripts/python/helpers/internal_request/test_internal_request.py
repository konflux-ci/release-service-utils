"""Test internal_request helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
import retry
from internal_request import internal_request as ir_module
from internal_request.internal_request import (
    EXIT_FAILED,
    EXIT_TIMEOUT,
    InternalRequestWaitError,
    wait_for_completion,
)


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


def _completed_process(stdout: str, returncode: int = 0) -> mock.MagicMock:
    result = mock.MagicMock()
    result.stdout = stdout
    result.returncode = returncode
    return result


def test_cleanup_existing_requests_deletes_matching_irs() -> None:
    """Delete existing InternalRequests before creating a new one."""
    calls: list[list[str]] = []

    def fake_run_cmd(cmd: list[str], **kwargs: Any) -> mock.MagicMock:
        calls.append(cmd)
        if cmd[0:3] == ["kubectl", "get", "internalrequest"]:
            body = {"items": [{"metadata": {"name": "old-ir-1"}}]}
            return _completed_process(json.dumps(body))
        return _completed_process("")

    with (
        mock.patch.object(ir_module, "run_cmd", side_effect=fake_run_cmd),
        mock.patch.object(ir_module.time, "sleep"),
    ):
        ir_module.cleanup_existing_requests(
            pipeline="create-advisory",
            labels={ir_module.PIPELINERUN_UID_LABEL: "uid-123"},
        )

    assert calls[0] == [
        "kubectl",
        "get",
        "internalrequest",
        "-l",
        (
            f"{ir_module.PIPELINERUN_UID_LABEL}=uid-123,"
            f"{ir_module.PIPELINE_NAME_LABEL}=create-advisory"
        ),
        "-o",
        "json",
    ]
    assert calls[1][:4] == ["kubectl", "delete", "internalrequest", "old-ir-1"]


def test_cleanup_existing_requests_skips_without_pipelinerun_uid() -> None:
    """Skip cleanup when the pipelinerun-uid label is absent."""
    with mock.patch.object(ir_module, "run_cmd") as fake_run_cmd:
        ir_module.cleanup_existing_requests(
            pipeline="create-advisory",
            labels={"other": "value"},
        )
    fake_run_cmd.assert_not_called()


def test_cleanup_existing_requests_skips_when_no_matching_items() -> None:
    """Skip deletion when no existing InternalRequests are found."""
    with mock.patch.object(ir_module, "run_cmd") as fake_run_cmd:
        fake_run_cmd.return_value = _completed_process(json.dumps({"items": []}))
        ir_module.cleanup_existing_requests(
            pipeline="create-advisory",
            labels={ir_module.PIPELINERUN_UID_LABEL: "uid-123"},
        )

    fake_run_cmd.assert_called_once()


def test_cleanup_existing_requests_skips_non_dict_items() -> None:
    """Ignore list entries that are not InternalRequest objects."""
    calls: list[list[str]] = []

    def fake_run_cmd(cmd: list[str], **kwargs: Any) -> mock.MagicMock:
        calls.append(cmd)
        if cmd[0:3] == ["kubectl", "get", "internalrequest"]:
            body = {"items": ["not-a-dict", {"metadata": {"name": "old-ir-1"}}]}
            return _completed_process(json.dumps(body))
        return _completed_process("")

    with (
        mock.patch.object(ir_module, "run_cmd", side_effect=fake_run_cmd),
        mock.patch.object(ir_module.time, "sleep"),
    ):
        ir_module.cleanup_existing_requests(
            pipeline="create-advisory",
            labels={ir_module.PIPELINERUN_UID_LABEL: "uid-123"},
        )

    assert calls[1][:4] == ["kubectl", "delete", "internalrequest", "old-ir-1"]


def test_cleanup_existing_requests_skips_invalid_ir_name() -> None:
    """Ignore InternalRequests whose metadata name is missing or not a string."""
    calls: list[list[str]] = []

    def fake_run_cmd(cmd: list[str], **kwargs: Any) -> mock.MagicMock:
        calls.append(cmd)
        if cmd[0:3] == ["kubectl", "get", "internalrequest"]:
            body = {
                "items": [
                    {"metadata": {}},
                    {"metadata": {"name": ""}},
                    {"metadata": {"name": 123}},
                ],
            }
            return _completed_process(json.dumps(body))
        return _completed_process("")

    with (
        mock.patch.object(ir_module, "run_cmd", side_effect=fake_run_cmd),
        mock.patch.object(ir_module.time, "sleep"),
    ):
        ir_module.cleanup_existing_requests(
            pipeline="create-advisory",
            labels={ir_module.PIPELINERUN_UID_LABEL: "uid-123"},
        )

    assert len(calls) == 1


def test_create_creates_internal_request_without_waiting() -> None:
    """Create an InternalRequest and return its name when sync is false."""
    calls: list[list[str]] = []

    def fake_run_cmd(cmd: list[str], **kwargs: Any) -> mock.MagicMock:
        calls.append(cmd)
        if cmd[0:2] == ["kubectl", "create"]:
            body = {"metadata": {"name": "create-advisory-abc"}}
            return _completed_process(json.dumps(body))
        return _completed_process(json.dumps({"items": []}))

    with (
        mock.patch.object(ir_module, "run_cmd", side_effect=fake_run_cmd),
        mock.patch.object(ir_module.time, "sleep"),
    ):
        name = ir_module.create(
            "create-advisory",
            params={
                "taskGitUrl": "https://example.test/catalog",
                "taskGitRevision": "main",
            },
            sync=False,
        )

    assert name == "create-advisory-abc"
    assert any(cmd[0:2] == ["kubectl", "create"] for cmd in calls)


def test_create_requires_task_git_params() -> None:
    """Reject creation when git resolver params are missing."""
    with pytest.raises(ValueError, match="taskGitUrl and taskGitRevision"):
        ir_module.create(
            "create-advisory",
            params={"componentGroup": "myapp"},
            sync=False,
        )


def test_create_requires_pipeline() -> None:
    """Reject creation when the pipeline name is empty."""
    with pytest.raises(ValueError, match="pipeline is required"):
        ir_module.create(
            "",
            params={
                "taskGitUrl": "https://example.test/catalog",
                "taskGitRevision": "main",
            },
            sync=False,
        )


def test_create_internal_request_raises_when_name_missing() -> None:
    """Raise when kubectl create does not return an InternalRequest name."""
    with (
        mock.patch.object(
            ir_module,
            "run_cmd",
            return_value=_completed_process(json.dumps({"metadata": {}})),
        ),
        pytest.raises(RuntimeError, match="did not return an InternalRequest name"),
    ):
        ir_module.create_internal_request({"kind": "InternalRequest"})


def test_create_waits_when_sync_is_true() -> None:
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
        )

    assert name == "create-advisory-abc"
    wait.assert_called_once_with(name="create-advisory-abc", timeout=3600)


def test_wait_for_completion_requires_exactly_one_selector() -> None:
    """Reject calls that provide both or neither selector."""
    with pytest.raises(ValueError, match="exactly one"):
        wait_for_completion()

    with pytest.raises(ValueError, match="exactly one"):
        wait_for_completion(name="ir-1", label_selector="foo=bar")


def _patch_ir_output_path(tmp_path: Path, ir_name: str = "ir-1") -> tuple[mock._patch, Path]:
    """Patch ``_ir_output_path`` to write under *tmp_path* for isolated tests."""
    output_path = tmp_path / f"{ir_name}-output.json"
    patch = mock.patch.object(ir_module, "_ir_output_path", return_value=output_path)
    return patch, output_path


def test_wait_for_completion_handles_running_before_success(tmp_path: Path) -> None:
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
    responses = [running_body, succeeded_body]
    output_patch, output_path = _patch_ir_output_path(tmp_path)

    def fake_run_cmd(cmd: list[str], **kwargs: Any) -> mock.MagicMock:
        return _completed_process(json.dumps(responses.pop(0)))

    with (
        output_patch,
        mock.patch.object(ir_module, "run_cmd", side_effect=fake_run_cmd),
        mock.patch.object(retry.time, "sleep") as sleep,
        mock.patch.object(ir_module.time, "time", side_effect=[0, 1]),
    ):
        wait_for_completion(name="ir-1", timeout=600)

    sleep.assert_called_once_with(5)
    assert output_path.read_text(encoding="utf-8") == (
        '{"name": "ir-1", "pipelineRun": "pr-1"}\n'
    )


def test_wait_for_completion_writes_output_json_on_success(tmp_path: Path) -> None:
    """Write name and pipelineRun to the IR output file on success."""
    ir_body = {
        "metadata": {"name": "ir-1"},
        "status": {
            "conditions": [{"reason": "Succeeded"}],
            "pipelineRun": "pr-1",
        },
    }
    output_patch, output_path = _patch_ir_output_path(tmp_path)

    def fake_run_cmd(cmd: list[str], **kwargs: Any) -> mock.MagicMock:
        return _completed_process(json.dumps(ir_body))

    with (
        output_patch,
        mock.patch.object(ir_module, "run_cmd", side_effect=fake_run_cmd),
        mock.patch.object(retry.time, "sleep"),
    ):
        wait_for_completion(name="ir-1", timeout=600)

    assert output_path.read_text(encoding="utf-8") == (
        '{"name": "ir-1", "pipelineRun": "pr-1"}\n'
    )


def test_wait_for_completion_raises_on_failure(tmp_path: Path) -> None:
    """Raise InternalRequestWaitError when an IR completes unsuccessfully."""
    ir_body = {
        "metadata": {"name": "ir-1"},
        "status": {
            "conditions": [{"reason": "Failed"}],
            "pipelineRun": "pr-1",
        },
    }
    output_patch, output_path = _patch_ir_output_path(tmp_path)

    def fake_run_cmd(cmd: list[str], **kwargs: Any) -> mock.MagicMock:
        return _completed_process(json.dumps(ir_body))

    with (
        output_patch,
        mock.patch.object(ir_module, "run_cmd", side_effect=fake_run_cmd),
        mock.patch.object(retry.time, "sleep"),
        pytest.raises(InternalRequestWaitError) as exc_info,
    ):
        wait_for_completion(name="ir-1", timeout=600)

    assert exc_info.value.exit_code == EXIT_FAILED
    assert output_path.read_text(encoding="utf-8") == (
        '{"name": "ir-1", "pipelineRun": "pr-1"}\n'
    )


def test_wait_for_completion_raises_on_timeout() -> None:
    """Raise InternalRequestWaitError when the wait timeout elapses."""
    ir_body = {
        "metadata": {"name": "ir-1"},
        "status": {"conditions": []},
    }

    def fake_run_cmd(cmd: list[str], **kwargs: Any) -> mock.MagicMock:
        return _completed_process(json.dumps(ir_body))

    with (
        mock.patch.object(ir_module, "run_cmd", side_effect=fake_run_cmd),
        mock.patch.object(retry.time, "sleep"),
        mock.patch.object(ir_module.time, "time", side_effect=[0, 601]),
        pytest.raises(InternalRequestWaitError) as exc_info,
    ):
        wait_for_completion(name="ir-1", timeout=600)

    assert exc_info.value.exit_code == EXIT_TIMEOUT


def test_wait_for_completion_keeps_polling_when_label_selector_matches_nothing() -> None:
    """Keep polling until timeout when a label selector matches no InternalRequests."""
    empty_list_body = {"items": []}

    def fake_run_cmd(cmd: list[str], **kwargs: Any) -> mock.MagicMock:
        return _completed_process(json.dumps(empty_list_body))

    with (
        mock.patch.object(ir_module, "run_cmd", side_effect=fake_run_cmd),
        mock.patch.object(retry.time, "sleep"),
        mock.patch.object(ir_module.time, "time", side_effect=[0, 601]),
        pytest.raises(InternalRequestWaitError) as exc_info,
    ):
        wait_for_completion(label_selector="foo=bar", timeout=600)

    assert exc_info.value.exit_code == EXIT_TIMEOUT
