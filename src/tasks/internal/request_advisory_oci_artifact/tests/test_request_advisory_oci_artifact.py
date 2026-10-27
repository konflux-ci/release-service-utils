"""Tests for the request-advisory-oci-artifact task."""

from __future__ import annotations

from pathlib import Path
from unittest import mock

import pytest

from release_service_utils.tasks.internal.request_advisory_oci_artifact import (
    request_advisory_oci_artifact as task,
)


def _params(result_path: Path) -> task.TaskParams:
    """Return representative task parameters using *result_path*."""
    return task.TaskParams(
        advisory_url="https://example.test/advisory.yaml",
        request_timeout="2700",
        pipeline_run_uid="pipeline-run-uid",
        task_git_url="https://example.test/catalog.git",
        task_git_revision="development",
        advisory_oci_artifact_result=result_path,
    )


def test_run_writes_advisory_oci_artifact_and_creates_expected_request(tmp_path: Path) -> None:
    """Successful InternalRequest results write the OCI artifact pullspec."""
    result_path = tmp_path / "advisory-oci-artifact"
    params = _params(result_path)
    results = {
        "result": "Success",
        "advisory_oci_artifact": "oci:quay.io/example/advisory@sha256:abc",
        "internalRequestPipelineRunName": "pipeline-run",
        "internalRequestTaskRunName": "task-run",
    }
    with (
        mock.patch.object(
            task.internal_request, "create", return_value="request-123"
        ) as create,
        mock.patch.object(
            task.internal_request, "fetch_results", return_value=results
        ) as fetch,
    ):
        task.run_request_advisory_oci_artifact(params)

    assert result_path.read_text(encoding="utf-8") == results["advisory_oci_artifact"]
    create.assert_called_once_with(
        "create-advisory-oci-artifact",
        params={
            "advisory_url": params.advisory_url,
            "advisory_secret_name": "create-advisory-stage-secret",
            "taskGitUrl": params.task_git_url,
            "taskGitRevision": params.task_git_revision,
        },
        labels={task.internal_request.PIPELINERUN_UID_LABEL: params.pipeline_run_uid},
        sync=True,
        timeout=int(params.request_timeout),
    )
    fetch.assert_called_once_with("request-123")


def test_run_raises_when_internal_request_result_is_not_success(tmp_path: Path) -> None:
    """Failed InternalRequest results retain the catalog task's failure behavior."""
    with (
        mock.patch.object(task.internal_request, "create", return_value="request-123"),
        mock.patch.object(
            task.internal_request,
            "fetch_results",
            return_value={"result": "Failed"},
        ),
        pytest.raises(RuntimeError, match="ADVISORY_OCI_ARTIFACT"),
    ):
        task.run_request_advisory_oci_artifact(_params(tmp_path / "result"))


def test_run_raises_when_successful_internal_request_omits_artifact(tmp_path: Path) -> None:
    """Successful InternalRequests must provide a non-empty OCI artifact."""
    with (
        mock.patch.object(task.internal_request, "create", return_value="request-123"),
        mock.patch.object(
            task.internal_request,
            "fetch_results",
            return_value={"result": "Success"},
        ),
        pytest.raises(RuntimeError, match="non-empty ADVISORY_OCI_ARTIFACT"),
    ):
        task.run_request_advisory_oci_artifact(_params(tmp_path / "result"))


@pytest.mark.parametrize(
    ("value", "expected"),
    [(None, "null"), (True, "true"), (4, "4"), ("artifact", "artifact")],
)
def test_jq_raw_value_matches_jq_raw_output(value: object, expected: str) -> None:
    """Scalar values use jq-compatible raw serialization."""
    assert task._jq_raw_value(value) == expected


def test_main_reads_tekton_environment_and_writes_success_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The entry point translates Tekton environment variables into task parameters."""
    artifact_result_path = tmp_path / "advisory-oci-artifact"
    result_path = tmp_path / "result"
    artifact_result_path.write_text("stale", encoding="utf-8")
    monkeypatch.setenv("RESULT_ADVISORY_OCI_ARTIFACT", str(artifact_result_path))
    monkeypatch.setenv("RESULT_RESULT", str(result_path))
    monkeypatch.setenv("PARAM_ADVISORY_URL", "https://example.test/advisory.yaml")
    monkeypatch.setenv("PARAM_REQUEST_TIMEOUT", "2700")
    monkeypatch.setenv("PARAM_PIPELINE_RUN_UID", "pipeline-run-uid")
    monkeypatch.setenv("PARAM_TASK_GIT_URL", "https://example.test/catalog.git")
    monkeypatch.setenv("PARAM_TASK_GIT_REVISION", "development")
    with mock.patch.object(task, "run_request_advisory_oci_artifact") as run:
        assert task.main() == 0

    assert artifact_result_path.read_text(encoding="utf-8") == ""
    assert result_path.read_text(encoding="utf-8") == "Success"
    assert run.call_args.args[0] == _params(artifact_result_path)


def test_main_writes_failure_result_and_exits_successfully(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Internal task errors are published in ``result`` without failing the step."""
    artifact_result_path = tmp_path / "advisory-oci-artifact"
    result_path = tmp_path / "result"
    monkeypatch.setenv("RESULT_ADVISORY_OCI_ARTIFACT", str(artifact_result_path))
    monkeypatch.setenv("RESULT_RESULT", str(result_path))
    monkeypatch.setenv("PARAM_ADVISORY_URL", "https://example.test/advisory.yaml")
    monkeypatch.setenv("PARAM_REQUEST_TIMEOUT", "2700")
    monkeypatch.setenv("PARAM_PIPELINE_RUN_UID", "pipeline-run-uid")
    monkeypatch.setenv("PARAM_TASK_GIT_URL", "https://example.test/catalog.git")
    monkeypatch.setenv("PARAM_TASK_GIT_REVISION", "development")
    with mock.patch.object(
        task,
        "run_request_advisory_oci_artifact",
        side_effect=RuntimeError("request failed"),
    ):
        assert task.main() == 0

    assert artifact_result_path.read_text(encoding="utf-8") == ""
    assert "request_advisory_oci_artifact.py" in result_path.read_text(encoding="utf-8")
    assert "request failed" in result_path.read_text(encoding="utf-8")


def test_params_from_env_rejects_non_positive_timeout(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Request timeouts must be positive seconds."""
    monkeypatch.setenv("RESULT_ADVISORY_OCI_ARTIFACT", str(tmp_path / "result"))
    monkeypatch.setenv("RESULT_RESULT", str(tmp_path / "task-result"))
    monkeypatch.setenv("PARAM_REQUEST_TIMEOUT", "0")
    with pytest.raises(ValueError, match="positive"):
        task._params_from_env()


@pytest.mark.parametrize(
    "missing_env_var",
    ["PARAM_ADVISORY_URL", "RESULT_ADVISORY_OCI_ARTIFACT"],
)
def test_main_writes_failure_result_for_missing_tekton_input(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    missing_env_var: str,
) -> None:
    """Missing inputs are reported through ``RESULT_RESULT`` when it is available."""
    artifact_result_path = tmp_path / "advisory-oci-artifact"
    result_path = tmp_path / "result"
    monkeypatch.setenv("RESULT_ADVISORY_OCI_ARTIFACT", str(artifact_result_path))
    monkeypatch.setenv("RESULT_RESULT", str(result_path))
    monkeypatch.setenv("PARAM_ADVISORY_URL", "https://example.test/advisory.yaml")
    monkeypatch.setenv("PARAM_REQUEST_TIMEOUT", "2700")
    monkeypatch.setenv("PARAM_PIPELINE_RUN_UID", "pipeline-run-uid")
    monkeypatch.setenv("PARAM_TASK_GIT_URL", "https://example.test/catalog.git")
    monkeypatch.setenv("PARAM_TASK_GIT_REVISION", "development")
    monkeypatch.delenv(missing_env_var)

    assert task.main() == 0

    failure_result = result_path.read_text(encoding="utf-8")
    assert "request_advisory_oci_artifact.py" in failure_result
    assert "Failed while requesting the advisory OCI artifact" in failure_result
