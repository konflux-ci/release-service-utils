#!/usr/bin/env python3
"""Request an advisory OCI artifact through an InternalRequest."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from release_service_utils.helpers import internal_request
from release_service_utils.helpers import tekton
from release_service_utils.helpers.logger import logger

PROG = "request_advisory_oci_artifact.py"
_PIPELINE = "create-advisory-oci-artifact"


@dataclass(frozen=True)
class TaskParams:
    """Hold Tekton parameters and the advisory OCI artifact result path."""

    advisory_url: str
    request_timeout: str
    pipeline_run_uid: str
    task_git_url: str
    task_git_revision: str
    advisory_oci_artifact_result: Path


def _jq_raw_value(value: Any) -> str:
    """Return *value* using the scalar representation emitted by ``jq -r``."""
    if isinstance(value, str):
        return value
    return json.dumps(value, separators=(",", ":"))


def run_request_advisory_oci_artifact(params: TaskParams) -> None:
    """Create the InternalRequest, validate it, and write its OCI artifact result."""
    internal_request_name = internal_request.create(
        _PIPELINE,
        params={
            "advisory_url": params.advisory_url,
            "advisory_secret_name": "create-advisory-stage-secret",
            "taskGitUrl": params.task_git_url,
            "taskGitRevision": params.task_git_revision,
        },
        labels={internal_request.PIPELINERUN_UID_LABEL: params.pipeline_run_uid},
        sync=True,
        timeout=int(params.request_timeout),
    )
    logger.info("done (%s)", internal_request_name)

    results = internal_request.fetch_results(internal_request_name)
    pipeline_run_name = str(results.get("internalRequestPipelineRunName") or "")
    task_run_name = str(results.get("internalRequestTaskRunName") or "")
    logger.info("** internalRequestPipelineRunName: %s", pipeline_run_name)
    logger.info("** internalRequestTaskRunName: %s", task_run_name)

    if results.get("result") != "Success":
        logger.error("The InternalRequest to find the ADVISORY_OCI_ARTIFACT was unsuccessful")
        logger.error("%s", _jq_raw_value(results.get("result")))
        msg = "The InternalRequest to find the ADVISORY_OCI_ARTIFACT was unsuccessful"
        raise RuntimeError(msg)

    advisory_oci_artifact = results.get("advisory_oci_artifact")
    if not isinstance(advisory_oci_artifact, str) or not advisory_oci_artifact.strip():
        msg = "The InternalRequest did not return a non-empty ADVISORY_OCI_ARTIFACT"
        raise RuntimeError(msg)
    logger.info("Got ADVISORY_OCI_ARTIFACT: %s", advisory_oci_artifact)
    params.advisory_oci_artifact_result.write_text(advisory_oci_artifact, encoding="utf-8")


def _params_from_env() -> TaskParams:
    """Build task parameters from Tekton ``PARAM_*`` and ``RESULT_*`` variables."""
    (advisory_oci_artifact_result,) = tekton.result_paths_from_env(
        "RESULT_ADVISORY_OCI_ARTIFACT",
    )
    request_timeout = tekton.require_env("PARAM_REQUEST_TIMEOUT")
    if int(request_timeout) <= 0:
        msg = "PARAM_REQUEST_TIMEOUT must be a positive integer"
        raise ValueError(msg)
    return TaskParams(
        advisory_url=tekton.require_env("PARAM_ADVISORY_URL"),
        request_timeout=request_timeout,
        pipeline_run_uid=tekton.require_env("PARAM_PIPELINE_RUN_UID"),
        task_git_url=tekton.require_env("PARAM_TASK_GIT_URL"),
        task_git_revision=tekton.require_env("PARAM_TASK_GIT_REVISION"),
        advisory_oci_artifact_result=advisory_oci_artifact_result,
    )


def main() -> int:
    """Read Tekton inputs and always publish the success or failure result."""
    (result_path,) = tekton.result_paths_from_env("RESULT_RESULT")
    try:
        params = _params_from_env()
        params.advisory_oci_artifact_result.write_text("", encoding="utf-8")
        run_request_advisory_oci_artifact(params)
    except (Exception, SystemExit) as exc:
        if isinstance(exc, SystemExit) and exc.code in (0, None):
            raise
        tekton.write_failure_result(
            result_path,
            PROG,
            exc,
            workflow_action="requesting the advisory OCI artifact",
        )
        return 0

    result_path.write_text("Success", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
