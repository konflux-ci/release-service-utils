"""Create and wait for InternalRequest resources in Kubernetes.

This module creates an InternalRequest resource in a Kubernetes cluster using
`kubectl`. Parameters are passed as Python mappings rather than CLI flags.


Sync and async behavior
-----------------------

In sync mode (the default), `create()` waits for the InternalRequest to reach
a completed status. On failure it raises `InternalRequestWaitError` with an
`exit_code` that mirrors the bash utilities:

    Succeeded          returns the InternalRequest name (no exception)
    Failed or rejected `exit_code` 21
    Timeout            `exit_code` 124

In async mode (`sync=False`), the InternalRequest is created and the function
returns immediately without waiting for status updates.


Usage
-----

::

    from internal_request import create

    ir_name = create(
        "create-advisory",
        params={
            "taskGitUrl": "https://github.com/konflux-ci/release-service-catalog.git",
            "taskGitRevision": "development",
            "advisory_name": "RHSA-2024:1234",
        },
        labels={
            "internal-services.appstudio.openshift.io/pipelinerun-uid": "<uid>",
        },
        sync=True,
        timeout=3600,
        service_account="release-service-account",
        pipeline_timeout="1h0m0s",
        task_timeout="0h55m0s",
        finally_timeout="0h5m0s",
    )


`create()` parameters
-------------------

`pipeline`
    Name of the pipeline under `internal/pipelines` in release-service-catalog.

`params`
    Mapping of parameter names to string values. Each entry becomes a
    `spec.params` field on the InternalRequest. Values may be valid JSON objects
    or arrays when serialized. `taskGitUrl` and `taskGitRevision` are required.

`labels`
    Optional mapping added to `metadata.labels`. When
    `internal-services.appstudio.openshift.io/pipelinerun-uid` is present,
    prior InternalRequests for the same pipeline run and pipeline name are
    deleted before creating a new one.

`sync`
    When true (default), block until the InternalRequest completes.

`timeout`
    Seconds to wait in sync mode. Defaults to 3600. Callers should allow at
    least ``duration_to_seconds(pipeline_timeout) + SPAWN_OVERHEAD_SECONDS`` so
    the poll loop outlives operator delay before the PipelineRun starts.

`service_account`
    Optional service account for the PipelineRun.

`pipeline_timeout`
    Total PipelineRun timeout in `XhYmZs` format. Defaults to `1h0m0s`.

`task_timeout`
    Task timeout in `XhYmZs` format. Defaults to `0h55m0s`.

`finally_timeout`
    Finally-task timeout in `XhYmZs` format. Defaults to `0h5m0s`.


Prerequisites
-------------

* `kubectl` must be installed and configured to communicate with the cluster.


Note:
----
Intended for clusters whose API includes the `InternalRequest` resource type
(`appstudio.redhat.com/v1alpha1`).

"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from release_service_utils.helpers import retry
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.subprocess_cmd import run_cmd

PIPELINE_NAME_LABEL = "internal-services.appstudio.openshift.io/pipeline-name"
PIPELINERUN_UID_LABEL = "internal-services.appstudio.openshift.io/pipelinerun-uid"
CLEANUP_PROPAGATION_SLEEP_SECONDS = 5
# Extra poll budget before the PipelineRun starts (operator reconcile and scheduling).
SPAWN_OVERHEAD_SECONDS = 300
EXIT_FAILED = 21
EXIT_TIMEOUT = 124
_DURATION_RE = re.compile(r"^(\d+)h(\d+)m(\d+)s$")


class InternalRequestWaitError(RuntimeError):
    """Raised when waiting for InternalRequests fails or times out."""

    def __init__(self, message: str, exit_code: int) -> None:
        """Store *message* and *exit_code* on the exception instance."""
        super().__init__(message)
        self.exit_code = exit_code


class _InternalRequestNotComplete(Exception):
    """Raised when InternalRequests have not yet reached a terminal state."""


def duration_to_seconds(duration: str) -> int:
    """Convert an `XhYmZs` duration string to seconds."""
    match = _DURATION_RE.fullmatch(duration)
    if match is None:
        msg = f"duration must use XhYmZs format: {duration!r}"
        raise ValueError(msg)
    hours, minutes, seconds = (int(match.group(i)) for i in range(1, 4))
    return (hours * 3600) + (minutes * 60) + seconds


def validate_timeouts(
    *,
    timeout: int,
    pipeline_timeout: str,
    task_timeout: str,
    finally_timeout: str,
) -> None:
    """Validate timeout strings and their relationships."""
    for label, value in (
        ("pipeline_timeout", pipeline_timeout),
        ("task_timeout", task_timeout),
        ("finally_timeout", finally_timeout),
    ):
        if _DURATION_RE.fullmatch(value) is None:
            msg = f"{label} must use XhYmZs format, where X, Y, and Z are integers"
            raise ValueError(msg)

    pipeline_timeout_secs = duration_to_seconds(pipeline_timeout)
    task_timeout_secs = duration_to_seconds(task_timeout)
    finally_timeout_secs = duration_to_seconds(finally_timeout)
    all_tasks_timeout = task_timeout_secs + finally_timeout_secs
    if all_tasks_timeout > pipeline_timeout_secs:
        msg = (
            "The sum of the task and finally timeout cannot exceed the pipeline "
            f"timeout. Pipeline timeout is {pipeline_timeout_secs} and the sum "
            f"of the others is {all_tasks_timeout}."
        )
        raise ValueError(msg)

    if pipeline_timeout_secs > timeout:
        logger.warning(
            "The passed pipeline timeout is greater than the script timeout. "
            "This means the script can fail before the pipeline times out, "
            "should it take that long.",
        )


def build_payload(
    *,
    pipeline: str,
    params: Mapping[str, str],
    labels: Mapping[str, str],
    pipeline_git_url: str,
    pipeline_git_revision: str,
    pipeline_timeout: str,
    task_timeout: str,
    finally_timeout: str,
    service_account: str | None,
) -> dict[str, Any]:
    """Build the InternalRequest manifest payload."""
    merged_labels = dict(labels)
    merged_labels[PIPELINE_NAME_LABEL] = pipeline

    payload: dict[str, Any] = {
        "apiVersion": "appstudio.redhat.com/v1alpha1",
        "kind": "InternalRequest",
        "metadata": {
            "generateName": f"{pipeline}-",
            "labels": merged_labels,
        },
        "spec": {
            "pipeline": {
                "pipelineRef": {
                    "resolver": "git",
                    "params": [
                        {"name": "url", "value": pipeline_git_url},
                        {"name": "revision", "value": pipeline_git_revision},
                        {
                            "name": "pathInRepo",
                            "value": f"pipelines/internal/{pipeline}/{pipeline}.yaml",
                        },
                    ],
                },
            },
            "params": dict(params),
            "timeouts": {
                "pipeline": pipeline_timeout,
                "tasks": task_timeout,
                "finally": finally_timeout,
            },
        },
    }
    if service_account:
        payload["spec"]["serviceAccount"] = service_account
    return payload


def _pipelinerun_uid_from_labels(labels: Mapping[str, str]) -> str:
    """Return the pipelinerun-uid label value when present."""
    return labels.get(PIPELINERUN_UID_LABEL, "")


def _fetch_internal_requests(
    *,
    name: str | None,
    label_selector: str | None,
) -> list[dict[str, Any]]:
    """Return InternalRequest objects as a list of parsed JSON dicts."""
    if name is not None:
        result = run_cmd(
            ["kubectl", "get", "internalrequest", name, "-o", "json"],
            check=True,
        )
        item = json.loads(result.stdout)
        return [item]

    result = run_cmd(
        [
            "kubectl",
            "get",
            "internalrequest",
            "-l",
            label_selector or "",
            "-o",
            "json",
        ],
        check=True,
    )
    data = json.loads(result.stdout)
    items = data.get("items")
    return items if isinstance(items, list) else []


def _print_conditions(internal_requests: Sequence[dict[str, Any]]) -> None:
    """Log InternalRequest condition summaries."""
    logger.info("Conditions:")
    for item in internal_requests:
        ir_name = item.get("metadata", {}).get("name", "")
        conditions = item.get("status", {}).get("conditions", [])
        logger.info("  %s: %s", ir_name, json.dumps(conditions, separators=(",", ":")))


def _ir_output_path(ir_name: str) -> Path:
    """Return the wait-sidecar output path for an InternalRequest name."""
    return Path(f"/tmp/{ir_name}-output.json")


def _append_ir_output(ir_name: str, pipeline_run: str) -> None:
    """Append InternalRequest completion metadata to the output file."""
    output_path = _ir_output_path(ir_name)
    payload = json.dumps({"name": ir_name, "pipelineRun": pipeline_run})
    logger.info("writing results to %s", output_path)
    with output_path.open("a", encoding="utf-8") as output_file:
        output_file.write(f"{payload}\n")


def wait_for_completion(
    *,
    name: str | None = None,
    label_selector: str | None = None,
    timeout: int = 600,
) -> None:
    """Block until InternalRequests complete or *timeout* seconds elapse.

    One of *name* or *label_selector* must be set, but not both.

    Raises:
        ValueError: When neither or both selectors are provided.
        InternalRequestWaitError: When an IR fails or the wait times out.

    """
    has_name = bool(name)
    has_labels = bool(label_selector)
    if has_name == has_labels:
        msg = "exactly one of name or label_selector must be set"
        raise ValueError(msg)

    end_time = time.time() + timeout
    written_outputs: set[str] = set()

    def _poll_once() -> None:
        logger.info("Checking IR statuses...")
        internal_requests = _fetch_internal_requests(
            name=name,
            label_selector=label_selector,
        )
        logger.info(
            "Found %d InternalRequests matching the name or label",
            len(internal_requests),
        )

        done_count = 0
        all_succeeded = True
        for item in internal_requests:
            ir_name = item.get("metadata", {}).get("name", "")
            conditions = item.get("status", {}).get("conditions") or []
            reason = ""
            if conditions and isinstance(conditions[0], dict):
                reason = str(conditions[0].get("reason") or "")
            pipeline_run = item.get("status", {}).get("pipelineRun") or ""

            if not reason:
                logger.info("  %s: no condition yet", ir_name)
            elif reason == "Running":
                logger.info("  %s: running - pipelineRun: %s", ir_name, pipeline_run)
            elif reason == "Succeeded":
                logger.info("  %s: succeeded - pipelineRun: %s", ir_name, pipeline_run)
                if ir_name and ir_name not in written_outputs:
                    _append_ir_output(ir_name, pipeline_run)
                    written_outputs.add(ir_name)
                done_count += 1
            else:
                logger.info("  %s: %s", ir_name, reason)
                if ir_name and ir_name not in written_outputs:
                    _append_ir_output(ir_name, pipeline_run)
                    written_outputs.add(ir_name)
                done_count += 1
                all_succeeded = False

        if internal_requests and done_count == len(internal_requests):
            if all_succeeded:
                _print_conditions(internal_requests)
                logger.info("Result: success")
                return
            _print_conditions(internal_requests)
            raise InternalRequestWaitError(
                "At least one InternalRequest failed",
                EXIT_FAILED,
            )

        if time.time() > end_time:
            _print_conditions(internal_requests)
            raise InternalRequestWaitError(
                "Timeout while waiting for the InternalRequests to complete",
                EXIT_TIMEOUT,
            )

        raise _InternalRequestNotComplete()

    # Enough attempts for the full timeout at the minimum 5s backoff interval.
    max_attempts = max(timeout // 5 + 1, 2)
    retry.retry_with_exponential_backoff(
        _poll_once,
        max_attempts=max_attempts,
        retry_on=_InternalRequestNotComplete,
        base_sleep_seconds=5,
    )


def cleanup_existing_requests(
    *,
    pipeline: str,
    labels: Mapping[str, str],
) -> None:
    """Delete prior InternalRequests for the same pipeline run and pipeline name."""
    pipelinerun_uid = _pipelinerun_uid_from_labels(labels)
    if not pipelinerun_uid:
        return

    label_selector = (
        f"{PIPELINERUN_UID_LABEL}={pipelinerun_uid}," f"{PIPELINE_NAME_LABEL}={pipeline}"
    )
    items = _fetch_internal_requests(name=None, label_selector=label_selector)
    if not items:
        return

    logger.info("Found existing InternalRequests from prior attempts. Cleaning up...")
    for item in items:
        if not isinstance(item, dict):
            continue
        ir_name = item.get("metadata", {}).get("name")
        if not isinstance(ir_name, str) or not ir_name:
            continue
        logger.info("Deleting InternalRequest %s...", ir_name)
        run_cmd(
            [
                "kubectl",
                "delete",
                "internalrequest",
                ir_name,
                "--wait=true",
                "--timeout=60s",
            ],
            check=True,
        )

    logger.info(
        "Cleanup complete. Waiting %ds for PipelineRun cancellation to propagate...",
        CLEANUP_PROPAGATION_SLEEP_SECONDS,
    )
    time.sleep(CLEANUP_PROPAGATION_SLEEP_SECONDS)


def create_internal_request(payload: dict[str, Any]) -> str:
    """Create an InternalRequest from *payload* and return its name."""
    result = run_cmd(
        ["kubectl", "create", "-f", "-", "-o", "json"],
        stdin=json.dumps(payload),
        check=True,
    )
    resource = json.loads(result.stdout)
    name = resource.get("metadata", {}).get("name")
    if not isinstance(name, str) or not name:
        msg = "kubectl create did not return an InternalRequest name"
        raise RuntimeError(msg)
    return name


def create(
    pipeline: str,
    *,
    params: Mapping[str, str],
    labels: Mapping[str, str] | None = None,
    sync: bool = True,
    timeout: int = 3600,
    service_account: str | None = None,
    pipeline_timeout: str = "1h0m0s",
    task_timeout: str = "0h55m0s",
    finally_timeout: str = "0h5m0s",
) -> str:
    """Create an InternalRequest and optionally wait for it to complete.

    Returns the created InternalRequest name. When *sync* is true, blocks until
    the InternalRequest completes using the same semantics as the bash utility.
    """
    if not pipeline:
        msg = "pipeline is required"
        raise ValueError(msg)

    merged_labels = dict(labels or {})
    merged_params = dict(params)
    pipeline_git_url = merged_params.get("taskGitUrl")
    pipeline_git_revision = merged_params.get("taskGitRevision")
    if not pipeline_git_url or not pipeline_git_revision:
        msg = (
            "params must include taskGitUrl and taskGitRevision for the git "
            "resolver pipeline reference"
        )
        raise ValueError(msg)

    validate_timeouts(
        timeout=timeout,
        pipeline_timeout=pipeline_timeout,
        task_timeout=task_timeout,
        finally_timeout=finally_timeout,
    )
    cleanup_existing_requests(pipeline=pipeline, labels=merged_labels)

    payload = build_payload(
        pipeline=pipeline,
        params=merged_params,
        labels=merged_labels,
        pipeline_git_url=pipeline_git_url,
        pipeline_git_revision=pipeline_git_revision,
        pipeline_timeout=pipeline_timeout,
        task_timeout=task_timeout,
        finally_timeout=finally_timeout,
        service_account=service_account,
    )
    internal_request_name = create_internal_request(payload)
    logger.info("InternalRequest '%s' created.", internal_request_name)

    if sync:
        logger.info("Sync flag set to true. Waiting for the InternalRequest to complete.")
        wait_for_completion(name=internal_request_name, timeout=timeout)

    return internal_request_name
