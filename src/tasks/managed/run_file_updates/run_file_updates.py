#!/usr/bin/env python3
"""Create InternalRequests for file updates and collect merge request results."""

from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from release_service_utils.helpers import file, tekton
from release_service_utils.helpers.internal_request import (
    InternalRequestWaitError,
    SPAWN_OVERHEAD_SECONDS,
    create,
    fetch_results,
    seconds_to_duration,
    wait_for_completion,
)
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.subprocess_cmd import run_cmd_text

TASK_LABEL = "internal-services.appstudio.openshift.io/group-id"
REQUEST_LABEL = "internal-services.appstudio.openshift.io/request-id"
PIPELINERUN_LABEL = "internal-services.appstudio.openshift.io/pipelinerun-uid"
DEFAULT_FILE_UPDATES_SECRET = "file-updates-secret"
DEFAULT_REF = "main"
_NO_CHANGE_MESSAGE = "nothing needs change"


class FileUpdatesError(Exception):
    """Raised when a file update request fails."""


@dataclass(frozen=True)
class FileUpdateItem:
    """One file update item parsed from the RPA data."""

    repo: str
    upstream_repo: str
    ref: str
    paths: list[dict[str, Any]]
    file_updates_secret: str


def _evaluate_path_expressions(
    paths: list[dict[str, Any]],
    snapshot_path: Path,
) -> list[dict[str, Any]]:
    """Evaluate {{ }} expressions with the update-paths CLI.

    The catalog runs ``update-paths -p paths -f snapshot``. That command
    owns the substitution behavior, including a null replacements list.
    """
    logger.info("Evaluating path expressions with update-paths")
    try:
        raw = run_cmd_text(
            [
                "update-paths",
                "-p",
                json.dumps(paths),
                "-f",
                str(snapshot_path),
            ]
        )
    except subprocess.CalledProcessError as e:
        detail = (e.stderr or e.output or "").strip()
        raise FileUpdatesError(f"Failed to evaluate path expressions: {detail}") from e
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise FileUpdatesError(
            f"update-paths did not return a paths list: {raw.strip()}"
        ) from e
    if not isinstance(parsed, list):
        raise FileUpdatesError("update-paths did not return a paths list")
    return parsed


def _jq_raw(value: Any) -> str:
    """Render *value* as ``jq -r`` prints it.

    Check bool before other types: ``bool`` is a subclass of ``int``, and
    ``str(True)`` is ``True`` rather than ``true``. JSON null is ``null``.
    """
    if isinstance(value, bool):
        return json.dumps(value)
    if value is None:
        return "null"
    return str(value)


def _jq_or(value: Any, default: str) -> str:
    """Render *value* as ``jq -r`` with a ``// default`` alternative.

    The alternative applies when *value* is null or false. An empty string
    is kept, and boolean true renders as ``true``.
    """
    if value is None or value is False:
        return default
    return _jq_raw(value)


def _parse_file_update_item(item: dict[str, Any]) -> FileUpdateItem:
    """Parse a single file update item from the RPA data."""
    return FileUpdateItem(
        repo=_jq_raw(item.get("repo")),
        upstream_repo=_jq_raw(item.get("upstream_repo")),
        ref=_jq_or(item.get("ref"), DEFAULT_REF),
        paths=item.get("paths") or [],
        file_updates_secret=_jq_or(
            item.get("file_updates_secret"),
            DEFAULT_FILE_UPDATES_SECRET,
        ),
    )


def _wait_timeout_seconds(request_timeout: int) -> int:
    """Return the poll budget for a file-update InternalRequest."""
    pipeline_timeout_secs = request_timeout + 300
    return pipeline_timeout_secs + SPAWN_OVERHEAD_SECONDS


def _create_file_update_request(
    item: FileUpdateItem,
    component_group: str,
    updated_paths: list[dict[str, Any]],
    request_timeout: int,
    task_git_url: str,
    task_git_revision: str,
    pipelinerun_uid: str,
    task_uid: str,
    request_id: str,
    pipeline_name: str,
    sync: bool,
) -> str:
    """Create an InternalRequest for a single file update item."""
    pipeline_timeout_secs = request_timeout + 300
    pipeline_timeout = seconds_to_duration(pipeline_timeout_secs)
    task_timeout = seconds_to_duration(request_timeout)
    wait_timeout = _wait_timeout_seconds(request_timeout)

    logger.info("Creating InternalRequest for repo: %s", item.repo)

    params = {
        "upstream_repo": item.upstream_repo,
        "repo": item.repo,
        "ref": item.ref,
        "paths": json.dumps(updated_paths),
        "componentGroup": component_group,
        "file_updates_secret": item.file_updates_secret,
        "taskGitUrl": task_git_url,
        "taskGitRevision": task_git_revision,
    }

    labels = {
        TASK_LABEL: task_uid,
        REQUEST_LABEL: request_id,
        PIPELINERUN_LABEL: pipelinerun_uid,
    }

    ir_name = create(
        pipeline_name,
        params=params,
        labels=labels,
        sync=sync,
        timeout=wait_timeout,
        pipeline_timeout=pipeline_timeout,
        task_timeout=task_timeout,
        finally_timeout="0h5m0s",
        cleanup=False,
    )

    return ir_name


def _merge_request_from_build_info(build_info: str) -> str | None:
    """Return a merge request URL from a successful file-updates result.

    Accept a standalone ``{"merge_request": url}`` object or that same object
    embedded in text. Return None for the explicit no-change message and when
    no merge request URL can be parsed.
    """
    text = build_info.strip()
    if text == _NO_CHANGE_MESSAGE:
        return None

    decoder = json.JSONDecoder()
    start = 0
    while True:
        brace = text.find("{", start)
        if brace < 0:
            return None
        try:
            parsed, _ = decoder.raw_decode(text, brace)
        except json.JSONDecodeError:
            start = brace + 1
            continue
        if isinstance(parsed, dict):
            merge_request = parsed.get("merge_request")
            if isinstance(merge_request, str) and merge_request.strip():
                return merge_request.strip()
        start = brace + 1


def _failure_error_from_results(results: dict[str, Any]) -> FileUpdatesError:
    """Build the catalog failure message from InternalRequest results."""
    build_state = results.get("buildState", "")
    json_build_info = results.get("jsonBuildInfo", "{}")
    try:
        build_info = json.loads(json_build_info)
        error_msg = build_info.get("error", "unknown error")
        diff_str = build_info.get("str", "")
    except json.JSONDecodeError:
        error_msg = "Failed to parse build info"
        diff_str = json_build_info
    return FileUpdatesError(
        f"FileUpdates Error (buildState={build_state}): {error_msg}\n"
        f"Diff (content might be truncated): {diff_str}"
    )


def _has_failure_report(results: dict[str, Any]) -> bool:
    """Return whether *results* include a failed build error and diff."""
    build_state = results.get("buildState")
    json_build_info = results.get("jsonBuildInfo")
    if build_state in ("Success", "Succeeded", None, ""):
        return False
    return isinstance(json_build_info, str) and bool(json_build_info.strip())


def _error_after_failed_wait(
    ir_name: str,
    wait_error: InternalRequestWaitError,
) -> FileUpdatesError:
    """Return task failure details, or the wait error when results are missing."""
    try:
        results = fetch_results(ir_name)
    except Exception as fetch_error:
        logger.error(
            "Could not read results for InternalRequest %s after wait failure: %s",
            ir_name,
            fetch_error,
        )
        return FileUpdatesError(str(wait_error))
    if results:
        _log_internal_run_names(results)
    if _has_failure_report(results):
        return _failure_error_from_results(results)
    return FileUpdatesError(str(wait_error))


def _log_internal_run_names(results: dict[str, Any]) -> None:
    """Log the internal PipelineRun and TaskRun that produced these results."""
    logger.info(
        "** internalRequestPipelineRunName: %s",
        results.get("internalRequestPipelineRunName", ""),
    )
    logger.info(
        "** internalRequestTaskRunName: %s",
        results.get("internalRequestTaskRunName", ""),
    )


def _process_ir_results(ir_name: str) -> str | None:
    """Fetch and process InternalRequest results.

    Return the merge request URL on success, or None only for the explicit
    no-change message. Raise FileUpdatesError when the request failed or a
    success result has no merge request URL.
    """
    results = fetch_results(ir_name)

    if not results:
        raise FileUpdatesError(f"InternalRequest {ir_name} returned empty results")

    _log_internal_run_names(results)
    build_state = results.get("buildState", "")
    logger.info("InternalRequest %s buildState: %s", ir_name, build_state)

    if build_state not in ("Success", "Succeeded"):
        raise _failure_error_from_results(results)

    json_build_info = results.get("jsonBuildInfo", "")
    if not isinstance(json_build_info, str):
        json_build_info = str(json_build_info)
    merge_request = _merge_request_from_build_info(json_build_info)
    if merge_request:
        logger.info("MR Created: %s", merge_request)
        return merge_request
    if json_build_info.strip() == _NO_CHANGE_MESSAGE:
        logger.info("No file changes for InternalRequest %s", ir_name)
        return None
    raise FileUpdatesError(
        f"InternalRequest {ir_name} succeeded without a merge request URL: {json_build_info}"
    )


def _write_results_json(results_file: Path, results_json: dict[str, Any]) -> None:
    """Write the file-updates results JSON."""
    with results_file.open("w", encoding="utf-8") as f:
        json.dump(results_json, f)


def _file_updates_from_json_key(file_updates_path: Path, json_key: str) -> list[Any]:
    """Evaluate *json_key* with jq and return the selected update list.

    The catalog passes ``jsonKey`` to ``jq`` unchanged, including operators
    and quoted keys. A null selection is zero updates. Any other non-array
    result is rejected.
    """
    logger.info("Evaluating jsonKey: %s", json_key)
    try:
        raw = run_cmd_text(["jq", "-c", json_key, str(file_updates_path)])
    except subprocess.CalledProcessError as e:
        detail = (e.stderr or e.output or "").strip()
        raise FileUpdatesError(f"Failed to evaluate jsonKey '{json_key}': {detail}") from e
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        raise FileUpdatesError(
            f"jsonKey '{json_key}' did not return an update list: {raw.strip()}"
        ) from e
    if parsed is None:
        return []
    if not isinstance(parsed, list):
        raise FileUpdatesError(f"jsonKey '{json_key}' did not return an update list")
    return parsed


def run(
    data_dir: Path,
    file_updates_path: str,
    snapshot_path: str,
    json_key: str,
    results_dir_path: str,
    request_timeout: int,
    task_git_url: str,
    task_git_revision: str,
    pipelinerun_uid: str,
    task_uid: str,
    pipeline_name: str,
    sync: bool,
    merge_request_url_result_path: Path,
) -> None:
    """Process file updates and create InternalRequests.

    Main workflow:
    1. Load snapshot and extract componentGroup
    2. Load file updates from the RPA data using jsonKey
    3. For each file update:
       - Evaluate {{ }} expressions in paths
       - Create an InternalRequest
       - Collect results
    4. Write merge request URLs to results
    """
    # Load snapshot and get componentGroup
    snapshot_file = file.resolve_path_under_base(data_dir, snapshot_path)
    snapshot_data = file.load_json_dict(snapshot_file)
    component_group = _jq_raw(snapshot_data.get("componentGroup"))
    logger.info("componentGroup: %s", component_group)

    # Extract file updates with the catalog jq expression, not dotted keys.
    file_updates_file = file.resolve_path_under_base(data_dir, file_updates_path)
    file_updates_raw = _file_updates_from_json_key(file_updates_file, json_key)
    logger.info("Found %d file updates", len(file_updates_raw))

    # Prepare results before any request so a later failure keeps earlier URLs.
    results_dir = file.resolve_path_under_base(data_dir, results_dir_path)
    results_dir.mkdir(parents=True, exist_ok=True)
    results_file = results_dir / "file-updates-results.json"
    results_json: dict[str, Any] = {"merge_requests": []}
    merge_request_url_result_path.write_text("", encoding="utf-8")
    _write_results_json(results_file, results_json)

    for i, item_data in enumerate(file_updates_raw):
        item = _parse_file_update_item(item_data)
        logger.info("=== Updates for repo: %s ===", item.repo)

        # Evaluate {{ }} expressions in paths
        logger.info("Evaluating '{{ }}' expressions...")
        updated_paths = _evaluate_path_expressions(item.paths, snapshot_file)
        logger.info("Done evaluating expressions")

        # Generate a unique request ID
        request_id = os.urandom(12).hex()

        ir_name: str | None = None
        try:
            ir_name = _create_file_update_request(
                item=item,
                component_group=component_group,
                updated_paths=updated_paths,
                request_timeout=request_timeout,
                task_git_url=task_git_url,
                task_git_revision=task_git_revision,
                pipelinerun_uid=pipelinerun_uid,
                task_uid=task_uid,
                request_id=request_id,
                pipeline_name=pipeline_name,
                sync=sync,
            )

            # create(sync=False) returns before results exist. Wait so an
            # unfinished request is not read as a failure. Sync mode already
            # waited inside create().
            if not sync:
                logger.info("Waiting for asynchronous InternalRequest %s", ir_name)
                wait_for_completion(
                    name=ir_name,
                    timeout=_wait_timeout_seconds(request_timeout),
                )
            mr_url = _process_ir_results(ir_name)
            if mr_url:
                with merge_request_url_result_path.open("a", encoding="utf-8") as f:
                    f.write(f"{mr_url}\n")
                results_json["merge_requests"].append({"url": mr_url})
                _write_results_json(results_file, results_json)

        except InternalRequestWaitError as e:
            # The wait helper raises before results are read. Report a task
            # error and diff when they were recorded; otherwise keep the wait
            # error, including timeouts with no results.
            logger.error("InternalRequest failed: %s", e)
            failed_name = ir_name or e.name
            if failed_name:
                raise _error_after_failed_wait(failed_name, e) from e
            raise FileUpdatesError(str(e)) from e

        logger.info("=== Finished ===\n")

    logger.info("Results written to %s", results_file)


def main() -> int:
    """Read environment variables and call run()."""
    data_dir = Path(tekton.require_env("PARAM_DATA_DIR"))
    file_updates_path = tekton.require_env("PARAM_FILE_UPDATES_PATH")
    snapshot_path = tekton.require_env("PARAM_SNAPSHOT_PATH")
    json_key = os.environ.get("PARAM_JSON_KEY", ".spec.data.fileUpdates")
    results_dir_path = tekton.require_env("PARAM_RESULTS_DIR_PATH")
    request_timeout = int(os.environ.get("PARAM_REQUEST_TIMEOUT", "900"))
    task_git_url = tekton.require_env("PARAM_TASK_GIT_URL")
    task_git_revision = tekton.require_env("PARAM_TASK_GIT_REVISION")
    pipelinerun_uid = tekton.require_env("PARAM_PIPELINE_RUN_UID")
    task_uid = tekton.require_env("PARAM_TASK_RUN_UID")
    pipeline_name = os.environ.get("PARAM_REQUEST", "process-file-updates")
    sync_str = os.environ.get("PARAM_SYNCHRONOUSLY", "true")
    sync = sync_str.lower() == "true"
    (merge_request_url_result_path,) = tekton.result_paths_from_env("RESULT_MERGE_REQUEST_URL")

    run(
        data_dir=data_dir,
        file_updates_path=file_updates_path,
        snapshot_path=snapshot_path,
        json_key=json_key,
        results_dir_path=results_dir_path,
        request_timeout=request_timeout,
        task_git_url=task_git_url,
        task_git_revision=task_git_revision,
        pipelinerun_uid=pipelinerun_uid,
        task_uid=task_uid,
        pipeline_name=pipeline_name,
        sync=sync,
        merge_request_url_result_path=merge_request_url_result_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
