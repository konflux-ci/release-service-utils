#!/usr/bin/env python3
"""Set releaseNotes.severity for RHSA advisories via InternalRequest.

For non-RHSA types the script removes a user-supplied severity key (if any)
and exits. RHSA releases with no fixed CVEs fail. Generic artifacts skip the
OSIDB lookup. Image releases submit ``content.images`` to the
``get-advisory-severity`` pipeline and write the returned severity.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from release_service_utils.helpers import file, tekton
from release_service_utils.helpers.internal_request import (
    PIPELINERUN_UID_LABEL,
    SPAWN_OVERHEAD_SECONDS,
    create,
    fetch_results,
    seconds_to_duration,
)
from release_service_utils.helpers.logger import logger

_RHSA_NO_CVES_MSG = (
    "Provided advisory type is RHSA, but no fixed CVEs were listed. "
    "RHSA should only be used if CVEs are fixed in the advisory."
)
_IR_FAILURE_MSG = "The InternalRequest to find the severity was unsuccessful"
_IR_MISSING_SEVERITY_MSG = "InternalRequest succeeded but did not return a severity"
_DEFAULT_REQUEST_TIMEOUT = "7200"


def _content_items(data: dict[str, Any], key: str) -> list[Any]:
    """Return the list at ``releaseNotes.content.<key>``, or an empty list."""
    notes = data.get("releaseNotes")
    if not isinstance(notes, dict):
        return []
    content = notes.get("content")
    if not isinstance(content, dict):
        return []
    items = content.get(key)
    return items if isinstance(items, list) else []


def count_fixed_cves(data: dict[str, Any]) -> int:
    """Count entries in ``cves.fixed`` across images and artifacts."""
    total = 0
    for key in ("images", "artifacts"):
        for item in _content_items(data, key):
            if not isinstance(item, dict):
                continue
            cves = item.get("cves")
            if not isinstance(cves, dict):
                continue
            fixed = cves.get("fixed")
            if isinstance(fixed, dict | list):
                total += len(fixed)
    return total


def _has_generic_artifacts(data: dict[str, Any]) -> bool:
    """Return True when ``releaseNotes.content.artifacts`` is a non-empty list."""
    return len(_content_items(data, "artifacts")) > 0


def _write_data(data_file: Path, data: dict[str, Any]) -> None:
    """Write *data* to *data_file* as indented JSON."""
    data_file.write_text(json.dumps(data, indent=2), encoding="utf-8")


def _request_advisory_severity(
    images: Any,
    *,
    pipeline_run_uid: str,
    request_timeout: int,
    task_git_url: str,
    task_git_revision: str,
) -> str:
    """Create the get-advisory-severity InternalRequest and return severity."""
    encoded_images = file.encode_json_gzip_b64(images)
    pipeline_timeout = seconds_to_duration(request_timeout + SPAWN_OVERHEAD_SECONDS)
    task_timeout = seconds_to_duration(request_timeout)
    wait_timeout = request_timeout + SPAWN_OVERHEAD_SECONDS

    ir_name = create(
        "get-advisory-severity",
        params={
            "releaseNotesImages": encoded_images,
            "taskGitUrl": task_git_url,
            "taskGitRevision": task_git_revision,
        },
        labels={PIPELINERUN_UID_LABEL: pipeline_run_uid},
        sync=True,
        timeout=wait_timeout,
        pipeline_timeout=pipeline_timeout,
        task_timeout=task_timeout,
    )
    logger.info("done (%s)", ir_name)

    results = fetch_results(ir_name)
    logger.info(
        "** internalRequestPipelineRunName: %s",
        results.get("internalRequestPipelineRunName", ""),
    )
    logger.info(
        "** internalRequestTaskRunName: %s",
        results.get("internalRequestTaskRunName", ""),
    )

    if results.get("result") != "Success":
        logger.error("%s: %s", _IR_FAILURE_MSG, results.get("result", ""))
        raise RuntimeError(f"{_IR_FAILURE_MSG}: {results.get('result', '')}")

    severity = results.get("severity")
    if not isinstance(severity, str) or not severity.strip():
        raise RuntimeError(_IR_MISSING_SEVERITY_MSG)
    return severity


def run(
    data_file: Path,
    *,
    pipeline_run_uid: str,
    request_timeout: int,
    task_git_url: str,
    task_git_revision: str,
) -> None:
    """Set or strip ``releaseNotes.severity`` according to advisory type."""
    data = file.load_json_dict(data_file)
    notes = data.get("releaseNotes")
    advisory_type = notes.get("type") if isinstance(notes, dict) else None

    if advisory_type != "RHSA":
        logger.info("Advisory is not of type RHSA. Not setting severity")
        if isinstance(notes, dict) and "severity" in notes:
            logger.info("User provided severity key for non RHSA advisory. Removing it")
            del notes["severity"]
            _write_data(data_file, data)
        return

    if count_fixed_cves(data) == 0:
        logger.error(_RHSA_NO_CVES_MSG)
        raise RuntimeError(_RHSA_NO_CVES_MSG)

    if _has_generic_artifacts(data):
        logger.info("Generic artifact type detected, not setting advisory severity")
        return

    images = _content_items(data, "images")
    severity = _request_advisory_severity(
        images,
        pipeline_run_uid=pipeline_run_uid,
        request_timeout=request_timeout,
        task_git_url=task_git_url,
        task_git_revision=task_git_revision,
    )
    logger.info("Setting severity to %s", severity)
    assert isinstance(notes, dict)
    notes["severity"] = severity
    _write_data(data_file, data)


def main() -> int:
    """Read Tekton environment variables and set advisory severity."""
    data_file = Path(tekton.require_env("DATA_FILE"))
    pipeline_run_uid = tekton.require_env("PIPELINE_RUN_UID")
    request_timeout = int(os.environ.get("REQUEST_TIMEOUT", _DEFAULT_REQUEST_TIMEOUT).strip())
    task_git_url = tekton.require_env("TASK_GIT_URL")
    task_git_revision = tekton.require_env("TASK_GIT_REVISION")

    run(
        data_file,
        pipeline_run_uid=pipeline_run_uid,
        request_timeout=request_timeout,
        task_git_url=task_git_url,
        task_git_revision=task_git_revision,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
