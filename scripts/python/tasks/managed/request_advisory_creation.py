#!/usr/bin/env python3
"""Update artifact PURLs and request advisory creation via InternalRequest.

This script is used for the create-advisory managed task. It uses a different name
so it does not conflict with the create-advisory internal task script.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import advisory_data
import file
import internal_request
import release_notes_purl
import tekton
from logger import logger

# params.environment accepts either spelling for non-production image/rpm releases.
_STAGE_ENV_RE = re.compile(r"^(stage|staging)$")
# InternalRequest wait and PipelineRun timeouts.
_IR_PIPELINE_TIMEOUT = "01h05m00s"
_IR_TASK_TIMEOUT = "01h00m00s"
_IR_FINALLY_TIMEOUT = "0h5m0s"
_IR_WAIT_TIMEOUT_SECONDS = (
    internal_request.duration_to_seconds(_IR_PIPELINE_TIMEOUT)
    + internal_request.SPAWN_OVERHEAD_SECONDS
)


@dataclass(frozen=True)
class TaskParams:
    """Paths and Tekton parameters for the request-advisory-creation step."""

    data_dir: Path
    data_path: Path
    snapshot_path: Path
    release_plan_admission_path: Path
    results_dir_path: Path
    environment: str
    request_pipeline: str
    synchronously: str
    pipeline_run_uid: str
    task_git_url: str
    task_git_revision: str
    task_name: str
    checksum_map: str
    dockerconfig_path: Path
    advisory_url_result: Path
    advisory_internal_url_result: Path


def _resolve_content_type(data: dict[str, Any]) -> str:
    """Derive advisory content type from mapping, GitHub, or image defaults."""
    content_type = advisory_data.first_mapping_content_type(data)
    if content_type:
        return content_type
    # GitHub-only releases have no mapping.components contentType; treat as generic.
    if "github" in data:
        return "generic"
    # Container image releases are the default when no artifact content type is present.
    return "image"


def _content_path_for_type(content_type: str) -> str:
    """Return the advisory JSON path used for CVE counting."""
    # File-based releases store CVEs under content.artifacts; images use content.images.
    if content_type in advisory_data.ARTIFACT_CONTENT_TYPES:
        return ".content.artifacts"
    return ".content.images"


def _count_fixed_cves(advisory: dict[str, Any], content_path: str) -> int:
    """Count fixed CVE ids across all content rows."""
    total = 0
    for item in advisory_data.content_array_from_decoded(advisory, content_path):
        if not isinstance(item, dict):
            continue
        cves = item.get("cves")
        if not isinstance(cves, dict):
            continue
        fixed = cves.get("fixed")
        # populate-release-notes stores fixed CVEs as a map (CVE id -> metadata).
        if isinstance(fixed, dict):
            total += len(fixed)
        elif isinstance(fixed, list):
            total += len(fixed)
    return total


def _prepare_advisory_data(
    release_notes: dict[str, Any],
    content_path: str,
) -> dict[str, Any]:
    """Validate advisory fields and apply RHBA default type when missing."""
    live_id = release_notes.get("live_id")
    allow_custom_live_id = release_notes.get("allow_custom_live_id", False) is True
    if not allow_custom_live_id and live_id is not None:
        msg = "advisory live id is only allowed if allow_custom_live_id is set to true"
        raise ValueError(msg)

    advisory_type = release_notes.get("type")
    if advisory_type is None:
        logger.info("Defaulting to type = %s", advisory_data.DEFAULT_ADVISORY_TYPE)
        release_notes = {**release_notes, "type": advisory_data.DEFAULT_ADVISORY_TYPE}
        advisory_type = advisory_data.DEFAULT_ADVISORY_TYPE

    if advisory_type not in advisory_data.VALID_ADVISORY_TYPES:
        msg = "advisory type must be one of RHSA, RHBA or RHEA"
        raise ValueError(msg)

    # RHSA requires at least one fixed CVE in the content array for this release type.
    if advisory_type == "RHSA" and _count_fixed_cves(release_notes, content_path) == 0:
        msg = (
            "Provided advisory type is RHSA, but no fixed CVEs were listed. "
            "RHSA should only be used if CVEs are fixed in the advisory. Failing..."
        )
        raise ValueError(msg)

    return release_notes


def _resolve_secret_names(
    content_type: str,
    *,
    environment: str,
    data: dict[str, Any],
) -> tuple[str, str]:
    """Select advisory and Errata secret names for the release content type.

    All content types use the same prod/staging GitLab repos and secret names.
    Which pair to use depends on how the release environment is determined:

    - image/rpm: ``environment`` (stage/staging vs everything else)
    - binary/generic/disk-image: ``data['intention']`` (production vs staging)
    """
    if content_type in {"image", "rpm"}:
        is_staging = bool(_STAGE_ENV_RE.match(environment.strip()))
    else:
        intention = str(data.get("intention", ""))
        if intention == "production":
            is_staging = False
        elif intention == "staging":
            is_staging = True
        else:
            msg = f"unsupported intention for advisory secrets: {intention!r}"
            raise ValueError(msg)

    if is_staging:
        return (
            advisory_data.ADVISORY_SECRET_STAGE,
            advisory_data.ERRATA_SECRET_STAGE,
        )
    return advisory_data.ADVISORY_SECRET_PROD, advisory_data.ERRATA_SECRET_PROD


def _sync_from_param(value: str) -> bool:
    """Parse Tekton synchronously param (``true`` / ``false``) to a bool."""
    return value.strip().lower() == "true"


def _create_internal_request(
    params: TaskParams,
    *,
    component_group: str,
    origin: str,
    advisory_json: str,
    config_map_name: str,
    content_type: str,
    advisory_secret_name: str,
    errata_secret_name: str,
) -> str:
    """Create an InternalRequest and optionally wait for it to complete."""
    logger.info("Creating InternalRequest to create advisory...")
    sync = _sync_from_param(params.synchronously)
    try:
        internal_request_name = internal_request.create(
            params.request_pipeline,
            params={
                "componentGroup": component_group,
                "origin": origin,
                "advisory_json": advisory_json,
                "config_map_name": config_map_name,
                "contentType": content_type,
                "advisory_secret_name": advisory_secret_name,
                "errata_secret_name": errata_secret_name,
                "taskGitUrl": params.task_git_url,
                "taskGitRevision": params.task_git_revision,
            },
            labels={
                internal_request.PIPELINERUN_UID_LABEL: params.pipeline_run_uid,
            },
            sync=sync,
            timeout=_IR_WAIT_TIMEOUT_SECONDS,
            pipeline_timeout=_IR_PIPELINE_TIMEOUT,
            task_timeout=_IR_TASK_TIMEOUT,
            finally_timeout=_IR_FINALLY_TIMEOUT,
        )
    except internal_request.InternalRequestWaitError as err:
        raise RuntimeError(str(err)) from err
    logger.info("done (%s)", internal_request_name)
    return internal_request_name


def _write_task_results(
    params: TaskParams,
    results: dict[str, Any],
) -> None:
    """Write Tekton results and the create-advisory results JSON file."""
    # Clear Tekton results before checking IR status (matches result initialization).
    params.advisory_url_result.write_text("", encoding="utf-8")
    params.advisory_internal_url_result.write_text("", encoding="utf-8")

    pipeline_run_name = str(results.get("internalRequestPipelineRunName") or "")
    task_run_name = str(results.get("internalRequestTaskRunName") or "")
    logger.info("** internalRequestPipelineRunName: %s", pipeline_run_name)
    logger.info("** internalRequestTaskRunName: %s", task_run_name)

    if results.get("result") != "Success":
        logger.info("Advisory creation failed")
        print(json.dumps(results))
        msg = "advisory creation failed"
        raise RuntimeError(msg)

    logger.info("Advisory created")
    advisory_url = str(results.get("advisory_url") or "")
    advisory_internal_url = str(results.get("advisory_internal_url") or "")
    params.advisory_url_result.write_text(advisory_url, encoding="utf-8")
    params.advisory_internal_url_result.write_text(
        advisory_internal_url,
        encoding="utf-8",
    )

    results_file = params.data_dir / params.results_dir_path / "create-advisory-results.json"
    results_file.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "advisory": {
            "url": advisory_url,
            "internal_url": advisory_internal_url,
        },
    }
    # Downstream tasks read advisory URLs from this results file under resultsDirPath.
    results_file.write_text(
        json.dumps(payload, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )


def run_request_advisory_creation(params: TaskParams) -> None:
    """Update PURLs, submit the InternalRequest, and write Tekton results."""
    data_file = params.data_dir / params.data_path
    # Populate releaseNotes PURLs from the checksum_map OCI artifact when applicable.
    # Pass the snapshot so disk-image staged.files use substituted filenames (not
    # unresolved {{ release_timestamp }} templates left in data.json).
    release_notes_purl.update_artifact_purls(
        data_file,
        checksum_map_param=params.checksum_map,
        dockerconfig_path=params.dockerconfig_path,
        snapshot_path=params.data_dir / params.snapshot_path,
    )

    snapshot_file = params.data_dir / params.snapshot_path
    rpa_file = params.data_dir / params.release_plan_admission_path

    data = file.load_json_dict(data_file)
    snapshot = file.load_json_dict(snapshot_file)
    release_plan_admission = file.load_json_dict(rpa_file)

    component_group = snapshot["componentGroup"]
    origin = release_plan_admission["spec"]["origin"]
    advisory_data_dict = data["releaseNotes"]
    if not isinstance(advisory_data_dict, dict):
        msg = "releaseNotes must be a JSON object"
        raise TypeError(msg)
    config_map_name = data["sign"]["configMapName"]

    content_type = _resolve_content_type(data)
    content_path = _content_path_for_type(content_type)
    advisory_data_dict = _prepare_advisory_data(dict(advisory_data_dict), content_path)

    advisory_secret_name, errata_secret_name = _resolve_secret_names(
        content_type,
        environment=params.environment,
        data=data,
    )
    # InternalRequest param is gzip+base64 advisory JSON for the create-advisory pipeline.
    advisory_json = advisory_data.encode_advisory_param(advisory_data_dict)

    internal_request_name = _create_internal_request(
        params,
        component_group=component_group,
        origin=origin,
        advisory_json=advisory_json,
        config_map_name=config_map_name,
        content_type=content_type,
        advisory_secret_name=advisory_secret_name,
        errata_secret_name=errata_secret_name,
    )
    results = internal_request.fetch_results(internal_request_name)
    _write_task_results(params, results)


def _params_from_env() -> TaskParams:
    """Build task parameters from Tekton PARAM_* and RESULT_* environment variables."""
    data_dir = Path(tekton.require_env("PARAM_DATA_DIR"))
    advisory_url_result, advisory_internal_url_result = tekton.result_paths_from_env(
        "RESULT_ADVISORY_URL",
        "RESULT_ADVISORY_INTERNAL_URL",
    )
    dockerconfig_raw = os.environ.get("PARAM_TA_DOCKERCONFIG_PATH", "").strip()
    dockerconfig_path = (
        Path(dockerconfig_raw)
        if dockerconfig_raw
        else release_notes_purl.TA_DOCKERCONFIG_DEFAULT
    )
    return TaskParams(
        data_dir=data_dir,
        data_path=Path(tekton.require_env("PARAM_DATA_PATH")),
        snapshot_path=Path(tekton.require_env("PARAM_SNAPSHOT_PATH")),
        release_plan_admission_path=Path(
            tekton.require_env("PARAM_RELEASE_PLAN_ADMISSION_PATH"),
        ),
        results_dir_path=Path(tekton.require_env("PARAM_RESULTS_DIR_PATH")),
        environment=os.environ.get("PARAM_ENVIRONMENT", "").strip(),
        request_pipeline=os.environ.get("PARAM_REQUEST", "create-advisory").strip()
        or "create-advisory",
        synchronously=os.environ.get("PARAM_SYNCHRONOUSLY", "true").strip() or "true",
        pipeline_run_uid=tekton.require_env("PARAM_PIPELINE_RUN_UID"),
        task_git_url=tekton.require_env("PARAM_TASK_GIT_URL"),
        task_git_revision=tekton.require_env("PARAM_TASK_GIT_REVISION"),
        task_name=tekton.require_env("PARAM_TASK_NAME"),
        checksum_map=os.environ.get("PARAM_CHECKSUM_MAP", "").strip(),
        dockerconfig_path=dockerconfig_path,
        advisory_url_result=advisory_url_result,
        advisory_internal_url_result=advisory_internal_url_result,
    )


def main() -> int:
    """Entry point: load params from the environment and run the workflow."""
    run_request_advisory_creation(_params_from_env())
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
