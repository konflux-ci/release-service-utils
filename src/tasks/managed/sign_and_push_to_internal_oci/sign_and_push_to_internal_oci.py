#!/usr/bin/env python3
"""Sign and push artifacts to internal Quay via an InternalRequest.

Strip snapshot component metadata, record staged artifact filenames, and
submit a signing request labeled with the managed PipelineRun and TaskRun UIDs.
Preserve custom Quay URLs and resolve the default repository by release intention.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from release_service_utils.helpers import (
    file,
    internal_request,
    tekton,
)
from release_service_utils.tasks.managed.push_artifacts_to_cdn import (
    extract_artifact_files,
    prepare_snapshot,
    resolve_quay_url,
    write_results_file,
)
from release_service_utils.helpers.logger import logger

_IR_PIPELINE_TIMEOUT = "24h0m0s"
_IR_TASK_TIMEOUT = "23h50m0s"
_IR_FINALLY_TIMEOUT = "0h10m0s"
_IR_WAIT_TIMEOUT_SECONDS = (
    internal_request.duration_to_seconds(_IR_PIPELINE_TIMEOUT)
    + internal_request.SPAWN_OVERHEAD_SECONDS
)

_DEFAULT_QUAY_URL = "quay.io/konflux-artifacts"
_DEST_QUAY_URL = "quay.io/redhat-user-workloads"


def extract_origin(rpa_path: Path) -> str:
    """Return the origin from a ReleasePlanAdmission."""
    rpa = file.load_json_dict(rpa_path)
    return rpa["spec"]["origin"]


def run(
    *,
    data_dir: Path,
    snapshot_path: str,
    rpa_path: str,
    data_path: str,
    pipeline_run_uid: str,
    task_run_uid: str,
    results_dir_path: str,
    mac_signing_script: str,
    windows_signing_script: str,
    task_git_url: str,
    task_git_revision: str,
    quay_url: str = _DEFAULT_QUAY_URL,
) -> None:
    """Orchestrate the sign-and-push-to-internal-oci workflow."""
    snapshot = prepare_snapshot(data_dir / snapshot_path)
    data = file.load_json_dict(data_dir / data_path)

    if quay_url == _DEFAULT_QUAY_URL:
        quay_url = resolve_quay_url(data.get("intention", ""))

    filenames = extract_artifact_files(snapshot)
    write_results_file(
        results_dir=data_dir / results_dir_path,
        filenames=filenames,
        results_file="sign-internal-oci.json",
    )

    snapshot_json = json.dumps(snapshot, separators=(",", ":"))
    origin = extract_origin(data_dir / rpa_path)

    logger.info("Creating InternalRequest to sign & push to internal oci...")
    try:
        ir_name = internal_request.create(
            "sign-and-push-to-internal-oci",
            params={
                "snapshot_json": snapshot_json,
                "quayURL": quay_url,
                "destQuayURL": _DEST_QUAY_URL,
                "origin": origin,
                "macSigningScript": mac_signing_script,
                "windowsSigningScript": windows_signing_script,
                "destQuaySecret": "quay-internal-oci",
                "taskGitUrl": task_git_url,
                "taskGitRevision": task_git_revision,
            },
            labels={
                internal_request.PIPELINERUN_UID_LABEL: pipeline_run_uid,
                internal_request.TASK_GROUP_LABEL: task_run_uid,
            },
            sync=True,
            timeout=_IR_WAIT_TIMEOUT_SECONDS,
            service_account="release-service-account",
            pipeline_timeout=_IR_PIPELINE_TIMEOUT,
            task_timeout=_IR_TASK_TIMEOUT,
            finally_timeout=_IR_FINALLY_TIMEOUT,
        )
    except internal_request.InternalRequestWaitError as err:
        raise RuntimeError(str(err)) from err
    logger.info("done (%s)", ir_name)
    results = internal_request.fetch_results(ir_name)

    if results.get("result") != "Success":
        logger.error("Internal pipeline failed")
        logger.error("%s", results.get("result"))
        raise RuntimeError("Internal pipeline failed")
    logger.info("Pipeline has succeeded")
    logger.info("%s", json.dumps(results, indent=2))


def main() -> int:
    """Read environment variables and execute the sign-and-push-to-internal-oci workflow."""
    run(
        data_dir=Path(tekton.require_env("PARAM_DATA_DIR")),
        snapshot_path=tekton.require_env("PARAM_SNAPSHOT_PATH"),
        rpa_path=tekton.require_env("PARAM_RELEASE_PLAN_ADMISSION_PATH"),
        data_path=tekton.require_env("PARAM_DATA_PATH"),
        pipeline_run_uid=tekton.require_env("PARAM_PIPELINE_RUN_UID"),
        task_run_uid=tekton.require_env("PARAM_TASK_RUN_UID"),
        results_dir_path=tekton.require_env("PARAM_RESULTS_DIR_PATH"),
        mac_signing_script=tekton.require_env("PARAM_MAC_SIGNING_SCRIPT"),
        windows_signing_script=tekton.require_env("PARAM_WINDOWS_SIGNING_SCRIPT"),
        task_git_url=tekton.require_env("PARAM_TASK_GIT_URL"),
        task_git_revision=tekton.require_env("PARAM_TASK_GIT_REVISION"),
        quay_url=os.environ.get("PARAM_QUAY_URL", _DEFAULT_QUAY_URL),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
