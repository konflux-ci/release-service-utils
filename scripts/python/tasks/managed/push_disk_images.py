#!/usr/bin/env python3
"""Push disk images via InternalRequest to Exodus CDN and Developer Portal.

Read the snapshot and data JSON files, resolve CDN environment configuration,
write a results file listing disk-image filenames, and submit an InternalRequest
for the ``push-disk-images`` internal pipeline.  The environment to use is
pulled from the ``cdn.env`` key in the data file.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import file
import internal_request
import tekton
from internal_request.internal_request import PIPELINERUN_UID_LABEL
from logger import logger

_IR_PIPELINE_TIMEOUT = "24h0m0s"
_IR_TASK_TIMEOUT = "23h50m0s"
_IR_FINALLY_TIMEOUT = "0h10m0s"
_IR_WAIT_TIMEOUT_SECONDS = (
    internal_request.duration_to_seconds(_IR_PIPELINE_TIMEOUT)
    + internal_request.SPAWN_OVERHEAD_SECONDS
)

# The Exodus GW secret is the same for production and stage — only the env
# (live vs pre) and the Pulp URL differ.  The stage CGW uses the 'qa' host:
# developers.qa.redhat.com.
_CDN_ENV_CONFIGS: dict[str, dict[str, str]] = {
    "production": {
        "exodusGwSecret": "exodus-prod-secret",
        "exodusGwEnv": "live",
        "pulpSecret": "rhsm-pulp-prod-secret",
        "udcacheSecret": "udcache-prod-secret",
        "cgwHostname": "https://developers.redhat.com/content-gateway/rest/admin",
        "cgwSecret": "cgw-service-account-prod-secret",
    },
    "stage": {
        "exodusGwSecret": "exodus-prod-secret",
        "exodusGwEnv": "pre",
        "pulpSecret": "rhsm-pulp-stage-secret",
        "udcacheSecret": "udcache-stage-secret",
        "cgwHostname": "https://developers.qa.redhat.com/content-gateway/rest/admin",
        "cgwSecret": "cgw-service-account-stage-secret",
    },
    "qa": {
        "exodusGwSecret": "exodus-stage-secret",
        "exodusGwEnv": "live",
        "pulpSecret": "rhsm-pulp-qa-secret",
        "udcacheSecret": "udcache-qa-secret",
        "cgwHostname": "https://developers.qa.redhat.com/content-gateway/rest/admin",
        "cgwSecret": "cgw-service-account-stage-secret",
    },
}


def resolve_cdn_env_config(env: str) -> dict[str, str]:
    """Return secret and gateway config for the given CDN environment.

    Raise ``ValueError`` when *env* is not one of production, stage, or qa.
    """
    config = _CDN_ENV_CONFIGS.get(env)
    if config is None:
        msg = f"cdn.env in the data file must be one of [production, stage, qa], got {env!r}"
        raise ValueError(msg)
    return dict(config)


def extract_disk_image_files(
    snapshot: dict[str, Any],
) -> list[str]:
    """Extract staged disk-image filenames from snapshot components."""
    filenames: list[str] = []
    for component in snapshot.get("components", []):
        staged = component.get("staged")
        if not staged or not isinstance(staged, dict):
            continue
        for file_entry in staged.get("files", []):
            if isinstance(file_entry, dict) and "filename" in file_entry:
                filenames.append(file_entry["filename"])
    return filenames


def prepare_snapshot(snapshot_path: Path) -> dict[str, Any]:
    """Load snapshot JSON and strip ``.metadata`` from each component."""
    snapshot = file.load_json_dict(snapshot_path)
    # The internal task doesn't need metadata (env_variables, labels, etc.)
    # and stripping it avoids "arg list too long" when the snapshot JSON
    # is passed as an InternalRequest parameter.
    for component in snapshot.get("components", []):
        component.pop("metadata", None)
    return snapshot


def write_results_file(
    results_dir: Path,
    filenames: list[str],
) -> None:
    """Write ``push-disk-images-results.json`` with disk-image filenames."""
    results_dir.mkdir(parents=True, exist_ok=True)
    results_file = results_dir / "push-disk-images-results.json"
    payload = {"disk-image-files": filenames}
    results_file.write_text(
        json.dumps(payload, separators=(",", ":")),
        encoding="utf-8",
    )


def run(
    *,
    data_dir: Path,
    snapshot_path: str,
    data_path: str,
    pipeline_run_uid: str,
    results_dir_path: str,
    task_git_url: str,
    task_git_revision: str,
) -> None:
    """Orchestrate the push-disk-images workflow."""
    snapshot = prepare_snapshot(data_dir / snapshot_path)
    data = file.load_json_dict(data_dir / data_path)

    env = data.get("cdn", {}).get("env", "")
    cdn_config = resolve_cdn_env_config(env)

    filenames = extract_disk_image_files(snapshot)
    write_results_file(data_dir / results_dir_path, filenames)

    snapshot_json = json.dumps(snapshot, separators=(",", ":"))

    logger.info("Creating InternalRequest to push disk images...")
    try:
        ir_name = internal_request.create(
            "push-disk-images",
            params={
                "snapshot_json": snapshot_json,
                "exodusGwSecret": cdn_config["exodusGwSecret"],
                "exodusGwEnv": cdn_config["exodusGwEnv"],
                "pulpSecret": cdn_config["pulpSecret"],
                "udcacheSecret": cdn_config["udcacheSecret"],
                "cgwHostname": cdn_config["cgwHostname"],
                "cgwSecret": cdn_config["cgwSecret"],
                "taskGitUrl": task_git_url,
                "taskGitRevision": task_git_revision,
            },
            labels={PIPELINERUN_UID_LABEL: pipeline_run_uid},
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
    if results.get("result") == "Success":
        logger.info("Disk images pushed")
        logger.info("%s", json.dumps(results, indent=2))
    else:
        logger.error("Disk image push failed")
        logger.error("%s", results.get("result"))
        msg = "Disk image push failed"
        raise RuntimeError(msg)


def main() -> int:
    """Read environment variables and execute the push-disk-images workflow."""
    run(
        data_dir=Path(tekton.require_env("PARAM_DATA_DIR")),
        snapshot_path=tekton.require_env("PARAM_SNAPSHOT_PATH"),
        data_path=tekton.require_env("PARAM_DATA_PATH"),
        pipeline_run_uid=tekton.require_env("PARAM_PIPELINE_RUN_UID"),
        results_dir_path=tekton.require_env("PARAM_RESULTS_DIR_PATH"),
        task_git_url=tekton.require_env("PARAM_TASK_GIT_URL"),
        task_git_revision=tekton.require_env("PARAM_TASK_GIT_REVISION"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
