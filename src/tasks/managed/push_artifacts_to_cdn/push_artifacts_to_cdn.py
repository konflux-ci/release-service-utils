#!/usr/bin/env python3
"""Push artifacts via InternalRequest to Exodus CDN and Developer Portal.

Read the snapshot, data, and release JSON files, resolve CDN environment
configuration and the checksum signing key, write a results file listing
staged artifact filenames, and submit an InternalRequest for the
``push-artifacts-to-cdn`` internal pipeline. The environment to use is pulled
from the ``cdn.env`` key in the data file.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from release_service_utils.helpers import (
    cdn,
    file,
    internal_request,
    kubectl,
    tekton,
)
from release_service_utils.helpers import snapshot as snapshot_helper
from release_service_utils.helpers.internal_request.internal_request import (
    PIPELINERUN_UID_LABEL,
)
from release_service_utils.helpers.logger import logger

_IR_PIPELINE_TIMEOUT = "24h0m0s"
_IR_TASK_TIMEOUT = "23h50m0s"
_IR_FINALLY_TIMEOUT = "0h10m0s"
_IR_WAIT_TIMEOUT_SECONDS = (
    internal_request.duration_to_seconds(_IR_PIPELINE_TIMEOUT)
    + internal_request.SPAWN_OVERHEAD_SECONDS
)

_QUAY_URL_STAGING = "quay.io/konflux-artifacts/nonprod"
_QUAY_URL_PRODUCTION = "quay.io/konflux-artifacts/prod"


def prepare_snapshot(snapshot_path: Path) -> dict[str, Any]:
    """Load snapshot JSON and strip ``.metadata`` from each component."""
    snapshot = file.load_json_dict(snapshot_path)
    return snapshot_helper.strip_component_metadata(snapshot)


def extract_artifact_files(snapshot: dict[str, Any]) -> list[str]:
    """Extract staged artifact filenames from snapshot components."""
    filenames: list[str] = []
    for component in snapshot.get("components", []):
        staged = component.get("staged")
        if not staged or not isinstance(staged, dict):
            continue
        for file_entry in staged.get("files", []):
            if isinstance(file_entry, dict) and "filename" in file_entry:
                filenames.append(file_entry["filename"])
    return filenames


def write_results_file(results_dir: Path, filenames: list[str]) -> None:
    """Write ``push-artifacts-results.json`` with staged artifact filenames."""
    results_dir.mkdir(parents=True, exist_ok=True)
    results_file = results_dir / "push-artifacts-results.json"
    payload = {"artifacts": filenames}
    results_file.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")


def resolve_quay_url(intention: str) -> str:
    """Return the shared Quay repository URL to use for a release *intention*."""
    if intention == "staging":
        return _QUAY_URL_STAGING
    return _QUAY_URL_PRODUCTION


def get_release_author(release: dict[str, Any]) -> str:
    """Return the release author from ``status.attribution.author``.

    Raise ``ValueError`` if no author is set.
    """
    author = release.get("status", {}).get("attribution", {}).get("author")
    if not author:
        raise ValueError("No author found in Release.Status. Failing...")
    return str(author)


def get_signing_key_name(config_map_name: str) -> str:
    """Return the checksum signing key name from a signing ConfigMap.

    Prefers the singular ``SIG_KEY_NAME`` field. Some ConfigMaps (e.g.
    e2e/staging ones) only define a comma-separated ``SIG_KEY_NAMES`` list
    rather than a single ``SIG_KEY_NAME``; in that case the first entry is
    used.

    Raise ``ValueError`` if neither field is set.
    """
    data = kubectl.get_configmap(config_map_name).get("data") or {}
    signing_key_name = data.get("SIG_KEY_NAME")
    if signing_key_name:
        return str(signing_key_name)
    signing_key_names = data.get("SIG_KEY_NAMES")
    if signing_key_names:
        return str(signing_key_names).split(",")[0].strip()
    raise ValueError(
        f"No SIG_KEY_NAME or SIG_KEY_NAMES found in configmap {config_map_name}. Failing..."
    )


def run(
    *,
    data_dir: Path,
    release_path: str,
    snapshot_path: str,
    data_path: str,
    pipeline_run_uid: str,
    results_dir_path: str,
    task_git_url: str,
    task_git_revision: str,
    checksum_map_path: Path,
) -> None:
    """Orchestrate the push-artifacts-to-cdn workflow."""
    snapshot = prepare_snapshot(data_dir / snapshot_path)
    data = file.load_json_dict(data_dir / data_path)
    release = file.load_json_dict(data_dir / release_path)

    author = get_release_author(release)
    config_map_name = data["sign"]["configMapName"]
    signing_key_name = get_signing_key_name(config_map_name)

    env = data.get("cdn", {}).get("env", "")
    cdn_config = cdn.cdn_env_secrets(env)

    quay_url = resolve_quay_url(data.get("intention", ""))

    filenames = extract_artifact_files(snapshot)
    write_results_file(data_dir / results_dir_path, filenames)

    snapshot_json = json.dumps(snapshot, separators=(",", ":"))

    logger.info("Creating InternalRequest to push artifacts...")
    try:
        ir_name = internal_request.create(
            "push-artifacts-to-cdn",
            params={
                "snapshot_json": snapshot_json,
                "author": author,
                "signingKeyName": signing_key_name,
                "exodusGwSecret": cdn_config["exodusGwSecret"],
                "exodusGwEnv": cdn_config["exodusGwEnv"],
                "pulpSecret": cdn_config["pulpSecret"],
                "udcacheSecret": cdn_config["udcacheSecret"],
                "cgwHostname": cdn_config["cgwHostname"],
                "cgwSecret": cdn_config["cgwSecret"],
                "quayURL": quay_url,
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
    checksum_map_path.write_text(str(results.get("checksum_map") or ""), encoding="utf-8")

    if results.get("result") != "Success":
        logger.error("Artifact push failed")
        logger.error("%s", results.get("result"))
        raise RuntimeError("Artifact push failed")
    logger.info("Artifacts pushed")
    logger.info("%s", json.dumps(results, indent=2))


def main() -> int:
    """Read environment variables and execute the push-artifacts-to-cdn workflow."""
    (checksum_map_path,) = tekton.result_paths_from_env("RESULT_CHECKSUM_MAP")

    run(
        data_dir=Path(tekton.require_env("PARAM_DATA_DIR")),
        release_path=tekton.require_env("PARAM_RELEASE_PATH"),
        snapshot_path=tekton.require_env("PARAM_SNAPSHOT_PATH"),
        data_path=tekton.require_env("PARAM_DATA_PATH"),
        pipeline_run_uid=tekton.require_env("PARAM_PIPELINE_RUN_UID"),
        results_dir_path=tekton.require_env("PARAM_RESULTS_DIR_PATH"),
        task_git_url=tekton.require_env("PARAM_TASK_GIT_URL"),
        task_git_revision=tekton.require_env("PARAM_TASK_GIT_REVISION"),
        checksum_map_path=checksum_map_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
