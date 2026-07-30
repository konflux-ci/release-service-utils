#!/usr/bin/env python3
"""Collect release data from K8s resources and merge into data.json.

Fetch Release, ReleasePlan, ReleasePlanAdmission, ReleaseServiceConfig, and
Snapshot CRs; merge collector results with spec.data from those resources
(priority: RPA > RP > Release > collectors); resolve pipeline ref metadata;
validate disallowed data key sources; and write JSON files plus Tekton results.
"""

from __future__ import annotations

import dataclasses
import json
import os
from pathlib import Path
from typing import Any

from release_service_utils.helpers import http_client
from release_service_utils.helpers.authentication import setup_ca_cert
from release_service_utils.helpers.file import resolve_path_under_base
from get_resource import get_resource_dict
from release_service_utils.helpers.vcs.github import owner_repo_from_url
from release_service_utils.helpers import tekton
from release_service_utils.helpers.logger import logger

_DISALLOWED_RELEASE_NOTE_KEYS: tuple[str, ...] = (
    "releaseNotes.product_id",
    "releaseNotes.product_name",
    "releaseNotes.product_version",
    "releaseNotes.product_stream",
    "releaseNotes.cpe",
    "releaseNotes.allow_custom_live_id",
)

_RESULT_ENV_VARS: dict[str, str] = {
    "release": "RESULT_RELEASE",
    "releasePlan": "RESULT_RELEASE_PLAN",
    "releasePlanAdmission": "RESULT_RELEASE_PLAN_ADMISSION",
    "releaseServiceConfig": "RESULT_RELEASE_SERVICE_CONFIG",
    "snapshotSpec": "RESULT_SNAPSHOT_SPEC",
    "data": "RESULT_DATA",
    "resultsDir": "RESULT_RESULTS_DIR",
    "singleComponentMode": "RESULT_SINGLE_COMPONENT_MODE",
    "snapshotName": "RESULT_SNAPSHOT_NAME",
    "snapshotNamespace": "RESULT_SNAPSHOT_NAMESPACE",
    "snapshotBuildId": "RESULT_SNAPSHOT_BUILD_ID",
    "releasePipelineMetadata": "RESULT_RELEASE_PIPELINE_METADATA",
    "subdirectory": "RESULT_SUBDIRECTORY",
}


def _resolve_result_paths() -> dict[str, Path]:
    """Resolve Tekton result file paths from environment variables."""
    paths = tekton.result_paths_from_env(*_RESULT_ENV_VARS.values())
    return dict(zip(_RESULT_ENV_VARS, paths))


@dataclasses.dataclass
class CollectDataResult:
    """All values computed by the collect-data workflow."""

    subdirectory: str
    release: dict[str, Any]
    release_plan: dict[str, Any]
    release_plan_admission: dict[str, Any]
    release_service_config: dict[str, Any]
    snapshot_spec: dict[str, Any]
    merged_data: dict[str, Any]
    pipeline_metadata: dict[str, str]
    single_component_mode: str
    snapshot_name: str
    snapshot_namespace: str
    snapshot_build_id: str


def deep_merge(base: Any, override: Any) -> Any:
    """Recursively merge *override* into *base*.

    - dict + dict  -> recursive merge
    - list + list  -> concatenate, deduplicate, then sort
    - otherwise    -> *override* wins (``None`` in override preserves *base*)
    """
    if isinstance(base, dict) and isinstance(override, dict):
        merged: dict[str, Any] = {}
        for key in dict.fromkeys(list(base) + list(override)):
            if key in base and key in override:
                merged[key] = deep_merge(base[key], override[key])
            elif key in override:
                merged[key] = override[key]
            else:
                merged[key] = base[key]
        return merged

    if isinstance(base, list) and isinstance(override, list):
        seen: list[Any] = []
        for item in base + override:
            if item not in seen:
                seen.append(item)
        try:
            return sorted(seen, key=lambda x: json.dumps(x, sort_keys=True))
        except TypeError:
            return seen

    if override is not None:
        return override
    return base


def flatten_collectors(
    collectors_status: dict[str, Any] | None,
) -> dict[str, Any]:
    """Deep-merge all collector values from managed and tenant sections."""
    collectors = collectors_status if collectors_status else {}
    managed_values = list((collectors.get("managed") or {}).values())
    tenant_values = list((collectors.get("tenant") or {}).values())
    all_values = managed_values + tenant_values

    result: dict[str, Any] = {}
    for item in all_values:
        result = deep_merge(result, item)
    return result


def transform_snapshot_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """Apply componentGroup fallback and remove application field.

    Return a new dict; *spec* is not modified.
    """
    result = {k: v for k, v in spec.items() if k != "application"}
    if not result.get("componentGroup"):
        result["componentGroup"] = spec.get("application")
    return result


_UNKNOWN_PIPELINE_METADATA: dict[str, str] = {
    "org": "unknown",
    "repo": "unknown",
    "revision": "unknown",
    "pathinrepo": "unknown",
    "sha": "unknown",
}


def _resolve_commit_sha(org: str, repo: str, revision: str) -> str:
    """Query the GitHub API for the full commit SHA, falling back to 'unknown'."""
    try:
        api_url = f"https://api.github.com/repos/{org}/{repo}/commits/{revision}"
        response_text = http_client.get_text(api_url, timeout=10)
        return json.loads(response_text).get("sha") or "unknown"
    except Exception:
        logger.warning(
            "Failed to resolve commit SHA from GitHub API",
            exc_info=True,
        )
        return "unknown"


def _resolve_git_pipeline_ref(pipeline_ref: dict[str, Any]) -> dict[str, str]:
    """Resolve org, repo, revision, path, and commit SHA from a git pipelineRef."""
    params = {p["name"]: p["value"] for p in pipeline_ref.get("params", [])}
    url = params.get("url", "")
    revision = params.get("revision", "unknown")
    pathinrepo = params.get("pathInRepo", "unknown")

    if url:
        owner_repo = owner_repo_from_url(url)
        org, _, repo = owner_repo.partition("/")
        repo = repo.removesuffix(".git") if repo else "unknown"
        org = org or "unknown"
    else:
        org = "unknown"
        repo = "unknown"

    sha = _resolve_commit_sha(org, repo, revision)

    return {
        "org": org,
        "repo": repo,
        "revision": revision,
        "pathinrepo": pathinrepo,
        "sha": sha,
    }


def resolve_pipeline_ref(rpa_data: dict[str, Any]) -> dict[str, str]:
    """Extract pipeline ref metadata from the RPA and resolve the commit SHA."""
    pipeline_ref = rpa_data.get("spec", {}).get("pipeline", {}).get("pipelineRef", {})

    if pipeline_ref.get("resolver") == "git":
        return _resolve_git_pipeline_ref(pipeline_ref)

    return dict(_UNKNOWN_PIPELINE_METADATA)


def check_data_key_sources(
    release: dict[str, Any],
    release_plan: dict[str, Any],
) -> None:
    """Validate that disallowed keys are absent from Release and ReleasePlan spec.data."""
    violations: list[str] = []

    for resource, kind in ((release, "Release"), (release_plan, "ReleasePlan")):
        spec_data = resource.get("spec", {}).get("data", {})
        if not spec_data or not isinstance(spec_data, dict):
            continue

        for dotted_key in _DISALLOWED_RELEASE_NOTE_KEYS:
            parts = dotted_key.split(".")
            value = spec_data
            for part in parts:
                if isinstance(value, dict):
                    value = value.get(part)
                else:
                    value = None
                    break
            if value is not None:
                msg = f"Found disallowed key: {dotted_key} in resource {kind}"
                logger.error(msg)
                violations.append(msg)

    if violations:
        raise ValueError("Disallowed keys found:\n" + "\n".join(violations))


def _write_json(path: Path, data: Any) -> None:
    """Write JSON data to a file, creating parent directories."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def collect(
    *,
    release: str,
    release_plan: str,
    release_plan_admission: str,
    release_service_config: str,
    snapshot: str,
    subdirectory: str,
) -> CollectDataResult:
    """Fetch K8s resources, merge data, and return computed results."""
    ns, name = release.split("/", 1)
    release_json = get_resource_dict("release", ns, name)

    ns, name = release_plan.split("/", 1)
    release_plan_json = get_resource_dict("releaseplan", ns, name)

    ns, name = release_plan_admission.split("/", 1)
    rpa_json = get_resource_dict("releaseplanadmission", ns, name)

    ns, name = release_service_config.split("/", 1)
    rsc_json = get_resource_dict("releaseserviceconfig", ns, name)

    logger.info("Fetching Snapshot Spec")
    snapshot_namespace, snapshot_name = snapshot.split("/", 1)
    snapshot_json = get_resource_dict("snapshot", snapshot_namespace, snapshot_name)
    snapshot_spec = transform_snapshot_spec(snapshot_json.get("spec", {}))

    labels = snapshot_json.get("metadata", {}).get("labels", {})
    snapshot_build_id = labels.get("appstudio.openshift.io/build-pipelinerun", "")

    logger.info("Generating collectors data")
    collectors_status = release_json.get("status", {}).get("collectors", {})
    logger.info(
        "collectors status: %s",
        json.dumps(collectors_status, indent=2),
    )
    collectors_result = flatten_collectors(collectors_status)
    logger.info(
        "collectors merged: %s",
        json.dumps(collectors_result, indent=2),
    )

    logger.info("Fetching merged data json")
    release_data = release_json.get("spec", {}).get("data", {})
    release_plan_data = release_plan_json.get("spec", {}).get("data", {})
    rpa_data = rpa_json.get("spec", {}).get("data", {})

    merged = deep_merge(collectors_result, release_data)
    merged = deep_merge(merged, release_plan_data)
    merged = deep_merge(merged, rpa_data)

    pipeline_metadata = resolve_pipeline_ref(rpa_json)
    logger.info("Release Pipeline Ref Info:")
    logger.info("--------------------------")
    logger.info("%s", json.dumps(pipeline_metadata, indent=2))

    raw = merged.get("singleComponentMode")
    single_component_mode = str(raw).lower() if raw is not None else "false"

    check_data_key_sources(release_json, release_plan_json)

    return CollectDataResult(
        subdirectory=subdirectory,
        release=release_json,
        release_plan=release_plan_json,
        release_plan_admission=rpa_json,
        release_service_config=rsc_json,
        snapshot_spec=snapshot_spec,
        merged_data=merged,
        pipeline_metadata=pipeline_metadata,
        single_component_mode=single_component_mode,
        snapshot_name=snapshot_name,
        snapshot_namespace=snapshot_namespace,
        snapshot_build_id=snapshot_build_id,
    )


def write_outputs(
    result: CollectDataResult,
    data_dir: Path,
    result_paths: dict[str, Path],
) -> None:
    """Write workspace files and Tekton result files."""
    if result.subdirectory:
        base_dir = resolve_path_under_base(data_dir, result.subdirectory)
        rel_base = Path(result.subdirectory)
    else:
        base_dir = data_dir
        rel_base = Path()
    base_dir.mkdir(parents=True, exist_ok=True)

    results_dir = base_dir / "results"
    results_dir.mkdir(parents=True, exist_ok=True)

    result_paths["subdirectory"].write_text(result.subdirectory)
    result_paths["resultsDir"].write_text(str(rel_base / "results"))

    result_paths["release"].write_text(str(rel_base / "release.json"))
    _write_json(base_dir / "release.json", result.release)

    result_paths["releasePlan"].write_text(str(rel_base / "release_plan.json"))
    _write_json(base_dir / "release_plan.json", result.release_plan)

    result_paths["releasePlanAdmission"].write_text(
        str(rel_base / "release_plan_admission.json")
    )
    _write_json(
        base_dir / "release_plan_admission.json",
        result.release_plan_admission,
    )

    result_paths["releaseServiceConfig"].write_text(
        str(rel_base / "release_service_config.json")
    )
    _write_json(
        base_dir / "release_service_config.json",
        result.release_service_config,
    )

    result_paths["snapshotSpec"].write_text(str(rel_base / "snapshot_spec.json"))
    _write_json(base_dir / "snapshot_spec.json", result.snapshot_spec)

    result_paths["data"].write_text(str(rel_base / "data.json"))
    _write_json(base_dir / "data.json", result.merged_data)

    result_paths["releasePipelineMetadata"].write_text(
        json.dumps(result.pipeline_metadata, separators=(",", ":"))
    )
    result_paths["singleComponentMode"].write_text(result.single_component_mode)
    result_paths["snapshotName"].write_text(result.snapshot_name)
    result_paths["snapshotNamespace"].write_text(result.snapshot_namespace)
    result_paths["snapshotBuildId"].write_text(result.snapshot_build_id)


def run(
    *,
    release: str,
    release_plan: str,
    release_plan_admission: str,
    release_service_config: str,
    snapshot: str,
    subdirectory: str,
    data_dir: Path,
    result_paths: dict[str, Path],
) -> None:
    """Collect data from K8s resources and write results."""
    setup_ca_cert()

    result = collect(
        release=release,
        release_plan=release_plan,
        release_plan_admission=release_plan_admission,
        release_service_config=release_service_config,
        snapshot=snapshot,
        subdirectory=subdirectory,
    )
    write_outputs(result, data_dir, result_paths)


def main() -> int:
    """Read environment variables and run the data collection workflow."""
    result_paths = _resolve_result_paths()

    run(
        release=tekton.require_env("RELEASE"),
        release_plan=tekton.require_env("RELEASE_PLAN"),
        release_plan_admission=tekton.require_env("RELEASE_PLAN_ADMISSION"),
        release_service_config=tekton.require_env("RELEASE_SERVICE_CONFIG"),
        snapshot=tekton.require_env("SNAPSHOT"),
        subdirectory=os.environ.get("PARAM_SUBDIRECTORY", ""),
        data_dir=Path(tekton.require_env("PARAM_DATA_DIR")),
        result_paths=result_paths,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
