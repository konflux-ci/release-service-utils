#!/usr/bin/env python3
"""Filter advisory-published images from a snapshot before advisory creation.

For each snapshot component, resolve architecture-specific image digests via
`get-image-architectures`, classify the component's mapped repository URLs as
pending (stage) or production advisories, and submit the resulting entries to
the `filter-already-released-advisory-images` internal pipeline via an
InternalRequest. Components the internal pipeline reports as still needing
release are kept in the snapshot; fully-released snapshots are reduced to an
empty component list and the run is marked skippable.
"""

from __future__ import annotations

import base64
import dataclasses
import gzip
import json
import re
import subprocess
from pathlib import Path
from typing import Any

from release_service_utils.helpers import internal_request, subprocess_cmd, tekton
from release_service_utils.helpers.file import load_json_dict
from release_service_utils.helpers.logger import logger

_PENDING_PATTERN = re.compile(r"quay\.io/redhat-pending/|quay\.io/rh-flatpaks-stage/")
_PROD_PATTERN = re.compile(r"quay\.io/redhat-prod/|quay\.io/rh-flatpaks-prod/")
_PIPELINE = "filter-already-released-advisory-images"
_PIPELINERUN_UID_LABEL = "internal-services.appstudio.openshift.io/pipelinerun-uid"


@dataclasses.dataclass(frozen=True)
class ResultPaths:
    """Tekton result file paths written by the filtering workflow."""

    result: Path
    skip_release: Path
    environment: Path
    latest_advisory_url: Path
    latest_advisory_internal_url: Path


@dataclasses.dataclass(frozen=True)
class FilterConfig:
    """Input configuration for the filtering workflow."""

    snapshot_file: Path
    rpa_file: Path
    data_file: Path
    results_file: Path
    pipeline_run_uid: str
    task_git_url: str
    task_git_revision: str
    synchronously: bool


def _sync_from_param(value: str) -> bool:
    """Parse Tekton synchronously param (``true`` / ``false``) to a bool."""
    return value.strip().lower() == "true"


def _repo_url_category(url: str) -> str:
    """Classify a mapped repository URL as "pending", "prod", or "orphan"."""
    if _PENDING_PATTERN.search(url):
        return "pending"
    if _PROD_PATTERN.search(url):
        return "prod"
    return "orphan"


def determine_environment(snapshot: dict[str, Any]) -> tuple[str, str]:
    """Classify every mapped repository URL and pick the advisory environment.

    Return ``(environment, advisory_secret_name)``. Raise `ValueError` when
    repositories mix pending and production URLs, contain orphaned URLs, or
    when no mapped repository is found at all.
    """
    found = {"pending": False, "prod": False, "orphan": False}
    for component in snapshot.get("components", []):
        for repo in component.get("repositories") or []:
            url = repo.get("url") or ""
            if not url:
                continue
            found[_repo_url_category(url)] = True

    logger.info("Repository status:")
    logger.info("- Pending repositories: %s", found["pending"])
    logger.info("- Production repositories: %s", found["prod"])
    logger.info("- Orphan repositories: %s", found["orphan"])

    if found["pending"] and found["prod"]:
        raise ValueError("cannot publish to both redhat-pending and redhat-prod repositories")
    if not found["pending"] and not found["prod"]:
        raise ValueError(
            "you must publish to either redhat-pending or redhat-prod repositories"
        )
    if found["orphan"]:
        raise ValueError(
            "you must publish to either redhat-pending or redhat-prod repositories"
        )

    if found["pending"]:
        return "stage", "create-advisory-stage-secret"
    return "production", "create-advisory-prod-secret"


def _resolve_image_architectures(image: str) -> list[dict[str, Any]]:
    """Resolve *image* to its per-architecture digest objects via `get-image-architectures`.

    Raises `subprocess.CalledProcessError` when architecture resolution fails.
    """
    output = subprocess_cmd.run_cmd_text(["get-image-architectures", image])
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def transform_component(component: dict[str, Any]) -> list[dict[str, Any]]:
    """Expand one snapshot component into arch-specific transformed entries.

    Return one minimal entry per (mapped repository, resolved architecture)
    pair, containing only the fields the internal pipeline needs. Raises
    `subprocess.CalledProcessError` when architecture resolution fails.
    """
    name = component.get("name", "")
    image = component.get("containerImage", "")
    digests = _resolve_image_architectures(image)

    entries: list[dict[str, Any]] = []
    for repo in component.get("repositories") or []:
        repo_url = repo.get("rh-registry-repo", "")
        tags = repo.get("tags")
        for arch in digests:
            entries.append(
                {
                    "name": name,
                    "containerImage": f"{repo_url}@{arch['digest']}",
                    "tags": tags,
                    "repository": repo_url,
                }
            )
    return entries


def transform_snapshot(snapshot: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    """Transform every component to arch-specific images.

    Return ``(transformed_entries, failed_component_names)``. A component
    whose architecture resolution fails is recorded as failed (assumed not
    yet released) instead of raising, matching the original bash behavior.
    """
    entries: list[dict[str, Any]] = []
    failed: list[str] = []

    for component in snapshot.get("components", []):
        name = component.get("name", "")
        image = component.get("containerImage", "")
        logger.info("Processing component: %s with image: %s", name, image)
        try:
            entries.extend(transform_component(component))
        except subprocess.CalledProcessError as exc:
            logger.warning(
                "Failed to resolve architectures for %s, assuming not released: %s",
                image,
                exc,
            )
            failed.append(name)

    return entries, failed


def check_skip_filter(data_file: Path) -> bool:
    """Return True when data.json sets `skipFilter` to boolean or string true."""
    if not data_file.is_file():
        return False
    value = load_json_dict(data_file).get("skipFilter")
    return value is True or value == "true"


def run_filter_request(
    entries: list[dict[str, Any]],
    *,
    origin: str,
    advisory_secret_name: str,
    config: FilterConfig,
) -> dict[str, Any]:
    """Gzip+base64 encode *entries* and submit the InternalRequest.

    Waits for it to complete when `config.synchronously` is true, then fetches
    and returns its results either way.
    """
    compressed = gzip.compress(json.dumps(entries).encode("utf-8"))
    transformed_snapshot = base64.b64encode(compressed).decode("ascii")

    ir_name = internal_request.create(
        _PIPELINE,
        params={
            "transformedSnapshot": transformed_snapshot,
            "origin": origin,
            "advisory_secret_name": advisory_secret_name,
            "internalRequestPipelineRunName": config.pipeline_run_uid,
            "taskGitUrl": config.task_git_url,
            "taskGitRevision": config.task_git_revision,
        },
        labels={_PIPELINERUN_UID_LABEL: config.pipeline_run_uid},
        sync=config.synchronously,
    )
    logger.info("Internal request created: %s", ir_name)

    results = internal_request.fetch_results(ir_name)
    if results.get("result") != "Success":
        raise RuntimeError(f"Filtering failed: {json.dumps(results)}")
    return results


def decode_unreleased_components(raw: str) -> list[str]:
    """Decode the gzip+base64 `unreleased_components` list from IR results."""
    if not raw:
        raise RuntimeError("No unreleased components list found in results")
    return json.loads(gzip.decompress(base64.b64decode(raw)))


def filter_snapshot(snapshot: dict[str, Any], unreleased_names: set[str]) -> dict[str, Any]:
    """Keep only snapshot components whose name is in *unreleased_names*."""
    filtered = dict(snapshot)
    filtered["components"] = [
        c for c in snapshot.get("components", []) if c.get("name") in unreleased_names
    ]
    return filtered


def run(config: FilterConfig, results: ResultPaths) -> None:
    """Orchestrate advisory image filtering and write Tekton results."""
    snapshot = load_json_dict(config.snapshot_file)

    entries, failed_components = transform_snapshot(snapshot)

    rpa = load_json_dict(config.rpa_file)
    origin = rpa["spec"]["origin"]
    if not origin:
        raise ValueError("'origin' in ReleasePlanAdmission spec is empty")

    environment, advisory_secret_name = determine_environment(snapshot)
    results.environment.write_text(environment, encoding="utf-8")
    logger.info("Environment: %s", environment)

    if check_skip_filter(config.data_file):
        logger.info(
            "skipFilter is true in data.json, skipping filtering and passing "
            "snapshot unchanged."
        )
        results.skip_release.write_text("false", encoding="utf-8")
        results.result.write_text("Success", encoding="utf-8")
        return

    ir_results = run_filter_request(
        entries,
        origin=origin,
        advisory_secret_name=advisory_secret_name,
        config=config,
    )

    unreleased_names = set(
        decode_unreleased_components(ir_results.get("unreleased_components", ""))
    ) | set(failed_components)

    filtered = filter_snapshot(snapshot, unreleased_names)
    config.snapshot_file.write_text(json.dumps(filtered, indent=2), encoding="utf-8")
    results.result.write_text("Success", encoding="utf-8")

    if not filtered["components"]:
        logger.info(
            "All images in the snapshot have already been released in advisories. "
            "Stopping pipeline."
        )
        results.skip_release.write_text("true", encoding="utf-8")
        # The internal pipeline always creates an advisory on this path, so these
        # keys must be present; fail loudly instead of silently writing "" if the
        # pipeline's contract is ever violated.
        advisory_url = ir_results["advisory_url"]
        advisory_internal_url = ir_results["advisory_internal_url"]
        results.latest_advisory_url.write_text(advisory_url, encoding="utf-8")
        results.latest_advisory_internal_url.write_text(
            advisory_internal_url, encoding="utf-8"
        )
        config.results_file.write_text(
            json.dumps(
                {"advisory": {"url": advisory_url, "internal_url": advisory_internal_url}}
            ),
            encoding="utf-8",
        )
        return

    results.skip_release.write_text("false", encoding="utf-8")
    results.latest_advisory_url.write_text("", encoding="utf-8")
    results.latest_advisory_internal_url.write_text("", encoding="utf-8")


def main() -> int:
    """Read environment variables and run the filtering workflow."""
    (
        result_result,
        result_skip_release,
        result_environment,
        result_latest_advisory_url,
        result_latest_advisory_internal_url,
    ) = tekton.result_paths_from_env(
        "RESULT_RESULT",
        "RESULT_SKIP_RELEASE",
        "RESULT_ENVIRONMENT",
        "RESULT_LATEST_ADVISORY_URL",
        "RESULT_LATEST_ADVISORY_INTERNAL_URL",
    )

    config = FilterConfig(
        snapshot_file=Path(tekton.require_env("SNAPSHOT_FILE")),
        rpa_file=Path(tekton.require_env("RPA_FILE")),
        data_file=Path(tekton.require_env("DATA_FILE")),
        results_file=Path(tekton.require_env("RESULTS_FILE")),
        pipeline_run_uid=tekton.require_env("PIPELINE_RUN_UID"),
        task_git_url=tekton.require_env("TASK_GIT_URL"),
        task_git_revision=tekton.require_env("TASK_GIT_REVISION"),
        synchronously=_sync_from_param(tekton.require_env("SYNCHRONOUSLY")),
    )
    result_paths = ResultPaths(
        result=result_result,
        skip_release=result_skip_release,
        environment=result_environment,
        latest_advisory_url=result_latest_advisory_url,
        latest_advisory_internal_url=result_latest_advisory_internal_url,
    )

    run(config, result_paths)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
