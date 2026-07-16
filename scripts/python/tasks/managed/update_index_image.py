#!/usr/bin/env python3
"""Update index image snapshot with current digests from the target registry.

Re-inspects floating tags in the TARGET registry to capture the current digest
after publish-index-image, as a parallel IIB build + publish might have changed it.
"""

from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from typing import Any

import file as file_utils
import tekton
from logger import logger
from subprocess_cmd import run_cmd

UPDATED_SNAPSHOT_FILENAME = "index_image_snapshot_updated.json"
_FLOATING_TAG = re.compile(r"^v[0-9]+\.[0-9]+$")

TASK_LABEL = "internal-services.appstudio.openshift.io/group-id"
PIPELINERUN_LABEL = "internal-services.appstudio.openshift.io/pipelinerun-uid"


def is_floating_tag(tag: str) -> bool:
    """Return True if *tag* is a bare OCP floating version (e.g. v4.13)."""
    return bool(_FLOATING_TAG.fullmatch(tag))


def validate_inspect_result(
    request_message: dict[str, Any], ir_name: str
) -> tuple[str, list[str]]:
    """Validate and extract sha and digests from an inspect InternalRequest result.

    Args:
        request_message: Parsed requestMessage from the InternalRequest status.
        ir_name: Name of the InternalRequest (for error messages).

    Returns:
        Tuple of (sha, digests).

    Raises:
        ValueError: If validation fails.

    """
    sha = request_message.get("sha")
    if not sha:
        raise ValueError(
            f"InternalRequest {ir_name} requestMessage missing or null 'sha' field"
        )
    if not str(sha).startswith("sha256:"):
        raise ValueError(f"InternalRequest {ir_name} returned invalid sha: {sha}")

    digests = request_message.get("digests")
    if not isinstance(digests, list):
        raise ValueError(f"InternalRequest {ir_name} returned non-array 'digests' field")
    if not digests:
        raise ValueError(f"InternalRequest {ir_name} returned empty 'digests' array")

    invalid = [d for d in digests if not str(d).startswith("sha256:")]
    if invalid:
        raise ValueError(f"InternalRequest {ir_name} returned invalid digests: {invalid}")

    return str(sha), [str(d) for d in digests]


def run_inspect_internal_request(
    floating_target: str,
    credentials: str,
    task_git_url: str,
    task_git_revision: str,
    task_id: str,
    pipelinerun_uid: str,
) -> dict[str, Any]:
    """Run an internal-request for inspect-target-index-pipeline and return the result.

    Args:
        floating_target: Image pullspec to inspect (e.g. quay.io/redhat/index:v4.13).
        credentials: Name of the Kubernetes secret with registry credentials.
        task_git_url: Git URL for task references.
        task_git_revision: Git revision for task references.
        task_id: Task run UID for labeling.
        pipelinerun_uid: Pipeline run UID for labeling.

    Returns:
        Parsed requestMessage dict from the InternalRequest status.

    Raises:
        RuntimeError: If the internal-request fails or returns invalid results.

    """
    result = run_cmd(
        [
            "internal-request",
            "--pipeline",
            "inspect-target-index-pipeline",
            "-p",
            f"sourceIndex={floating_target}",
            "-p",
            f"inspectCredentials={credentials}",
            "-p",
            f"taskGitUrl={task_git_url}",
            "-p",
            f"taskGitRevision={task_git_revision}",
            "-l",
            f"{TASK_LABEL}={task_id}",
            "-l",
            f"{PIPELINERUN_LABEL}={pipelinerun_uid}",
            "-t",
            "120",
        ]
    )

    # internal-request CLI outputs: "InternalRequest '<name>' created"
    ir_name = ""
    for line in result.stdout.splitlines():
        if "created" in line and "'" in line:
            ir_name = line.split("'")[1]
            break

    if not ir_name:
        raise RuntimeError(
            "Failed to extract InternalRequest name from internal-request output"
        )

    kubectl_result = run_cmd(
        [
            "kubectl",
            "get",
            "internalrequest",
            ir_name,
            "-o=jsonpath={.status.results}",
        ]
    )

    results = json.loads(kubectl_result.stdout)
    request_message_str = results.get("requestMessage", "")
    if not request_message_str:
        raise RuntimeError(f"InternalRequest {ir_name} returned empty requestMessage")

    return json.loads(request_message_str)


def update_index_images(
    snapshot: dict[str, Any],
    credentials: str,
    task_git_url: str,
    task_git_revision: str,
    task_id: str,
    pipelinerun_uid: str,
) -> dict[str, Any]:
    """Update snapshot components that have floating tags with current digests.

    Args:
        snapshot: Parsed snapshot JSON with components array.
        credentials: Name of the Kubernetes secret with registry credentials.
        task_git_url: Git URL for task references.
        task_git_revision: Git revision for task references.
        task_id: Task run UID for labeling.
        pipelinerun_uid: Pipeline run UID for labeling.

    Returns:
        Updated snapshot dict (deep copy of input with modified components).

    """
    updated = copy.deepcopy(snapshot)
    components = updated.get("components", [])

    for i, component in enumerate(components):
        repos = component.get("repositories", [])
        if not repos:
            continue

        tag = repos[0].get("tags", [""])[0]
        repository = repos[0].get("url", "")

        if not is_floating_tag(tag):
            logger.info(
                "Skipping component %d: tag '%s' is not a floating tag",
                i,
                tag,
            )
            continue

        floating_target = f"{repository}:{tag}"
        logger.info("Inspecting floating target index: %s", floating_target)

        request_message = run_inspect_internal_request(
            floating_target,
            credentials,
            task_git_url,
            task_git_revision,
            task_id,
            pipelinerun_uid,
        )

        component_label = f"component-{i}"
        sha, digests = validate_inspect_result(request_message, component_label)

        component["containerImage"] = f"{repository}@{sha}"
        component["imageDigests"] = digests
        logger.info(
            "Updated component %d: containerImage=%s, %d digests",
            i,
            component["containerImage"],
            len(digests),
        )

    return updated


def run_update_index_image(
    *,
    data_dir: Path,
    snapshot_path: str,
    data_path: str,
    task_git_url: str,
    task_git_revision: str,
    task_id: str,
    pipelinerun_uid: str,
    index_image_snapshot_result_path: Path,
) -> None:
    """Load files, run the update logic, and write outputs."""
    snapshot_file = data_dir / snapshot_path
    if not snapshot_file.is_file():
        raise FileNotFoundError(f"No valid snapshot file was provided: {snapshot_file}")

    data_file = data_dir / data_path
    if not data_file.is_file():
        raise FileNotFoundError(f"No data JSON was provided: {data_file}")

    snapshot = file_utils.load_json_dict(snapshot_file)
    data = file_utils.load_json_dict(data_file)

    credentials = data.get("fbc", {}).get("publishingCredentials", "")
    if not credentials:
        raise ValueError("data.fbc.publishingCredentials is missing or empty")

    updated_snapshot = update_index_images(
        snapshot,
        credentials,
        task_git_url,
        task_git_revision,
        task_id,
        pipelinerun_uid,
    )

    output_dir = snapshot_file.parent
    output_file = output_dir / UPDATED_SNAPSHOT_FILENAME
    output_file.write_text(json.dumps(updated_snapshot) + "\n", encoding="utf-8")
    index_image_snapshot_result_path.write_text(
        UPDATED_SNAPSHOT_FILENAME,
        encoding="utf-8",
    )
    logger.info("Wrote updated snapshot to %s", output_file)


def main() -> int:
    """Entry point for update-index-image task."""
    data_dir = Path(tekton.require_env("PARAM_DATA_DIR"))
    run_update_index_image(
        data_dir=data_dir,
        snapshot_path=tekton.require_env("PARAM_SNAPSHOT_PATH"),
        data_path=tekton.require_env("PARAM_DATA_PATH"),
        task_git_url=tekton.require_env("PARAM_TASK_GIT_URL"),
        task_git_revision=tekton.require_env("PARAM_TASK_GIT_REVISION"),
        task_id=tekton.require_env("PARAM_TASK_ID"),
        pipelinerun_uid=tekton.require_env("PARAM_PIPELINERUN_UID"),
        index_image_snapshot_result_path=tekton.result_paths_from_env(
            "RESULT_INDEX_IMAGE_SNAPSHOT_PATH",
        )[0],
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
