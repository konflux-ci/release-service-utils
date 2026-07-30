#!/usr/bin/env python3
"""Push Konflux build artifacts to artifact storage via pulp-tool."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from release_service_utils.helpers import file
from release_service_utils.helpers import oras_utils
from release_service_utils.helpers import tekton
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.subprocess_cmd import run_cmd

DEFAULT_DATA_DIR = "/var/workdir/release"
ROK_ACCESS_PATH = Path("/etc/rok-access")
RESULTS_DIR = Path("/var/workdir/results")


def pull_component_artifacts(
    snapshot: dict[str, Any],
    results_dir: Path,
) -> None:
    """Pull OCI artifacts for every component in the snapshot."""
    components = snapshot.get("components", [])
    logger.info("Pulling artifacts for %d component(s)", len(components))

    results_dir.mkdir(parents=True, exist_ok=True)

    for component in components:
        container_image = component["containerImage"]
        logger.info("Pulling %s", container_image)
        oras_utils.oras_pull(container_image, results_dir)


def push_to_storage(
    snapshot: dict[str, Any],
    data: dict[str, Any],
    rok_access_path: Path,
    results_dir: Path,
    snapshot_build_id: str,
    snapshot_namespace: str,
) -> None:
    """Push pulled RPM artifacts to artifact storage via pulp-tool.

    Skips silently when the rok-access config is missing or when
    ``pushOptions.koji_import_draft`` is ``false`` in the data file.
    """
    config_file = rok_access_path / "cli.toml"
    if not config_file.is_file():
        logger.info("No rok-access config found at %s, skipping", config_file)
        return

    koji_import_draft = data.get("pushOptions", {}).get("koji_import_draft", False)
    if koji_import_draft is False:
        logger.info("Draft build — skipping Artifact Storage (koji_import_draft is False)")
        return

    component_group = snapshot.get("componentGroup", "")
    logger.info(
        "Uploading artifacts for build %s in namespace %s",
        snapshot_build_id,
        snapshot_namespace,
    )

    run_cmd(
        [
            "pulp-tool",
            "--config",
            str(config_file),
            "--build-id",
            snapshot_build_id,
            "--namespace",
            snapshot_namespace,
            "upload",
            "--rpm-path",
            str(results_dir),
        ]
    )

    logger.info('Completed push-artifacts-to-storage for "%s"', component_group)


def run(
    *,
    data_dir: Path,
    snapshot_path: str,
    data_path: str,
    snapshot_build_id: str,
    snapshot_namespace: str,
    rok_access_path: Path,
    results_dir: Path,
) -> None:
    """Pull OCI artifacts and push them to artifact storage."""
    snapshot_file = file.resolve_path_under_base(data_dir, snapshot_path)
    data_file = file.resolve_path_under_base(data_dir, data_path)

    snapshot = file.load_json_dict(snapshot_file)
    data = file.load_json_dict(data_file)

    pull_component_artifacts(snapshot, results_dir)

    push_to_storage(
        snapshot,
        data,
        rok_access_path,
        results_dir,
        snapshot_build_id,
        snapshot_namespace,
    )


def main() -> int:
    """Read Tekton env vars and run the push workflow."""
    data_dir = file.path_from_env_variable("PARAM_DATA_DIR", DEFAULT_DATA_DIR)
    snapshot_path = tekton.require_env("PARAM_SNAPSHOT_PATH")
    data_path = tekton.require_env("PARAM_DATA_PATH")
    snapshot_build_id = tekton.require_env("PARAM_SNAPSHOT_BUILD_ID")
    snapshot_namespace = tekton.require_env("PARAM_SNAPSHOT_NAMESPACE")

    run(
        data_dir=data_dir,
        snapshot_path=snapshot_path,
        data_path=data_path,
        snapshot_build_id=snapshot_build_id,
        snapshot_namespace=snapshot_namespace,
        rok_access_path=ROK_ACCESS_PATH,
        results_dir=RESULTS_DIR,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
