"""Extract Python packages from OCI artifacts and populate release notes."""

from __future__ import annotations

from release_service_utils.tasks.managed.extract_py_artifacts.extract_py_artifacts import (
    collect_wheel_artifacts,
    container_images_from_snapshot,
    fetch_chains_provenance,
    main,
    parse_wheel_filename,
    parse_wheel_platform,
    pull_oci_artifacts,
    run,
    update_mapping_components,
    update_release_notes_artifacts,
)

__all__ = [
    "collect_wheel_artifacts",
    "container_images_from_snapshot",
    "fetch_chains_provenance",
    "main",
    "parse_wheel_filename",
    "parse_wheel_platform",
    "pull_oci_artifacts",
    "run",
    "update_mapping_components",
    "update_release_notes_artifacts",
]
