#!/usr/bin/env python3
"""Validate that all FBC fragment components in a snapshot target one OCP version.

For each snapshot component, inspect its FBC fragment image with
`skopeo inspect` and read the `org.opencontainers.image.base.name`
annotation to get the OCP version tag. Multi-arch images (OCI index or
Docker manifest-list) are resolved to a single platform's manifest first via
`get-image-architectures`. Every component must report the same `vX.Y`
version, which is written to the Tekton result.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

from release_service_utils.helpers import tekton
from release_service_utils.helpers.file import load_json_dict
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.ocp_version import resolve_ocp_version

PROG = "get_ocp_version.py"

_VERSION_PATTERN = re.compile(r"^v[0-9]+\.[0-9]+$")


def validate_ocp_versions(snapshot: dict) -> str:
    """Validate every component targets the same, correctly formatted OCP version.

    Return the unified `vX.Y` version string. Raise `ValueError` if a
    component's version is malformed or a version mismatch is found.
    """
    components = snapshot.get("components") or []
    logger.info("Found %d FBC components to validate", len(components))
    if not components:
        raise ValueError("No components found in snapshot")

    validated_version = ""
    for i, component in enumerate(components):
        logger.info("Processing component %d...", i)
        fbc_fragment = component["containerImage"]
        logger.info("  Container image: %s", fbc_fragment)

        image_base_name = resolve_ocp_version(fbc_fragment)

        if not _VERSION_PATTERN.match(image_base_name):
            raise ValueError(
                f"Invalid OCP version format in component {i}: '{image_base_name}'. "
                "Expected format: vX.Y (e.g., v4.12)"
            )
        logger.info("  Extracted OCP version: %s", image_base_name)

        if i == 0:
            validated_version = image_base_name
            logger.info("  First component OCP version: %s", validated_version)
        elif image_base_name != validated_version:
            raise ValueError(
                "OCP version mismatch detected! "
                f"Component 0 OCP version: {validated_version}, "
                f"component {i} OCP version: {image_base_name}. "
                "All FBC fragments in a release must target the same OCP version"
            )
        else:
            logger.info("  Component %d OCP version matches: %s", i, image_base_name)

    logger.info("All %d components validated successfully", len(components))
    logger.info("Unified OCP version: %s", validated_version)
    return validated_version


def run(snapshot_path: Path, result_stored_version: Path) -> None:
    """Orchestrate OCP version validation and write the stored-version result."""
    snapshot = load_json_dict(snapshot_path)
    version = validate_ocp_versions(snapshot)
    result_stored_version.write_text(version, encoding="utf-8")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__, prog=PROG)
    parser.add_argument(
        "--snapshot-path",
        required=True,
        help="Path to the snapshot JSON file",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Parse arguments, resolve the Tekton result path, and run validation."""
    args = _parse_args(argv)
    (result_stored_version,) = tekton.result_paths_from_env("RESULT_STORED_VERSION")
    run(Path(args.snapshot_path), result_stored_version)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
