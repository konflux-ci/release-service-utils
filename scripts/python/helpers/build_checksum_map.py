#!/usr/bin/env python3
"""Build an OCI artifact containing a checksum map of published files.

For each component's ``ready_for_distribution`` directory, computes sha256 checksums
for all files (excluding ``sha256sum.txt*``) and assembles them into a JSON manifest.
The manifest is packaged as a tar archive and pushed to OCI using ``oras push``.
The resulting ``store@digest`` reference is returned by ``run()`` for the caller to record.
The manifest is stored as an OCI artifact so that downstream advisory tooling has a stable,
addressable pointer it can pull independently to construct PURLs for released files.

Secret mounts:
  ``TRUSTED_ARTIFACTS_DOCKERCONFIG_MOUNT``  (default: ``/mnt/trusted_artifacts_dockerconfig``)

Other env vars:
  ``SNAPSHOT_JSON``        – JSON string of the Snapshot spec
  ``CONTENT_DIR``          – override base directory (default: ``/shared/artifacts``)
  ``SHARED_DIR``           – override shared volume root (default: ``/shared``)
  ``OCI_STORE``            – OCI repository for checksum map artifacts
                             (default:
                             ``quay.io/konflux-ci/release-service-trusted-artifacts``)
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import tarfile
import tempfile
from pathlib import Path

import authentication
import file as file_utils
import oras_utils

PROG = "build_checksum_map.py"

TRUSTED_ARTIFACTS_DOCKERCONFIG_MOUNT = Path(
    os.environ.get(
        "TRUSTED_ARTIFACTS_DOCKERCONFIG_MOUNT", "/mnt/trusted_artifacts_dockerconfig"
    )
)
CONTENT_DIR = Path(os.environ.get("CONTENT_DIR", "/shared/artifacts"))
SHARED_DIR = Path(os.environ.get("SHARED_DIR", "/shared"))

OCI_STORE = os.environ.get("OCI_STORE", "quay.io/konflux-ci/release-service-trusted-artifacts")

logger = logging.getLogger(__name__)


def _setup_docker_config() -> None:
    """Copy the mounted dockerconfig into ~/.docker/config.json if present and non-empty."""
    authentication.setup_docker_config(
        TRUSTED_ARTIFACTS_DOCKERCONFIG_MOUNT / ".dockerconfigjson",
        optional=True,
    )
    logger.info("Docker config loaded for OCI push")


def run() -> str:
    """Build a checksum manifest, push as an OCI artifact, and return the store@digest ref."""
    shared_snapshot = SHARED_DIR / "snapshot.json"
    if shared_snapshot.exists():
        snapshot = json.loads(shared_snapshot.read_text())
    else:
        snapshot = json.loads(os.environ["SNAPSHOT_JSON"])

    _setup_docker_config()

    checksum_manifest: list[dict] = []

    for component in snapshot.get("components", []):
        name = component.get("name", "")
        ready_dir = CONTENT_DIR / name / "ready_for_distribution"

        logger.info("Generating checksum manifest for component: %s", name)

        if not ready_dir.is_dir():
            logger.warning("ready_for_distribution directory not found for %s", name)
            continue

        checksum_files: dict[str, str] = {}
        for f in sorted(ready_dir.rglob("*")):
            if f.is_file() and not f.name.startswith("sha256sum.txt"):
                checksum = file_utils.sha256(f)
                checksum_files[f.name] = f"sha256:{checksum}"

        checksum_manifest.append({"component": name, "files": checksum_files})

    archive_dir = Path(tempfile.mkdtemp())
    try:
        manifest_path = archive_dir / "checksum_map.json"
        manifest_path.write_text(json.dumps(checksum_manifest, separators=(",", ":")))
        logger.info("Checksum manifest contents:")
        logger.info("%s", manifest_path.read_text())

        archive_path = archive_dir / "checksum_map"
        with tarfile.open(str(archive_path), "w:gz", compresslevel=9) as tf:
            tf.add(str(manifest_path), arcname="checksum_map.json")

        pushed_digest = oras_utils.oras_push(
            OCI_STORE, archive_dir, "checksum_map", "checksum_map"
        )
        ref = f"{OCI_STORE}@{pushed_digest}"
        logger.info("Checksum map pushed to OCI: %s", ref)
        return ref
    finally:
        shutil.rmtree(str(archive_dir), ignore_errors=True)


def main(argv: list[str] | None = None) -> int:
    """Run checksum map build and push; return exit code."""
    logging.basicConfig(level=logging.INFO)
    try:
        ref = run()
        logger.info("Checksum map pushed to: %s", ref)
    except Exception as exc:
        logger.error("ERROR: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
