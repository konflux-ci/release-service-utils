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
  ``SNAPSHOT_JSON``  – JSON string of the Snapshot spec
  ``CONTENT_DIR``    – override base directory (default: ``/shared/artifacts``)
  ``SHARED_DIR``     – override shared volume root (default: ``/shared``)
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

PROG = "build_checksum_map.py"

TRUSTED_ARTIFACTS_DOCKERCONFIG_MOUNT = Path(
    os.environ.get(
        "TRUSTED_ARTIFACTS_DOCKERCONFIG_MOUNT", "/mnt/trusted_artifacts_dockerconfig"
    )
)
CONTENT_DIR = Path(os.environ.get("CONTENT_DIR", "/shared/artifacts"))
SHARED_DIR = Path(os.environ.get("SHARED_DIR", "/shared"))

OCI_STORE = "quay.io/konflux-ci/release-service-trusted-artifacts"

logger = logging.getLogger(__name__)


def _sha256(path: Path) -> str:
    """Return the hex SHA-256 digest of the file at path."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _setup_docker_config() -> None:
    """Copy the mounted dockerconfig into ~/.docker/config.json if present and non-empty."""
    dockerconfig_path = TRUSTED_ARTIFACTS_DOCKERCONFIG_MOUNT / ".dockerconfigjson"
    if dockerconfig_path.is_file() and dockerconfig_path.stat().st_size > 0:
        docker_dir = Path.home() / ".docker"
        docker_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(dockerconfig_path), str(docker_dir / "config.json"))
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
                checksum = _sha256(f)
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

        auth_data = subprocess.check_output(
            ["select-oci-auth", OCI_STORE], stderr=subprocess.PIPE
        )
        auth_file = Path(tempfile.mktemp())
        auth_file.write_bytes(auth_data)

        try:
            oras_output = subprocess.check_output(
                [
                    "oras",
                    "push",
                    "--annotation=quay.expires-after=1d",
                    "--registry-config",
                    str(auth_file),
                    OCI_STORE,
                    "checksum_map",
                ],
                cwd=str(archive_dir),
                stderr=subprocess.STDOUT,
                text=True,
            )
        finally:
            auth_file.unlink(missing_ok=True)

        logger.info("%s", oras_output)

        match = re.search(r"^Digest:\s+(sha256:[a-f0-9]{64})", oras_output, re.MULTILINE)
        if not match:
            raise RuntimeError("Failed to extract digest from oras push output")

        pushed_digest = match.group(1)
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
