#!/usr/bin/env python3
"""Update trusted-tasks list OCI artifact with released task bundles.

Tekton injects ``PARAM_SNAPSHOT_PATH`` and ``PARAM_DATA_DIR`` via env.

For each component/repository/tag in the snapshot, the script:
1. Derives a sibling ``data-acceptable-bundles`` OCI repo in the same org
2. Checks if ``:latest`` exists via ``skopeo inspect``
3. Runs ``ec track bundle`` to append the bundle
4. Tags the result as ``:latest`` via ``skopeo copy``
"""

from __future__ import annotations

import subprocess
import time
from pathlib import Path
from typing import Any

from release_service_utils.helpers import file, skopeo, tekton
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.subprocess_cmd import run_cmd_text

DEFAULT_DATA_DIR = "/var/workdir/release"


def derive_acceptable_bundles_repo(repository_url: str) -> str:
    """Replace the last path segment of *repository_url* with ``data-acceptable-bundles``."""
    parts = repository_url.rsplit("/", 1)
    return f"{parts[0]}/data-acceptable-bundles"


def check_latest_exists(acceptable_bundles: str) -> bool:
    """Return ``True`` if ``:latest`` tag exists for *acceptable_bundles*."""
    result = skopeo.inspect(
        f"{acceptable_bundles}:latest",
        raw=True,
        no_tags=True,
        check=False,
    )
    return result.returncode == 0


def _ec_track_bundle(
    bundle_ref: str,
    output_ref: str,
    input_ref: str | None = None,
) -> None:
    """Run ``ec track bundle`` to append or create the OCI data bundle."""
    cmd: list[str] = [
        "ec",
        "track",
        "bundle",
        "--bundle",
        bundle_ref,
    ]
    if input_ref is not None:
        cmd.extend(["--input", f"oci:{input_ref}"])
    cmd.extend(["--output", f"oci:{output_ref}"])
    run_cmd_text(cmd)


def _tag_as_latest(acceptable_bundles: str, timestamp_tag: str) -> None:
    """Copy the timestamp-tagged image to ``:latest``."""
    result = skopeo.copy(
        f"docker://{acceptable_bundles}:{timestamp_tag}",
        f"docker://{acceptable_bundles}:latest",
        retry_times=3,
    )
    if result.returncode != 0:
        logger.error("skopeo copy failed: %s", result.stderr)
        raise subprocess.CalledProcessError(
            result.returncode,
            result.args,
            output=result.stdout,
            stderr=result.stderr,
        )


def _process_repository(
    repository: dict[str, Any],
    digest: str,
    timestamp_tag: str,
) -> None:
    """Track all tags for a single repository into its acceptable-bundles artifact."""
    repo_url = str(repository["url"])
    acceptable_bundles = derive_acceptable_bundles_repo(repo_url)
    latest_exists = check_latest_exists(acceptable_bundles)

    if latest_exists:
        logger.info(
            "%s:latest exists - using it as an input",
            acceptable_bundles,
        )
    else:
        logger.info("%s:latest does not exist", acceptable_bundles)

    for tag in repository.get("tags") or []:
        bundle_ref = f"{repo_url}:{tag}@{digest}"
        output_ref = f"{acceptable_bundles}:{timestamp_tag}"

        if latest_exists:
            logger.info("Adding %s to %s", bundle_ref, output_ref)
            _ec_track_bundle(
                bundle_ref,
                output_ref,
                input_ref=f"{acceptable_bundles}:latest",
            )
        else:
            logger.info("Creating %s with %s", output_ref, bundle_ref)
            _ec_track_bundle(bundle_ref, output_ref)

        logger.info("Tagging %s as :latest", output_ref)
        _tag_as_latest(acceptable_bundles, timestamp_tag)
        latest_exists = True


def run(*, snapshot_file: Path) -> None:
    """Process all components, repositories, and tags in the snapshot."""
    snapshot = file.load_json_dict(snapshot_file)
    component_group = snapshot.get("componentGroup", "")
    timestamp_tag = str(int(time.time()))

    logger.info('Beginning "update-trusted-tasks" for "%s"', component_group)

    for component in snapshot.get("components") or []:
        container_image = component["containerImage"]
        if "@" not in container_image:
            raise ValueError(f"containerImage missing digest: {container_image!r}")
        _, digest = container_image.rsplit("@", 1)

        for repository in component.get("repositories") or []:
            _process_repository(repository, digest, timestamp_tag)


def main() -> int:
    """Read Tekton env vars and run the update workflow."""
    data_dir = file.path_from_env_variable("PARAM_DATA_DIR", DEFAULT_DATA_DIR)
    snapshot_path = tekton.require_env("PARAM_SNAPSHOT_PATH")
    snapshot_file = file.resolve_path_under_base(data_dir, snapshot_path)

    run(snapshot_file=snapshot_file)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
