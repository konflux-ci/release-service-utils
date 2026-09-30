#!/usr/bin/env python3
"""Validate Helm OCI artifacts and their mapped delivery repositories."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from release_service_utils.helpers import file, image_ref, skopeo, tekton
from release_service_utils.helpers.logger import logger

HELM_CONFIG_MEDIA_TYPE = "application/vnd.cncf.helm.config.v1+json"


def _validate_component(component: dict[str, Any]) -> None:
    """Check a component's Helm manifest against every mapped repository."""
    name = component["name"]
    result = skopeo.inspect(component["containerImage"], raw=True, check=True)
    manifest = json.loads(result.stdout)
    config_media_type = (manifest.get("config") or {}).get("mediaType") or ""
    if config_media_type != HELM_CONFIG_MEDIA_TYPE:
        raise ValueError(
            f"component ({name}) is not a Helm OCI artifact (mediaType: {config_media_type})"
        )

    annotations = manifest.get("annotations") or {}
    title = annotations.get("org.opencontainers.image.title") or ""
    version = annotations.get("org.opencontainers.image.version") or ""
    if not title or not version:
        raise ValueError(
            f"component ({name}) Helm manifest missing org.opencontainers.image.title "
            f"({title}) or org.opencontainers.image.version ({version}) annotations"
        )

    repositories = component.get("repositories") or []
    if not repositories:
        raise ValueError(f"component ({name}) has no repositories")

    for repository in repositories:
        url = image_ref.repository(repository["url"])
        expected_basename = url.replace("----", "/").rsplit("/", 1)[-1]
        if title != expected_basename:
            raise ValueError(
                f"component ({name}) chart title ({title}) does not match delivery "
                f"repository basename ({expected_basename})"
            )

        tags = repository.get("tags") or []
        if not tags:
            raise ValueError(f"component ({name}) repository ({url}) has no tags")
        # Helm represents the SemVer build-metadata '+' separator as '_' in OCI tags.
        if not any(tag.replace("_", "+", 1) == version for tag in tags):
            raise ValueError(
                f"component ({name}) repository ({url}) — none of the tags "
                f"[{', '.join(tags)}] match chart version ({version})"
            )

    logger.info(
        "Validated Helm OCI artifact for component (%s) chart (%s) version (%s)",
        name,
        title,
        version,
    )


def run(*, data_dir: Path, snapshot_path: Path) -> None:
    """Load the mapped snapshot and validate each component's Helm metadata."""
    snapshot = file.load_json_dict(data_dir / snapshot_path)
    for component in snapshot.get("components") or []:
        _validate_component(component)


def main() -> int:
    """Read Tekton environment variables and validate the Helm chart snapshot."""
    run(
        data_dir=Path(tekton.require_env("PARAM_DATA_DIR")),
        snapshot_path=Path(tekton.require_env("PARAM_SNAPSHOT_PATH")),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
