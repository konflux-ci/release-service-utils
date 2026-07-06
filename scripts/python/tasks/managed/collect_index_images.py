#!/usr/bin/env python3
"""Build a Pyxis index-image snapshot JSON from internal-request results."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import file
import image_ref
import tekton
from logger import logger

SNAPSHOT_FILENAME = "index_image_snapshot.json"
_BARE_OCP_TAG = re.compile(r"^v[0-9]+\.[0-9]+$")


def split_target_index(target_index: str) -> tuple[str, str]:
    """Split *target_index* into repository and tag at the last colon."""
    repository, _, tag = target_index.rpartition(":")
    if not repository or not tag:
        msg = f"target_index must contain a repository and tag: {target_index!r}"
        raise ValueError(msg)
    return repository, tag


def build_tags(tag: str, build_timestamp: str) -> list[str]:
    """Return snapshot tags, appending a timestamp tag for bare OCP versions."""
    tags = [tag]
    if _BARE_OCP_TAG.fullmatch(tag):
        tags.append(f"{tag}-{build_timestamp}")
    return tags


def translation_repo_url(translated: list[dict[str, Any]], repo_key: str) -> str:
    """Return the host/path portion of a translated delivery-repo URL."""
    for entry in translated:
        if entry.get("repo") != repo_key:
            continue
        url = entry.get("url")
        if not isinstance(url, str) or not url:
            return ""
        return url.split(":", 1)[0]
    return ""


def build_repo_object(
    repository: str,
    tags: list[str],
    rh_registry_repo: str,
    registry_access_repo: str,
) -> dict[str, Any]:
    """Build the repositories entry for one index image component."""
    repo_object: dict[str, Any] = {
        "url": repository,
        "tags": tags,
    }
    if rh_registry_repo:
        repo_object["rh-registry-repo"] = rh_registry_repo
    if registry_access_repo:
        repo_object["registry-access-repo"] = registry_access_repo
    return repo_object


def build_index_component(
    *,
    source_index: str,
    repository: str,
    tags: list[str],
    image_digests: list[Any],
    repo_object: dict[str, Any],
) -> dict[str, Any]:
    """Build one snapshot component object."""
    return {
        "containerImage": source_index,
        "repository": repository,
        "repositories": [repo_object],
        "tags": tags,
        "imageDigests": image_digests,
    }


def collect_index_image_components(
    results: dict[str, Any],
    build_timestamp: str,
) -> dict[str, Any]:
    """Transform internal-request results into an index-image snapshot."""
    components_in = results.get("components")
    if not isinstance(components_in, list):
        msg = "internal request results components must be a JSON array"
        raise ValueError(msg)

    snapshot_components: list[dict[str, Any]] = []
    for index, row in enumerate(components_in):
        if not isinstance(row, dict):
            msg = f"components[{index}] must be a JSON object"
            raise ValueError(msg)

        target_index = row.get("target_index")
        source_index = row.get("index_image_resolved")
        if not isinstance(target_index, str) or not target_index.strip():
            msg = f"components[{index}].target_index must be a non-empty string"
            raise ValueError(msg)
        if not isinstance(source_index, str) or not source_index.strip():
            msg = f"components[{index}].index_image_resolved must be a non-empty string"
            raise ValueError(msg)

        repository, tag = split_target_index(target_index)
        tags = build_tags(tag, build_timestamp)
        image_digests = row.get("image_digests", [])
        if not isinstance(image_digests, list):
            msg = f"components[{index}].image_digests must be a JSON array"
            raise ValueError(msg)

        logger.info(
            "Processing index image %s (%d/%d)",
            target_index,
            index + 1,
            len(components_in),
        )
        translated = image_ref.translate_delivery_repo(repository)
        rh_registry_repo = translation_repo_url(translated, "redhat.io")
        registry_access_repo = translation_repo_url(translated, "access.redhat.com")
        repo_object = build_repo_object(
            repository,
            tags,
            rh_registry_repo,
            registry_access_repo,
        )
        snapshot_components.append(
            build_index_component(
                source_index=source_index,
                repository=repository,
                tags=tags,
                image_digests=image_digests,
                repo_object=repo_object,
            ),
        )

    return {"components": snapshot_components}


def run_collect_index_images(
    *,
    data_dir: Path,
    internal_request_results_file: Path,
    build_timestamp: str,
    snapshot_path: Path,
    index_image_snapshot_result_path: Path,
) -> None:
    """Load results, build the snapshot JSON, and write Tekton outputs."""
    results_path = data_dir / internal_request_results_file
    if not results_path.is_file():
        msg = f"internal request results file not found: {results_path}"
        raise FileNotFoundError(msg)

    logger.info("Loading internal request results from %s", results_path)
    results = file.load_json_dict(results_path)
    snapshot = collect_index_image_components(results, build_timestamp)

    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_path.write_text(json.dumps(snapshot) + "\n", encoding="utf-8")
    index_image_snapshot_result_path.write_text(SNAPSHOT_FILENAME, encoding="utf-8")
    logger.info("Wrote index image snapshot to %s", snapshot_path)


def main() -> int:
    """Run the collect-index-images workflow."""
    data_dir = Path(tekton.require_env("PARAM_DATA_DIR"))
    run_collect_index_images(
        data_dir=data_dir,
        internal_request_results_file=Path(
            tekton.require_env("PARAM_INTERNAL_REQUEST_RESULTS_FILE"),
        ),
        build_timestamp=tekton.require_env("PARAM_BUILD_TIMESTAMP"),
        snapshot_path=data_dir / SNAPSHOT_FILENAME,
        index_image_snapshot_result_path=tekton.result_paths_from_env(
            "RESULT_INDEX_IMAGE_SNAPSHOT_PATH",
        )[0],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
