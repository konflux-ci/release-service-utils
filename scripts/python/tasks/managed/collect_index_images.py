#!/usr/bin/env python3
"""Build a Pyxis index-image snapshot JSON from internal-request results."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import file
import image_ref
import tekton
from logger import logger

SNAPSHOT_FILENAME = "index_image_snapshot.json"


def split_target_index(target_index: str) -> tuple[str, str]:
    """Split *target_index* into repository and tag at the last colon."""
    repository, _, tag = target_index.rpartition(":")
    if not repository or not tag:
        msg = f"target_index must contain a repository and tag: {target_index!r}"
        raise ValueError(msg)
    return repository, tag


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
    tag: str,
    rh_registry_repo: str,
    registry_access_repo: str,
) -> dict[str, Any]:
    """Build the repositories entry for one index image component."""
    repo_object: dict[str, Any] = {
        "url": repository,
        "tags": [tag],
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
    tag: str,
    image_digests: list[Any],
    repo_object: dict[str, Any],
) -> dict[str, Any]:
    """Build one snapshot component object."""
    return {
        "containerImage": source_index,
        "repository": repository,
        "repositories": [repo_object],
        "tags": [tag],
        "imageDigests": image_digests,
    }


def collect_index_image_components(
    results: dict[str, Any],
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
        target_index_with_timestamp = row.get("target_index_with_timestamp")
        source_index = row.get("index_image_resolved")

        if not isinstance(source_index, str) or not source_index.strip():
            msg = (
                f"components[{index}].index_image_resolved must be a non-empty string, "
                f"got {type(source_index).__name__}: {source_index!r}"
            )
            raise ValueError(msg)

        image_digests = row.get("image_digests", [])
        if not isinstance(image_digests, list):
            msg = (
                f"components[{index}].image_digests must be a JSON array, "
                f"got {type(image_digests).__name__}: {image_digests!r}"
            )
            raise ValueError(msg)

        # Validate target index fields are strings or missing
        if target_index is not None and not isinstance(target_index, str):
            msg = (
                f"components[{index}].target_index must be a string, "
                f"got {type(target_index).__name__}: {target_index!r}"
            )
            raise ValueError(msg)
        if target_index_with_timestamp is not None and not isinstance(
            target_index_with_timestamp, str
        ):
            msg = (
                f"components[{index}].target_index_with_timestamp must be a string, "
                f"got {type(target_index_with_timestamp).__name__}: "
                f"{target_index_with_timestamp!r}"
            )
            raise ValueError(msg)

        # Normalize missing to empty string
        target_index = (target_index or "").strip()
        target_index_with_timestamp = (target_index_with_timestamp or "").strip()

        # Build list of targets to process, filtering empty values
        targets = []
        if target_index:
            targets.append(target_index)
        if target_index_with_timestamp and target_index_with_timestamp != target_index:
            targets.append(target_index_with_timestamp)

        # Create one component per target
        for target in targets:
            repository, tag = split_target_index(target)

            logger.info(
                "Processing index image %s (%d/%d)",
                target,
                index + 1,
                len(components_in),
            )

            translated = image_ref.translate_delivery_repo(repository)
            rh_registry_repo = translation_repo_url(translated, "redhat.io")
            registry_access_repo = translation_repo_url(translated, "access.redhat.com")
            repo_object = build_repo_object(
                repository,
                tag,
                rh_registry_repo,
                registry_access_repo,
            )
            snapshot_components.append(
                build_index_component(
                    source_index=source_index,
                    repository=repository,
                    tag=tag,
                    image_digests=image_digests,
                    repo_object=repo_object,
                ),
            )

    return {"components": snapshot_components}


def run_collect_index_images(
    *,
    data_dir: Path,
    internal_request_results_file: Path,
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
    snapshot = collect_index_image_components(results)

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
        snapshot_path=data_dir / SNAPSHOT_FILENAME,
        index_image_snapshot_result_path=tekton.result_paths_from_env(
            "RESULT_INDEX_IMAGE_SNAPSHOT_PATH",
        )[0],
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
