#!/usr/bin/env python3
"""Filter already-released images from a snapshot before downstream validation.

Check target registries to determine if push-snapshot has completed successfully
for each component by validating that ALL required tags exist with the correct
digest.  Components that are fully released (all tags present in at least one
target repository) are filtered out.  The snapshot file is overwritten in place.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import tekton
from authentication import setup_ca_cert
from logger import logger
from oras_utils import oras_resolve

PROG = "filter_already_released_images.py"


def _check_tag(repo_url: str, tag: str, expected_digest: str) -> bool:
    """Return True when *repo_url*:*tag* resolves to *expected_digest*."""
    actual = oras_resolve(f"{repo_url}:{tag}", auth_ref=repo_url, check=False)

    if actual is None:
        logger.info("    Tag %s: cannot resolve (treating as not found)", tag)
        return False
    if actual != expected_digest:
        logger.info("    Tag %s: DIGEST MISMATCH", tag)
        logger.info("      Expected: %s", expected_digest)
        logger.info("      Found:    %s", actual)
        return False

    logger.info("    Tag %s: MATCH (%s)", tag, actual)
    return True


def is_component_released(component: dict, digest: str) -> bool:
    """Return True when *component* is fully released to any target repository.

    A component is considered released when at least one of its mapped
    repositories has ALL required tags pointing to *digest*.  Components
    without repositories or with only invalid repository entries return False.
    """
    repositories = component.get("repositories") or []
    if not repositories:
        return False

    component_name = component.get("name", "?")
    logger.info(
        "Checking component: %s (%d target repositories)",
        component_name,
        len(repositories),
    )

    for j, repo_obj in enumerate(repositories):
        repo_url = repo_obj.get("url") or ""
        tags = repo_obj.get("tags") or []

        if not repo_url:
            logger.warning("  Repository #%d has empty URL, skipping", j + 1)
            continue
        if not tags:
            logger.warning(
                "  Repository %s has no tags specified, skipping",
                repo_url,
            )
            continue

        logger.info("  Checking repository: %s (%d tags)", repo_url, len(tags))

        if all(_check_tag(repo_url, tag, digest) for tag in tags):
            return True

    return False


def _partition_components(
    components: list[dict],
) -> tuple[list[dict], int]:
    """Classify components as kept or already-released.

    Return ``(kept_list, filtered_count)``.  Components that cannot be
    resolved or have no target repositories are always kept.
    """
    kept: list[dict] = []
    filtered = 0

    for component in components:
        component_name = component.get("name", "?")
        container_image = component.get("containerImage", "")

        digest = oras_resolve(container_image, check=False)
        if digest is None:
            logger.warning(
                "Cannot resolve component image %s, treating as not yet released",
                container_image,
            )
            kept.append(component)
            continue

        logger.info("  Component digest: %s", digest)

        if is_component_released(component, digest):
            logger.info("Component %s: FILTERED (already released)", component_name)
            filtered += 1
        else:
            logger.info("Component %s: KEPT (needs to be released)", component_name)
            kept.append(component)

    return kept, filtered


def filter_snapshot(snapshot_path: Path) -> tuple[int, int]:
    """Filter already-released components from the snapshot file.

    Overwrite *snapshot_path* in place with only the components that still
    need to be released.  Return ``(total_count, filtered_count)``.
    """
    snapshot = json.loads(snapshot_path.read_text(encoding="utf-8"))
    components = snapshot.get("components") or []
    total = len(components)

    kept, filtered = _partition_components(components)

    snapshot["components"] = kept
    snapshot_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")

    logger.info("SUMMARY:")
    logger.info("  Total components: %d", total)
    logger.info("  Filtered (already released): %d", filtered)
    logger.info("  To be released: %d", total - filtered)

    return total, filtered


def run(snapshot_path: Path, result_skip_release: Path) -> None:
    """Orchestrate filtering and write the skip_release result."""
    if not snapshot_path.is_file():
        raise RuntimeError(f"Snapshot file not found: {snapshot_path}")

    total, filtered = filter_snapshot(snapshot_path)

    skip = filtered == total and total > 0
    result_skip_release.write_text("true" if skip else "false", encoding="utf-8")


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
    """Parse arguments, resolve Tekton result paths, and run the filter."""
    setup_ca_cert()
    args = _parse_args(argv)
    (result_skip_release,) = tekton.result_paths_from_env("RESULT_SKIP_RELEASE")
    run(Path(args.snapshot_path), result_skip_release)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
