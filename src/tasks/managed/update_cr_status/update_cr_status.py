#!/usr/bin/env python3
"""Update the passed CR status with the contents stored in the files in the results dir."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from release_service_utils.helpers import file, json_merge, kubectl, tekton
from release_service_utils.helpers.logger import logger

DEFAULT_RESOURCE_TYPE = "release"
DEFAULT_STATUS_KEY = "artifacts"


def merge_results_dir(results_dir: Path) -> dict[str, Any]:
    """Merge the JSON files in the results directory into one object."""
    merged: dict[str, Any] = {}
    if not results_dir.is_dir():
        logger.info("Results directory %s does not exist, skipping", results_dir)
        return merged

    result_files = [path for path in results_dir.rglob("*") if path.is_file()]
    for results_file in sorted(result_files):
        try:
            content = file.load_json_dict(results_file)
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Passed results JSON file {results_file} in results directory"
                " was not proper JSON."
            ) from e
        # Merge with array concatenation for array fields and object merging
        merged = json_merge.merge_concat_arrays(merged, content)
    return merged


def patch_status(
    resource: str,
    resource_type: str,
    status_key: str,
    results: dict[str, Any],
) -> None:
    """Patch the status of the namespaced resource with the merged results."""
    namespace, _, name = resource.partition("/")
    if not namespace or not name:
        raise ValueError(f"resource must be namespace/name, got '{resource}'")

    # Create patch file to avoid "Argument list too long" error
    patch = json.dumps({"status": {status_key: results}}).encode("utf-8")
    patch_file = file.make_tempfile_path("patch-", data=patch)
    try:
        kubectl.patch_resource(
            resource_type,
            name,
            namespace=namespace,
            patch_file=patch_file,
            subresource="status",
            patch_type="merge",
            warnings_as_errors=True,
        )
    finally:
        # Clean up the temporary patch file
        patch_file.unlink()


def run(
    data_dir: Path,
    results_dir_path: str,
    resource: str,
    resource_type: str,
    status_key: str,
) -> None:
    """Merge the results files and patch them into the resource status."""
    results_dir = file.resolve_path_under_base(data_dir, results_dir_path)
    results = merge_results_dir(results_dir)
    patch_status(resource, resource_type, status_key, results)
    logger.info("Updated status.%s of %s %s", status_key, resource_type, resource)


def main() -> int:
    """Read environment variables and call run()."""
    run(
        data_dir=Path(tekton.require_env("PARAM_DATA_DIR")),
        results_dir_path=tekton.require_env("PARAM_RESULTS_DIR_PATH"),
        resource=tekton.require_env("PARAM_RESOURCE"),
        resource_type=os.environ.get("PARAM_RESOURCE_TYPE", "").strip()
        or DEFAULT_RESOURCE_TYPE,
        status_key=os.environ.get("PARAM_STATUS_KEY", "").strip() or DEFAULT_STATUS_KEY,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
