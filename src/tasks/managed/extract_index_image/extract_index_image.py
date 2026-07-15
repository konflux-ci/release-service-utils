#!/usr/bin/env python3
"""Extract index image fields from InternalRequest build results."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import file
import tekton
from logger import logger

RESULTS_FILENAME = "extract-index-image-results.json"


def extract_index_image_results(
    internal_request_results: dict[str, Any],
) -> dict[str, Any]:
    """Return index image data keyed by OCP version.

    For each ``ocp_version`` in ``components``, keep the last component entry.
    """
    raw_components = internal_request_results.get("components")
    if not isinstance(raw_components, list):
        msg = "components must be a JSON array"
        raise ValueError(msg)

    dict_components = [row for row in raw_components if isinstance(row, dict)]

    index_image: dict[str, dict[str, Any]] = {}
    for row in dict_components:
        ocp_version = row.get("ocp_version")
        key = "" if ocp_version is None else str(ocp_version)
        index_image[key] = {
            "index_image": row.get("index_image"),
            "index_image_resolved": row.get("index_image_resolved"),
        }

    return {"index_image": index_image}


def extract_index_image(
    *,
    data_dir: Path,
    results_dir_path: Path,
    internal_request_results_file: Path,
) -> Path:
    """Load InternalRequest results and write extract-index-image output."""
    input_path = data_dir / internal_request_results_file
    if not input_path.is_file():
        msg = f"InternalRequest results file not found: {input_path}"
        raise FileNotFoundError(msg)

    logger.info("Loading InternalRequest results from %s", input_path)
    internal_request_results = file.load_json_dict(input_path)
    payload = extract_index_image_results(internal_request_results)

    output_path = data_dir / results_dir_path / RESULTS_FILENAME
    logger.info("Writing index image results to %s", output_path)
    text = json.dumps(payload, indent=2) + "\n"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(text, encoding="utf-8")
    sys.stdout.write(text)
    return output_path


def main() -> int:
    """Run the extract-index-image workflow."""
    extract_index_image(
        data_dir=Path(tekton.require_env("PARAM_DATA_DIR")),
        results_dir_path=Path(tekton.require_env("PARAM_RESULTS_DIR_PATH")),
        internal_request_results_file=Path(
            tekton.require_env("PARAM_INTERNAL_REQUEST_RESULTS_FILE"),
        ),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
