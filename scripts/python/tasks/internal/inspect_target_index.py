#!/usr/bin/env python3
"""Inspect a built FBC target index image using skopeo."""

from __future__ import annotations

import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any

from logger import logger
from subprocess_cmd import run_cmd

import tekton


def inspect_image(source_index: str, auth_file: Path) -> dict[str, Any]:
    """Inspect a container image with skopeo and return its sha and per-arch digests.

    Args:
        source_index: Image pullspec to inspect (e.g. quay.io/redhat/index:v4.13).
        auth_file: Path to a Docker-style auth JSON file for registry authentication.

    Returns:
        Dict with "sha" (overall image digest) and "digests" (list of per-arch digests).

    """
    result = run_cmd(
        [
            "skopeo",
            "inspect",
            "--retry-times",
            "3",
            "--authfile",
            str(auth_file),
            f"docker://{source_index}",
        ]
    )
    image_info = json.loads(result.stdout)
    sha = image_info["Digest"]

    raw_result = run_cmd(
        [
            "skopeo",
            "inspect",
            "--retry-times",
            "3",
            "--raw",
            "--authfile",
            str(auth_file),
            f"docker://{source_index}",
        ]
    )
    manifest = json.loads(raw_result.stdout)
    digests = [m["digest"] for m in manifest["manifests"]]

    logger.info("Inspected %s: sha=%s, %d arch digests", source_index, sha, len(digests))
    return {"sha": sha, "digests": digests}


def main() -> int:
    """Entry point for inspect-target-index task."""
    source_index = tekton.require_env("PARAM_SOURCE_INDEX")
    (result_path,) = tekton.result_paths_from_env("RESULT_REQUEST_MESSAGE_PATH")

    auth_file: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", delete=False) as auth_fp:
            auth_file = Path(auth_fp.name)
            auth_data = subprocess.check_output(
                ["select-oci-auth", source_index], stderr=subprocess.PIPE
            )
            auth_fp.write(auth_data)

        result = inspect_image(source_index, auth_file)
        result_path.write_text(json.dumps(result, separators=(",", ":")), encoding="utf-8")
    except Exception as exc:  # Tekton task: always exit 0, report via result
        logger.error("Failed to inspect target index: %s", exc)
        result_path.write_text("Error: Failed to inspect target index", encoding="utf-8")
    finally:
        if auth_file is not None:
            auth_file.unlink(missing_ok=True)

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
