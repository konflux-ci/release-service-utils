#!/usr/bin/env python3
"""Inspect a built FBC target index image using skopeo."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from logger import logger
from subprocess_cmd import run_cmd

import tekton


def read_credential(path: Path) -> str:
    """Read a registry credential from a mounted secret file.

    Args:
        path: Path to the credential file (user:password format).

    Returns:
        The credential string.

    """
    return path.read_text(encoding="utf-8").strip()


def inspect_image(source_index: str, credential: str) -> dict[str, Any]:
    """Inspect a container image with skopeo and return its sha and per-arch digests.

    Args:
        source_index: Image pullspec to inspect (e.g. quay.io/redhat/index:v4.13).
        credential: Registry credential in user:password format.

    Returns:
        Dict with "sha" (overall image digest) and "digests" (list of per-arch digests).

    """
    result = run_cmd(
        [
            "skopeo",
            "inspect",
            "--retry-times",
            "3",
            "--creds",
            credential,
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
            "--creds",
            credential,
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
    credentials_path = Path(tekton.require_env("PARAM_INSPECT_CREDENTIALS_PATH"))
    (result_path,) = tekton.result_paths_from_env("RESULT_REQUEST_MESSAGE_PATH")

    try:
        credential = read_credential(credentials_path)
        result = inspect_image(source_index, credential)
        result_path.write_text(json.dumps(result, separators=(",", ":")), encoding="utf-8")
    except Exception as exc:  # Tekton task: always exit 0, report via result
        logger.error("Failed to inspect target index: %s", exc)
        result_path.write_text("Error: Failed to inspect target index", encoding="utf-8")

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
