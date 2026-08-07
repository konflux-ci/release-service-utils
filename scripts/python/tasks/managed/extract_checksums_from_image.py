#!/usr/bin/env python3
"""Extract SHA256SUMS checksum files from container images for GitHub releases.

Processes container images listed in a snapshot specification, extracts
binaries to a temporary directory, retains only checksum files (``*SHA256SUMS``)
in the output, and writes the relative output path to a Tekton result file.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from collections.abc import Callable
from pathlib import Path

import extract_artifacts
import file
import skopeo
import tekton
from logger import logger

BINARIES_DIR = "binaries"


def load_components(data_path: Path) -> list[str]:
    """Return the component name whitelist from the data file, or ``[]``."""
    if not data_path.is_file():
        logger.info("No data JSON was provided.")
        return []

    data = json.loads(data_path.read_text(encoding="utf-8"))
    components = (data.get("mapping") or {}).get("components")
    if not components:
        return []

    names: list[str] = []
    for comp in components:
        name = comp.get("name")
        if name is None:
            raise ValueError("Component entry in data file is missing 'name' field")
        names.append(name)
    return names


def copy_to_binaries(source_dir: Path, binaries_path: Path) -> None:
    """Copy all files from *source_dir* into *binaries_path*."""
    for item in source_dir.iterdir():
        if item.is_file():
            shutil.copy2(item, binaries_path)


def remove_non_checksum_files(binaries_path: Path) -> None:
    """Remove files in *binaries_path* not ending with ``SHA256SUMS``."""
    for item in list(binaries_path.iterdir()):
        if item.is_file() and not item.name.endswith("SHA256SUMS"):
            item.unlink()


def extract_checksums(
    snapshot_path: Path,
    data_path: Path,
    data_dir: Path,
    image_binaries_path: str,
    snapshot_rel_path: str,
    *,
    copy_image: Callable[..., subprocess.CompletedProcess[str]] = skopeo.copy,
) -> str:
    """Extract SHA256SUMS files from container images in the snapshot.

    Returns the relative binaries path (e.g. ``uid123/binaries``) for
    writing to the Tekton result file.
    """
    snapshot = file.load_json_dict(snapshot_path)

    relative_binaries = f"{Path(snapshot_rel_path).parent}/{BINARIES_DIR}"
    binaries_path = data_dir / relative_binaries
    binaries_path.mkdir(parents=True, exist_ok=True)

    desired_set = set(load_components(data_path))

    for component in snapshot.get("components") or []:
        component_name = component.get("name", "")

        if desired_set and component_name not in desired_set:
            continue

        image_url = component.get("containerImage") or ""
        if not image_url:
            raise ValueError("Unable to get image url from snapshot.")

        tmp_dir = Path(tempfile.mkdtemp())
        try:
            result = copy_image(f"docker://{image_url}", f"dir:{tmp_dir}")
            if result.returncode != 0:
                logger.error("skopeo copy failed: %s", result.stderr)
                raise subprocess.CalledProcessError(
                    result.returncode,
                    result.args,
                    output=result.stdout,
                    stderr=result.stderr,
                )

            extract_artifacts.extract_binaries_from_layers(tmp_dir, image_binaries_path)

            extracted_dir = tmp_dir / image_binaries_path
            if not extracted_dir.is_dir():
                raise ValueError(
                    f"Image {image_url} does not contain"
                    f" the '{image_binaries_path}' directory"
                )
            copy_to_binaries(extracted_dir, binaries_path)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    remove_non_checksum_files(binaries_path)

    logger.info("%s", relative_binaries)
    return relative_binaries


def main() -> int:
    """Read environment, extract checksums, write Tekton result."""
    (result_path,) = tekton.result_paths_from_env("RESULT_BINARIES_PATH")

    data_dir = Path(tekton.require_env("DATA_DIR"))
    snapshot_rel_path = tekton.require_env("SNAPSHOT_PATH")
    data_path_str = os.environ.get("DATA_PATH", "")
    image_binaries_path = os.environ.get("IMAGE_PATH", "releases")

    snapshot_path = data_dir / snapshot_rel_path
    data_path = data_dir / data_path_str

    relative_binaries = extract_checksums(
        snapshot_path=snapshot_path,
        data_path=data_path,
        data_dir=data_dir,
        image_binaries_path=image_binaries_path,
        snapshot_rel_path=snapshot_rel_path,
    )

    result_path.write_text(relative_binaries, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
