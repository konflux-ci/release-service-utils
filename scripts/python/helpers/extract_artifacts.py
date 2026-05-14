#!/usr/bin/env python3
"""Extract artifacts from container images.

For each component in SNAPSHOT_JSON that has ``files`` or ``staged.files`` entries:
* Pulls the container image with ``skopeo copy`` (authenticated via ``select-oci-auth``).
* Identifies the specific files listed in the RPA and extracts them from the container layers.
* Creates OS flag files (``has_mac``, ``has_windows``, ``has_linux``) to indicate
  which signing paths are needed.

Components are processed in parallel, bounded by ``--concurrent-limit``.

CLI arguments:
  ``--concurrent-limit``

Secret mounts (paths can be overridden via env vars for testing):
  ``REDHAT_WORKLOADS_TOKEN_MOUNT``  (default: ``/mnt/redhat-workloads-token``)

Other env vars:
  ``SNAPSHOT_JSON``   – JSON string of the Snapshot spec (set by the task)
  ``CONTENT_DIR``     – override base directory (default: ``/shared/artifacts``)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

PROG = "extract_artifacts.py"

REDHAT_WORKLOADS_TOKEN_MOUNT = Path(
    os.environ.get("REDHAT_WORKLOADS_TOKEN_MOUNT", "/mnt/redhat-workloads-token")
)
CONTENT_DIR = Path(os.environ.get("CONTENT_DIR", "/shared/artifacts"))

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and return CLI arguments."""
    p = argparse.ArgumentParser(prog=PROG)
    p.add_argument(
        "--concurrent-limit",
        type=int,
        default=3,
        help="Maximum number of components to process in parallel",
    )
    return p.parse_args(argv)


def _setup_docker_config() -> None:
    """Write ~/.docker/config.json from the mounted dockerconfig secret."""
    raw = (REDHAT_WORKLOADS_TOKEN_MOUNT / ".dockerconfigjson").read_text()
    # Strip any outer non-JSON noise (quotes added by k8s secret encoding)
    clean = re.sub(r"(^|\})[^{}]+(\{|$)", r"\1\2", raw)
    docker_dir = Path.home() / ".docker"
    docker_dir.mkdir(parents=True, exist_ok=True)
    (docker_dir / "config.json").write_text(clean)


def _get_source_paths(component: dict) -> tuple[list[str], list[str]]:
    """Return (wanted_files, layer_extract_dirs) from files[] and staged.files[] entries."""
    wanted: list[str] = []
    # Parent directories to extract from container image layers via `tar`. We extract
    # whole directories rather than individual files because tar requires the full path
    # prefix to be present in the layer for selective extraction to work.
    layer_extract_dirs: list[str] = []

    for entry in list(component.get("files") or []) + list(
        (component.get("staged") or {}).get("files") or []
    ):
        source = entry.get("source", "")
        if not source:
            continue
        stripped = source.lstrip("/")
        wanted.append(stripped)
        parent = str(Path(stripped).parent)
        if parent and parent != ".":
            layer_extract_dirs.append(parent)

    if not layer_extract_dirs:
        layer_extract_dirs = ["releases"]

    unique_files = sorted(set(wanted))
    unique_dirs = sorted(set(layer_extract_dirs))
    return unique_files, unique_dirs


def process_component(component: dict) -> None:
    """Pull and extract one component's artifacts into CONTENT_DIR/<name>/."""
    name = component.get("name")

    num_files = len(component.get("files") or [])
    # staged files are for Customer Portal (Pulp)
    num_staged = len((component.get("staged") or {}).get("files") or [])
    if num_files == 0 and num_staged == 0:
        logger.info(
            "Skipping component '%s' - no files defined (not a binary artifact component)",
            name,
        )
        return

    pullspec = component.get("containerImage")
    if not pullspec:
        raise ValueError(f"Component '{name}' is missing 'containerImage'")

    destination = CONTENT_DIR / name
    logger.info("Extracting component '%s' to: %s", name, destination)
    destination.mkdir(parents=True, exist_ok=True)

    tmp_dir = Path(tempfile.mkdtemp())
    try:
        auth_file = Path(tempfile.mktemp())
        try:
            auth_data = subprocess.check_output(
                ["select-oci-auth", pullspec], stderr=subprocess.PIPE
            )
            auth_file.write_bytes(auth_data)

            subprocess.check_call(
                [
                    "skopeo",
                    "copy",
                    "--retry-times",
                    "3",
                    "--authfile",
                    str(auth_file),
                    f"docker://{pullspec}",
                    f"dir:{tmp_dir}",
                ]
            )
        finally:
            auth_file.unlink(missing_ok=True)

        wanted_files, extract_dirs = _get_source_paths(component)
        logger.info("Files to extract from RPA: %s", wanted_files)

        manifest = json.loads((tmp_dir / "manifest.json").read_text())
        layer_digests = [layer["digest"] for layer in manifest.get("layers", [])]

        for digest in layer_digests:
            layer_file = tmp_dir / digest.removeprefix("sha256:")
            if not layer_file.exists():
                continue
            result = subprocess.run(
                ["tar", "-tf", str(layer_file)],
                capture_output=True,
                text=True,
            )
            layer_entries = result.stdout.splitlines()
            for image_path in extract_dirs:
                if any(line.startswith(f"{image_path}/") for line in layer_entries):
                    logger.info("Extracting %s/ from %s...", image_path, layer_file.name)
                    subprocess.check_call(
                        ["tar", "-xzvf", str(layer_file), image_path],
                        cwd=str(tmp_dir),
                    )
                else:
                    logger.info(
                        "skipping %s. It doesn't contain the %s dir",
                        layer_file.name,
                        image_path,
                    )

        for wanted in wanted_files:
            src = tmp_dir / wanted
            if src.is_file():
                shutil.copy2(str(src), str(destination / src.name))
            else:
                logger.warning("Expected file not found in container: %s", wanted)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _create_os_flag_files(snapshot: dict) -> None:
    """Create has_mac / has_windows / has_linux flag files based on RPA entries."""
    for component in snapshot.get("components", []):
        name = component.get("name", "")
        component_dir = CONTENT_DIR / name
        if not component_dir.is_dir():
            continue

        logger.info("Checking configured OS types for component: %s", name)

        all_file_entries = list(component.get("files") or []) + list(
            (component.get("staged") or {}).get("files") or []
        )

        def _matches(entry: dict, os_name: str) -> bool:
            """Return True if the file entry is associated with the given OS."""
            if entry.get("os") == os_name:
                return True
            for field in ("source", "filename"):
                if os_name in (entry.get(field) or ""):
                    return True
            return False

        if any(_matches(e, "darwin") for e in all_file_entries):
            (component_dir / "has_mac").touch()
            logger.info("  - macOS content detected")

        if any(_matches(e, "windows") for e in all_file_entries):
            (component_dir / "has_windows").touch()
            logger.info("  - Windows content detected")

        if any(_matches(e, "linux") for e in all_file_entries):
            (component_dir / "has_linux").touch()
            logger.info("  - Linux content detected")


def run(concurrent_limit: int) -> None:
    """Extract artifacts from all snapshot components and write OS flag files."""
    snapshot = json.loads(os.environ["SNAPSHOT_JSON"])

    _setup_docker_config()
    CONTENT_DIR.mkdir(parents=True, exist_ok=True)

    components = snapshot.get("components", [])
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=concurrent_limit) as executor:
        futures = {
            executor.submit(process_component, comp): comp.get("name", f"index {idx}")
            for idx, comp in enumerate(components)
        }
        for future in as_completed(futures):
            comp_name = futures[future]
            try:
                future.result()
            except Exception as exc:
                errors.append(f"Component '{comp_name}': {exc}")

    if errors:
        raise RuntimeError("\n".join(errors))

    _create_os_flag_files(snapshot)


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run artifact extraction; return exit code."""
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv[1:] if argv is not None else None)
    try:
        run(args.concurrent_limit)
    except Exception as exc:
        logger.error("ERROR: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
