#!/usr/bin/env python3
"""Extract artifacts from OCI artifact images (pushed via ORAS).

Unlike container images where files are embedded in filesystem layers,
OCI artifacts store each file as a standalone layer identified by its
``org.opencontainers.image.title`` annotation.  This module matches
layers by title against the RPA's ``source`` / ``filename`` fields and
copies the matching blobs directly to the component output directory.

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

import json
import logging
import os
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from extract_artifacts import create_os_flag_files, setup_docker_config, parse_args

PROG = "extract_oci_artifacts.py"

CONTENT_DIR = Path(os.environ.get("CONTENT_DIR", "/shared/artifacts"))

logger = logging.getLogger(__name__)


def _get_platform_overrides(component: dict) -> list[str]:
    """Return skopeo --override-os/--override-arch flags from file entries.

    OCI artifact images are built for a specific platform (e.g. darwin/arm64).
    Without these flags, skopeo defaults to linux/amd64 and fails on
    manifest lists that lack that combination.
    """
    for entry in list(component.get("files") or []) + list(
        (component.get("staged") or {}).get("files") or []
    ):
        os_name = entry.get("os")
        arch = entry.get("arch")
        if os_name and arch:
            return ["--override-os", os_name, "--override-arch", arch]
    return []


def _get_wanted_filenames(component: dict) -> set[str]:
    """Return the set of filenames to extract from layer titles."""
    wanted: set[str] = set()
    for entry in list(component.get("files") or []) + list(
        (component.get("staged") or {}).get("files") or []
    ):
        source = entry.get("source", "")
        if source:
            wanted.add(Path(source).name)
        filename = entry.get("filename", "")
        if filename:
            wanted.add(filename)
    return wanted


def _extract_oci_component(
    component: dict, manifest: dict, tmp_dir: Path, destination: Path
) -> None:
    """Copy matching OCI artifact layers to the component destination directory."""
    wanted = _get_wanted_filenames(component)
    name = component.get("name", "")
    logger.info("Matching layers by title for '%s'", name)
    logger.info("Wanted files: %s", sorted(wanted))

    layers = manifest.get("layers", [])
    found: set[str] = set()

    for layer in layers:
        annotations = layer.get("annotations") or {}
        title = annotations.get("org.opencontainers.image.title", "")
        if not title or title not in wanted:
            continue

        digest = layer.get("digest", "")
        blob_path = tmp_dir / digest.removeprefix("sha256:")
        if not blob_path.exists():
            logger.error("Blob not found for layer '%s': %s", title, blob_path)
            raise RuntimeError(f"Blob for layer '{title}' (digest {digest}) not found on disk")

        dest_file = destination / title
        shutil.copy2(str(blob_path), str(dest_file))
        logger.info("  Extracted layer '%s' (%s bytes)", title, blob_path.stat().st_size)
        found.add(title)

    missing = wanted - found
    if missing:
        raise RuntimeError(f"OCI artifact for '{name}' is missing layers: {sorted(missing)}")


def process_component(component: dict) -> None:
    """Pull and extract one OCI artifact component into CONTENT_DIR/<name>/."""
    name = component.get("name")

    num_files = len(component.get("files") or [])
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
        auth_file: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(mode="wb", delete=False) as auth_fp:
                auth_file = Path(auth_fp.name)
                auth_data = subprocess.check_output(
                    ["select-oci-auth", pullspec], stderr=subprocess.PIPE
                )
                auth_fp.write(auth_data)

            subprocess.check_call(
                [
                    "skopeo",
                    "copy",
                    "--retry-times",
                    "3",
                    "--authfile",
                    str(auth_file),
                    *_get_platform_overrides(component),
                    f"docker://{pullspec}",
                    f"dir:{tmp_dir}",
                ]
            )
        finally:
            if auth_file is not None:
                auth_file.unlink(missing_ok=True)

        manifest = json.loads((tmp_dir / "manifest.json").read_text())
        _extract_oci_component(component, manifest, tmp_dir, destination)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def run(concurrent_limit: int) -> None:
    """Extract OCI artifact components from all snapshot components and write OS flag files."""
    snapshot = json.loads(os.environ["SNAPSHOT_JSON"])

    setup_docker_config()
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

    create_os_flag_files(snapshot)


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run OCI artifact extraction; return exit code."""
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv[1:] if argv is not None else None, PROG)
    try:
        run(args.concurrent_limit)
    except Exception as exc:
        logger.error("ERROR: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
