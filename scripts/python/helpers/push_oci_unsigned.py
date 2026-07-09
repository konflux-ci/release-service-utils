#!/usr/bin/env python3
"""Push unsigned OCI artifact content to Quay for the sign-and-push-to-internal-oci pipeline.

This module is specific to the OCI artifact signing pipeline where macOS and Windows
archives are stored as ORAS OCI artifacts (not traditional container image layers).
Unlike push_unsigned.py, archives are moved as-is rather than extracted, so that
symlinks in macOS .app bundles survive the ORAS round-trip. The signing orchestrator
on each signing host is responsible for extraction.

CLI arguments:
  ``--quay-url``
  ``--pipeline-run-uid``

Secret mounts:
  ``QUAY_SECRET_MOUNT``  (default: ``/mnt/quaySecret``)

Other env vars:
  ``SNAPSHOT_JSON``   – JSON string of the Snapshot spec
  ``CONTENT_DIR``     – override base directory (default: ``/shared/artifacts``)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
from pathlib import Path

import tarfile

import oras_utils
from push_unsigned import QUAY_SECRET_MOUNT, CONTENT_DIR, move_supplementary_out

PROG = "push_oci_unsigned.py"

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and return CLI arguments."""
    p = argparse.ArgumentParser(prog=PROG)
    p.add_argument("--quay-url", required=True, help="Quay repository URL base")
    p.add_argument("--pipeline-run-uid", required=True, help="Unique ID for this pipeline run")
    return p.parse_args(argv)


def _stage_file_entries(entries: list[dict], component_dir: Path, unsigned_dir: Path) -> None:
    """Stage archives into OS/arch subdirectories under unsigned_dir.

    macOS and Windows archives are moved as-is to preserve symlinks through
    the ORAS round-trip. The signing orchestrator on each host extracts them
    natively where symlinks are supported. Linux archives are extracted here.

    Unlike push_unsigned._stage_file_entries(), darwin and windows archives
    are NOT extracted — they are transferred intact so framework bundle symlinks
    survive the ORAS push/pull cycle.
    """
    for entry in entries:
        source = entry.get("source", "")
        os_name = entry.get("os", "")
        arch = entry.get("arch", "")
        if not source or not os_name or not arch:
            continue

        archive_name = Path(source).name
        archive_path = component_dir / archive_name

        if not archive_path.is_file():
            logger.warning("  Archive not found: %s", archive_path)
            continue

        target_dir = oras_utils.os_arch_dir(
            os_name, arch, mac_windows_base=unsigned_dir, linux_base=component_dir / "linux"
        )
        if target_dir is None:
            continue

        target_dir.mkdir(parents=True, exist_ok=True)

        if os_name in ("darwin", "windows"):
            shutil.move(str(archive_path), str(target_dir / archive_name))
        else:
            with tarfile.open(str(archive_path)) as tf:
                oras_utils.safe_extract_archive(tf, target_dir, archive_name)
            archive_path.unlink()


def run(quay_url: str, pipeline_run_uid: str) -> None:
    """Organise OCI artifact binaries and push unsigned Mac/Windows content to Quay."""
    snapshot = json.loads(os.environ["SNAPSHOT_JSON"])

    quay_user = (QUAY_SECRET_MOUNT / "username").read_text().strip()
    quay_pass = (QUAY_SECRET_MOUNT / "password").read_text().strip()

    logger.info("Logging into Quay...")
    oras_utils.oras_login("quay.io", quay_user, quay_pass)

    for component in snapshot.get("components", []):
        name = component.get("name", "")
        logger.info("Processing component: %s", name)

        num_files = len(component.get("files") or [])
        num_staged = len((component.get("staged") or {}).get("files") or [])
        if num_files == 0 and num_staged == 0:
            logger.info("Skipping component '%s' - no files or staged.files defined", name)
            continue

        component_dir = CONTENT_DIR / name
        unsigned_dir = component_dir / "unsigned"

        has_mac = (component_dir / "has_mac").exists()
        has_windows = (component_dir / "has_windows").exists()
        has_linux = (component_dir / "has_linux").exists()

        if has_mac:
            (unsigned_dir / "macos").mkdir(parents=True, exist_ok=True)
        if has_windows:
            (unsigned_dir / "windows").mkdir(parents=True, exist_ok=True)
        if has_linux:
            (component_dir / "linux").mkdir(parents=True, exist_ok=True)

        _stage_file_entries(component.get("files") or [], component_dir, unsigned_dir)
        _stage_file_entries(
            (component.get("staged") or {}).get("files") or [], component_dir, unsigned_dir
        )

        supp_hold = component_dir / "supplementary"
        logger.info("Moving supplementary files out of signing directories...")
        move_supplementary_out(unsigned_dir / "macos", supp_hold / "macos")
        move_supplementary_out(unsigned_dir / "windows", supp_hold / "windows")

        if has_mac:
            logger.info("Pushing unsigned macOS content for %s to %s...", name, quay_url)
            tag = f"{quay_url}/unsigned/{name}:{pipeline_run_uid}-mac"
            mac_digest = oras_utils.oras_push(tag, unsigned_dir, "macos", name)
            logger.info("Digest for %s mac content: %s", name, mac_digest)
            (component_dir / "unsigned_mac_digest.txt").write_text(mac_digest)
        else:
            logger.info("No macOS content for %s, skipping unsigned push...", name)

        if has_windows:
            logger.info("Pushing unsigned Windows content for %s to %s...", name, quay_url)
            tag = f"{quay_url}/unsigned/{name}:{pipeline_run_uid}-windows"
            win_digest = oras_utils.oras_push(tag, unsigned_dir, "windows", name)
            logger.info("Digest for %s windows content: %s", name, win_digest)
            (component_dir / "unsigned_windows_digest.txt").write_text(win_digest)
        else:
            logger.info("No Windows content for %s, skipping unsigned push...", name)


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run unsigned OCI artifact push; return exit code."""
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv[1:] if argv is not None else None)
    try:
        run(args.quay_url, args.pipeline_run_uid)
    except Exception as exc:
        logger.error("ERROR: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
