#!/usr/bin/env python3
"""Organise extracted binaries by OS/arch and push unsigned Mac/Windows content to Quay.

For each component that has ``files`` or ``staged.files``:
* Unpacks each archive into an ``unsigned/<os>/<arch>/`` directory tree.
* Moves supplementary files (readme, license, changelog) out of signing directories so
  signing tools don't attempt to process them.
* Pushes the unsigned macOS and Windows content to Quay as OCI artifacts with a 1-day
  expiry tag so the signing steps can pull them.

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
import tarfile
from pathlib import Path

import disk_image_utils
import oras_utils

PROG = "push_unsigned.py"

QUAY_SECRET_MOUNT = Path(os.environ.get("QUAY_SECRET_MOUNT", "/mnt/quaySecret"))
CONTENT_DIR = Path(os.environ.get("CONTENT_DIR", "/shared/artifacts"))

SUPPLEMENTARY_NAMES = {"readme", "license", "changelog"}
SUPPLEMENTARY_EXTS = {".md", ".txt"}


logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and return CLI arguments."""
    p = argparse.ArgumentParser(prog=PROG)
    p.add_argument("--quay-url", required=True, help="Quay repository URL base")
    p.add_argument("--pipeline-run-uid", required=True, help="Unique ID for this pipeline run")
    return p.parse_args(argv)


def is_supplementary_file(path: Path) -> bool:
    """Return True if a file is a supplementary (readme/license/changelog) file."""
    lower = path.name.lower()
    if "." in lower:
        base, ext = lower.rsplit(".", 1)
        ext = f".{ext}"
    else:
        base, ext = lower, ""
    if base in SUPPLEMENTARY_NAMES:
        if not ext:
            return True
        if ext in SUPPLEMENTARY_EXTS:
            return True
    return False


def move_supplementary_out(src_root: Path, hold_root: Path) -> None:
    """Move supplementary files from src_root to hold_root, preserving relative paths."""
    if not src_root.is_dir():
        return
    for file in src_root.rglob("*"):
        if file.is_file() and is_supplementary_file(file):
            rel = file.relative_to(src_root)
            dest = hold_root / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(file), str(dest))
            logger.info("  Held supplementary file: %s", rel)


def _unpack_file_entries(
    entries: list[dict],
    component_dir: Path,
    unsigned_dir: Path,
    *,
    is_disk_image_component: bool = False,
) -> None:
    """Extract each archive from entries into its OS/arch subdirectory under unsigned_dir.

    Files are moved directly (without unpacking) when either:
    - *is_disk_image_component* is True (set when contentType: disk-image), or
    - the filename has an unambiguous disk-image suffix (.qcow2, .iso, .iso.gz,
      .raw.gz, .vhd.gz).
    All other files are treated as tar archives and extracted.
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
        if is_disk_image_component or disk_image_utils.is_disk_image_file(archive_name):
            shutil.move(str(archive_path), str(target_dir / archive_name))
        else:
            with tarfile.open(str(archive_path)) as tf:
                oras_utils.safe_extract_archive(tf, target_dir, archive_name)
            archive_path.unlink()


def run(quay_url: str, pipeline_run_uid: str) -> None:
    """Organise extracted binaries and push unsigned Mac/Windows content to Quay."""
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

        is_disk_image = disk_image_utils.is_disk_image_component(component)
        _unpack_file_entries(
            component.get("files") or [],
            component_dir,
            unsigned_dir,
            is_disk_image_component=is_disk_image,
        )
        _unpack_file_entries(
            (component.get("staged") or {}).get("files") or [],
            component_dir,
            unsigned_dir,
            is_disk_image_component=is_disk_image,
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
    """Parse arguments and run unsigned artifact push; return exit code."""
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
