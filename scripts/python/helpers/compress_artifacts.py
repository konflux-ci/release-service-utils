#!/usr/bin/env python3
"""Pull signed binaries from Quay, restore supplementary files, and compress artifacts.

For each component:
* Pulls signed macOS and Windows OCI artifacts from Quay into a ``signed/`` directory.
* Restores supplementary files (readme, license, changelog) that were held during signing.
* Compresses each file entry into the final deliverable format:
  - macOS / Linux (non-disk-image) → ``.tar.gz`` (from ``os/arch/`` directory)
  - Linux disk images (``.qcow2``, ``.iso``) → copied as-is to ``ready_for_distribution/``
  - Windows → ``.zip`` (from ``os/arch/`` directory, extension corrected from
    ``.tar.gz``/``.tar``)
* Updates ``SNAPSHOT_JSON`` to reflect corrected Windows filenames in ``files[]``.
* Saves the modified snapshot to ``/shared/snapshot.json`` for downstream use.

CLI arguments:
  ``--quay-url``

Secret mounts:
  ``QUAY_SECRET_MOUNT``  (default: ``/mnt/quaySecret``)

Other env vars:
  ``SNAPSHOT_JSON``   – JSON string of the Snapshot spec
  ``CONTENT_DIR``     – override base directory (default: ``/shared/artifacts``)
  ``SHARED_DIR``      – override shared volume root (default: ``/shared``)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import tarfile
import zipfile
from pathlib import Path

import disk_image_utils
import oras_utils

PROG = "compress_artifacts.py"


QUAY_SECRET_MOUNT = Path(os.environ.get("QUAY_SECRET_MOUNT", "/mnt/quaySecret"))
CONTENT_DIR = Path(os.environ.get("CONTENT_DIR", "/shared/artifacts"))
SHARED_DIR = Path(os.environ.get("SHARED_DIR", "/shared"))

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and return CLI arguments."""
    p = argparse.ArgumentParser(prog=PROG)
    p.add_argument("--quay-url", required=True, help="Quay repository URL base")
    return p.parse_args(argv)


def _pull_signed_content(quay_url: str, component_name: str, component_dir: Path) -> None:
    """Pull signed Mac and Windows OCI artifacts from Quay into the component's signed/ dir."""
    signed_dir = component_dir / "signed"
    signed_dir.mkdir(parents=True, exist_ok=True)

    if (component_dir / "has_mac").exists():
        signed_mac_digest = (component_dir / "signed_mac_digest.txt").read_text().strip()
        subprocess.check_call(
            ["oras", "pull", f"{quay_url}/signed/{component_name}@{signed_mac_digest}"],
            cwd=str(signed_dir),
        )

    if (component_dir / "has_windows").exists():
        signed_windows_digest = (
            (component_dir / "signed_windows_digest.txt").read_text().strip()
        )
        signed_windows_digest = signed_windows_digest.strip()
        subprocess.check_call(
            ["oras", "pull", f"{quay_url}/signed/{component_name}@{signed_windows_digest}"],
            cwd=str(signed_dir),
        )


def _restore_supplementary(component_dir: Path) -> None:
    """Move supplementary files from the hold directory back into the signed content tree."""
    signed_dir = component_dir / "signed"
    supp_hold = component_dir / "supplementary"
    for os_name in ("macos", "windows"):
        supp_os_dir = supp_hold / os_name
        if not supp_os_dir.is_dir():
            continue
        for file in supp_os_dir.rglob("*"):
            if file.is_file():
                rel = file.relative_to(supp_os_dir)
                dest = signed_dir / os_name / rel
                dest.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(file), str(dest))
                logger.info("  Restored supplementary file: %s/%s", os_name, rel)


def _windows_filename(source_filename: str) -> str:
    """Replace .tar.gz or .tar extension with .zip for Windows archives."""
    if source_filename.endswith(".tar.gz"):
        return source_filename[: -len(".tar.gz")] + ".zip"
    if source_filename.endswith(".tar"):
        return source_filename[: -len(".tar")] + ".zip"
    return source_filename


def _compress_file_entry(
    entry: dict,
    array_name: str,
    component_dir: Path,
    ready_dir: Path,
    *,
    is_disk_image_component: bool = False,
) -> str:
    """Compress one file entry into ready_dir and return the (possibly normalized) source path.

    For macOS and Linux entries the source path is returned unchanged. For Windows entries
    the archive is created as a ``.zip`` instead of ``.tar.gz``/``.tar``, and the returned
    source path reflects the corrected filename so the snapshot can be updated accordingly.

    Files are copied directly to ``ready_dir`` (without archiving) when either:
    - *is_disk_image_component* is True (set when contentType: disk-image), or
    - the filename has an unambiguous disk-image suffix (.qcow2, .iso, .iso.gz,
      .raw.gz, .vhd.gz).

    Raises RuntimeError on failure (missing source, unknown OS, or empty arch directory).
    """
    source = entry.get("source")
    if not source:
        raise RuntimeError(f"Missing source field in {array_name}[] entry: {entry}")

    source_filename = Path(source).name
    os_name = entry.get("os", "")
    arch = entry.get("arch", "")

    arch_dir = oras_utils.os_arch_dir(
        os_name,
        arch,
        mac_windows_base=component_dir / "signed",
        linux_base=component_dir / "linux",
    )
    if arch_dir is None:
        raise RuntimeError(f"Unknown OS '{os_name}' in {array_name}[] entry (arch: {arch})")

    if not arch_dir.is_dir() or not any(arch_dir.iterdir()):
        raise RuntimeError(f"Architecture directory is empty or not found: {arch_dir}")

    # macOS and Linux follow the Unix convention of tar.gz archives; Windows uses zip
    # because that is the standard expected by Windows users and Developer Portal tooling.
    # Disk images are an exception: they are delivered as-is without any archiving.
    if os_name in ("darwin", "linux"):
        out_path = ready_dir / source_filename
        if is_disk_image_component or disk_image_utils.is_disk_image_file(source_filename):
            # Use the known filename directly — multiple disk images may share
            # the same arch directory, so scanning the whole dir is incorrect.
            src_file = arch_dir / source_filename
            if not src_file.is_file():
                raise RuntimeError(
                    f"Disk image file '{source_filename}' not found in {arch_dir}"
                )
            shutil.copy2(str(src_file), str(out_path))
        else:
            with tarfile.open(str(out_path), "w:gz") as tf:
                for item in sorted(arch_dir.rglob("*")):
                    if item.is_file():
                        tf.add(str(item), arcname=str(item.relative_to(arch_dir)))
        logger.info("  Created (%s): %s", array_name, source_filename)
        return source

    win_filename = _windows_filename(source_filename)
    out_path = ready_dir / win_filename
    with zipfile.ZipFile(str(out_path), "w", zipfile.ZIP_DEFLATED) as zf:
        for item in sorted(arch_dir.rglob("*")):
            if item.is_file():
                zf.write(str(item), arcname=str(item.relative_to(arch_dir)))
    logger.info("  Created (%s): %s", array_name, win_filename)
    return str(Path(source).parent / win_filename)


def compress_component(component: dict, snapshot: dict) -> dict:
    """Compress all file entries for one component. Returns updated component dict."""
    name = component.get("name", "")
    component_dir = CONTENT_DIR / name
    ready_dir = component_dir / "ready_for_distribution"
    ready_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Compressing artifacts for component: %s", name)

    # files[] entries are for Developer Portal delivery; staged.files[] entries are
    # for Customer Portal (Pulp/CDN) delivery.  Both produce archives in
    # ready_for_distribution/, but only files[] source paths are updated in the snapshot
    # (Windows .zip correction).
    files_entries = list(component.get("files") or [])
    staged_entries = list((component.get("staged") or {}).get("files") or [])

    is_disk_image = disk_image_utils.is_disk_image_component(component)

    normalized_files = []
    if files_entries:
        logger.info(
            "  Processing %d files from files[] (Developer Portal):", len(files_entries)
        )
        for entry in files_entries:
            normalized_source = _compress_file_entry(
                entry, "files", component_dir, ready_dir, is_disk_image_component=is_disk_image
            )
            normalized_entry = dict(entry)
            # no-op for mac/linux, .zip correction for windows
            normalized_entry["source"] = normalized_source
            normalized_files.append(normalized_entry)

    if staged_entries:
        logger.info(
            "  Processing %d files from staged.files[] (Customer Portal):", len(staged_entries)
        )
        for entry in staged_entries:
            _compress_file_entry(
                entry,
                "staged.files",
                component_dir,
                ready_dir,
                is_disk_image_component=is_disk_image,
            )

    updated_component = dict(component)
    if files_entries:
        updated_component["files"] = normalized_files

    return updated_component


def run(quay_url: str) -> None:
    """Pull signed artifacts, restore supplementary files, and compress all components."""
    snapshot = json.loads(os.environ["SNAPSHOT_JSON"])

    quay_user = (QUAY_SECRET_MOUNT / "username").read_text().strip()
    quay_pass = (QUAY_SECRET_MOUNT / "password").read_text().strip()

    # Log in to the quay.io registry using the hostname only; quay_url contains the full
    # repository path (e.g. quay.io/org/repo) and is passed to oras pull commands below.
    logger.info("Logging into Quay...")
    oras_utils.oras_login("quay.io", quay_user, quay_pass)

    updated_components = []
    for component in snapshot.get("components", []):
        name = component.get("name", "")
        component_dir = CONTENT_DIR / name

        _pull_signed_content(quay_url, name, component_dir)
        _restore_supplementary(component_dir)

        updated = compress_component(component, snapshot)
        updated_components.append(updated)

    snapshot["components"] = updated_components
    snapshot_path = SHARED_DIR / "snapshot.json"
    snapshot_path.write_text(json.dumps(snapshot), encoding="utf-8")
    logger.info("Saved modified snapshot to %s", snapshot_path)


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run artifact compression; return exit code."""
    logging.basicConfig(level=logging.INFO)
    args = parse_args(argv[1:] if argv is not None else None)
    try:
        run(args.quay_url)
    except Exception as exc:
        logger.error("ERROR: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
