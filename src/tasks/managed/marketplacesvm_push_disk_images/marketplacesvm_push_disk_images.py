#!/usr/bin/env python3
"""Push VM disk images to cloud marketplaces via pubtools-marketplacesvm.

Validates marketplace credentials, pulls OCI disk-image artifacts with oras,
stages them for pushsource, then invokes ``marketplacesvm_push_wrapper``.

CLI arguments map to Tekton task parameters. ``CLOUD_CREDENTIALS`` is set from
validated secret files under ``--secrets-dir`` (default ``/etc/secrets``).
``UPLOAD_CONTAINER_NAME`` remains an environment variable consumed by the
underlying pubtools-marketplacesvm tooling.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from release_service_utils.helpers import memory_throttle
from release_service_utils.helpers import oras_utils
from release_service_utils.helpers import subprocess_cmd
import yaml
from release_service_utils.helpers.file import load_json_dict, make_tempfile_path
from release_service_utils.helpers.logger import logger

PROG = "marketplacesvm_push_disk_images.py"

DEFAULT_SECRETS_DIR = Path("/etc/secrets")
DEFAULT_WORKDIR = Path("/var/workdir")
MEMORY_THRESHOLD = 80

_EXTENSION_STRIP_RE = re.compile(r"\.[.a-zA-Z0-9]*$")

# An index mediaType alone is not sufficient to mean multi-arch: it can
# legally wrap a single manifest. See is_multiarch_manifest().
MULTI_ARCH_MEDIA_TYPES = frozenset(
    {
        "application/vnd.docker.distribution.manifest.list.v2+json",
        "application/vnd.oci.image.index.v1+json",
    }
)

_RH_ARCH_TO_OCI_PLATFORM = {
    "x86_64": "linux/amd64",
    "aarch64": "linux/arm64",
}


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse and return CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__, prog=PROG)
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Base directory where release data and artifacts are stored",
    )
    parser.add_argument(
        "--snapshot-path",
        required=True,
        help="Path to the mapped snapshot JSON relative to --data-dir",
    )
    parser.add_argument(
        "--pre-push",
        default="false",
        choices=["true", "false"],
        help="When true, pass --nochannel to marketplacesvm_push_wrapper",
    )
    parser.add_argument(
        "--concurrent-limit",
        type=int,
        default=3,
        help="Maximum number of components to prepare in parallel",
    )
    parser.add_argument(
        "--secrets-dir",
        default=str(DEFAULT_SECRETS_DIR),
        help="Directory containing marketplace credential JSON files",
    )
    parser.add_argument(
        "--workdir",
        default=str(DEFAULT_WORKDIR),
        help="Parent directory for temporary staging directories",
    )
    return parser.parse_args(argv)


def log_command_failure(exc: BaseException) -> None:
    """Log captured subprocess stdout/stderr from *exc*, if any.

    ``subprocess.CalledProcessError.__str__`` only reports the exit status, so
    without this the operator gets no diagnostic output when a shelled-out
    command (oras, pushsource-ls, marketplacesvm_push_wrapper) fails.
    """
    if not isinstance(exc, subprocess.CalledProcessError):
        return
    if exc.stdout:
        logger.error("command stdout:\n%s", exc.stdout)
    if exc.stderr:
        logger.error("command stderr:\n%s", exc.stderr)


def require_field(data: Mapping[str, Any], *keys: str) -> Any:
    """Return ``data[keys...]`` or raise ``ValueError`` when missing/empty."""
    cur: Any = data
    path = ""
    for key in keys:
        path = f"{path}.{key}" if path else key
        if not isinstance(cur, Mapping) or key not in cur:
            raise ValueError(f"Missing {path} value for component")
        cur = cur[key]
    if cur in (None, ""):
        raise ValueError(f"Missing {path} value for component")
    return cur


def validate_credentials(secrets_dir: Path) -> list[Path]:
    """Validate marketplace credential JSON files and return their paths.

    Each ``*.json`` file under *secrets_dir* must contain ``marketplace_account``
    and ``auth`` keys. Raises ``RuntimeError`` when no files exist or validation
    fails.
    """
    logger.info("Sanity validation of credentials")
    json_files = sorted(secrets_dir.glob("*.json"))
    if not json_files:
        raise RuntimeError(f"No credential files found in {secrets_dir}/")

    for path in json_files:
        try:
            payload = load_json_dict(path)
        except (json.JSONDecodeError, TypeError, OSError) as exc:
            raise RuntimeError(f"Validation failed for credential file: '{path}'") from exc
        if not ("marketplace_account" in payload and "auth" in payload):
            raise RuntimeError(
                f"Validation failed for credential file: '{path}'\n"
                "The file is missing required keys "
                "('marketplace_account' or 'auth')."
            )
    return json_files


def set_cloud_credentials(credential_files: Sequence[Path]) -> str:
    """Set ``CLOUD_CREDENTIALS`` from *credential_files* and return the value."""
    value = ",".join(str(path) for path in credential_files)
    os.environ["CLOUD_CREDENTIALS"] = value
    return value


def write_starmap_file(snapshot: Mapping[str, Any], snapshot_file: Path) -> Path:
    """Flatten component starmap entries and write ``starmap.yaml`` beside snapshot."""
    components = snapshot.get("components") or []
    mapping: list[Any] = []
    for component in components:
        if not isinstance(component, Mapping):
            continue
        starmap = component.get("starmap") or []
        if isinstance(starmap, list):
            mapping.extend(starmap)

    starmap_file = snapshot_file.parent / "starmap.yaml"
    starmap_file.write_text(
        yaml.safe_dump(mapping, default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )
    return starmap_file


def strip_extensions(filename: str) -> str:
    """Remove a trailing compound extension (bash sed equivalent)."""
    return _EXTENSION_STRIP_RE.sub("", filename)


def parse_build_respin(filename: str) -> str:
    """Extract the unix-timestamp respin from a staged filename."""
    without_ext = strip_extensions(filename)
    without_arch = without_ext.rsplit("-", 1)[0]
    return without_arch.rsplit("-", 1)[-1]


def parse_build_name(file_prefix: str) -> str:
    """Return build name by stripping the version suffix from *file_prefix*."""
    return file_prefix.rsplit("-", 1)[0]


def parse_architecture(filename: str) -> str:
    """Return architecture token from a staged filename (before extension(s))."""
    return strip_extensions(filename).rsplit("-", 1)[-1]


def rh_arch_to_oci_platform(arch: str) -> str:
    """Map a Red Hat architecture name to an OCI ``os/arch`` platform string."""
    return _RH_ARCH_TO_OCI_PLATFORM.get(arch, f"linux/{arch}")


def is_multiarch_manifest(manifest: Mapping[str, Any]) -> bool:
    """Return True if *manifest* is an OCI/Docker index wrapping >1 manifests.

    An index mediaType alone is not sufficient, since it can legally wrap a
    single manifest, so this only reports True when it actually contains more
    than one manifest entry.
    """
    if manifest.get("mediaType") not in MULTI_ARCH_MEDIA_TYPES:
        return False
    return len(manifest.get("manifests") or []) > 1


def validate_manifest_platforms(manifest: Mapping[str, Any], pullspec: str) -> None:
    """Raise RuntimeError if any index entry lacks platform.architecture.

    Some multi-arch OCI artifact indexes are built without per-manifest
    platform metadata (see RHELOPC-2342), which makes oras ``--platform``
    selection impossible. Fail loudly instead of guessing which manifest
    belongs to which architecture.
    """
    entries = manifest.get("manifests") or []
    if any((entry.get("platform") or {}).get("architecture") is None for entry in entries):
        raise RuntimeError(
            f"Multi-arch image index for {pullspec} has one or more manifests "
            "without platform.architecture metadata; oras cannot select the "
            "correct architecture."
        )


def staged_file_architectures(staged_files: Sequence[Any]) -> list[str]:
    """Return the architecture parsed from each staged file's filename.

    Raises RuntimeError if the same architecture appears more than once,
    since that scenario is untested and its correct handling is undefined
    for multi-arch images.
    """
    archs = [
        parse_architecture(str(require_field(entry, "filename"))) for entry in staged_files
    ]
    counts: dict[str, int] = {}
    for arch in archs:
        counts[arch] = counts.get(arch, 0) + 1
    duplicates = sorted(arch for arch, count in counts.items() if count > 1)
    if duplicates:
        raise RuntimeError(
            "staged.files has more than one file for the same architecture, "
            f"which is not supported for multi-arch images: {duplicates}"
        )
    return archs


def fetch_manifest(pullspec: str) -> dict[str, Any]:
    """Fetch and parse the OCI manifest for *pullspec* via oras.

    Used to detect whether an artifact is a genuine multi-arch image index
    before deciding whether to pull a single architecture or all of them.
    """
    auth_file = make_tempfile_path("oras-auth-")
    try:
        auth_out = subprocess_cmd.run_cmd(["select-oci-auth", pullspec], check=True).stdout
        auth_file.write_text(auth_out, encoding="utf-8")
        raw = oras_utils.oras_manifest_fetch(pullspec, auth_file)
    finally:
        auth_file.unlink(missing_ok=True)
    return json.loads(raw)


def image_type_for_filename(filename: str) -> str | None:
    """Return marketplace image type for *filename*, or None if unsupported."""
    lower = filename.lower()
    if lower.endswith(".vhd"):
        return "VHD"
    if lower.endswith(".raw"):
        return "AMI"
    return None


def build_date_from_respin(respin: str) -> str:
    """Convert a unix-timestamp respin to ``YYYYMMDD`` UTC."""
    return datetime.fromtimestamp(int(respin), tz=timezone.utc).strftime("%Y%m%d")


def decompress_gzip_source(source_path: Path) -> Path:
    """Decompress *source_path* when it ends with ``.gz``; return final path."""
    if not source_path.name.endswith(".gz"):
        return source_path

    destination = Path(str(source_path)[: -len(".gz")])
    with gzip.open(source_path, "rb") as src, destination.open("wb") as dst:
        shutil.copyfileobj(src, dst)
    source_path.unlink()
    return destination


def write_resources_yaml(destination: Path, resources: Mapping[str, Any]) -> None:
    """Write *resources* as YAML to ``destination/resources.yaml``."""
    (destination / "resources.yaml").write_text(
        yaml.safe_dump(dict(resources), default_flow_style=False, sort_keys=False),
        encoding="utf-8",
    )


def prepare_component(
    component: Mapping[str, Any],
    disk_imgs_dir: Path,
    workdir: Path,
) -> None:
    """Pull one component's OCI artifact and stage cloud-image files.

    Blocks on ``memory_throttle.wait_for_memory`` before pulling/decompressing,
    so a bounded number of large disk images are held in memory at once even
    when several worker threads are active concurrently.

    A genuine multi-arch OCI image index (more than one manifest entry) is
    pulled once per architecture found in ``staged.files``, each into its own
    subdirectory, since ``oras pull`` only selects a single architecture at a
    time via ``--platform``.
    """
    product_info = require_field(component, "productInfo")
    pullspec = str(require_field(component, "containerImage"))
    img_name = str(require_field(component, "name"))

    try:
        memory_throttle.wait_for_memory(MEMORY_THRESHOLD)
        file_prefix = str(require_field(product_info, "filePrefix"))
        build_name = parse_build_name(file_prefix)
        build_version = str(require_field(product_info, "productVersionName"))
        staged_files = require_field(component, "staged", "files")
        if not isinstance(staged_files, list) or not staged_files:
            raise ValueError("staged.files must be a non-empty list")
        first_filename = str(require_field(staged_files[0], "filename"))
        build_respin = parse_build_respin(first_filename)

        resources: dict[str, Any] = {
            "api": "v1",
            "resource": "CloudImage",
            "description": "",
            "build": {
                "name": build_name,
                "respin": build_respin,
                "version": build_version,
            },
            "release": {"date": build_date_from_respin(build_respin)},
            "images": [],
        }

        destination = disk_imgs_dir / img_name
        destination.mkdir(parents=True, exist_ok=True)

        with tempfile.TemporaryDirectory(dir=workdir) as download_dir_name:
            download_dir = Path(download_dir_name)
            manifest = fetch_manifest(pullspec)
            is_multiarch = is_multiarch_manifest(manifest)

            if is_multiarch:
                validate_manifest_platforms(manifest, pullspec)
                for arch in staged_file_architectures(staged_files):
                    arch_dir = download_dir / arch
                    arch_dir.mkdir(parents=True, exist_ok=True)
                    oras_utils.oras_pull(
                        pullspec, arch_dir, platform=rh_arch_to_oci_platform(arch)
                    )
            else:
                oras_utils.oras_pull(pullspec, download_dir)

            for entry in staged_files:
                if not isinstance(entry, Mapping):
                    raise ValueError("staged.files entries must be objects")
                source = str(require_field(entry, "source"))
                filename = str(require_field(entry, "filename"))
                source_dir = (
                    download_dir / parse_architecture(filename)
                    if is_multiarch
                    else download_dir
                )
                source_path = source_dir / source

                if not source_path.is_file():
                    raise RuntimeError(
                        f"Source file '{source_path}' for component '{filename}' "
                        "was not found after oras pull."
                    )

                if source.endswith(".gz"):
                    source_path = decompress_gzip_source(source_path)
                    if filename.endswith(".gz"):
                        filename = filename[: -len(".gz")]

                dest_file = destination / filename
                if dest_file.exists():
                    raise RuntimeError(
                        "Multiple files use the same destination value: "
                        f"{destination} and filename value: {filename}. "
                        "Failing..."
                    )

                image_type = image_type_for_filename(filename)
                if image_type is None:
                    logger.info("Skipping unsupported file: %s", filename)
                    continue

                build_arch = parse_architecture(filename)
                shutil.move(str(source_path), str(dest_file))
                resources["images"].append({"path": filename, "architecture": build_arch})
                resources["type"] = image_type

        write_resources_yaml(destination, resources)
    except Exception as exc:
        logger.error("ERROR: prepare_component failed for component: %s", img_name)
        log_command_failure(exc)
        raise


def prepare_components(
    components: Sequence[Mapping[str, Any]],
    disk_imgs_dir: Path,
    workdir: Path,
    concurrent_limit: int,
) -> None:
    """Prepare all *components* in parallel, bounded by *concurrent_limit*.

    Each worker also throttles on memory usage via
    ``memory_throttle.wait_for_memory`` before doing its pull/decompress work,
    since disk images can be large enough to risk OOMKills even within a
    small concurrency limit.
    """
    memory_throttle.log_memory_throttle_status(MEMORY_THRESHOLD)
    workers = max(1, concurrent_limit)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [
            pool.submit(
                prepare_component,
                component,
                disk_imgs_dir,
                workdir,
            )
            for component in components
        ]
        errors: list[Exception] = []
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:  # collect worker failures, then re-raise once
                errors.append(exc)
        if errors:
            raise RuntimeError(
                "prepare_component failed for at least one component"
            ) from errors[0]


def validate_staged_structure(base_dir: Path) -> None:
    """Validate the staged directory layout with ``pushsource-ls``."""
    try:
        subprocess_cmd.run_cmd(["pushsource-ls", f"staged:{base_dir}"], check=True)
    except subprocess.CalledProcessError as exc:
        log_command_failure(exc)
        raise


def run_marketplacesvm_push(
    base_dir: Path,
    starmap_file: Path,
    *,
    pre_push: bool,
    env: Mapping[str, str] | None = None,
) -> None:
    """Invoke ``marketplacesvm_push_wrapper`` against the staged content."""
    cmd = [
        "marketplacesvm_push_wrapper",
        "--debug",
    ]
    if pre_push:
        cmd.append("--nochannel")
    cmd.extend(["--source", str(base_dir), "--starmap-file", str(starmap_file)])
    try:
        subprocess_cmd.run_cmd(cmd, cwd=base_dir, env=env, check=True)
    except subprocess.CalledProcessError as exc:
        log_command_failure(exc)
        raise


def copy_artifacts(base_dir: Path, data_dir: Path) -> None:
    """Copy wrapper-generated artifacts into *data_dir* when present."""
    artifacts = base_dir / "artifacts"
    if artifacts.is_dir():
        shutil.copytree(artifacts, data_dir / "artifacts", dirs_exist_ok=True)


def run(
    *,
    data_dir: Path,
    snapshot_path: str,
    pre_push: bool,
    concurrent_limit: int,
    secrets_dir: Path,
    workdir: Path,
) -> int:
    """Execute the full marketplacesvm disk-image push workflow."""
    credential_files = validate_credentials(secrets_dir)
    set_cloud_credentials(credential_files)

    snapshot_file = data_dir / snapshot_path
    snapshot = load_json_dict(snapshot_file)

    components = snapshot.get("components")
    if not isinstance(components, list) or not components:
        raise ValueError("snapshot must contain a non-empty components list")
    if not all(isinstance(component, Mapping) for component in components):
        raise ValueError("snapshot components must all be JSON objects")

    starmap_file = write_starmap_file(snapshot, snapshot_file)

    workdir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(dir=workdir) as base_dir_name:
        base_dir = Path(base_dir_name)
        disk_imgs_dir = base_dir / "starmap" / "CLOUD_IMAGES"
        disk_imgs_dir.mkdir(parents=True, exist_ok=True)

        prepare_components(
            components,
            disk_imgs_dir,
            workdir,
            concurrent_limit,
        )

        validate_staged_structure(base_dir)
        run_marketplacesvm_push(
            base_dir,
            starmap_file,
            pre_push=pre_push,
        )
        copy_artifacts(base_dir, data_dir)

    return 0


def main(argv: list[str] | None = None) -> int:
    """Parse CLI flags and run the marketplacesvm push workflow."""
    args = parse_args(argv)
    return run(
        data_dir=Path(args.data_dir),
        snapshot_path=args.snapshot_path,
        pre_push=args.pre_push == "true",
        concurrent_limit=args.concurrent_limit,
        secrets_dir=Path(args.secrets_dir),
        workdir=Path(args.workdir),
    )


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
