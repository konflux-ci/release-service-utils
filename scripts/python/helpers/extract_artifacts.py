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
import shutil
import subprocess
import tarfile
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import authentication
import disk_image_utils

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
    authentication.setup_docker_config(
        REDHAT_WORKLOADS_TOKEN_MOUNT / ".dockerconfigjson",
        strip_noise=True,
    )


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
        # No explicit parent directories from the RPA entries (e.g. source paths are bare
        # filenames with no directory component).  Fall back to "releases/", which is the
        # conventional top-level directory used in Red Hat release container images.
        layer_extract_dirs = ["releases"]

    unique_files = sorted(set(wanted))
    unique_dirs = sorted(set(layer_extract_dirs))
    return unique_files, unique_dirs


def _safe_extract_layer(
    tf: tarfile.TarFile, image_path: str, target_dir: Path, layer_name: str
) -> bool:
    """Extract members under image_path/ from a layer tarfile with path safety checks.

    Returns True if any matching members were found, False otherwise.
    Raises RuntimeError for unsafe entries (path traversal, symlinks, hardlinks, devices).
    """
    target_real = target_dir.resolve()
    found = False
    for member in tf.getmembers():
        if not (member.name == image_path or member.name.startswith(f"{image_path}/")):
            continue
        found = True
        if member.issym() or member.islnk() or member.isdev():
            raise RuntimeError(
                f"Layer {layer_name} contains unsupported entry type: {member.name}"
            )
        member_real = (target_dir / member.name).resolve()
        if member_real != target_real and target_real not in member_real.parents:
            raise RuntimeError(f"Layer {layer_name} contains unsafe path: {member.name}")
        tf.extract(member, path=str(target_dir), filter="data")
    return found


def extract_binaries_from_layers(
    image_dir: Path,
    image_binaries_path: str,
) -> None:
    """Extract files from image layers that contain *image_binaries_path*.

    Read the manifest.json from *image_dir*, iterate through each layer,
    and extract any files under the *image_binaries_path* directory.
    """
    manifest_path = image_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    for layer in manifest.get("layers", []):
        digest: str = layer["digest"]
        filename = digest.removeprefix("sha256:")
        tar_path = image_dir / filename

        with tarfile.open(tar_path) as tf:
            matching_entries = [
                m for m in tf.getmembers() if m.name.startswith(f"{image_binaries_path}/")
            ]
            if matching_entries:
                logger.info(
                    "Extracting %s/ from %s...",
                    image_binaries_path,
                    filename,
                )
                tf.extractall(
                    path=image_dir,
                    members=matching_entries,
                    filter="data",
                )
            else:
                logger.info(
                    "skipping %s. It doesn't contain the %s dir",
                    filename,
                    image_binaries_path,
                )


def _extract_from_oras(
    manifest: dict,
    tmp_dir: Path,
    wanted_files: list[str],
    destination: Path,
    component_name: str,
) -> None:
    """Copy raw ORAS blob layers to destination, matching by filename.

    ORAS artifacts store raw file blobs as layers with an
    ``org.opencontainers.image.title`` annotation containing the filename.
    We match each wanted file (by basename) to its blob and copy it directly.
    """
    title_to_blob: dict[str, Path] = {}
    for layer in manifest.get("layers", []):
        title = (layer.get("annotations") or {}).get("org.opencontainers.image.title")
        digest = layer.get("digest", "")
        if title and digest:
            blob_path = tmp_dir / digest.removeprefix("sha256:")
            title_to_blob[title] = blob_path

    logger.info(
        "ORAS artifact detected for '%s'; available blobs: %s",
        component_name,
        list(title_to_blob),
    )

    for wanted in wanted_files:
        basename = Path(wanted).name
        blob = title_to_blob.get(basename)
        if blob is None or not blob.is_file():
            available = sorted(title_to_blob)
            raise RuntimeError(
                f"ORAS layer with title '{basename}' not found in component "
                f"'{component_name}'. Available titles: {available}"
            )
        out = destination / basename
        shutil.copy2(str(blob), str(out))
        logger.info("Copied ORAS blob '%s' -> %s", basename, out)


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
                    f"docker://{pullspec}",
                    f"dir:{tmp_dir}",
                ]
            )
        finally:
            if auth_file is not None:
                auth_file.unlink(missing_ok=True)

        wanted_files, extract_dirs = _get_source_paths(component)
        logger.info("Files to extract from RPA: %s", wanted_files)

        manifest = json.loads((tmp_dir / "manifest.json").read_text())

        config_media_type = manifest.get("config", {}).get("mediaType", "")
        if config_media_type == "application/vnd.oci.empty.v1+json":
            # ORAS artifact: layers are raw file blobs, not tar archives.
            # Each layer carries an org.opencontainers.image.title annotation
            # that holds the original filename.  Copy blobs directly to destination.
            _extract_from_oras(manifest, tmp_dir, wanted_files, destination, name)
        else:
            layer_digests = [layer["digest"] for layer in manifest.get("layers", [])]

            for digest in layer_digests:
                layer_file = tmp_dir / digest.removeprefix("sha256:")
                if not layer_file.exists():
                    continue
                with tarfile.open(str(layer_file)) as tf:
                    for image_path in extract_dirs:
                        if _safe_extract_layer(tf, image_path, tmp_dir, layer_file.name):
                            logger.info(
                                "Extracting %s/ from %s...", image_path, layer_file.name
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
                    logger.error("Expected file not found in container: %s", wanted)
                    raise RuntimeError(
                        f"File '{wanted}' declared in RPA was not found in any container layer"
                    )
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


def _validate_disk_image_components(components: list[dict]) -> None:
    """Fail fast if any disk-image component has non-linux file entries.

    Disk images must always target os: linux. Detecting this before pulling
    images avoids wasting time on downloads only to fail deep in the pipeline.
    """
    for component in components:
        if not disk_image_utils.is_disk_image_component(component):
            continue
        name = component.get("name", "<unknown>")
        all_file_entries = list(component.get("files") or []) + list(
            (component.get("staged") or {}).get("files") or []
        )
        for entry in all_file_entries:
            entry_os = entry.get("os", "")
            if entry_os in ("darwin", "windows"):
                raise RuntimeError(
                    f"Component '{name}' has contentType: disk-image but entry "
                    f"'{entry.get('source', '<unknown>')}' has os: {entry_os}. "
                    f"Disk images must be os: linux. Fix the RPA before releasing."
                )


def run(concurrent_limit: int) -> None:
    """Extract artifacts from all snapshot components and write OS flag files."""
    snapshot = json.loads(os.environ["SNAPSHOT_JSON"])

    components = snapshot.get("components", [])

    # Validate disk-image component constraints before doing any image pulls.
    _validate_disk_image_components(components)

    _setup_docker_config()
    CONTENT_DIR.mkdir(parents=True, exist_ok=True)

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
