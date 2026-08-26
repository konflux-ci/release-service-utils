#!/usr/bin/env python3
"""Extract out-of-tree kernel modules from a container image.

Detects the architectures present in the source container image,
copies each architecture-specific image via skopeo, scans the image
layers for ``.ko`` files under the configured kmods path, and extracts
them preserving directory structure.  An ``envfile`` adjacent to the
kmods directory is also extracted when present and used to determine
the final architecture directory name.
"""

from __future__ import annotations

import json
import re
import tarfile
import tempfile
from pathlib import Path

import skopeo
import snapshot as snapshot_helper
import tekton
from logger import logger
from subprocess_cmd import run_cmd_text


def get_image_architectures(source_image: str) -> list[dict]:
    """Call the ``get-image-architectures`` binary and parse NDJSON output.

    Returns a list of architecture descriptor dicts, each containing at
    least ``platform.architecture`` and ``digest``.
    """
    raw = run_cmd_text(["get-image-architectures", source_image])
    architectures: list[dict] = []
    for line in raw.strip().splitlines():
        line = line.strip()
        if line:
            architectures.append(json.loads(line))
    if not architectures:
        raise ValueError(f"get-image-architectures returned no data for {source_image}")
    return architectures


def _ko_prefix(kmods_path: str) -> str:
    """Return the tar-entry prefix for matching ``.ko`` files."""
    return kmods_path.strip("/")


def extract_single_arch(
    image: str,
    dest_dir: Path,
    kmods_path: str,
    arch_name: str,
) -> bool:
    """Copy *image* via skopeo and extract ``.ko`` files from its layers.

    Returns ``True`` if at least one ``.ko`` file was extracted.
    """
    logger.info(
        "Extracting .ko files from %s to %s (arch: %s)",
        image,
        dest_dir,
        arch_name,
    )

    prefix = _ko_prefix(kmods_path)
    ko_pattern = re.compile(rf"^{re.escape(prefix)}/.*\.ko$")
    parent = Path(prefix).parent
    envfile_rel = str(parent / "envfile") if parent != Path(".") else "envfile"

    with tempfile.TemporaryDirectory() as tmp_str:
        tmp_dir = Path(tmp_str)
        skopeo.copy(f"docker://{image}", f"dir:{tmp_dir}", check=True)

        manifest = json.loads((tmp_dir / "manifest.json").read_text(encoding="utf-8"))

        extracted = False
        for layer in manifest.get("layers", []):
            digest: str = layer["digest"]
            layer_file = tmp_dir / digest.removeprefix("sha256:")

            extracted = _extract_layer(
                layer_file,
                prefix,
                ko_pattern,
                envfile_rel,
                dest_dir,
            )
            if extracted:
                logger.info(
                    "Extracted %s/ from %s for %s",
                    kmods_path,
                    digest,
                    arch_name,
                )
                break
            logger.info(
                "%s not found in %s, continuing...",
                kmods_path,
                digest,
            )

        if extracted:
            ko_count = sum(1 for _ in dest_dir.rglob("*.ko") if _.is_file())
            logger.info(
                "Extracted .ko files for %s: %d files",
                arch_name,
                ko_count,
            )
        else:
            logger.warning("No kernel modules found for %s", arch_name)

        return extracted


def _extract_layer(
    layer_file: Path,
    prefix: str,
    ko_pattern: re.Pattern[str],
    envfile_rel: str,
    dest_dir: Path,
) -> bool:
    """Extract ``.ko`` files and the envfile from a single layer in one pass.

    Opens *layer_file* once, classifies members, and extracts ``.ko``
    files directly to *dest_dir* (stripping the kmods prefix).  The
    envfile is extracted alongside if present.
    Returns ``True`` if at least one ``.ko`` file was extracted.
    """
    prefix_slash = f"{prefix}/"
    with tarfile.open(layer_file) as tf:
        members = tf.getmembers()

        ko_members = [m for m in members if ko_pattern.match(m.name)]
        if not ko_members:
            return False

        dest_dir.mkdir(parents=True, exist_ok=True)
        for member in ko_members:
            rel = member.name[len(prefix_slash) :]
            member.name = rel
            tf.extract(member, path=dest_dir, filter="data")
            logger.info("  Extracted: %s", rel)

        envfile_members = [m for m in members if m.name == envfile_rel]
        if envfile_members:
            envfile_member = envfile_members[0]
            envfile_member.name = "envfile"
            tf.extract(envfile_member, path=dest_dir, filter="data")
            logger.info("Extracted envfile for destination %s", dest_dir)

    return True


def resolve_arch_name(dest_dir: Path, platform_arch: str) -> str:
    """Determine the final architecture name from the envfile.

    If the ``envfile`` in *dest_dir* defines an ``ARCH`` variable that
    is non-empty and not ``MULTI_PLATFORM``, that value is used.
    Otherwise *platform_arch* is returned.
    """
    envfile = dest_dir / "envfile"
    if envfile.is_file():
        for line in envfile.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("ARCH="):
                arch_value = line[len("ARCH=") :].strip("'\"")
                if arch_value and arch_value != "MULTI_PLATFORM":
                    logger.info(
                        "Using ARCH=%s from envfile (platform: %s)",
                        arch_value,
                        platform_arch,
                    )
                    return arch_value
                if arch_value == "MULTI_PLATFORM":
                    logger.info(
                        "ARCH=MULTI_PLATFORM in envfile, " "using platform architecture: %s",
                        platform_arch,
                    )
                    return platform_arch
    logger.info(
        "No ARCH variable in envfile, using platform architecture: %s",
        platform_arch,
    )
    return platform_arch


def _write_summary(output_base: Path, arch_count: int) -> None:
    """Write a multi-architecture extraction summary file."""
    summary = output_base / "extraction_summary.txt"
    lines = [
        "Multi-architecture extraction summary:",
        f"Total architectures processed: {arch_count}",
        "Extraction details:",
    ]
    for arch_dir in sorted(output_base.iterdir()):
        if arch_dir.is_dir():
            ko_files = list(arch_dir.rglob("*.ko"))
            lines.append(f"  {arch_dir.name}: {len(ko_files)} .ko files")
            if ko_files:
                lines.append("    Files:")
                for ko in ko_files:
                    lines.append(f"      {ko.name}")
    summary.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("Multi-architecture extraction completed.")


def run(
    data_dir: Path,
    snapshot_path: str,
    kmods_path: str,
    signed_kmods_path: str,
) -> None:
    """Detect architectures and extract kernel modules for each one."""
    snapshot_file = data_dir / snapshot_path
    source_image = snapshot_helper.first_component(snapshot_file)["container_image"]
    logger.info("Detecting architectures for image: %s", source_image)

    architectures = get_image_architectures(source_image)
    arch_count = len(architectures)
    logger.info("Found %d architecture(s)", arch_count)

    output_base = data_dir / signed_kmods_path
    output_base.mkdir(parents=True, exist_ok=True)

    if arch_count == 1:
        logger.info("Single architecture image, using standard extraction")
    else:
        logger.info(
            "Multi-architecture image detected, processing each architecture separately"
        )

    source_base = source_image.split("@")[0]
    for arch_info in architectures:
        platform_arch: str = arch_info["platform"]["architecture"]
        arch_image = (
            source_image if arch_count == 1 else f"{source_base}@{arch_info['digest']}"
        )
        logger.info("Processing architecture: %s", platform_arch)

        arch_dir = output_base / platform_arch
        extract_single_arch(arch_image, arch_dir, kmods_path, platform_arch)

        final_arch = resolve_arch_name(arch_dir, platform_arch)
        if final_arch != platform_arch:
            logger.info(
                "Renaming directory from %s to %s",
                platform_arch,
                final_arch,
            )
            arch_dir.rename(output_base / final_arch)

    if arch_count > 1:
        _write_summary(output_base, arch_count)


def main() -> int:
    """Read environment variables and run extraction."""
    data_dir = Path(tekton.require_env("PARAM_DATA_DIR"))
    snapshot_path = tekton.require_env("PARAM_SNAPSHOT_PATH")
    kmods_path = tekton.require_env("PARAM_KMODS_PATH")
    signed_kmods_path = tekton.require_env("PARAM_SIGNED_KMODS_PATH")

    run(
        data_dir=data_dir,
        snapshot_path=snapshot_path,
        kmods_path=kmods_path,
        signed_kmods_path=signed_kmods_path,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
