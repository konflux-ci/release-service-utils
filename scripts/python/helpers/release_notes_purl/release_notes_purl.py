"""Populate releaseNotes artifact PURLs from a checksum map OCI artifact."""

from __future__ import annotations

import json
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Any
from urllib.parse import quote

import authentication
import content_gateway
import disk_image_utils
import file
import subprocess_cmd
from logger import logger

PURL_CONTENT_TYPES = frozenset({"binary", "generic", "disk-image"})
TA_DOCKERCONFIG_DEFAULT = Path("/mnt/trusted_artifacts_dockerconfig/.dockerconfigjson")


def _artifact_rows(release_notes: dict[str, Any]) -> list[dict[str, Any]]:
    """Return releaseNotes.content.artifacts as a list of dict rows."""
    content = release_notes.get("content")
    if not isinstance(content, dict):
        return []
    artifacts = content.get("artifacts")
    if not isinstance(artifacts, list):
        return []
    return [row for row in artifacts if isinstance(row, dict)]


def _all_artifacts_have_purls(artifacts: list[dict[str, Any]]) -> bool:
    """Return true when every artifact row has a non-placeholder PURL."""
    if not artifacts:
        return False
    for row in artifacts:
        purl = row.get("purl")
        if not isinstance(purl, str) or not purl or purl == "placeholder":
            return False
    return True


def _component_content_type(component: dict[str, Any]) -> str:
    """Return contentGateway.contentType, else top-level contentType, else empty."""
    content_gateway_cfg = component.get("contentGateway")
    if isinstance(content_gateway_cfg, dict):
        content_type = content_gateway_cfg.get("contentType")
        if content_type:
            return str(content_type)
    content_type = component.get("contentType")
    return str(content_type) if content_type else ""


def _first_purl_content_type(data: dict[str, Any]) -> str:
    """Return the first mapping component content type that needs PURL updates.

    A release can have mixed content types (e.g. binary + disk-image), so callers
    scan across all components rather than stopping at the first one.
    """
    components = data.get("mapping", {}).get("components")
    if not isinstance(components, list):
        return ""
    for component in components:
        if not isinstance(component, dict):
            continue
        content_type = _component_content_type(component)
        if content_type in PURL_CONTENT_TYPES:
            return content_type
    return ""


def _staged_files_by_component(
    snapshot: dict[str, Any],
) -> dict[str, list[dict[str, Any]]]:
    """Build component-name → staged.files[] from the snapshot spec.

    apply-mapping substitutes ``{{ release_timestamp }}`` (and other template vars)
    in the snapshot's components but does NOT write those substitutions back to the
    data file. Reading staged.files from data.json would produce filenames like
    ``foo-{{ release_timestamp }}-x86_64.iso`` which Jinja2 would then render as
    empty, giving ``foo--x86_64.iso`` in the advisory.
    """
    out: dict[str, list[dict[str, Any]]] = {}
    components = snapshot.get("components")
    if not isinstance(components, list):
        return out
    for component in components:
        if not isinstance(component, dict):
            continue
        name = component.get("name")
        if not isinstance(name, str) or not name:
            continue
        staged = component.get("staged")
        files = staged.get("files") if isinstance(staged, dict) else None
        if not isinstance(files, list):
            out[name] = []
            continue
        out[name] = [row for row in files if isinstance(row, dict)]
    return out


def load_checksum_map(checksum_map_param: str) -> list[dict[str, Any]]:
    """Pull checksum_map from OCI and return the decoded JSON list."""
    pull_dir = Path(tempfile.mkdtemp(prefix="checksum-map-"))
    try:
        logger.info("Pulling checksum_map from OCI: %s", checksum_map_param)
        subprocess_cmd.run_cmd(["oras", "pull", checksum_map_param], cwd=pull_dir, check=True)

        # oras may leave a gzip tarball or a plain checksum_map.json depending on push format.
        archive_path = pull_dir / "checksum_map"
        json_path = pull_dir / "checksum_map.json"
        if archive_path.is_file():
            with tarfile.open(archive_path, "r:gz") as archive:
                archive.extractall(path=pull_dir)
        if not json_path.is_file():
            msg = "checksum_map.json not found in OCI artifact"
            raise FileNotFoundError(msg)

        parsed = json.loads(json_path.read_text(encoding="utf-8"))
        if not isinstance(parsed, list) or not parsed:
            msg = "checksum map is empty or invalid JSON"
            raise ValueError(msg)
        logger.info("Checksum manifest loaded with %d component(s).", len(parsed))
        return parsed
    finally:
        shutil.rmtree(pull_dir, ignore_errors=True)


def _checksum_for_file(
    checksum_map: list[dict[str, Any]],
    *,
    component_name: str,
    filename_basename: str,
) -> str:
    """Look up a checksum for *component_name* and *filename_basename*."""
    for row in checksum_map:
        if not isinstance(row, dict):
            continue
        if row.get("component") != component_name:
            continue
        files = row.get("files")
        if not isinstance(files, dict):
            continue
        checksum = files.get(filename_basename)
        if isinstance(checksum, str) and checksum:
            return checksum
    logger.warning(
        "No checksum found for %s/%s in manifest",
        component_name,
        filename_basename,
    )
    return ""


def _build_purl(
    *,
    component_name: str,
    version_name: str,
    filename_basename: str,
    checksum: str,
    download_url: str,
) -> str:
    """Build a pkg:generic PURL with filename, checksum, and download_url params.

    Filename is added first so it uniquely identifies the file in the PURL.
    """
    purl = f"pkg:generic/{component_name}@{version_name}"
    query_parts: list[str] = []
    if filename_basename:
        query_parts.append(f"filename={quote(filename_basename, safe='')}")
    if checksum:
        query_parts.append(f"checksum={quote(checksum, safe='')}")
    if download_url:
        query_parts.append(f"download_url={quote(download_url, safe='')}")
    if query_parts:
        purl = f"{purl}?{'&'.join(query_parts)}"
    return purl


def _component_version_name(component: dict[str, Any]) -> str:
    """Return CGW productVersionName, else staged.version, else empty."""
    component_cgw = component.get("contentGateway")
    if isinstance(component_cgw, dict):
        version_name = str(component_cgw.get("productVersionName") or "")
        if version_name:
            return version_name
    staged = component.get("staged")
    if isinstance(staged, dict):
        return str(staged.get("version") or "")
    return ""


def _download_url_for_component(
    component: dict[str, Any],
    *,
    cgw_base_url: str,
    cdn_base_url: str,
) -> str:
    """Return the canonical download_url base for a component.

    When a component has both contentGateway (Developer Portal) and staged
    (Customer Portal/CDN), CGW is used. If only staged is present, the CDN URL
    is used instead.
    """
    component_cgw = component.get("contentGateway")
    has_cgw = isinstance(component_cgw, dict) and bool(component_cgw)
    return cgw_base_url if has_cgw else cdn_base_url


def _dedupe_artifact_key(row: dict[str, Any]) -> str:
    """Return the component|architecture|os deduplication key for an artifact row."""
    return (
        f"{row.get('component', '')}|" f"{row.get('architecture', '')}|" f"{row.get('os', '')}"
    )


def _updated_binary_or_generic_entries(
    data: dict[str, Any],
    component: dict[str, Any],
    *,
    checksum_map: list[dict[str, Any]],
    cgw_base_url: str,
    cdn_base_url: str,
) -> list[dict[str, Any]]:
    """Build updated artifact rows for one binary/generic mapping component."""
    component_name = str(component.get("name", ""))
    version_name = _component_version_name(component)
    if not version_name:
        logger.warning(
            "No version found for component %s (checked contentGateway and staged)",
            component_name,
        )
        return []

    release_notes = data["releaseNotes"]
    matching_entries = [
        row for row in _artifact_rows(release_notes) if row.get("component") == component_name
    ]
    if not matching_entries:
        logger.warning(
            "no releaseNotes.content.artifacts entries found for component: %s",
            component_name,
        )
        return []

    download_url = _download_url_for_component(
        component,
        cgw_base_url=cgw_base_url,
        cdn_base_url=cdn_base_url,
    )
    updated: list[dict[str, Any]] = []
    for entry in matching_entries:
        architecture = str(entry.get("architecture", ""))
        operating_system = str(entry.get("os", ""))
        # Match files by arch/os, falling back to staged.files for teams that use
        # the CDN staged structure instead of a top-level files array.
        filename = content_gateway.filename_for_binary_or_generic(
            component,
            architecture=architecture,
            operating_system=operating_system,
        )
        # compress-artifacts renames Windows .tar.gz to .zip; checksum map keys use .zip.
        filename_basename = content_gateway.windows_archive_basename(
            filename,
            operating_system,
        )
        checksum = _checksum_for_file(
            checksum_map,
            component_name=component_name,
            filename_basename=filename_basename,
        )
        purl = _build_purl(
            component_name=component_name,
            version_name=version_name,
            filename_basename=filename_basename,
            checksum=checksum,
            download_url=download_url,
        )
        updated.append({**entry, "purl": purl})
    return updated


def _updated_disk_image_entries(
    data: dict[str, Any],
    component: dict[str, Any],
    *,
    staged_files: list[dict[str, Any]],
    checksum_map: list[dict[str, Any]],
    cgw_base_url: str,
    cdn_base_url: str,
) -> list[dict[str, Any]]:
    """Expand one advisory artifact row per snapshot staged file for a disk-image.

    Multiple files can share the same os+arch (e.g. ISO + QCOW2 both linux/x86_64),
    so iterating over advisory entries (one per os+arch) would produce malformed
    PURLs. Instead, use the first advisory entry as a metadata template and expand
    it per staged file.
    """
    component_name = str(component.get("name", ""))
    version_name = _component_version_name(component)
    if not version_name:
        logger.warning(
            "No version found for component %s (checked contentGateway and staged)",
            component_name,
        )
        return []

    release_notes = data["releaseNotes"]
    matching_entries = [
        row for row in _artifact_rows(release_notes) if row.get("component") == component_name
    ]
    if not matching_entries:
        logger.warning(
            "no releaseNotes.content.artifacts entries found for component: %s",
            component_name,
        )
        return []

    # Fail loudly instead of silently leaving placeholder PURLs if the snapshot
    # has no staged.files[] (e.g. snapshot/mapping mismatch).
    if not staged_files:
        msg = (
            f"disk-image component {component_name} has releaseNotes.content.artifacts "
            "entries but no staged.files[] in the snapshot"
        )
        raise ValueError(msg)

    template_entry = matching_entries[0]
    download_url = _download_url_for_component(
        component,
        cgw_base_url=cgw_base_url,
        cdn_base_url=cdn_base_url,
    )
    updated: list[dict[str, Any]] = []
    for staged_file in staged_files:
        filename = staged_file.get("filename")
        # Reject missing/empty values and the literal "null" string (JSON null mishandling).
        if not isinstance(filename, str) or not filename or filename == "null":
            msg = (
                f"staged.files[].filename is required for disk-image "
                f"component {component_name}"
            )
            raise ValueError(msg)
        filename_basename = Path(filename).name
        # Disk-image architecture is encoded in the filename; os defaults to linux.
        architecture = disk_image_utils.architecture_from_filename(filename_basename)
        checksum = _checksum_for_file(
            checksum_map,
            component_name=component_name,
            filename_basename=filename_basename,
        )
        purl = _build_purl(
            component_name=component_name,
            version_name=version_name,
            filename_basename=filename_basename,
            checksum=checksum,
            download_url=download_url,
        )
        updated.append(
            {
                **template_entry,
                "purl": purl,
                "architecture": architecture,
                "os": disk_image_utils.DISK_IMAGE_DEFAULT_OS,
            },
        )
    return updated


def _merge_updated_artifacts(
    data: dict[str, Any],
    *,
    updated_entries: list[dict[str, Any]],
    updated_disk_entries: list[dict[str, Any]],
) -> None:
    """Merge binary/generic and disk-image updates back into data.json."""
    release_notes = data["releaseNotes"]
    content = release_notes.setdefault("content", {})
    if not isinstance(content, dict):
        content = {}
        release_notes["content"] = content
    artifacts = content.get("artifacts")
    if not isinstance(artifacts, list):
        artifacts = []
        content["artifacts"] = artifacts

    # Disk-image replacements: drop existing entries for updated components and
    # substitute the new per-file expanded entries. Cannot deduplicate by
    # component|arch|os because multiple files (e.g. ISO + QCOW2) legitimately
    # share the same arch/os.
    disk_components = {
        str(row.get("component", "")) for row in updated_disk_entries if row.get("component")
    }
    if updated_disk_entries:
        kept = [
            row
            for row in artifacts
            if isinstance(row, dict) and str(row.get("component", "")) not in disk_components
        ]
        artifacts = kept + updated_disk_entries
        content["artifacts"] = artifacts

    if not updated_entries:
        return

    # Merge binary/generic updated entries. Deduplicate by component|architecture|os
    # when multiple files share the same arch/os. Disk-image components are excluded
    # from this pass (and re-appended untouched) because they were already expanded
    # above and can legitimately have multiple entries sharing the same
    # component|architecture|os -- deduplicating them here would drop all but one
    # file's PURL/checksum.
    updated_map = {_dedupe_artifact_key(row): row for row in updated_entries}
    disk_items = [
        row
        for row in artifacts
        if isinstance(row, dict) and str(row.get("component", "")) in disk_components
    ]
    other_items = [
        row
        for row in artifacts
        if isinstance(row, dict) and str(row.get("component", "")) not in disk_components
    ]
    merged_rows: dict[str, dict[str, Any]] = {}
    for row in other_items:
        key = _dedupe_artifact_key(row)
        merged_rows[key] = updated_map.get(key, row)
    for key, row in updated_map.items():
        merged_rows.setdefault(key, row)
    content["artifacts"] = list(merged_rows.values()) + disk_items


def update_artifact_purls(
    data_file: Path,
    *,
    checksum_map_param: str,
    dockerconfig_path: Path = TA_DOCKERCONFIG_DEFAULT,
    snapshot_path: Path | None = None,
) -> None:
    """Update releaseNotes artifact PURLs in *data_file* when a checksum map exists."""
    data = file.load_json_dict(data_file)
    # Marketplace releases ship pre-built PURLs; checksum map is not used.
    marketplace_secret = data.get("mapping", {}).get("cloudMarketplacesSecret")
    if marketplace_secret:
        logger.info("Marketplace release detected. Skipping PURL updates.")
        return

    # Check whether any component requires PURL updates (binary/generic/disk-image).
    # Mixed releases (e.g. binary + disk-image) must look across all components.
    purl_content_type = _first_purl_content_type(data)
    if not purl_content_type:
        if "github" in data:
            logger.info("Github release. Skipping update-purl.")
            return
        logger.info("No binary/generic/disk-image content type found, skipping update-purl")
        return

    release_notes = data["releaseNotes"]
    artifacts = _artifact_rows(release_notes)
    if _all_artifacts_have_purls(artifacts):
        logger.info(
            "All %d artifacts already have PURLs populated. Skipping update-purl.",
            len(artifacts),
        )
        return

    logger.info("Processing artifacts for PURL updates...")
    cgw_base_url, cdn_base_url = content_gateway.cdn_base_urls(data)
    logger.info(
        "Using %s environment: CGW=%s, CDN=%s",
        content_gateway.cdn_env(data),
        cgw_base_url,
        cdn_base_url,
    )

    authentication.setup_docker_config(dockerconfig_path, optional=True)
    if not checksum_map_param or checksum_map_param in {"null", "empty"}:
        msg = (
            f"checksum map is required for content type '{purl_content_type}' "
            "but was not provided."
        )
        raise ValueError(msg)
    checksum_map = load_checksum_map(checksum_map_param)

    # Snapshot staged.files (with template vars already substituted) — see
    # _staged_files_by_component for why data.json must not be used here.
    staged_by_component: dict[str, list[dict[str, Any]]] = {}
    if snapshot_path is not None:
        snapshot = file.load_json_dict(snapshot_path)
        staged_by_component = _staged_files_by_component(snapshot)

    components = data.get("mapping", {}).get("components")
    if not isinstance(components, list):
        components = []

    updated_entries: list[dict[str, Any]] = []
    updated_disk_entries: list[dict[str, Any]] = []
    for component in components:
        if not isinstance(component, dict):
            continue
        # Resolve content type per component so mixed releases (e.g. binary +
        # disk-image) each go through the correct code path. Unspecified or
        # container "image" types do not need PURL updates here.
        content_type = _component_content_type(component)
        if content_type not in PURL_CONTENT_TYPES:
            logger.info(
                "Component %s content type '%s'. Skipping.",
                component.get("name", ""),
                content_type,
            )
            continue
        if content_type == "disk-image":
            component_name = str(component.get("name", ""))
            updated_disk_entries.extend(
                _updated_disk_image_entries(
                    data,
                    component,
                    staged_files=staged_by_component.get(component_name, []),
                    checksum_map=checksum_map,
                    cgw_base_url=cgw_base_url,
                    cdn_base_url=cdn_base_url,
                ),
            )
        else:
            updated_entries.extend(
                _updated_binary_or_generic_entries(
                    data,
                    component,
                    checksum_map=checksum_map,
                    cgw_base_url=cgw_base_url,
                    cdn_base_url=cdn_base_url,
                ),
            )

    if not updated_entries and not updated_disk_entries:
        logger.info("No advisory entries were updated.")
        return

    _merge_updated_artifacts(
        data,
        updated_entries=updated_entries,
        updated_disk_entries=updated_disk_entries,
    )
    data_file.write_text(
        json.dumps(data, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )
