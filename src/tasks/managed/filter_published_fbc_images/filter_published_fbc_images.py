#!/usr/bin/env python3
"""Filter already-published FBC fragments from a snapshot via Pyxis index queries.

Inspect each snapshot component for its OCP version, query Pyxis for published
index images, and drop fragments that are already present in the target catalog.
Stage builds and Pyxis failures keep every component.  The filtered snapshot is
written next to the data directory and the relative filename is recorded as a
Tekton result.
"""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

from release_service_utils.helpers import file, http_client, pyxis_api, tekton
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.ocp_version import resolve_ocp_version

FILTERED_SNAPSHOT_FILENAME = "filtered-snapshot.json"
PAGE_SIZE = 500
DATE_FILTER_DAYS = 30
DEFAULT_PYXIS_SECRET_MOUNT = "/etc/secrets"
VERSION_PATTERN = re.compile(r"^v[0-9]+\.[0-9]{1,2}$")
OCP_PLACEHOLDER = re.compile(r"\{\{(\s+)?OCP_VERSION(\s+)?\}\}")


def last_update_date_filter() -> str:
    """Return a UTC calendar date 30 days ago for the Pyxis date filter."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=DATE_FILTER_DAYS)
    return cutoff.strftime("%Y-%m-%d")


def resolve_pyxis_api_url(server: str) -> str:
    """Use `PYXIS_URL` when set; otherwise map *server* to a Pyxis API URL."""
    override = os.environ.get("PYXIS_URL", "").strip()
    if override:
        return override.rstrip("/")
    return pyxis_api.pyxis_api_url_for_server(server)


def pyxis_server_name(data: dict[str, Any]) -> str:
    """Return the Pyxis server name from merged data, defaulting to production."""
    pyxis = data.get("pyxis")
    if not isinstance(pyxis, dict):
        return "production"
    return str(pyxis.get("server") or "production")


def is_staged_index(data: dict[str, Any]) -> bool:
    """Return whether `fbc.stagedIndex` is enabled."""
    fbc = data.get("fbc")
    if not isinstance(fbc, dict):
        return False
    return str(fbc.get("stagedIndex", False)).lower() == "true"


def fbc_target_index(data: dict[str, Any]) -> str:
    """Return the `fbc.targetIndex` template, or an empty string if unset."""
    fbc = data.get("fbc")
    if not isinstance(fbc, dict):
        return ""
    raw = fbc.get("targetIndex") or ""
    return str(raw).strip()


def extract_component_ocp_version(component: dict[str, Any]) -> str:
    """Inspect *component* and return its validated OCP version."""
    name = component.get("name", "?")
    container_image = component["containerImage"]
    logger.info("Extracting OCP version for %s from %s", name, container_image)
    raw = resolve_ocp_version(container_image)
    if not raw:
        raise ValueError(
            f"Could not extract OCP version from {container_image}: "
            "annotation 'org.opencontainers.image.base.name' not found"
        )
    if not raw.startswith("v"):
        raw = f"v{raw}"
    if not VERSION_PATTERN.match(raw):
        raise ValueError(
            f"Invalid OCP version format: '{raw}'. Expected format: vX.Y (e.g., v4.12)"
        )
    logger.info("OCP version validated: %s", raw)
    return raw


def resolve_target_index(template: str, ocp_version: str) -> str:
    """Replace `{{ OCP_VERSION }}` placeholders in *template* with *ocp_version*."""
    return OCP_PLACEHOLDER.sub(ocp_version, template)


def fragment_digest(container_image: str) -> str:
    """Return the digest suffix of *container_image*, or the whole reference."""
    if "@" in container_image:
        return container_image.rsplit("@", 1)[1]
    return container_image


def unique_target_indexes(ocp_versions: list[list[str]], template: str) -> list[str]:
    """Return unique resolved targetIndex values in first-seen order."""
    return list(
        dict.fromkeys(
            resolve_target_index(template, version)
            for versions in ocp_versions
            for version in versions
        )
    )


def _digests_from_related_images(related_images: Any) -> list[str]:
    """Collect digest strings from a related_images array."""
    if not isinstance(related_images, list):
        return []
    digests: list[str] = []
    for item in related_images:
        if not isinstance(item, dict):
            continue
        digest = item.get("digest") or ""
        if digest:
            digests.append(str(digest))
        image = item.get("image") or ""
        if isinstance(image, str) and "@" in image:
            image_digest = image.rsplit("@", 1)[1]
            if image_digest:
                digests.append(image_digest)
    return digests


def extract_published_digests(index_images: list[Any]) -> set[str]:
    """Extract unique fragment digests from Pyxis index-image records."""
    digests: set[str] = set()
    for image in index_images:
        if not isinstance(image, dict):
            continue
        digests.update(_digests_from_related_images(image.get("related_images")))
        bundles = image.get("bundles")
        if not isinstance(bundles, list):
            continue
        for bundle in bundles:
            if isinstance(bundle, dict):
                digests.update(_digests_from_related_images(bundle.get("related_images")))
    return digests


def pyxis_images_url(pyxis_api_url: str, target_index: str) -> str:
    """Build the Pyxis v1 images query URL for *target_index*."""
    filter_value = (
        f"docker_image_id=={target_index};" f"last_update_date>={last_update_date_filter()}"
    )
    encoded = quote(filter_value, safe="")
    return f"{pyxis_api_url.rstrip('/')}/images?filter={encoded}&page_size={PAGE_SIZE}"


def query_published_digests(
    pyxis_api_url: str,
    target_index: str,
    cert: tuple[str, str],
) -> set[str] | None:
    """Query Pyxis for published fragment digests.

    Return a set of digests, an empty set when no index exists, or ``None`` when
    the query fails and filtering should be skipped.
    """
    url = pyxis_images_url(pyxis_api_url, target_index)
    logger.info("Querying Pyxis for targetIndex: %s", target_index)
    try:
        body = http_client.get_text(
            url,
            cert=cert,
            headers={"Content-Type": "application/json"},
            timeout=60,
        )
    except requests.RequestException:
        logger.warning(
            "Failed to query Pyxis API for %s",
            target_index,
            exc_info=True,
        )
        return None
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        logger.warning("Invalid JSON response from Pyxis API")
        return None
    if not isinstance(payload, dict) or "data" not in payload:
        logger.warning("Pyxis response missing .data field")
        return None
    records = payload["data"]
    if not isinstance(records, list):
        logger.warning("Pyxis .data is not a list")
        return None
    if not records:
        logger.info("No index images found (first release to this catalog)")
        return set()
    digests = extract_published_digests(records)
    logger.info("Extracted %d unique fragment digest(s)", len(digests))
    return digests


def query_all_target_indexes(
    pyxis_api_url: str,
    ocp_versions: list[list[str]],
    template: str,
    cert: tuple[str, str],
) -> dict[str, set[str]] | None:
    """Query Pyxis for each unique targetIndex.

    Return ``None`` if any query fails so the caller can keep all components.
    """
    indexes = unique_target_indexes(ocp_versions, template)
    logger.info("Found %d unique resolved targetIndex value(s)", len(indexes))
    published_by_index: dict[str, set[str]] = {}
    for target_index in indexes:
        digests = query_published_digests(pyxis_api_url, target_index, cert)
        if digests is None:
            return None
        published_by_index[target_index] = digests
    return published_by_index


def attach_ocp_versions(
    snapshot: dict[str, Any],
    ocp_versions: list[list[str]],
) -> dict[str, Any]:
    """Return a copy of *snapshot* with ``ocpVersion`` set on every component."""
    result = dict(snapshot)
    components: list[dict[str, Any]] = []
    for component, versions in zip(
        snapshot.get("components") or [],
        ocp_versions,
        strict=True,
    ):
        updated = dict(component)
        updated["ocpVersion"] = list(versions)
        components.append(updated)
    result["components"] = components
    return result


def filter_unpublished_components(
    snapshot: dict[str, Any],
    ocp_versions: list[list[str]],
    template: str,
    published_by_index: dict[str, set[str]],
) -> dict[str, Any]:
    """Keep components that still have at least one unpublished OCP version.

    Each component is checked per version against that version's catalog. A
    component is kept when any version is unpublished; ``ocpVersion`` then
    lists only those unpublished versions.
    """
    kept: list[dict[str, Any]] = []
    filtered_out = 0
    components = snapshot.get("components") or []
    for component, versions in zip(components, ocp_versions, strict=True):
        name = component.get("name", "?")
        digest = fragment_digest(str(component.get("containerImage", "")))
        versions_to_keep: list[str] = []
        for version in versions:
            target_index = resolve_target_index(template, version)
            published = published_by_index.get(target_index, set())
            if digest in published:
                logger.info(
                    "Component %s version %s: found in catalog, filter out",
                    name,
                    version,
                )
                continue
            logger.info(
                "Component %s version %s: not in catalog, keep",
                name,
                version,
            )
            versions_to_keep.append(version)
        if not versions_to_keep:
            logger.info("Component %s: filter out (all versions published)", name)
            filtered_out += 1
            continue
        logger.info("Component %s: keep (versions: %s)", name, " ".join(versions_to_keep))
        updated = dict(component)
        updated["ocpVersion"] = list(versions_to_keep)
        kept.append(updated)

    logger.info(
        "Filtering summary: total=%d kept=%d filtered_out=%d",
        len(components),
        len(kept),
        filtered_out,
    )
    result = dict(snapshot)
    result["components"] = kept
    return result


def write_filtered_snapshot(
    data_dir: Path,
    result_path: Path,
    snapshot: dict[str, Any],
) -> None:
    """Write the filtered snapshot JSON and the Tekton result filename."""
    out = data_dir / FILTERED_SNAPSHOT_FILENAME
    out.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    result_path.write_text(FILTERED_SNAPSHOT_FILENAME, encoding="utf-8")
    logger.info("Wrote filtered snapshot to %s", out)


def run(
    *,
    data_dir: Path,
    snapshot_path: Path,
    data_path: Path,
    result_path: Path,
    pyxis_secret_mount: Path,
) -> None:
    """Load inputs, filter published FBC fragments, and write outputs."""
    snapshot = file.load_json_dict(snapshot_path)
    data = file.load_json_dict(data_path)
    components = snapshot.get("components") or []
    logger.info("Found %d component(s) in snapshot", len(components))

    if not components:
        logger.info("Empty snapshot - no components to filter")
        write_filtered_snapshot(data_dir, result_path, snapshot)
        return

    ocp_versions = [[extract_component_ocp_version(component)] for component in components]
    logger.info(
        "OCP versions extracted and validated for all %d component(s)",
        len(components),
    )
    output = attach_ocp_versions(snapshot, ocp_versions)
    template = fbc_target_index(data)

    if is_staged_index(data):
        logger.info("Stage build detected (stagedIndex=true)")
        logger.info("Skipping idempotence filtering (keeping all components)")
    elif not template:
        logger.info("No fbc.targetIndex found in data file")
        logger.info("Skipping idempotence filtering (keeping all components)")
    else:
        logger.info("targetIndex template: %s", template)
        pyxis_url = resolve_pyxis_api_url(pyxis_server_name(data))
        logger.info("Using Pyxis API URL: %s", pyxis_url)
        cert = (str(pyxis_secret_mount / "cert"), str(pyxis_secret_mount / "key"))
        published_by_index = query_all_target_indexes(
            pyxis_url,
            ocp_versions,
            template,
            cert,
        )
        if published_by_index is None:
            logger.warning("Pyxis query failed - skipping idempotence filtering")
        else:
            output = filter_unpublished_components(
                snapshot,
                ocp_versions,
                template,
                published_by_index,
            )

    write_filtered_snapshot(data_dir, result_path, output)


def main() -> int:
    """Read Tekton environment variables and run the filter workflow."""
    data_dir = Path(tekton.require_env("PARAM_DATA_DIR"))
    run(
        data_dir=data_dir,
        snapshot_path=data_dir / tekton.require_env("PARAM_SNAPSHOT_PATH"),
        data_path=data_dir / tekton.require_env("PARAM_DATA_PATH"),
        result_path=tekton.result_paths_from_env("RESULT_FILTERED_SNAPSHOT_PATH")[0],
        pyxis_secret_mount=file.path_from_env_variable(
            "PYXIS_SECRET_MOUNT",
            DEFAULT_PYXIS_SECRET_MOUNT,
        ),
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
