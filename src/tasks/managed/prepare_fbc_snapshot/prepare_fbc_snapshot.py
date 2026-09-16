#!/usr/bin/env python3
"""Update snapshot with multi-OCP version data and resolved index templates.

Extract OCP versions from container image annotations, resolve index templates
with ``{{ OCP_VERSION }}`` placeholders, and update the snapshot with
component-specific resolved indexes.  Indexes are resolved according to the
release strategy (hotfix, preGA, staged).
"""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

from release_service_utils.helpers import file, tekton
from release_service_utils.helpers.logger import logger

RESERVED_TAG_NAMES = frozenset({"latest", "main", "master", "HEAD"})
MAX_TAG_LENGTH = 128
OCP_VERSION_LENGTH = 5


def sanitize_tag_component(
    value: str,
    component_name: str,
    max_length: int,
) -> str:
    """Validate and sanitize a string for use in an OCI tag.

    Replace characters outside ``[a-zA-Z0-9._-]`` with hyphens, collapse
    consecutive special characters, strip leading/trailing specials, check
    reserved names, and truncate to *max_length*.
    """
    if not value:
        raise ValueError(f"{component_name} cannot be empty")

    sanitized = re.sub(r"[^a-zA-Z0-9._-]", "-", value)

    while re.search(r"[._-]{2}", sanitized):
        sanitized = re.sub(r"[._-]{2,}", "-", sanitized)

    sanitized = sanitized.strip("._-")

    if not sanitized:
        raise ValueError(
            f"{component_name} '{value}' sanitization resulted" " in empty string"
        )

    if sanitized in RESERVED_TAG_NAMES:
        raise ValueError(
            f"{component_name} cannot use reserved name:" f" '{sanitized}' (from '{value}')"
        )

    if value != sanitized:
        logger.info(
            "%s sanitized from '%s' to '%s'",
            component_name,
            value,
            sanitized,
        )

    if len(sanitized) > max_length:
        truncated = sanitized[:max_length].rstrip("._-")
        logger.warning(
            "%s truncated from '%s' to '%s' (max %d chars)",
            component_name,
            sanitized,
            truncated,
            max_length,
        )
        return truncated

    return sanitized


def replace_ocp_version(template: str, ocp_version: str) -> str:
    """Replace ``{{ OCP_VERSION }}`` placeholders in *template*."""
    return re.sub(r"\{\{\s*OCP_VERSION\s*\}\}", ocp_version, template)


def validate_ocp_version(index: str, expected_version: str) -> None:
    """Validate that the tag portion of *index* matches *expected_version*."""
    tag = index.split(":")[-1]
    if not re.match(rf"^{re.escape(expected_version)}(\b|$)", tag):
        raise ValueError(
            "The OCP version of the index does not match the base image\n"
            f"  - index version: {tag}\n"
            f"  - base image version: {expected_version}\n"
            f"  - index: {index}"
        )


def generate_target_index(
    ocp_version: str,
    raw_target_index: str,
    suffix: str,
) -> str:
    """Resolve target index with OCP version placeholder and suffix."""
    if not raw_target_index:
        return ""

    resolved = replace_ocp_version(raw_target_index, ocp_version)
    if suffix:
        resolved = f"{resolved}-{suffix}"
    return resolved


def build_suffix(
    data: dict[str, Any],
    *,
    hotfix: bool,
    pre_ga: bool,
    timestamp: int,
) -> str:
    """Build the common tag suffix for hotfix or preGA releases."""
    if hotfix:
        issue_id = data.get("fbc", {}).get("issueId", "")
        if not issue_id:
            raise ValueError(
                "Hotfix releases require the issue id set in" " the 'fbc.issueId' key"
            )
        separator_chars = 3
        available = MAX_TAG_LENGTH - len(str(timestamp)) - OCP_VERSION_LENGTH - separator_chars
        max_issue_len = min(available, 50)
        if max_issue_len < 15:
            max_issue_len = 15

        sanitized = sanitize_tag_component(issue_id, "fbc.issueId", max_issue_len)
        return f"{sanitized}-{timestamp}"

    if pre_ga:
        product_name = data.get("fbc", {}).get("productName", "")
        product_version = data.get("fbc", {}).get("productVersion", "")
        if not product_name or not product_version:
            raise ValueError(
                "Pre-GA releases require 'fbc.productName'" " and 'fbc.productVersion'"
            )

        separator_chars = 4
        available = MAX_TAG_LENGTH - len(str(timestamp)) - OCP_VERSION_LENGTH - separator_chars
        max_name_len = max(available * 6 // 10, 5)
        max_ver_len = max(available * 4 // 10, 3)

        sanitized_name = sanitize_tag_component(product_name, "fbc.productName", max_name_len)
        sanitized_version = sanitize_tag_component(
            product_version, "fbc.productVersion", max_ver_len
        )
        return f"{sanitized_name}-{sanitized_version}-{timestamp}"

    return ""


def _parse_bool(value: Any) -> bool:
    """Interpret a JSON value as a boolean (handles both bool and string)."""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return False


def run_prepare(
    *,
    snapshot_path: Path,
    data_path: Path,
) -> None:
    """Update the snapshot file with resolved per-component OCP metadata."""
    data = file.load_json_dict(data_path)
    snapshot = file.load_json_dict(snapshot_path)

    fbc = data.get("fbc", {})
    hotfix = _parse_bool(fbc.get("hotfix", False))
    pre_ga = _parse_bool(fbc.get("preGA", False))
    staged_index = _parse_bool(fbc.get("stagedIndex", False))

    logger.info(
        "Release configuration: hotfix=%s, preGA=%s, stagedIndex=%s",
        hotfix,
        pre_ga,
        staged_index,
    )

    raw_from_index: str = fbc.get("fromIndex", "")
    raw_target_index: str = fbc.get("targetIndex", "")

    if not raw_from_index:
        raise ValueError(
            "'fbc.fromIndex' must be set in the data file" " and cannot be empty."
        )

    if not staged_index and not raw_target_index:
        raise ValueError(
            "'fbc.targetIndex' must be set for non-staged" " releases and cannot be empty."
        )

    logger.info(
        "Raw index templates: fromIndex=%s, targetIndex=%s",
        raw_from_index,
        raw_target_index,
    )

    timestamp = int(time.time())
    common_suffix = build_suffix(data, hotfix=hotfix, pre_ga=pre_ga, timestamp=timestamp)
    if common_suffix:
        logger.info("Generated suffix: %s", common_suffix)

    components: list[dict[str, Any]] = snapshot.get("components", [])
    if not components:
        raise ValueError("No components found in snapshot")

    logger.info("Found %d components to process", len(components))

    for i, component in enumerate(components):
        component_name = component.get("name", f"component-{i}")
        logger.info(
            "Processing component %d/%d: %s",
            i + 1,
            len(components),
            component_name,
        )

        raw_ocp = component.get("ocpVersion")
        if not raw_ocp:
            raise ValueError(
                f"ocpVersion not found for component {component_name}."
                " This field should be attached by"
                " filter-published-fbc-images task"
            )
        ocp_versions: list[str] = [raw_ocp] if isinstance(raw_ocp, str) else raw_ocp

        logger.info(
            "Component supports %d OCP version(s): %s",
            len(ocp_versions),
            ocp_versions,
        )

        ocp_metadata: list[dict[str, str]] = []

        for ocp_version in ocp_versions:
            logger.info("Resolving indexes for %s...", ocp_version)

            updated_from_index = replace_ocp_version(raw_from_index, ocp_version)
            resolved_target_index = generate_target_index(
                ocp_version, raw_target_index, common_suffix
            )

            logger.info("  fromIndex: %s", updated_from_index)
            logger.info("  targetIndex: %s", resolved_target_index)

            validate_ocp_version(updated_from_index, ocp_version)
            if resolved_target_index:
                validate_ocp_version(resolved_target_index, ocp_version)
            logger.info("OCP version validation passed for %s", ocp_version)

            ocp_metadata.append(
                {
                    "version": ocp_version,
                    "updatedFromIndex": updated_from_index,
                    "targetIndex": resolved_target_index,
                }
            )

        component["ocpVersionMetadata"] = ocp_metadata
        logger.info(
            "Updated component with metadata for %d version(s)",
            len(ocp_metadata),
        )

    snapshot_path.write_text(json.dumps(snapshot, indent=2) + "\n", encoding="utf-8")
    logger.info(
        "Snapshot update completed — updated %d components",
        len(components),
    )


def main() -> int:
    """Read Tekton env and run the snapshot preparation."""
    data_dir = Path(tekton.require_env("PARAM_DATA_DIR"))
    snapshot_path = data_dir / tekton.require_env("PARAM_SNAPSHOT_PATH")
    data_path = data_dir / tekton.require_env("PARAM_DATA_PATH")

    run_prepare(snapshot_path=snapshot_path, data_path=data_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
