#!/usr/bin/env python3
"""Validate container image labels against expected values from snapshot and data files.

Check that each image component's ``name`` label matches either its
``canonicalName`` or the repository URL, and that its ``cpe`` label matches
the ``releaseNotes.cpe`` value from the data file.

With ``--enforce true``, mismatches cause a non-zero exit. Without it, mismatches
are logged as warnings and the script exits successfully.
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any

from file import load_json_dict
from logger import logger

PROG = "check_labels.py"

IMAGE_MEDIA_TYPES = frozenset(
    {
        "application/vnd.oci.image.config.v1+json",
        "application/vnd.docker.container.image.v1+json",
    }
)


class LabelValidationError(Exception):
    """Raise when a label validation fails in enforce mode."""


def derive_name_from_url(url: str) -> str:
    """Derive the namespace/repo path from a container image URL.

    Strip the scheme (e.g. ``docker://``), the registry host (first path
    segment), any tag or digest suffix, and leading slashes.
    """
    m = re.match(r"(?:[^/]+://)?[^/]+/(?P<name>[^:@]+)", url.strip())
    return m.group("name") if m else ""


def get_label_value(component: dict[str, Any], label_name: str) -> str | None:
    """Extract a label value from a component's metadata labels list.

    Return ``None`` when the label is absent or has no value.
    """
    labels = component.get("metadata", {}).get("labels") or []
    for label in labels:
        if label.get("name") == label_name:
            val = label.get("value")
            if val is not None and str(val).strip():
                return str(val)
    return None


def is_image_media_type(component: dict[str, Any]) -> bool:
    """Return True if the component is a container image (OCI or Docker)."""
    media_type = component.get("metadata", {}).get("media_type")
    return media_type in IMAGE_MEDIA_TYPES


def _check_name_label(component: dict[str, Any], enforce: bool) -> None:
    """Validate the ``name`` label for a single component.

    Raise ``LabelValidationError`` on validation failures when ``enforce``
    is True, or on hard data errors (e.g. missing repository URL)
    regardless of enforce mode. When ``enforce`` is False, log a warning
    and return normally.
    """
    comp_name = component["name"]
    name_label = get_label_value(component, "name")

    if not name_label:
        msg = (
            f"Component '{comp_name}' is missing the required container "
            f"label 'name' in its metadata."
        )
        if enforce:
            raise LabelValidationError(msg)
        logger.warning(msg)
        return

    canonical_name = component.get("canonicalName") or ""
    repos = component.get("repositories") or []

    if len(repos) > 1 and not canonical_name:
        msg = (
            f"Component '{comp_name}' has multiple repositories, but is "
            f"missing the component-level 'canonicalName'."
        )
        if enforce:
            raise LabelValidationError(msg)
        logger.warning(msg)
        return

    if canonical_name:
        expected_name = canonical_name
    else:
        url = (repos[0].get("rh-registry-repo") or "").strip() if repos else ""
        if not url:
            raise LabelValidationError(
                f"Component '{comp_name}' repositories[0].\"rh-registry-repo\" is missing"
            )
        expected_name = derive_name_from_url(url)

    if name_label != expected_name:
        msg = (
            f"Component '{comp_name}' name label ('{name_label}') does not "
            f"match expected name ('{expected_name}')"
        )
        if enforce:
            raise LabelValidationError(msg)
        logger.warning(msg)


def _check_cpe_label(component: dict[str, Any], cpe_data: str, enforce: bool) -> None:
    """Validate the ``cpe`` label for a single component."""
    comp_name = component["name"]
    cpe_label = get_label_value(component, "cpe")

    if not cpe_label:
        logger.info(
            "Component '%s' is missing the 'cpe' label. Skipping enforcement.",
            comp_name,
        )
        return

    if cpe_label != cpe_data:
        msg = (
            f"Component '{comp_name}' 'cpe' label ('{cpe_label}') does not "
            f"match the single required CPE value from the data file "
            f"('{cpe_data}')."
        )
        if enforce:
            raise LabelValidationError(msg)
        logger.warning(msg)
        return

    logger.info("Component '%s' 'cpe' label matches the data file value.", comp_name)


def check_labels(snapshot_path: Path, data_path: Path, enforce: bool) -> None:
    """Validate name and CPE labels for all image components.

    Raise ``LabelValidationError`` on validation failure (enforce mode) or
    on hard data errors.
    """
    snapshot = load_json_dict(snapshot_path)
    data = load_json_dict(data_path)

    cpe_data = (data.get("releaseNotes") or {}).get("cpe") or ""
    if not cpe_data:
        raise LabelValidationError(
            "The required static value 'releaseNotes.cpe' is missing or "
            "empty in the data file."
        )

    components = snapshot.get("components") or []

    for component in components:
        comp_name = component.get("name")
        if not comp_name:
            raise LabelValidationError("A component is missing a 'name' field")

        if not is_image_media_type(component):
            media_type = component.get("metadata", {}).get("media_type", "null")
            logger.info(
                "Skipping check for artifact '%s' of type '%s'",
                comp_name,
                media_type,
            )
            continue

        _check_name_label(component, enforce)
        _check_cpe_label(component, cpe_data, enforce)


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Parse CLI arguments for the check-labels script.

    Accept ``--snapshot-file``, ``--data-file`` (required), and an optional
    ``--enforce`` flag.
    """
    p = argparse.ArgumentParser(prog=PROG, description=__doc__)
    p.add_argument(
        "--snapshot-file",
        required=True,
        help="Path to the mapped snapshot JSON file",
    )
    p.add_argument(
        "--data-file",
        required=True,
        help="Path to the merged data JSON file",
    )
    p.add_argument(
        "--enforce",
        type=lambda s: s.strip().lower() == "true",
        default=False,
        help="Set to 'true' to treat validation failures as errors (default: 'false')",
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run the label checks."""
    args = parse_args(argv[1:] if argv is not None else None)
    check_labels(Path(args.snapshot_file), Path(args.data_file), args.enforce)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
