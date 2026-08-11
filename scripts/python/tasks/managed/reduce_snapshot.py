#!/usr/bin/env python3
"""Reduce a Snapshot to a single component based on CR labels.

When single-component mode is enabled, fetch the custom resource's labels
to determine which component was built, then filter the Snapshot JSON to
retain only that component. If the mode is disabled, copy the Snapshot
through unchanged.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import file
import tekton
from get_resource import get_resource
from logger import logger

_LABEL_TYPE = "test.appstudio.openshift.io/type"
_LABEL_COMPONENT = "appstudio.openshift.io/component"
_SA_NAMESPACE_PATH = Path("/var/run/secrets/kubernetes.io/serviceaccount/namespace")


def resolve_namespace(custom_resource_namespace: str) -> str:
    """Return the namespace to use for the custom resource lookup.

    If the caller provided a non-empty namespace, return it directly.
    Otherwise fall back to the pod's service-account namespace file.
    """
    if custom_resource_namespace:
        return custom_resource_namespace
    try:
        return _SA_NAMESPACE_PATH.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ValueError(
            f"CUSTOM_RESOURCE_NAMESPACE is empty and cannot read {_SA_NAMESPACE_PATH}: {exc}"
        ) from exc


def get_cr_labels(resource_type: str, namespace: str, name: str) -> dict[str, str]:
    """Fetch metadata.labels from a Kubernetes custom resource."""
    raw = get_resource(resource_type, namespace, name, "{.metadata.labels}")
    if not raw or raw == "{}":
        return {}
    return json.loads(raw)


def validate_labels(labels: dict[str, str]) -> str:
    """Validate single-component labels and return the component name.

    Raises ValueError when the required labels are missing or invalid.
    """
    creation_type = labels.get(_LABEL_TYPE, "")
    component_name = labels.get(_LABEL_COMPONENT, "")

    logger.info("SNAPSHOT_CREATION_TYPE: %s", creation_type)
    logger.info("SNAPSHOT_CREATION_COMPONENT: %s", component_name)

    if creation_type != "component" or not component_name:
        raise ValueError(
            "Single component mode is enabled, but the snapshot is missing "
            "the required labels to use it. "
            "This is likely due to a manually created snapshot. "
            f"The {_LABEL_TYPE} label must exist with value 'component'. "
            f"The {_LABEL_COMPONENT} label must also exist saying which "
            "component to use."
        )
    return component_name


def reduce_snapshot(snapshot: dict[str, object], component_name: str) -> dict[str, object]:
    """Filter snapshot components, keeping only *component_name*.

    Returns the reduced snapshot dict if exactly one component remains,
    or the original snapshot if the component was not found.
    """
    components = snapshot.get("components", [])
    if not isinstance(components, list):
        return snapshot

    filtered = [
        c for c in components if isinstance(c, dict) and c.get("name") == component_name
    ]

    if len(filtered) == 1:
        reduced = dict(snapshot)
        reduced["components"] = filtered
        return reduced

    logger.warning(
        "Reduced Snapshot has %d components (expected 1). "
        "Verify that the Snapshot contains the built component: %s. "
        "Using original Snapshot.",
        len(filtered),
        component_name,
    )
    return snapshot


def run(
    *,
    single_component: str,
    custom_resource: str,
    custom_resource_namespace: str,
    snapshot_path: Path,
    snapshot_output_path: Path,
) -> None:
    """Execute the reduce-snapshot workflow."""
    snapshot = file.load_json_dict(snapshot_path)

    if single_component != "true":
        logger.info("Single component mode is not enabled, skipping reduction")
    else:
        resource_type, _, resource_name = custom_resource.partition("/")
        if not resource_type or not resource_name:
            raise ValueError(
                f"CUSTOM_RESOURCE must be in 'type/name' format, got: {custom_resource!r}"
            )

        namespace = resolve_namespace(custom_resource_namespace)

        labels = get_cr_labels(resource_type, namespace, resource_name)
        component_name = validate_labels(labels)

        logger.info("Single Component mode is true and Snapshot type is component")

        snapshot = reduce_snapshot(snapshot, component_name)

    snapshot_output_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    logger.info("Wrote snapshot to %s", snapshot_output_path)


def main() -> int:
    """Read Tekton env vars and run the reduce-snapshot workflow."""
    single_component = os.environ.get("SINGLE_COMPONENT", "").strip()
    custom_resource = os.environ.get("CUSTOM_RESOURCE", "").strip()
    custom_resource_namespace = os.environ.get("CUSTOM_RESOURCE_NAMESPACE", "").strip()
    snapshot = tekton.require_env("SNAPSHOT")
    snapshot_path_str = tekton.require_env("SNAPSHOT_PATH")

    run(
        single_component=single_component,
        custom_resource=custom_resource,
        custom_resource_namespace=custom_resource_namespace,
        snapshot_path=Path(snapshot),
        snapshot_output_path=Path(snapshot_path_str),
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
