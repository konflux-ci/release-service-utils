#!/usr/bin/env python3
"""Verify access to Release pipeline resources via kubectl auth can-i."""

from __future__ import annotations

import os

import kubectl
import tekton
from logger import logger


def parse_namespaced_resource(value: str) -> tuple[str, str]:
    """Split a 'namespace/name' string into its components."""
    parts = value.split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Expected 'namespace/name', got '{value}'")
    return parts[0], parts[1]


def run(
    release: str,
    release_plan: str,
    release_plan_admission: str,
    release_service_config: str,
    snapshot: str,
    require_internal_services: bool,
) -> None:
    """Verify that the service account can access all required resources."""
    origin_ns, release_name = parse_namespaced_resource(release)
    release_plan_ns, release_plan_name = parse_namespaced_resource(release_plan)
    target_ns, rpa_name = parse_namespaced_resource(release_plan_admission)
    rsc_ns, rsc_name = parse_namespaced_resource(release_service_config)
    snapshot_ns, snapshot_name = parse_namespaced_resource(snapshot)

    if release_plan_ns != origin_ns:
        raise ValueError(
            f"ReleasePlan namespace '{release_plan_ns}' "
            f"does not match Release namespace '{origin_ns}'"
        )
    if snapshot_ns != origin_ns:
        raise ValueError(
            f"Snapshot namespace '{snapshot_ns}' "
            f"does not match Release namespace '{origin_ns}'"
        )

    checks = [
        ("CAN_I_READ_RELEASES", "get", "release", release_name, origin_ns),
        (
            "CAN_I_READ_RELEASEPLANS",
            "get",
            "releaseplan",
            release_plan_name,
            origin_ns,
        ),
        (
            "CAN_I_READ_RELEASEPLANADMISSIONS",
            "get",
            "releaseplanadmission",
            rpa_name,
            target_ns,
        ),
        (
            "CAN_I_READ_RELEASESERVICECONFIG",
            "get",
            "releaseserviceconfig",
            rsc_name,
            rsc_ns,
        ),
        ("CAN_I_READ_SNAPSHOTS", "get", "snapshot", snapshot_name, origin_ns),
    ]

    results: dict[str, bool] = {}
    for label, verb, resource, name, namespace in checks:
        allowed = kubectl.auth_can_i(verb, resource, name=name, namespace=namespace)
        results[label] = allowed
        logger.info("%s? %s", label, "yes" if allowed else "no")

    if require_internal_services:
        allowed = kubectl.auth_can_i("create", "internalrequest", namespace=target_ns)
        results["CAN_I_CREATE_INTERNALREQUESTS"] = allowed
        logger.info(
            "CAN_I_CREATE_INTERNALREQUESTS? %s",
            "yes" if allowed else "no",
        )
    else:
        logger.info("CAN_I_CREATE_INTERNALREQUESTS? skipped")

    denied = [label for label, allowed in results.items() if not allowed]
    if denied:
        denied_str = ", ".join(sorted(denied))
        raise RuntimeError(
            "Cannot read or create required Release resources!\n"
            f"Denied permission checks: {denied_str}\n"
            "This indicates that your workspace is not correctly set up.\n"
            "Please reach out to a workspace administrator."
        )

    logger.info("Access to Release resources verified")


def main() -> int:
    """Read Tekton params from environment and verify resource access."""
    release = tekton.require_env("PARAM_RELEASE")
    release_plan = tekton.require_env("PARAM_RELEASE_PLAN")
    release_plan_admission = tekton.require_env("PARAM_RELEASE_PLAN_ADMISSION")
    release_service_config = tekton.require_env("PARAM_RELEASE_SERVICE_CONFIG")
    snapshot = tekton.require_env("PARAM_SNAPSHOT")
    require_internal_services = (
        os.environ.get("PARAM_REQUIRE_INTERNAL_SERVICES", "false").lower() == "true"
    )

    run(
        release=release,
        release_plan=release_plan,
        release_plan_admission=release_plan_admission,
        release_service_config=release_service_config,
        snapshot=snapshot,
        require_internal_services=require_internal_services,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
