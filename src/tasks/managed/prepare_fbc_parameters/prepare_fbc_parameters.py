#!/usr/bin/env python3
"""Prepare FBC parameters with validation and strategy-aware publishing decisions."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from release_service_utils.helpers import file, internal_request, tekton
from release_service_utils.helpers.internal_request import (
    SPAWN_OVERHEAD_SECONDS,
    seconds_to_duration,
)
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.subprocess_cmd import run_cmd

TASK_LABEL = "internal-services.appstudio.openshift.io/group-id"
PIPELINERUN_LABEL = "internal-services.appstudio.openshift.io/pipelinerun-uid"

PYXIS_STAGE_URL = "https://pyxis.stage.engineering.redhat.com/v1"


def detect_release_mode(data: dict[str, Any]) -> str:
    """Detect release mode from FBC data, enforcing mutual exclusivity.

    Returns one of: ``"hotfix"``, ``"preGA"``, ``"stagedIndex"``,
    or ``"standard"``.
    """
    fbc = data.get("fbc", {})
    modes = {
        "hotfix": fbc.get("hotfix", False) is True,
        "preGA": fbc.get("preGA", False) is True,
        "stagedIndex": fbc.get("stagedIndex", False) is True,
    }
    active = [name for name, enabled in modes.items() if enabled]
    if len(active) > 1:
        raise ValueError(
            "Multiple release modes cannot be active simultaneously: " + ", ".join(active)
        )
    return active[0] if active else "standard"


def render_fbc_fragment(
    fbc_fragment: str,
    *,
    run: Any = None,
) -> list[dict[str, Any]]:
    """Run ``opm render`` on *fbc_fragment* and return parsed catalog entries."""
    runner = run or run_cmd
    logger.info("Rendering FBC fragment: %s", fbc_fragment)
    result = runner(
        ["opm", "render", fbc_fragment],
        check=True,
    )
    entries: list[dict[str, Any]] = []
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if line:
            entries.append(json.loads(line))
    logger.info("Rendered %d catalog entries from %s", len(entries), fbc_fragment)
    return entries


def extract_packages(catalog_entries: list[dict[str, Any]]) -> list[str]:
    """Return sorted unique package names from catalog entries."""
    return sorted(
        {
            e["name"]
            for e in catalog_entries
            if e.get("schema") == "olm.package" and "name" in e
        }
    )


def extract_bundle_images(
    catalog_entries: list[dict[str, Any]],
) -> list[str]:
    """Return sorted unique bundle image repos (digest stripped)."""
    images: set[str] = set()
    for e in catalog_entries:
        if e.get("schema") == "olm.bundle":
            ref = e.get("image", "")
            images.add(ref.split("@")[0])
    if not images:
        logger.warning("No olm.bundle entries found in catalog; bundle image list is empty")
    return sorted(images)


def validate_allowed_packages(
    packages: list[str],
    allowed: list[str],
) -> list[str]:
    """Return disallowed packages (empty list means all valid)."""
    allowed_set = set(allowed)
    return [p for p in packages if p not in allowed_set]


def validate_no_duplicate_packages(
    ocp_to_components: dict[str, list[int]],
    component_packages: dict[int, list[str]],
    snapshot: dict[str, Any],
) -> list[str]:
    """Check for duplicate packages within the same OCP version.

    Returns a list of error messages (empty means valid).
    """
    errors: list[str] = []
    components = snapshot.get("components", [])
    for ocp_ver, indices in ocp_to_components.items():
        if len(indices) <= 1:
            continue
        pkg_to_component: dict[str, int] = {}
        for idx in indices:
            for pkg in component_packages.get(idx, []):
                logger.debug(
                    "Checking duplicates for package '%s' of component %d"
                    " in OCP version %s",
                    pkg,
                    idx,
                    ocp_ver,
                )
                if pkg in pkg_to_component:
                    first_idx = pkg_to_component[pkg]
                    first_name = (
                        components[first_idx].get("name", str(first_idx))
                        if first_idx < len(components)
                        else str(first_idx)
                    )
                    current_name = (
                        components[idx].get("name", str(idx))
                        if idx < len(components)
                        else str(idx)
                    )
                    msg = (
                        f"Duplicate package '{pkg}' in OCP version"
                        f" {ocp_ver}: component {first_idx}"
                        f" ({first_name}) and component {idx}"
                        f" ({current_name})"
                    )
                    logger.error(msg)
                    errors.append(msg)
                else:
                    pkg_to_component[pkg] = idx
    return errors


def aggregate_opt_in(opt_in_results: list[dict[str, Any]]) -> bool:
    """Return True only when every result has fbcOptIn explicitly set to True."""
    if not opt_in_results:
        return False
    return all(r.get("fbcOptIn") is True for r in opt_in_results)


def compute_publishing_decisions(mode: str, opt_in: bool) -> tuple[bool, bool, bool]:
    """Return (must_publish, must_sign, must_overwrite) for the mode."""
    if mode == "stagedIndex":
        return (False, False, False)
    if mode in ("hotfix", "preGA"):
        return (True, True, False)
    return (opt_in, opt_in, opt_in)


def select_iib_service_account(staged: bool) -> str:
    """Return IIB service account name based on environment."""
    if staged:
        return "iib-service-account-stage"
    return "iib-service-account-prod"


def fetch_ir_opt_in_results(
    ir_name: str,
    *,
    run: Any = None,
) -> list[dict[str, Any]]:
    """Fetch optInResults from a completed InternalRequest."""
    runner = run or run_cmd
    result = runner(
        [
            "kubectl",
            "get",
            "internalrequest",
            ir_name,
            "-o",
            "json",
        ],
        check=True,
    )
    ir_data = json.loads(result.stdout)
    results_raw = ir_data.get("status", {}).get("results", {})
    opt_in_raw = results_raw.get("optInResults")
    if not opt_in_raw:
        raise tekton.CheckStepError(
            "fetching opt-in results",
            ValueError(f"InternalRequest {ir_name} returned empty optInResults"),
        )
    return json.loads(opt_in_raw)


def check_fbc_opt_in(
    bundle_images: list[str],
    iib_service_account_secret: str,
    pyxis_server: str,
    task_git_url: str,
    task_git_revision: str,
    pipeline_run_uid: str,
    task_run_uid: str,
    *,
    timeout: int = 3600,
    create_ir: Any = None,
    run: Any = None,
) -> list[dict[str, Any]]:
    """Create check-fbc-opt-in InternalRequest and return results."""
    ir_create = create_ir or internal_request.create
    params: dict[str, str] = {
        "containerImages": json.dumps(bundle_images),
        "iibServiceAccountSecret": iib_service_account_secret,
        "pyxisServer": pyxis_server,
        "taskGitUrl": task_git_url,
        "taskGitRevision": task_git_revision,
    }
    if pyxis_server == "stage":
        params["pyxisUrl"] = PYXIS_STAGE_URL

    labels = {
        TASK_LABEL: task_run_uid,
        PIPELINERUN_LABEL: pipeline_run_uid,
    }

    pipeline_timeout = seconds_to_duration(timeout + SPAWN_OVERHEAD_SECONDS)
    task_timeout = seconds_to_duration(timeout)
    wait_timeout = timeout + SPAWN_OVERHEAD_SECONDS

    ir_name = ir_create(
        "check-fbc-opt-in",
        params=params,
        labels=labels,
        sync=True,
        timeout=wait_timeout,
        pipeline_timeout=pipeline_timeout,
        task_timeout=task_timeout,
    )
    logger.info("InternalRequest '%s' completed.", ir_name)
    return fetch_ir_opt_in_results(ir_name, run=run)


def run_prepare(
    snapshot_path: Path,
    data_path: Path,
    *,
    pyxis_server: str = "production",
    task_git_url: str,
    task_git_revision: str,
    pipeline_run_uid: str,
    task_run_uid: str = "",
    render: Any = None,
    create_ir: Any = None,
    run: Any = None,
) -> dict[str, str]:
    """Orchestrate all validation phases and return Tekton results."""
    render_fn = render or render_fbc_fragment
    snapshot = file.load_json_dict(snapshot_path)
    data = file.load_json_dict(data_path)

    mode = detect_release_mode(data)
    logger.info("Release mode: %s", mode)

    fbc_data = data.get("fbc", {})
    request_timeout: int = fbc_data.get("requestTimeoutSeconds", 3600)

    components = snapshot.get("components", [])
    if not components:
        raise tekton.CheckStepError(
            "reading snapshot components",
            ValueError("No components found in snapshot"),
        )

    allowed_packages = fbc_data.get("allowedPackages", [])
    validation_failed = False
    all_bundle_images: list[str] = []
    component_packages: dict[int, list[str]] = {}
    ocp_to_components: dict[str, list[int]] = {}

    for i, comp in enumerate(components):
        fbc_fragment = comp.get("containerImage", "")
        logger.info(
            "Processing component %d/%d: %s",
            i + 1,
            len(components),
            fbc_fragment,
        )

        catalog = render_fn(fbc_fragment)
        packages = extract_packages(catalog)
        bundle_images = extract_bundle_images(catalog)
        all_bundle_images.extend(bundle_images)
        component_packages[i] = packages

        disallowed = validate_allowed_packages(packages, allowed_packages)
        if disallowed:
            logger.error(
                "Component %d has disallowed packages: %s",
                i,
                ", ".join(disallowed),
            )
            validation_failed = True

        ocp_versions = comp.get("ocpVersion")
        if not ocp_versions:
            logger.error("ocpVersion not found for component %d", i)
            validation_failed = True
        else:
            for ver in ocp_versions:
                ocp_to_components.setdefault(ver, []).append(i)

    all_bundle_images = sorted(set(all_bundle_images))

    dup_errors = validate_no_duplicate_packages(
        ocp_to_components,
        component_packages,
        snapshot,
    )
    if dup_errors:
        for err in dup_errors:
            logger.error("%s", err)
        validation_failed = True

    if validation_failed:
        raise tekton.CheckStepError(
            "validating FBC parameters",
            ValueError("Validation failed"),
        )

    iib_sa = select_iib_service_account(mode == "stagedIndex")
    logger.info("IIB service account: %s", iib_sa)

    opt_in_results = check_fbc_opt_in(
        all_bundle_images,
        iib_sa,
        pyxis_server,
        task_git_url,
        task_git_revision,
        pipeline_run_uid,
        task_run_uid,
        timeout=request_timeout,
        create_ir=create_ir,
        run=run,
    )
    logger.info("Opt-in results: %s", json.dumps(opt_in_results))

    unified_opt_in = aggregate_opt_in(opt_in_results)
    logger.info("Unified opt-in: %s", unified_opt_in)

    must_publish, must_sign, must_overwrite = compute_publishing_decisions(
        mode,
        unified_opt_in,
    )

    return {
        "fbcOptIn": str(unified_opt_in).lower(),
        "validationPassed": "true",
        "mustPublishIndexImage": str(must_publish).lower(),
        "mustSignIndexImage": str(must_sign).lower(),
        "mustOverwriteFromIndexImage": str(must_overwrite).lower(),
        "iibServiceAccountSecret": iib_sa,
    }


def main() -> int:
    """Read env vars, run preparation, write Tekton results."""
    (
        fbc_opt_in_path,
        validation_passed_path,
        must_publish_path,
        must_sign_path,
        must_overwrite_path,
        iib_sa_path,
    ) = tekton.result_paths_from_env(
        "RESULT_FBC_OPT_IN",
        "RESULT_VALIDATION_PASSED",
        "RESULT_MUST_PUBLISH_INDEX_IMAGE",
        "RESULT_MUST_SIGN_INDEX_IMAGE",
        "RESULT_MUST_OVERWRITE_FROM_INDEX_IMAGE",
        "RESULT_IIB_SERVICE_ACCOUNT_SECRET",
    )

    data_dir = os.environ.get("PARAM_DATA_DIR", "/var/workdir/release")
    snapshot_path = Path(data_dir) / os.environ.get("PARAM_SNAPSHOT_PATH", "")
    data_path = Path(data_dir) / os.environ.get("PARAM_DATA_PATH", "")
    pyxis_server = os.environ.get("PARAM_PYXIS_SERVER", "production")
    task_git_url = os.environ.get("TASK_GIT_URL", "")
    task_git_revision = os.environ.get("TASK_GIT_REVISION", "")
    pipeline_run_uid = os.environ.get("PIPELINE_RUN_UID", "")
    task_run_uid = os.environ.get("TASK_RUN_UID", "")

    if not task_git_url:
        raise tekton.CheckStepError(
            "reading configuration",
            ValueError("TASK_GIT_URL must be set"),
        )
    if not task_git_revision:
        raise tekton.CheckStepError(
            "reading configuration",
            ValueError("TASK_GIT_REVISION must be set"),
        )

    results = run_prepare(
        snapshot_path,
        data_path,
        pyxis_server=pyxis_server,
        task_git_url=task_git_url,
        task_git_revision=task_git_revision,
        pipeline_run_uid=pipeline_run_uid,
        task_run_uid=task_run_uid,
    )

    fbc_opt_in_path.write_text(
        results["fbcOptIn"],
        encoding="utf-8",
    )
    validation_passed_path.write_text(
        results["validationPassed"],
        encoding="utf-8",
    )
    must_publish_path.write_text(
        results["mustPublishIndexImage"],
        encoding="utf-8",
    )
    must_sign_path.write_text(
        results["mustSignIndexImage"],
        encoding="utf-8",
    )
    must_overwrite_path.write_text(
        results["mustOverwriteFromIndexImage"],
        encoding="utf-8",
    )
    iib_sa_path.write_text(
        results["iibServiceAccountSecret"],
        encoding="utf-8",
    )

    logger.info("FBC parameter preparation completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
