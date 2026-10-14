#!/usr/bin/env python3
"""Sign binary artifacts via the generic-signing internal pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import os
import shlex
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from release_service_utils.helpers import file, skopeo
from release_service_utils.helpers.internal_request import (
    InternalRequestWaitError,
    PIPELINERUN_UID_LABEL,
    TASK_GROUP_LABEL,
    create as create_internal_request,
)
from release_service_utils.helpers.kubectl import get_configmap
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.oras_utils import (
    FLAT_ARTIFACT_CONFIG_MEDIA_TYPE,
    copy_all_flat_artifact_files,
    copy_all_layered_image_files,
    oras_push,
)
from release_service_utils.tasks.managed.rh_direct_sign_image import (
    get_signing_keys,
    validate_file,
)

_INTENTION_LABEL = "internal-services.appstudio.openshift.io/intention"


@dataclass(frozen=True)
class SubmitConfig:
    """Configuration for submitting artifact signing requests."""

    pipeline: str
    requester: str
    kerberos_keytab_secret: str
    kerberos_keytab: str
    kerberos_principal: str
    signing_repo: str
    signing_revision: str
    service_account: str
    request_timeout: str
    task_id: str
    pipelinerun_uid: str
    concurrent_limit: int
    intention: str
    oci_storage: str
    oras_options: str
    sign_method: str
    ta_dockerconfig_secret: str


def setup_argparser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser."""
    parser = argparse.ArgumentParser(
        description="Sign binary artifacts via generic-signing pipeline."
    )
    parser.add_argument(
        "--snapshot",
        required=True,
        type=validate_file,
        help="Mapped snapshot JSON path",
    )
    parser.add_argument(
        "--data-file",
        required=True,
        type=validate_file,
        help="Merged data file JSON path",
    )
    parser.add_argument(
        "--requester",
        required=True,
        help="User who requested signing (onbehalfof)",
    )
    parser.add_argument(
        "--pipeline",
        default="generic-signing",
        help="Internal pipeline name for signing (default: %(default)s)",
    )
    parser.add_argument(
        "--service-account",
        default="signing-pipeline-sa",
        help="Service account for the signing pipeline (default: %(default)s)",
    )
    parser.add_argument(
        "--request-timeout",
        default="1800",
        help="InternalRequest timeout in seconds (default: %(default)s)",
    )
    parser.add_argument(
        "--task-id",
        default="",
        help="Task run UID used as a label on internal requests",
    )
    parser.add_argument(
        "--pipelinerun-uid",
        default="",
        help="Pipeline run UID used as a label on internal requests",
    )
    parser.add_argument(
        "--signing-repo",
        default="https://gitlab.cee.redhat.com/signing/signing.git",
        help="Git repository URL for signing tasks (default: %(default)s)",
    )
    parser.add_argument(
        "--signing-revision",
        default="main",
        help="Git revision in the signing repository (default: %(default)s)",
    )
    parser.add_argument(
        "--concurrent-limit",
        type=int,
        default=4,
        help="Maximum number of parallel signing requests (default: %(default)s)",
    )
    parser.add_argument(
        "--oci-storage",
        default="empty",
        help="OCI repository for Trusted Artifacts (default: %(default)s)",
    )
    parser.add_argument(
        "--oras-options",
        default="",
        help="ORAS options passed to the signing pipeline",
    )
    parser.add_argument(
        "--sign-method",
        default="detachsign",
        choices=["gpgsign", "detachsign"],
        help="Signing method (default: %(default)s)",
    )
    parser.add_argument(
        "--ta-dockerconfig-secret",
        default="quay-token-konflux-release-trusted-artifacts-secret",
        help="Secret for TA registry auth (default: %(default)s)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser


def pull_and_extract(container_image: str, work_dir: Path) -> Path:
    """Pull an OCI artifact and extract its file contents."""
    image_dir = work_dir / "image"
    image_dir.mkdir(parents=True, exist_ok=True)
    skopeo.copy(container_image, image_dir, authenticated=True, check=True)

    manifest = json.loads((image_dir / "manifest.json").read_text())
    config_media_type = manifest.get("config", {}).get("mediaType", "")

    extract_dir = work_dir / "extracted"
    extract_dir.mkdir(parents=True, exist_ok=True)

    if config_media_type == FLAT_ARTIFACT_CONFIG_MEDIA_TYPE:
        copy_all_flat_artifact_files(manifest, image_dir, extract_dir)
    else:
        copy_all_layered_image_files(manifest, image_dir, extract_dir)

    return extract_dir


def prepare_component(
    component_name: str,
    container_image: str,
    oci_storage: str,
    oras_extra_args: list[str] | None,
) -> str:
    """Pull, extract, and push a TA for one component."""
    with tempfile.TemporaryDirectory() as tmp:
        work_dir = Path(tmp)
        extract_dir = pull_and_extract(container_image, work_dir)

        files = sorted(f for f in extract_dir.rglob("*") if f.is_file())
        if not files:
            raise RuntimeError(
                f"No files extracted from '{component_name}'" f" ({container_image})"
            )

        logger.info(
            "Extracted %d file(s) from '%s'",
            len(files),
            component_name,
        )

        tag = f"{oci_storage}:{component_name}"
        digest = oras_push(
            tag,
            extract_dir,
            ".",
            component_name,
            extra_args=oras_extra_args,
        )
        ta_uri = f"oci://{oci_storage}@{digest}"
        logger.info("Pushed TA for '%s': %s", component_name, ta_uri)
        return ta_uri


def prepare_all_components(
    components: list[dict[str, Any]],
    oci_storage: str,
    oras_extra_args: list[str] | None,
    concurrent_limit: int,
) -> dict[str, str]:
    """Prepare TAs for all components concurrently."""
    logger.info(
        "Preparing %d component(s) with concurrent limit %d",
        len(components),
        concurrent_limit,
    )
    results: dict[str, str] = {}
    failures: list[Exception] = []

    with ThreadPoolExecutor(max_workers=concurrent_limit) as pool:
        futures = {
            pool.submit(
                prepare_component,
                comp.get("name", "unknown"),
                comp["containerImage"],
                oci_storage,
                oras_extra_args,
            ): comp.get("name", "unknown")
            for comp in components
        }
        for future in as_completed(futures):
            name = futures[future]
            exc = future.exception()
            if exc is not None:
                failures.append(exc)
                logger.error("Prepare failed for '%s'", name, exc_info=exc)
            else:
                results[name] = future.result()

    if failures:
        raise RuntimeError(f"{len(failures)} component(s) failed to prepare")

    return results


def submit_signing_request(
    component_name: str,
    source_data_artifact: str,
    signing_key: str,
    config: SubmitConfig,
) -> None:
    """Submit a single signing InternalRequest for one component and key."""
    params: dict[str, str] = {
        "sourceDataArtifact": source_data_artifact,
        "keyname": signing_key,
        "signMethod": config.sign_method,
        "onbehalfof": config.requester,
        "kerberos_keytab_secret": config.kerberos_keytab_secret,
        "kerberos_keytab": config.kerberos_keytab,
        "kerberos_principal": config.kerberos_principal,
        "taskGitUrl": config.signing_repo,
        "taskGitRevision": config.signing_revision,
        "ociStorage": config.oci_storage,
        "orasOptions": config.oras_options,
        "trusted_artifacts_dockerconfig_secret": config.ta_dockerconfig_secret,
    }

    labels = {
        TASK_GROUP_LABEL: config.task_id,
        PIPELINERUN_UID_LABEL: config.pipelinerun_uid,
        _INTENTION_LABEL: config.intention,
        "internal-services.appstudio.openshift.io/rate-limited": "true",
        "internal-services.appstudio.openshift.io/rate-limiting-group": ("signing-server"),
    }

    start = time.monotonic()
    try:
        ir_name = create_internal_request(
            config.pipeline,
            params=params,
            labels=labels,
            sync=True,
            timeout=int(config.request_timeout),
            service_account=config.service_account,
            cleanup=False,
        )
    except InternalRequestWaitError as exc:
        duration = time.monotonic() - start
        logger.error(
            "Signing failed for '%s', key '%s' after %.1fs: %s",
            component_name,
            signing_key,
            duration,
            exc,
        )
        raise RuntimeError(
            f"Signing failed for '{component_name}'," f" key '{signing_key}': {exc}"
        ) from exc

    duration = time.monotonic() - start
    logger.info(
        "Component '%s' key '%s' signed in %.1fs (InternalRequest: %s)",
        component_name,
        signing_key,
        duration,
        ir_name,
    )


def submit_all_signing_requests(
    prepared_artifacts: dict[str, str],
    signing_keys: list[str],
    config: SubmitConfig,
) -> None:
    """Submit signing requests for all component x key combinations."""
    work_items = [
        (name, ta_uri, key)
        for name, ta_uri in prepared_artifacts.items()
        for key in signing_keys
    ]
    logger.info(
        "Submitting %d signing request(s) (%d component(s) x %d key(s)) "
        "with concurrent limit %d",
        len(work_items),
        len(prepared_artifacts),
        len(signing_keys),
        config.concurrent_limit,
    )

    failures: list[Exception] = []
    with ThreadPoolExecutor(max_workers=config.concurrent_limit) as pool:
        futures = {
            pool.submit(submit_signing_request, name, ta_uri, key, config): (name, key)
            for name, ta_uri, key in work_items
        }
        for future in as_completed(futures):
            exc = future.exception()
            if exc is not None:
                failures.append(exc)

    succeeded = len(work_items) - len(failures)
    logger.info("Signing summary: %d succeeded, %d failed", succeeded, len(failures))
    if failures:
        for i, failure in enumerate(failures, 1):
            logger.error("Signing failure %d/%d: %s", i, len(failures), failure)
        raise RuntimeError(f"{len(failures)} signing request(s) failed")


def main() -> None:
    """Prepare TAs and submit signing requests."""
    parser = setup_argparser()
    args = parser.parse_args()
    logger.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    data_file = file.load_json_dict(args.data_file)
    snapshot = file.load_json_dict(args.snapshot)

    config_map_name = data_file.get("sign", {}).get("configMapName", "signing-config-map")
    configmap = get_configmap(config_map_name)
    signing_keys = get_signing_keys(configmap)
    logger.info("Signing keys: %s", signing_keys)

    cm_data = configmap["data"]
    config = SubmitConfig(
        pipeline=args.pipeline,
        requester=args.requester,
        kerberos_keytab_secret=cm_data["KERBEROS_KEYTAB_SECRET"],
        kerberos_keytab=cm_data["KERBEROS_KEYTAB"],
        kerberos_principal=cm_data["KERBEROS_PRINCIPAL"],
        signing_repo=args.signing_repo,
        signing_revision=args.signing_revision,
        service_account=args.service_account,
        request_timeout=args.request_timeout,
        task_id=args.task_id,
        pipelinerun_uid=args.pipelinerun_uid,
        concurrent_limit=args.concurrent_limit,
        intention=data_file.get("intention", "unknown"),
        oci_storage=args.oci_storage,
        oras_options=args.oras_options,
        sign_method=args.sign_method,
        ta_dockerconfig_secret=args.ta_dockerconfig_secret,
    )

    components = snapshot.get("components", [])
    logger.info("Found %d component(s) in snapshot", len(components))

    if not components:
        logger.info("No components to sign")
        return

    oras_env = os.environ.get("ORAS_OPTIONS", "")
    oras_extra_args = shlex.split(oras_env) if oras_env else None

    prepared = prepare_all_components(
        components,
        config.oci_storage,
        oras_extra_args,
        config.concurrent_limit,
    )

    submit_all_signing_requests(prepared, signing_keys, config)


if __name__ == "__main__":  # pragma: no cover
    main()
