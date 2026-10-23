#!/usr/bin/env python3
"""Download image SBOMs and upload their RPM manifests and content sets to Pyxis."""

from __future__ import annotations

import base64
import json
import os
import string
import subprocess
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

from release_service_utils.helpers import (
    authentication,
    cosign,
    file,
    image_ref,
    memory_throttle,
    pyxis_api,
    subprocess_cmd,
    tekton,
)
from release_service_utils.helpers.logger import logger

MEMORY_THRESHOLD = 80
BURST_SIZE = 5
STABILIZATION_DELAY = 2.0
SBOM_DIR = Path("/var/workdir/downloaded-sboms")


@dataclass(frozen=True)
class ImageJob:
    """Describe one Pyxis image and the source image that supplies its SBOM."""

    image_id: str
    container_image: str
    platform: str | None
    arch_digest: str


def _image_job(component: dict[str, Any], image: dict[str, Any]) -> ImageJob:
    """Build and validate an image job from one component/Pyxis-image pair."""
    try:
        image_id = str(image["imageId"])
        container_image = str(component["containerImage"])
        digest = str(image["digest"])
        arch_digest = str(image["arch_digest"])
    except KeyError as exc:
        raise ValueError(f"Missing required Pyxis image field: {exc.args[0]}") from exc

    if not image_id or not all(character in string.hexdigits for character in image_id):
        raise ValueError(
            f"imageId is invalid, a non-empty hexadecimal value is expected: {image_id!r}"
        )

    platform: str | None = None
    if digest != arch_digest:
        os_name = str(image.get("os") or "")
        architecture = str(image.get("arch") or "")
        if not os_name or not architecture:
            raise ValueError(
                f"Image {image_id} is multi-architecture but has no complete platform"
            )
        platform = f"{os_name}/{architecture}"

    return ImageJob(
        image_id=image_id,
        container_image=container_image,
        platform=platform,
        arch_digest=arch_digest,
    )


def collect_image_jobs(pyxis_data: dict[str, Any]) -> list[ImageJob]:
    """Flatten all Pyxis images while retaining their component image reference."""
    components = pyxis_data.get("components", [])
    if not isinstance(components, list):
        raise TypeError("Pyxis data field 'components' must be an array")

    jobs: list[ImageJob] = []
    for component in components:
        if not isinstance(component, dict):
            raise TypeError("Each Pyxis component must be an object")
        images = component.get("pyxisImages", [])
        if images is None:
            images = []
        if not isinstance(images, list):
            raise TypeError("Pyxis component field 'pyxisImages' must be an array")
        for image in images:
            if not isinstance(image, dict):
                raise TypeError("Each Pyxis image must be an object")
            jobs.append(_image_job(component, image))
    return jobs


def unique_download_jobs(jobs: list[ImageJob]) -> list[ImageJob]:
    """Return the first job for each image ID, preserving input order."""
    unique: dict[str, ImageJob] = {}
    for job in jobs:
        unique.setdefault(job.image_id, job)
    return list(unique.values())


def _log_process_output(
    result: subprocess.CompletedProcess[str] | subprocess.CalledProcessError,
    *,
    include_stdout: bool = True,
) -> None:
    """Write captured command output to the task log.

    ``select-oci-auth`` stdout contains registry credentials, so callers can
    suppress stdout while retaining its diagnostic stderr.
    """
    if include_stdout and result.stdout and result.stdout.strip():
        logger.info(result.stdout.rstrip())
    if result.stderr and result.stderr.strip():
        logger.info(result.stderr.rstrip())


def _ecp_public_key(policy_json: str | None) -> str:
    """Return the ``publicKey`` of an EnterpriseContractPolicy resource or spec."""
    no_policy = (
        "No EnterpriseContractPolicy provided; it is needed for the referrers API "
        "SPDX attestation fallback"
    )
    if policy_json is None or not policy_json.strip():
        raise ValueError(no_policy)
    try:
        policy = json.loads(policy_json)
    except json.JSONDecodeError as exc:
        raise ValueError("EnterpriseContractPolicy is not valid JSON") from exc
    if not policy:
        raise ValueError(no_policy)
    if not isinstance(policy, dict):
        raise ValueError("EnterpriseContractPolicy must be a JSON object")
    spec = policy.get("spec", policy)
    if not isinstance(spec, dict):
        raise ValueError("EnterpriseContractPolicy 'spec' must be a JSON object")
    key = spec.get("publicKey")
    if not isinstance(key, str) or not key:
        raise ValueError(
            "Provided EnterpriseContractPolicy has no publicKey to verify the SPDX "
            "attestation signature against (needed for referrers API SPDX fallback)"
        )
    return key


def _statement(line: str, context: str) -> dict[str, Any]:
    """Decode one verified envelope or bundle *line* into its in-toto statement.

    Keep the intermediate copies local so they are freed on return.
    """
    try:
        attestation = json.loads(line)
        envelope = attestation.get("dsseEnvelope", attestation)
        statement = json.loads(base64.b64decode(envelope["payload"]))
    except (ValueError, KeyError, TypeError, AttributeError) as exc:
        raise ValueError(f"Malformed attestation envelope for {context}: {exc!r}") from exc
    if not isinstance(statement, dict):
        raise ValueError(f"Attestation payload for {context} is not a JSON object")
    return statement


def _write_spdx_from_attestations(
    verified_attestations: str, reference: str, arch_digest: str, output_path: Path
) -> None:
    """Write the SPDX predicate whose subject matches *arch_digest* to *output_path*.

    *verified_attestations* contains DSSE envelopes or Cosign v3 bundles, one per
    line. Decode lines individually and stream the predicate to disk without
    building another JSON string.
    """
    algorithm, _, digest = arch_digest.partition(":")
    for index, line in enumerate(verified_attestations.splitlines(), start=1):
        if not line.strip():
            continue
        statement = _statement(line, f"{reference} (line {index})")
        if any(
            subject.get("digest", {}).get(algorithm) == digest
            for subject in statement.get("subject", [])
        ):
            predicate = statement.get("predicate")
            if predicate is None:
                raise ValueError(f"SPDX attestation for {reference} has no predicate")
            with output_path.open("w", encoding="utf-8") as handle:
                json.dump(predicate, handle)
            return
    raise ValueError(f"No SPDX attestation found for {arch_digest}")


def download_sbom(job: ImageJob, retries: int, enterprise_contract_policy: str | None) -> None:
    """Download a legacy SBOM or a verified SPDX attestation via OCI referrers."""
    output_path = SBOM_DIR / f"{job.image_id}.json"
    try:
        cosign.download(
            "sbom",
            job.container_image,
            output_path,
            max_attempts=retries + 1,
            platform=job.platform,
        )
    except cosign.SbomNotFoundError as exc:
        logger.info("No legacy SBOM for image %s; trying OCI referrers", job.image_id)
        reference = f"{image_ref.repository(job.container_image)}@{job.arch_digest}"
        try:
            verified_attestations = cosign.verify_spdx_attestation(
                reference,
                key=_ecp_public_key(enterprise_contract_policy),
                max_attempts=retries + 1,
            )
            _write_spdx_from_attestations(
                verified_attestations, reference, job.arch_digest, output_path
            )
        except (RuntimeError, ValueError, OSError) as fallback_exc:
            raise RuntimeError(
                f"Legacy SBOM download for {job.container_image} failed "
                f"({exc}) and the SPDX attestation "
                f"fallback failed too: {fallback_exc}"
            ) from fallback_exc


def upload_rpm_data(job: ImageJob) -> None:
    """Validate one SPDX SBOM and upload its RPM data to Pyxis."""
    sbom_path = SBOM_DIR / f"{job.image_id}.json"
    if not sbom_path.is_file():
        raise RuntimeError(f"SBOM file {sbom_path} not found for image {job.image_id}")

    sbom = file.load_json_dict(sbom_path)
    if not sbom.get("spdxVersion"):
        raise ValueError(f"{sbom_path}: not a valid SPDX SBOM")

    try:
        result = subprocess_cmd.run_cmd(
            [
                "upload_rpm_data",
                "--retry",
                "--image-id",
                job.image_id,
                "--sbom-path",
                str(sbom_path),
                "--verbose",
            ],
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        _log_process_output(exc)
        raise
    _log_process_output(result)


def _run_phase(
    jobs: list[ImageJob],
    operation: Callable[[ImageJob], None],
    concurrent_limit: int,
    phase_name: str,
) -> None:
    """Run one concurrent phase and raise after collecting every job failure."""
    futures: dict[Future[None], ImageJob] = {}
    with ThreadPoolExecutor(max_workers=concurrent_limit) as executor:
        for index, job in enumerate(jobs, start=1):
            memory_throttle.wait_for_memory(MEMORY_THRESHOLD)
            futures[executor.submit(operation, job)] = job
            if index % BURST_SIZE == 0:
                time.sleep(STABILIZATION_DELAY)

        failures: list[str] = []
        for future in as_completed(futures):
            job = futures[future]
            try:
                future.result()
            except Exception as exc:
                failures.append(f"{job.image_id}: {exc}")
                logger.error(
                    "%s failed for image %s: %s",
                    phase_name,
                    job.image_id,
                    exc,
                    exc_info=True,
                )

    if failures:
        details = "\n".join(failures)
        raise RuntimeError(f"One or more {phase_name} jobs failed:\n{details}")


def run(
    pyxis_file: Path,
    concurrent_limit: int,
    retries: int,
    *,
    enterprise_contract_policy: str | None,
) -> None:
    """Download unique SBOMs and upload RPM data for every listed Pyxis image.

    Raise ``RuntimeError`` when the Pyxis file contains no images.
    """
    if concurrent_limit < 1:
        raise ValueError("CONCURRENT_LIMIT must be at least 1")
    if retries < 0:
        raise ValueError("RETRIES must not be negative")

    jobs = collect_image_jobs(file.load_json_dict(pyxis_file))
    if not jobs:
        raise RuntimeError("No Pyxis images found")

    SBOM_DIR.mkdir(parents=True, exist_ok=True)
    download_jobs = unique_download_jobs(jobs)
    logger.info(
        "Downloading SBOMs for %d unique images with concurrency %d",
        len(download_jobs),
        concurrent_limit,
    )
    memory_throttle.log_memory_throttle_status(MEMORY_THRESHOLD)
    _run_phase(
        download_jobs,
        partial(
            download_sbom,
            retries=retries,
            enterprise_contract_policy=enterprise_contract_policy,
        ),
        concurrent_limit,
        "SBOM download",
    )

    logger.info(
        "Uploading RPM data for %d images with concurrency %d",
        len(jobs),
        concurrent_limit,
    )
    _run_phase(jobs, upload_rpm_data, concurrent_limit, "RPM data upload")


def main() -> int:
    """Read Tekton configuration from the environment and execute the task."""
    pyxis_file = Path(tekton.require_env("PYXIS_FILE"))
    secret_path = Path(tekton.require_env("PYXIS_SECRET_PATH"))
    server = tekton.require_env("PYXIS_SERVER")
    concurrent_limit = int(tekton.require_env("CONCURRENT_LIMIT"))
    retries = int(os.environ.get("RETRIES", "3"))

    os.environ["PYXIS_CERT_PATH"] = str(secret_path / "cert")
    os.environ["PYXIS_KEY_PATH"] = str(secret_path / "key")
    os.environ["PYXIS_GRAPHQL_API"] = pyxis_api.pyxis_graphql_url_for_server(server)
    authentication.setup_ca_cert()

    run(
        pyxis_file,
        concurrent_limit,
        retries,
        enterprise_contract_policy=os.environ.get("ENTERPRISE_CONTRACT_POLICY"),
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
