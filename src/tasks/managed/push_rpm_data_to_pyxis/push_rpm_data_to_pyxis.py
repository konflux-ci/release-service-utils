#!/usr/bin/env python3
"""Download image SBOMs and upload their RPM manifests and content sets to Pyxis."""

from __future__ import annotations

import os
import string
import subprocess
import tempfile
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from typing import Any

from release_service_utils.helpers import (
    authentication,
    file,
    memory_throttle,
    pyxis_api,
    retry,
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

    return ImageJob(image_id=image_id, container_image=container_image, platform=platform)


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


def download_sbom(job: ImageJob, retries: int) -> None:
    """Download one image SBOM with registry authentication and retries."""
    output_path = SBOM_DIR / f"{job.image_id}.json"
    with tempfile.TemporaryDirectory(prefix="docker-config-") as config_dir_name:
        config_dir = Path(config_dir_name)
        try:
            auth_result = subprocess_cmd.run_cmd(
                ["select-oci-auth", job.container_image], check=True
            )
        except subprocess.CalledProcessError as exc:
            _log_process_output(exc, include_stdout=False)
            raise
        (config_dir / "config.json").write_text(
            auth_result.stdout.strip() or "{}", encoding="utf-8"
        )

        command = ["cosign", "download", "sbom", "--output-file", str(output_path)]
        if job.platform is not None:
            command.extend(["--platform", job.platform])
        command.append(job.container_image)

        def run_cosign() -> subprocess.CompletedProcess[str]:
            try:
                return subprocess_cmd.run_cmd(
                    command,
                    env={"DOCKER_CONFIG": str(config_dir)},
                    check=True,
                )
            except subprocess.CalledProcessError as exc:
                _log_process_output(exc)
                raise

        result = retry.retry_with_exponential_backoff(
            run_cosign,
            max_attempts=retries + 1,
            retry_on=subprocess.CalledProcessError,
            base_sleep_seconds=2,
        )
        _log_process_output(result)

    if not output_path.is_file():
        raise RuntimeError(f"SBOM file {output_path} was not created for image {job.image_id}")


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


def run(pyxis_file: Path, concurrent_limit: int, retries: int) -> None:
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
        partial(download_sbom, retries=retries),
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

    run(pyxis_file, concurrent_limit, retries)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
