"""Download SBOM and attestation attachments from container images via cosign."""

from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Literal

from release_service_utils.helpers import oci_auth, retry, subprocess_cmd
from release_service_utils.helpers.logger import logger

DownloadKind = Literal["sbom", "attestation"]
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_MAX_ATTACHMENT_SIZE = 200_000_000
docker_config = oci_auth.docker_config


def _log_stderr(
    result: subprocess.CompletedProcess[str] | subprocess.CalledProcessError,
) -> None:
    """Log command stderr without exposing credential-bearing stdout."""
    if result.stderr and result.stderr.strip():
        logger.info(result.stderr.rstrip())


def _run_cosign(
    command: list[str],
    config_dir: Path,
    *,
    max_attempts: int,
    max_attachment_size: int,
) -> subprocess.CompletedProcess[str]:
    """Run cosign *command* with *config_dir* as DOCKER_CONFIG, retrying on failure.

    Log stderr of each failed attempt and re-raise the last
    ``subprocess.CalledProcessError`` once *max_attempts* is exhausted.
    """

    def run() -> subprocess.CompletedProcess[str]:
        try:
            return subprocess_cmd.run_cmd(
                command,
                env={
                    "DOCKER_CONFIG": str(config_dir),
                    "COSIGN_MAX_ATTACHMENT_SIZE": str(max_attachment_size),
                },
                check=True,
            )
        except subprocess.CalledProcessError as exc:
            _log_stderr(exc)
            raise

    return retry.retry_with_exponential_backoff(
        run,
        max_attempts=max_attempts,
        retry_on=subprocess.CalledProcessError,
        base_sleep_seconds=2,
    )


def _cosign_download(
    kind: DownloadKind,
    image: str,
    output_file: Path,
    config_dir: Path,
    *,
    max_attempts: int,
    max_attachment_size: int,
    platform: str | None,
) -> None:
    """Run ``cosign download`` for *kind* using *config_dir* as DOCKER_CONFIG."""
    command = [
        "cosign",
        "download",
        kind,
        "--output-file",
        str(output_file),
    ]
    if platform:
        command.extend(["--platform", platform])
    command.append(image)

    try:
        _run_cosign(
            command,
            config_dir,
            max_attempts=max_attempts,
            max_attachment_size=max_attachment_size,
        )
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(
            f"Failed to download {kind} for {image} after {max_attempts} attempts."
        ) from exc


def download(
    kind: DownloadKind,
    image: str,
    output_file: Path,
    *,
    docker_config_dir: Path | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    max_attachment_size: int = DEFAULT_MAX_ATTACHMENT_SIZE,
    platform: str | None = None,
) -> None:
    """Download a cosign *kind* attachment for *image* into *output_file*.

    Authenticates with ``select-oci-auth`` unless *docker_config_dir* is
    provided, raises the attachment-size limit via
    ``COSIGN_MAX_ATTACHMENT_SIZE``, and retries transient cosign failures
    with exponential backoff. When *platform* is set, pass ``--platform``
    so a multi-arch image yields one architecture.
    """
    output_file.parent.mkdir(parents=True, exist_ok=True)
    logger.info("Downloading %s from %s...", kind, image)

    def invoke(config_dir: Path) -> None:
        _cosign_download(
            kind,
            image,
            output_file,
            config_dir,
            max_attempts=max_attempts,
            max_attachment_size=max_attachment_size,
            platform=platform,
        )

    if docker_config_dir is None:
        with docker_config(image) as config_dir:
            invoke(config_dir)
    else:
        invoke(docker_config_dir)

    if not output_file.is_file():
        raise RuntimeError(
            f"Failed to download {kind} for {image}: {output_file} was not created"
        )


def verify_attestation(
    image: str,
    *,
    key: str,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    max_attachment_size: int = DEFAULT_MAX_ATTACHMENT_SIZE,
) -> str:
    """Return ``cosign verify-attestation`` output for SPDX attestations of *image*.

    Verify against the public *key* only; transparency-log inclusion is skipped,
    like the existing rh-advisories Conforma policy check (IGNORE_REKOR=true).
    """
    command = [
        "cosign",
        "verify-attestation",
        "--type",
        "spdxjson",
        "--key",
        key,
        "--insecure-ignore-tlog=true",
        image,
    ]
    logger.info("Verifying SPDX attestations for %s...", image)

    with docker_config(image) as config_dir:
        try:
            result = _run_cosign(
                command,
                config_dir,
                max_attempts=max_attempts,
                max_attachment_size=max_attachment_size,
            )
        except subprocess.CalledProcessError as exc:
            raise RuntimeError(
                f"Failed to verify SPDX attestation for {image} "
                f"after {max_attempts} attempts."
            ) from exc
    return result.stdout or ""
