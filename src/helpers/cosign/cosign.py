"""Download SBOM and attestation attachments from container images via cosign."""

from __future__ import annotations

import functools
import subprocess
from pathlib import Path
from typing import Literal

from release_service_utils.helpers import oci_auth, retry, subprocess_cmd
from release_service_utils.helpers.logger import logger

DownloadKind = Literal["sbom", "attestation"]
DEFAULT_MAX_ATTEMPTS = 3
DEFAULT_MAX_ATTACHMENT_SIZE = 200_000_000
docker_config = oci_auth.docker_config

# Lower-cased stderr of ``cosign download sbom`` when the image has no SBOM attached.
SBOM_NOT_FOUND_MARKERS = (
    "no sbom attached to reference",
    "no sbom found attached to image index",
)


class SbomNotFoundError(RuntimeError):
    """Raised when the image has no SBOM attached under the legacy ``.sbom`` tag."""


def _stderr_matches(exc: subprocess.CalledProcessError, markers: tuple[str, ...]) -> bool:
    """Return whether the lower-cased stderr of *exc* contains any of *markers*."""
    stderr = (exc.stderr or "").lower()
    return any(marker in stderr for marker in markers)


def _is_missing_sbom(exc: BaseException) -> bool:
    """Return whether *exc* explicitly reports a missing legacy SBOM."""
    return isinstance(exc, subprocess.CalledProcessError) and _stderr_matches(
        exc, SBOM_NOT_FOUND_MARKERS
    )


def _log_stderr(
    result: subprocess.CompletedProcess[str] | subprocess.CalledProcessError,
) -> None:
    """Log command stderr without exposing credential-bearing stdout."""
    if result.stderr and result.stderr.strip():
        logger.info(result.stderr.rstrip())


def _run_once(
    command: list[str], config_dir: Path, max_attachment_size: int
) -> subprocess.CompletedProcess[str]:
    """Run cosign *command* with *config_dir* as DOCKER_CONFIG, logging stderr on failure."""
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


def _run_cosign(
    command: list[str],
    config_dir: Path,
    *,
    max_attempts: int,
    max_attachment_size: int,
) -> subprocess.CompletedProcess[str]:
    """Run cosign *command*, retrying failures with exponential backoff.

    Re-raise the last ``subprocess.CalledProcessError`` once *max_attempts* is
    exhausted, or at once when a legacy SBOM download explicitly reports absence.
    """
    is_sbom_download = command[1:3] == ["download", "sbom"]
    return retry.retry_with_exponential_backoff(
        functools.partial(_run_once, command, config_dir, max_attachment_size),
        max_attempts=max_attempts,
        retry_on=subprocess.CalledProcessError,
        retry_if=lambda exc: not (is_sbom_download and _is_missing_sbom(exc)),
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
        if kind == "sbom" and _is_missing_sbom(exc):
            raise SbomNotFoundError(
                f"No SBOM attached to {image}: {(exc.stderr or '').strip()}"
            ) from exc
        raise RuntimeError(
            f"Failed to download {kind} for {image} after {max_attempts} attempts: "
            f"{(exc.stderr or '').strip() or str(exc)}"
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
    ``COSIGN_MAX_ATTACHMENT_SIZE``, and retries cosign failures with exponential
    backoff, except when a legacy SBOM is explicitly reported as missing.
    When *platform* is set, pass ``--platform`` so a multi-arch image yields
    one architecture. Raise ``SbomNotFoundError`` when no SBOM is attached.
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


def verify_spdx_attestation(
    image: str,
    *,
    key: str,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    max_attachment_size: int = DEFAULT_MAX_ATTACHMENT_SIZE,
) -> str:
    """Return verified SPDX attestations discovered through OCI referrers.

    Use the image's Cosign v3 binary, which supports referrers and bundles.
    Verify against the public *key* only; transparency-log inclusion is skipped.
    Pass *key* directly to ``--key`` as a path, KMS URI or ``k8s://`` reference.
    """
    command = [
        "cosign3",
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
                f"after {max_attempts} attempts: "
                f"{(exc.stderr or '').strip() or str(exc)}"
            ) from exc
    return result.stdout or ""
