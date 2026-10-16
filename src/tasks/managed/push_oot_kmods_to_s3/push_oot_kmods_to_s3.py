#!/usr/bin/env python3
"""Upload signed out-of-tree kernel modules to an S3-compatible bucket."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config

from release_service_utils.helpers import authentication, file, oot_kmods, tekton
from release_service_utils.helpers.logger import logger

DEFAULT_S3_CREDENTIALS_MOUNT = Path("/var/secrets/s3")
_S3_REGION = "us-east-1"
_ACCESS_KEY_FILE = "aws_access_key_id"
_SECRET_KEY_FILE = "aws_secret_access_key"
_SIGNING_SUMMARY = "signing_summary.txt"
_EXTRACTION_SUMMARY = "extraction_summary.txt"
_MULTI_ARCH_SUFFIX = "multi-arch-summary"


def _s3_client(endpoint: str, access_key: str, secret_key: str) -> Any:
    """Return an S3 client for *endpoint* using path-style addressing."""
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=_S3_REGION,
        config=Config(
            s3={"addressing_style": "path"},
            # boto3 1.36+ defaults these to when_supported, which sends CRC
            # headers many S3-compatible endpoints reject.
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


def _upload_file(client: Any, local: Path, bucket: str, key: str) -> None:
    """Upload *local* to ``s3://bucket/key``."""
    logger.info("Uploading %s to s3://%s/%s", local, bucket, key)
    client.upload_file(str(local), bucket, key)


def _upload_arch(client: Any, arch_dir: Path, bucket: str) -> None:
    """Upload ``.ko`` files, checksums, and envfile for one architecture."""
    env = oot_kmods.load_kmod_envfile(arch_dir / oot_kmods.ENVFILE_NAME)
    original_kernel = env["KERNEL_VERSION"]
    kernel = oot_kmods.clean_kernel_version(original_kernel)
    logger.info("Original KERNEL_VERSION: %s", original_kernel)
    logger.info("Cleaned KERNEL_VERSION: %s", kernel)

    final_arch = oot_kmods.arch_from_env(env, arch_dir.name)
    prefix = oot_kmods.destination_prefix(
        env["DRIVER_VENDOR"],
        env["DRIVER_VERSION"],
        kernel,
        final_arch,
    )
    logger.info("S3 target path: s3://%s/%s", bucket, prefix)

    ko_files = sorted(path for path in arch_dir.rglob("*.ko") if path.is_file())
    if not ko_files:
        raise FileNotFoundError(f"No .ko files found for architecture {arch_dir.name}")

    logger.info(
        "Uploading %d .ko files for %s (preserving directory structure)",
        len(ko_files),
        arch_dir.name,
    )
    for ko_file in ko_files:
        relative = ko_file.relative_to(arch_dir).as_posix()
        _upload_file(client, ko_file, bucket, prefix + relative)

    checksum = arch_dir / f"signed_kmods_checksums_{arch_dir.name}.txt"
    if checksum.is_file():
        logger.info("Uploading architecture-specific checksums for %s", arch_dir.name)
        _upload_file(client, checksum, bucket, prefix + checksum.name)

    envfile = arch_dir / oot_kmods.ENVFILE_NAME
    logger.info("Uploading envfile for %s", arch_dir.name)
    _upload_file(client, envfile, bucket, prefix + oot_kmods.ENVFILE_NAME)


def _upload_multi_arch_summaries(
    client: Any,
    signed_kmods: Path,
    arch_dirs: list[Path],
    bucket: str,
) -> None:
    """Upload multi-arch signing and extraction summaries when present."""
    signing = signed_kmods / _SIGNING_SUMMARY
    if not signing.is_file():
        return

    env = oot_kmods.load_kmod_envfile(arch_dirs[0] / oot_kmods.ENVFILE_NAME)
    kernel = oot_kmods.clean_kernel_version(env["KERNEL_VERSION"])
    prefix = oot_kmods.destination_prefix(
        env["DRIVER_VENDOR"],
        env["DRIVER_VERSION"],
        kernel,
        _MULTI_ARCH_SUFFIX,
    )
    logger.info("Uploading multi-architecture summary to s3://%s/%s", bucket, prefix)
    _upload_file(client, signing, bucket, prefix + _SIGNING_SUMMARY)

    extraction = signed_kmods / _EXTRACTION_SUMMARY
    if extraction.is_file():
        logger.info("Uploading extraction_summary.txt to S3")
        _upload_file(client, extraction, bucket, prefix + _EXTRACTION_SUMMARY)


def run(
    data_dir: Path,
    signed_kmods_path: str,
    s3_endpoint: str,
    s3_bucket: str,
    creds_mount: Path,
) -> None:
    """Extract signed kmods if needed and upload them to S3."""
    signed_kmods = file.resolve_path_under_base(data_dir, signed_kmods_path)
    oot_kmods.extract_signed_kmods_archive(data_dir, signed_kmods)
    arch_dirs = oot_kmods.arch_directories(signed_kmods)
    logger.info(
        "Detected %d architecture(s) from directory structure",
        len(arch_dirs),
    )

    access_key = authentication.read_mounted_text(creds_mount, _ACCESS_KEY_FILE)
    secret_key = authentication.read_mounted_text(creds_mount, _SECRET_KEY_FILE)
    client = _s3_client(s3_endpoint, access_key, secret_key)

    if len(arch_dirs) > 1:
        _upload_multi_arch_summaries(client, signed_kmods, arch_dirs, s3_bucket)

    for arch_dir in arch_dirs:
        logger.info("Processing S3 upload for architecture: %s", arch_dir.name)
        _upload_arch(client, arch_dir, s3_bucket)

    logger.info("S3 upload complete for %d architecture(s).", len(arch_dirs))


def main() -> int:
    """Read Tekton environment variables and upload signed kmods to S3."""
    data_dir = Path(tekton.require_env("PARAM_DATA_DIR"))
    signed_kmods_path = tekton.require_env("PARAM_SIGNED_KMODS_PATH")
    s3_endpoint = tekton.require_env("PARAM_S3_ENDPOINT")
    s3_bucket = tekton.require_env("PARAM_S3_BUCKET")
    creds_mount = file.path_from_env_variable(
        "PARAM_S3_CREDENTIALS_MOUNT",
        DEFAULT_S3_CREDENTIALS_MOUNT,
    )
    run(
        data_dir=data_dir,
        signed_kmods_path=signed_kmods_path,
        s3_endpoint=s3_endpoint,
        s3_bucket=s3_bucket,
        creds_mount=creds_mount,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
