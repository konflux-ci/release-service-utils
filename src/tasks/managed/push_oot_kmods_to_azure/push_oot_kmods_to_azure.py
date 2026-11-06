#!/usr/bin/env python3
"""Upload signed out-of-tree kernel modules to Azure Blob Storage."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from azure.core.exceptions import ResourceExistsError
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient

from release_service_utils.helpers import authentication, file, oot_kmods, tekton
from release_service_utils.helpers.logger import logger

DEFAULT_AZURE_SP_MOUNT = Path("/var/secrets/azure-sp")
_TENANT_ID_FILE = "AZURE_TENANT_ID"
_CLIENT_ID_FILE = "AZURE_CLIENT_ID"
_CLIENT_SECRET_FILE = "AZURE_CLIENT_SECRET"
_SIGNING_SUMMARY = "signing_summary.txt"
_EXTRACTION_SUMMARY = "extraction_summary.txt"
_MULTI_ARCH_SUFFIX = "multi-arch-summary"


def _blob_service(
    account: str,
    tenant_id: str,
    client_id: str,
    client_secret: str,
) -> Any:
    """Return a BlobServiceClient for *account* using a service principal."""
    credential = ClientSecretCredential(tenant_id, client_id, client_secret)
    return BlobServiceClient(
        account_url=f"https://{account}.blob.core.windows.net",
        credential=credential,
    )


def _ensure_container(service: Any, container: str) -> None:
    """Create *container* when it does not already exist."""
    try:
        service.create_container(container)
    except ResourceExistsError:
        logger.info("Container %s already exists", container)


def _upload_file(service: Any, local: Path, container: str, blob_name: str) -> None:
    """Upload *local* to *container* as *blob_name*, overwriting if present."""
    logger.info("Uploading %s to %s/%s", local, container, blob_name)
    blob = service.get_blob_client(container=container, blob=blob_name)
    with local.open("rb") as handle:
        blob.upload_blob(handle, overwrite=True)


def _upload_arch(service: Any, arch_dir: Path, account: str, container: str) -> None:
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
    logger.info("Azure target path: %s/%s/%s", account, container, prefix)

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
        _upload_file(service, ko_file, container, prefix + relative)

    checksum = arch_dir / f"signed_kmods_checksums_{arch_dir.name}.txt"
    if checksum.is_file():
        logger.info("Uploading architecture-specific checksums for %s", arch_dir.name)
        _upload_file(service, checksum, container, prefix + checksum.name)

    envfile = arch_dir / oot_kmods.ENVFILE_NAME
    logger.info("Uploading envfile for %s", arch_dir.name)
    _upload_file(service, envfile, container, prefix + oot_kmods.ENVFILE_NAME)


def _upload_multi_arch_summaries(
    service: Any,
    signed_kmods: Path,
    arch_dirs: list[Path],
    account: str,
    container: str,
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
    logger.info(
        "Uploading multi-architecture summary to Azure: %s/%s/%s",
        account,
        container,
        prefix,
    )
    _upload_file(service, signing, container, prefix + _SIGNING_SUMMARY)

    extraction = signed_kmods / _EXTRACTION_SUMMARY
    if extraction.is_file():
        logger.info("Uploading extraction_summary.txt to Azure")
        _upload_file(service, extraction, container, prefix + _EXTRACTION_SUMMARY)


def run(
    data_dir: Path,
    signed_kmods_path: str,
    azure_storage_account: str,
    azure_container: str,
    creds_mount: Path,
) -> None:
    """Extract signed kmods if needed and upload them to Azure Blob Storage."""
    signed_kmods = file.resolve_path_under_base(data_dir, signed_kmods_path)
    oot_kmods.extract_signed_kmods_archive(data_dir, signed_kmods)
    arch_dirs = oot_kmods.arch_directories(signed_kmods)
    logger.info(
        "Detected %d architecture(s) from directory structure",
        len(arch_dirs),
    )

    tenant_id = authentication.read_mounted_text(creds_mount, _TENANT_ID_FILE)
    client_id = authentication.read_mounted_text(creds_mount, _CLIENT_ID_FILE)
    client_secret = authentication.read_mounted_text(creds_mount, _CLIENT_SECRET_FILE)
    service = _blob_service(
        azure_storage_account,
        tenant_id,
        client_id,
        client_secret,
    )
    _ensure_container(service, azure_container)

    if len(arch_dirs) > 1:
        _upload_multi_arch_summaries(
            service,
            signed_kmods,
            arch_dirs,
            azure_storage_account,
            azure_container,
        )

    for arch_dir in arch_dirs:
        logger.info("Processing Azure upload for architecture: %s", arch_dir.name)
        _upload_arch(service, arch_dir, azure_storage_account, azure_container)

    logger.info("Azure upload complete for %d architecture(s).", len(arch_dirs))


def main() -> int:
    """Read Tekton environment variables and upload signed kmods to Azure."""
    data_dir = Path(tekton.require_env("PARAM_DATA_DIR"))
    signed_kmods_path = tekton.require_env("PARAM_SIGNED_KMODS_PATH")
    azure_storage_account = tekton.require_env("PARAM_AZURE_STORAGE_ACCOUNT")
    azure_container = tekton.require_env("PARAM_AZURE_CONTAINER")
    creds_mount = file.path_from_env_variable(
        "PARAM_AZURE_SP_MOUNT",
        DEFAULT_AZURE_SP_MOUNT,
    )
    run(
        data_dir=data_dir,
        signed_kmods_path=signed_kmods_path,
        azure_storage_account=azure_storage_account,
        azure_container=azure_container,
        creds_mount=creds_mount,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
