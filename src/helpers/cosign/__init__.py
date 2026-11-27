"""Download SBOM and attestation attachments from container images via cosign."""

from .cosign import (  # noqa: F401
    docker_config,
    SbomNotFoundError,
    download,
    verify_spdx_attestation,
)
