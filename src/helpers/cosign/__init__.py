"""Download SBOM and attestation attachments from container images via cosign."""

from .cosign import docker_config, download, verify_attestation  # noqa: F401
