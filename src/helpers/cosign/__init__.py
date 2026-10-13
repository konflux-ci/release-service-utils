"""Download SBOM and attestation attachments from container images via cosign."""

from .cosign import docker_config, download  # noqa: F401
