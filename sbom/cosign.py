"""
This module contains the Cosign protocol and the real Cosign implementation.
The protocol is used mainly for testing. The tests inject a testing cosign
client implementing the Cosign protocol.
"""

from pathlib import Path
import typing

from sbom.sbomlib import (
    SBOM,
    Image,
    Provenance02,
    SBOMError,
    make_oci_auth_file,
    run_async_subprocess,
)
from sbom.logging import get_sbom_logger

logger = get_sbom_logger()


class Cosign(typing.Protocol):
    async def fetch_latest_provenance(self, image: Image) -> Provenance02:
        return NotImplemented

    async def fetch_sbom(self, image: Image) -> SBOM:
        return NotImplemented


class CosignClient(Cosign):
    """
    Client used to get OCI artifacts using Cosign.
    """

    def __init__(self, verification_key: Path) -> None:
        """
        Args:
            verification_key: Path to public key used to verify attestations.
        """
        self.verification_key = verification_key

    async def fetch_latest_provenance(self, image: Image) -> Provenance02:
        """
        Fetch the latest provenance based on the supplied image based on the
        time the image build finished.
        """
        with make_oci_auth_file(image) as authfile:
            cmd = [
                "cosign",
                "verify-attestation",
                f"--key={self.verification_key}",
                "--type=slsaprovenance02",
                "--insecure-ignore-tlog=true",
                image.reference,
            ]
            logger.debug("Fetching provenance for %s using '%s'", image, " ".join(cmd))
            code, stdout, stderr = await run_async_subprocess(
                cmd,
                env={"DOCKER_CONFIG": authfile},
                retry_times=3,
            )

        if code != 0:
            raise SBOMError(f"Failed to fetch provenance for {image}: {stderr.decode()}.")

        provenances: list[Provenance02] = []
        for raw_attestation in stdout.splitlines():
            prov = Provenance02.from_cosign_output(raw_attestation)
            provenances.append(prov)

        if len(provenances) == 0:
            raise SBOMError(f"No provenances parsed for image {image}.")

        return sorted(provenances, key=lambda x: x.build_finished_on, reverse=True)[0]

    async def fetch_sbom(self, image: Image) -> SBOM:
        """
        Fetch and save the SBOM for the supplied image to a directory.
        """
        with make_oci_auth_file(image) as authfile:
            code, stdout, stderr = await run_async_subprocess(
                ["cosign", "download", "sbom", image.reference],
                env={"DOCKER_CONFIG": authfile},
                retry_times=3,
            )

        if code != 0:
            raise SBOMError(f"Failed to fetch SBOM {image}: {stderr.decode()}")

        return await SBOM.from_cosign_output(stdout)
