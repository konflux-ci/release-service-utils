#!/usr/bin/env python3
"""
This script parses the mapped snapshot spec file (result from apply-mapping
Tekton task), downloads SBOMs for all images that are being released to a
directory and updates them with release time data.

Example usage:
$ update_component_sbom --snapshot-path snapshot_spec.json --output-path sboms/
"""
import argparse
import asyncio
import json
import hashlib
import base64
from typing import Union, Optional, Any
import dateutil.parser
import datetime
from pathlib import Path

import aiofiles

from sbom import sbomlib
from sbom.handlers import CycloneDXVersion1, SPDXVersion2
from sbom.logging import get_sbom_logger, setup_sbom_logger
from sbom.sbomlib import (
    Component,
    SBOMError,
    Snapshot,
    Image,
    IndexImage,
)


logger = get_sbom_logger()


class Provenance02:
    """
    Object containing the data of an provenance attestation.
    """

    predicate_type = "https://slsa.dev/provenance/v0.2"

    def __init__(self, predicate: Any) -> None:
        self.predicate = predicate

    @staticmethod
    def from_cosign_output(raw: bytes) -> "Provenance02":
        encoded = json.loads(raw)
        att = json.loads(base64.b64decode(encoded["payload"]))
        if att.get("predicateType" != Provenance02.predicate_type):
            # FIXME: add error message
            raise ValueError()

        predicate = att.get("predicate", {})
        return Provenance02(predicate)

    @property
    def build_finished_on(self) -> datetime.datetime:
        """
        Return datetime of the build being finished.
        If it's not available, fallback to datetime.min.
        """
        if self.predicate is None:
            raise ValueError("Cannot get build time from uninitialized provenance.")

        finished_on: str | None = self.predicate.get("metadata", {}).get("buildFinishedOn")
        if finished_on:
            return dateutil.parser.isoparse(finished_on)

        return datetime.datetime.min

    def get_sbom_digest(self, reference: str) -> Optional[str]:
        image_digest = reference.split("@", 1)[1]

        sbom_blob_urls: dict[str, str] = {}
        tasks = self.predicate.get("buildConfig", {}).get("tasks", [])
        for task in tasks:
            curr_digest, sbom_url = "", ""
            for result in task.get("results", []):
                if result.get("name") == "SBOM_BLOB_URL":
                    sbom_url = result.get("value")
                if result.get("name") == "IMAGE_DIGEST":
                    curr_digest = result.get("value")
            if not all([curr_digest, sbom_url]):
                continue
            sbom_blob_urls[curr_digest] = sbom_url

        blob_url = sbom_blob_urls.get(image_digest)
        if blob_url is None:
            raise SBOMError(f"No SBOM_BLOB_URL found in attestation for image {reference}.")

        return blob_url.split("@", 1)[1]


async def fetch_sbom(destination_dir: Path, reference: str) -> Path:
    """
    Download an SBOM for an image reference to a destination directory.
    """
    with sbomlib.make_oci_auth_file(reference) as authfile:
        code, stdout, stderr = await sbomlib.run_async_subprocess(
            ["cosign", "download", "sbom", reference],
            env={"DOCKER_CONFIG": authfile},
            retry_times=3,
        )

    if code != 0:
        raise SBOMError(f"Failed to fetch SBOM {reference}: {stderr.decode()}")

    digest = reference.split("@", 1)[1]
    path = destination_dir.joinpath(digest)
    async with aiofiles.open(path, "wb") as file:
        await file.write(stdout)

    return path


async def get_latest_provenance(reference: str, key: Path) -> Provenance02:
    with sbomlib.make_oci_auth_file(reference) as authfile:
        cmd = [
            "cosign",
            "verify-attestation",
            f"--key={key}",
            "--type=slsaprovenance02",
            "--insecure-ignore-tlog=true",
            reference,
        ]
        logger.debug(f"Fetching provenance for {reference} using '{' '.join(cmd)}.'")
        code, stdout, stderr = await sbomlib.run_async_subprocess(
            cmd,
            env={"DOCKER_CONFIG": authfile},
            retry_times=3,
        )

    if code != 0:
        raise SBOMError(f"Failed to fetch provenance for {reference}: {stderr.decode()}.")

    attestations = []
    for raw_attestation in stdout.splitlines():
        att = Provenance02.from_cosign_output(raw_attestation)
        attestations.append(att)

    if len(attestations) == 0:
        raise SBOMError(f"No provenances parsed for image {reference}.")

    return sorted(attestations, key=lambda x: x.build_finished_on, reverse=True)[0]


async def get_sbom_digest(path: Path) -> str:
    sha256_hash = hashlib.sha256()
    async with aiofiles.open(path, "rb") as fp:
        chunk_size = 4096
        while chunk := await fp.read(chunk_size):
            sha256_hash.update(chunk)

    return f"sha256:{sha256_hash.hexdigest()}"


async def verify_sbom(path: Path, reference: str, verification_key: Path) -> bool:
    prov = await get_latest_provenance(reference, verification_key)
    prov_sbom_digest = prov.get_sbom_digest(reference)

    sbom_digest = await get_sbom_digest(path)

    if prov_sbom_digest != sbom_digest:
        return False

    return True


async def load_sbom(
    reference: str, destination: Path, verification_key: Path
) -> tuple[dict, Path]:
    """
    Download the sbom for the image reference, save it to a directory and parse
    it into a dictionary.
    """
    path = await fetch_sbom(destination, reference)
    if not verify_sbom(path, reference, verification_key):
        raise SBOMError(
            f"The digest of the SBOM for image {reference} "
            "does not match the digest in the provenance!"
        )

    async with aiofiles.open(path, "r") as sbom_raw:
        sbom = json.loads(await sbom_raw.read())
        return sbom, path


async def write_sbom(sbom: dict, path: Path) -> None:
    """
    Write an SBOM dictionary to a file.
    """
    async with aiofiles.open(path, "w") as fp:
        await fp.write(json.dumps(sbom))


def update_sbom_in_situ(
    component: Component, image: Union[IndexImage, Image], sbom: dict
) -> bool:
    """
    Determine the matching SBOM handler and update the SBOM with release-time
    information in situ.

    Args:
        component (Component): The component the image belongs to.
        image (IndexImage | Image): Object representing an image or an index
                                    image being released.
        sbom (dict): SBOM parsed as dictionary.
    """
    if SPDXVersion2.supports(sbom):
        SPDXVersion2().update_sbom(component, image, sbom)
        return True

    # The CDX handler does not support updating SBOMs for index images, as those
    # are generated only as SPDX in Konflux.
    if CycloneDXVersion1.supports(sbom) and isinstance(image, Image):
        CycloneDXVersion1().update_sbom(component, image, sbom)
        return True

    return False


async def update_sbom(
    component: Component,
    image: Union[IndexImage, Image],
    destination: Path,
    verification_key: Path,
) -> None:
    """
    Update an SBOM of an image in a repository and save it to a directory.
    Determines format of the SBOM and calls the correct handler or throws
    SBOMError if the format of the SBOM is unsupported.

    Args:
        component (Component): The component the image belongs to.
        image (IndexImage | Image): Object representing an image or an index
                                    image being released.
        destination (Path): Path to the directory to save the SBOMs to.
    """

    try:
        reference = f"{component.repository}@{image.digest}"
        sbom, sbom_path = await load_sbom(reference, destination, verification_key)

        if not update_sbom_in_situ(component, image, sbom):
            raise SBOMError(f"Unsupported SBOM format for image {reference}.")

        await write_sbom(sbom, sbom_path)
        logger.info("Successfully enriched SBOM for image %s", reference)
    except (SBOMError, ValueError):
        logger.exception("Failed to enrich SBOM for image %s.", reference)
        raise


async def update_component_sboms(
    component: Component, destination: Path, verification_key: Path
) -> None:
    """
    Update SBOMs for a component and save them to a directory.

    Handles multiarch images as well.

    Args:
        component (Component): Object representing a component being released.
        destination (Path): Path to the directory to save the SBOMs to.
    """
    if isinstance(component.image, IndexImage):
        # If the image of a component is a multiarch image, we update the SBOMs
        # for both the index image and the child single arch images.
        index = component.image
        update_tasks = [
            update_sbom(component, index, destination, verification_key),
        ]
        for child in index.children:
            update_tasks.append(update_sbom(component, child, destination, verification_key))

        await asyncio.gather(*update_tasks)
        return

    # Single arch image
    await update_sbom(component, component.image, destination, verification_key)


async def update_sboms(snapshot: Snapshot, destination: Path, verification_key: Path) -> None:
    """
    Update component SBOMs with release-time information based on a Snapshot and
    save them to a directory.

    Args:
        Snapshot: A object representing a snapshot being released.
        destination (Path): Path to the directory to save the SBOMs to.
    """
    await asyncio.gather(
        *[
            update_component_sboms(component, destination, verification_key)
            for component in snapshot.components
        ]
    )


async def main() -> None:
    """
    Script entrypoint.
    """
    parser = argparse.ArgumentParser(
        prog="update-component-sbom",
        description="Update component SBOM purls with release info.",
    )
    parser.add_argument(
        "--snapshot-path",
        required=True,
        type=Path,
        help="Path to the snapshot spec file in JSON format.",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        type=Path,
        help="Path to the directory to save the updated SBOM files.",
    )
    parser.add_argument(
        "--verification-key",
        required=True,
        type=Path,
        help="Path to public key to verify the attestation with.",
    )
    args = parser.parse_args()

    setup_sbom_logger()

    snapshot = await sbomlib.make_snapshot(args.snapshot_path)
    await update_sboms(snapshot, args.output_path, args.verification_key)


if __name__ == "__main__":
    asyncio.run(main())
