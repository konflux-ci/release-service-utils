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
from typing import Union
from pathlib import Path

import aiofiles

from sbom import sbomlib
from sbom.handlers import CycloneDXVersion1, SPDXVersion2
from sbom.logging import get_sbom_logger, setup_sbom_logger
from sbom.sbomlib import (
    Component,
    Cosign,
    CosignClient,
    SBOMError,
    SBOMVerificationError,
    Snapshot,
    Image,
    IndexImage,
)


logger = get_sbom_logger()


async def get_sbom_digest(path: Path) -> str:
    """
    Read the specified SBOM by chunks and calculate its sha256 digest.
    """
    sha256_hash = hashlib.sha256()
    async with aiofiles.open(path, "rb") as fp:
        chunk_size = 2**20  # 1MiB
        while chunk := await fp.read(chunk_size):
            sha256_hash.update(chunk)

    return f"sha256:{sha256_hash.hexdigest()}"


async def verify_sbom(path: Path, reference: str, cosign: Cosign) -> None:
    """
    Verify that the sha256 digest of the specified SBOM matches the value of
    SBOM_BLOB_URL in the provenance for the provided image reference. Cosign is
    used to fetch the provenance. If it doesn't match, an SBOMVerificationError
    is raised.
    """

    prov = await cosign.fetch_latest_provenance(reference)
    prov_sbom_digest = prov.get_sbom_digest(reference)
    sbom_digest = await get_sbom_digest(path)

    if prov_sbom_digest != sbom_digest:
        raise SBOMVerificationError(
            prov_sbom_digest,
            sbom_digest,
        )


async def load_sbom(reference: str, destination: Path, cosign: Cosign) -> tuple[dict, Path]:
    """
    Download the sbom for the image reference, save it to a directory, verify
    that its digest matches that in the image provenance and parse it into a
    dictionary.
    """
    path = await cosign.fetch_sbom(destination, reference)
    await verify_sbom(path, reference, cosign)
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
    component: Component, image: Union[IndexImage, Image], destination: Path, cosign: Cosign
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
        sbom, sbom_path = await load_sbom(reference, destination, cosign)

        if not update_sbom_in_situ(component, image, sbom):
            raise SBOMError(f"Unsupported SBOM format for image {reference}.")

        await write_sbom(sbom, sbom_path)
        logger.info("Successfully enriched SBOM for image %s", reference)
    except (SBOMError, ValueError):
        logger.exception("Failed to enrich SBOM for image %s.", reference)
        raise


async def update_component_sboms(
    component: Component, destination: Path, cosign: Cosign
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
            update_sbom(component, index, destination, cosign),
        ]
        for child in index.children:
            update_tasks.append(update_sbom(component, child, destination, cosign))

        results = await asyncio.gather(*update_tasks, return_exceptions=True)
        for res in results:
            if isinstance(res, BaseException):
                raise res
        return

    # Single arch image
    await update_sbom(component, component.image, destination, cosign)


async def update_sboms(snapshot: Snapshot, destination: Path, cosign: Cosign) -> None:
    """
    Update component SBOMs with release-time information based on a Snapshot and
    save them to a directory.

    Args:
        Snapshot: A object representing a snapshot being released.
        destination (Path): Path to the directory to save the SBOMs to.
    """
    # use return_exceptions=True to avoid crashing non-finished tasks if one
    # task raises an exception.
    results = await asyncio.gather(
        *[
            update_component_sboms(component, destination, cosign)
            for component in snapshot.components
        ],
        return_exceptions=True,
    )
    # Python 3.11 ExceptionGroup would be nice here, so we can re-raise
    # all the exceptions that were raised and not just one.
    for res in results:
        if isinstance(res, BaseException):
            raise res


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
        help="Path to public key to verify attestations with.",
    )
    args = parser.parse_args()

    setup_sbom_logger()

    snapshot = await sbomlib.make_snapshot(args.snapshot_path)
    cosign = CosignClient(args.verification_key)
    await update_sboms(snapshot, args.output_path, cosign)


if __name__ == "__main__":
    asyncio.run(main())
