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
import contextlib
import json
from typing import Union
from pathlib import Path

import aiofiles

from sbom import sbomlib
from sbom.handlers import CycloneDXVersion1, SPDXVersion2
from sbom.log import get_sbom_logger, setup_sbom_logger
from sbom.sbomlib import (
    Component,
    SBOMError,
    Snapshot,
    Image,
    IndexImage,
)


logger = get_sbom_logger()

CONCURRENCY_LIMIT = 8

locks = {}


@contextlib.asynccontextmanager
async def lock(key: Path):
    """Retrieve a lock specific to a given path, to ensure only one writer"""
    if not locks.get(key):
        locks[key] = asyncio.Lock()
    async with locks[key]:
        yield


async def fetch_sbom(
    destination_dir: Path, reference: str, component_name: str = None
) -> Path:
    """
    Download an SBOM for an image reference to a destination directory.
    """
    with sbomlib.make_oci_auth_file(reference) as authfile:
        code, stdout, stderr = await sbomlib.run_async_subprocess(
            ["cosign", "download", "sbom", reference],
            env={"DOCKER_CONFIG": str(Path(authfile).parent)},
            retry_times=3,
        )

    if code != 0:
        raise SBOMError(f"Failed to fetch SBOM {reference}: {stderr.decode()}")

    digest = reference.split("@", 1)[1]
    # Include component name in the file path to prevent data mixing
    if component_name:
        # Sanitize component name for use in file path
        safe_component_name = component_name.replace("/", "_").replace("\\", "_")
        path = destination_dir.joinpath(f"{safe_component_name}-{digest}")
    else:
        path = destination_dir.joinpath(digest)

    async with lock(path):
        async with aiofiles.open(path, "wb") as file:
            await file.write(stdout)

    return path


async def load_sbom(
    reference: str, destination: Path, component_name: str = None
) -> tuple[dict, Path]:
    """
    Download the sbom for the image reference, save it to a directory and parse
    it into a dictionary.
    """
    path = await fetch_sbom(destination, reference, component_name)
    async with aiofiles.open(path, "r") as sbom_raw:
        sbom = json.loads(await sbom_raw.read())
        return sbom, path


async def write_sbom(sbom: dict, path: Path) -> None:
    """
    Write an SBOM dictionary to a file.
    """
    async with lock(path):
        async with aiofiles.open(path, "w") as fp:
            await fp.write(json.dumps(sbom))


def update_sbom_in_situ(
    component: Component, image: Union[IndexImage, Image], sbom: dict, release_id: str
) -> bool:
    """
    Determine the matching SBOM handler and update the SBOM with release-time
    information in situ.

    Args:
        component (Component): The component the image belongs to.
        image (IndexImage | Image): Object representing an image or an index
                                    image being released.
        sbom (dict): SBOM parsed as dictionary.
        semaphore: An asyncio semaphore to limit update concurrency
        release_id: Pipelinerun UID of the release.
    """
    if SPDXVersion2.supports(sbom):
        SPDXVersion2().update_sbom(component, image, sbom, release_id)
        return True

    # The CDX handler does not support updating SBOMs for index images, as those
    # are generated only as SPDX in Konflux.
    if CycloneDXVersion1.supports(sbom) and isinstance(image, Image):
        CycloneDXVersion1().update_sbom(component, image, sbom, release_id)
        return True

    return False


async def update_sbom(
    component: Component,
    image: Union[IndexImage, Image],
    destination: Path,
    semaphore: asyncio.Semaphore,
    release_id: str,
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
        semaphore: An asyncio semaphore to limit update concurrency
        release_id: Pipelinerun UID of the release.
    """

    async with semaphore:
        reference = f"{component.repository}@{image.digest}"
        logger.debug("Starting SBOM enrichment for image %s", reference)
        try:
            sbom, sbom_path = await load_sbom(reference, destination, component.name)

            if not update_sbom_in_situ(component, image, sbom, release_id):
                raise SBOMError(f"Unsupported SBOM format for image {reference}.")

            await write_sbom(sbom, sbom_path)
            logger.info("Successfully enriched SBOM for image %s", reference)
        except (SBOMError, ValueError):
            logger.exception("Failed to enrich SBOM for image %s.", reference)
            raise


async def update_component_sboms(
    component: Component, destination: Path, semaphore: asyncio.Semaphore, release_id: str
) -> None:
    """
    Update SBOMs for a component and save them to a directory.

    Handles multiarch images as well.

    Args:
        component (Component): Object representing a component being released.
        destination (Path): Path to the directory to save the SBOMs to.
        semaphore: An asyncio semaphore to limit update concurrency
        release_id: Pipelinerun UID of the release.
    """
    if isinstance(component.image, IndexImage):
        # If the image of a component is a multiarch image, we update the SBOMs
        # for both the index image and the child single arch images.
        index = component.image
        update_tasks = [
            update_sbom(component, index, destination, semaphore, release_id),
        ]
        for child in index.children:
            update_tasks.append(
                update_sbom(component, child, destination, semaphore, release_id)
            )

        await asyncio.gather(*update_tasks)
        return

    # Single arch image
    await update_sbom(component, component.image, destination, semaphore, release_id)


async def update_sboms(snapshot: Snapshot, destination: Path, release_id: str) -> None:
    """
    Update component SBOMs with release-time information based on a Snapshot and
    save them to a directory.

    Args:
        Snapshot: A object representing a snapshot being released.
        destination (Path): Path to the directory to save the SBOMs to.
        release_id: Pipelinerun UID of the release.
    """
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    await asyncio.gather(
        *[
            update_component_sboms(component, destination, semaphore, release_id)
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
        "--release-id",
        required=True,
        type=str,
        help="Release pipelinerun UID, used to match augmented SBOMs with their input data.",
    )
    args = parser.parse_args()

    setup_sbom_logger()

    logger.debug("Starting snapshot parsing.")
    snapshot = await sbomlib.make_snapshot(args.snapshot_path)
    logger.debug("Starting SBOM update.")
    await update_sboms(snapshot, args.output_path, args.release_id)
    logger.debug("Finished SBOM update.")


if __name__ == "__main__":
    asyncio.run(main())
