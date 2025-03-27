#!/usr/bin/env python3
"""
This script updates the purls in component-level SBOMs with release time info.
"""
import argparse
import asyncio
import json
import logging
from typing import Union
from pathlib import Path
import aiofiles

from sbom.handlers import get_handler
from sbom import sbomlib
from sbom.sbomlib import (
    Component,
    SBOMError,
    Snapshot,
    Image,
    IndexImage,
)


LOG = logging.getLogger(__name__)


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
        raise SBOMError(f"Failed to fetch SBOM {reference}: {stderr}")

    digest = reference.split("@", 1)[1]
    path = destination_dir.joinpath(digest)
    async with aiofiles.open(path, "wb") as file:
        await file.write(stdout)

    return path


async def load_sbom(reference: str, destination: Path) -> tuple[dict, Path]:
    """
    Download the sbom for the image reference, save it to a directory and parse
    it into a dictionary.
    """
    path = await fetch_sbom(destination, reference)
    async with aiofiles.open(path, "r") as sbom_raw:
        return json.loads(await sbom_raw.read())


async def write_sbom(sbom: dict, path: Path) -> None:
    """
    Write an SBOM dictionary to a file.
    """
    async with aiofiles.open(path, "w") as fp:
        await fp.write(json.dumps(sbom))


async def update_sbom(
    component: Component, image: Union[IndexImage, Image], destination: Path
) -> None:
    """
    Update an SBOM of an image in a repository and save it to a directory.
    Determines format of the SBOM and calls the correct handler or throws
    SBOMError if the format of the SBOM is unsupported.

    Args:
        repository (str): Repository of the image
        image (IndexImage | Image): Object representing an image or an index
                                    image being released.
        destination (Path): Path to the directory to save the SBOMs to.
    """

    try:
        reference = f"{component.repository}@{image.digest}"
        sbom, sbom_path = await load_sbom(reference, destination)

        handler = get_handler(sbom)
        if not handler:
            raise SBOMError(f"Unsupported SBOM format for image {reference}.")

        handler.update_sbom(component, image, sbom)

        await write_sbom(sbom, sbom_path)
    except SBOMError:
        LOG.exception("Failed to enrich SBOM for image %s.", reference)


async def update_component_sboms(component: Component, destination: Path) -> None:
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
            update_sbom(component, index, destination),
        ]
        for child in index.children:
            update_tasks.append(update_sbom(component, child, destination))

        await asyncio.gather(*update_tasks)
        return

    # Single arch image
    await update_sbom(component, component.image, destination)


async def update_sboms(snapshot: Snapshot, destination: Path) -> None:
    """
    Update component SBOMs with release-time information based on a Snapshot and
    save them to a directory.

    Args:
        Snapshot: A object representing a snapshot being released.
        destination (Path): Path to the directory to save the SBOMs to.
    """
    await asyncio.gather(
        *[update_component_sboms(component, destination) for component in snapshot.components]
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
        type=str,
        help="Path to the directory to save the updated SBOM files.",
    )
    args = parser.parse_args()

    snapshot = await sbomlib.make_snapshot(args.snapshot_path)
    asyncio.run(update_sboms(snapshot, args.output_path))


if __name__ == "__main__":
    asyncio.run(main())
