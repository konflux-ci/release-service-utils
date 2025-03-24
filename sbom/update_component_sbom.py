#!/usr/bin/env python3
"""
This script updates the purls in component-level SBOMs with release time info.
"""
import argparse
import asyncio
import json
import logging
import os
from typing import Dict, List, Optional, Union
from pathlib import Path
import tempfile
import aiofiles

from packageurl import PackageURL

import sbomlib
from sbomlib import (
    Component,
    SBOMError,
    Snapshot,
    Image,
    IndexImage,
    construct_purl,
    make_reference,
)

LOG = logging.getLogger("update_component_sbom")


def update_cyclonedx_sbom(sbom: Dict, component_to_purls_map: Dict[str, List[str]]) -> None:
    """
    Update the purl in an SBOM with CycloneDX format
    Args:
        sbom: CycloneDX SBOM file to update.
        component_to_purls_map: dictionary mapping of component names to list of purls.
    """
    LOG.info("Updating CycloneDX sbom")

    component_name = sbom["metadata"]["component"]["name"]
    if component_name in component_to_purls_map:
        # only one purl is supported for CycloneDX
        sbom["metadata"]["component"]["purl"] = component_to_purls_map[component_name][0]

    for component in sbom["components"]:
        if component["name"] in component_to_purls_map:
            # only one purl is supported for CycloneDX
            component["purl"] = component_to_purls_map[component["name"]][0]


async def fetch_sbom(destination_dir: Path, reference: str) -> Path:
    """
    Download an SBOM for an image reference to a destination directory.
    """
    with tempfile.NamedTemporaryFile(mode="+w") as authfile:
        if not sbomlib.get_oci_auth_file(
            reference, Path(os.path.expanduser("~/.docker/config.json")), authfile
        ):
            raise RuntimeError(f"Could not find auth for {reference}")

        code, stdout, stderr = await sbomlib.run_async_subprocess(
            ["cosign", "download", "sbom", reference], env={"DOCKER_CONFIG": authfile.name}
        )

    if code != 0:
        raise RuntimeError(f"Failed to fetch SBOM {reference}: {stderr}")

    digest = reference.split("@", 1)[1]
    path = destination_dir.joinpath(digest)
    async with aiofiles.open(path, "wb") as file:
        await file.write(stdout)

    return path


def without_sha_header(digest: str) -> str:
    """
    Returns a digest without the "sha256:" header.
    """
    return digest.removeprefix("sha256:")


def get_purl_arch(purl_str: str) -> Optional[str]:
    """
    Get the arch qualifier from a PackageURL.
    """
    purl = PackageURL.from_string(purl_str).to_dict()
    return purl["qualifiers"].get("arch")


def get_purl_digest(purl_str: str) -> str:
    """
    Get the image digest from a PackageURL.
    """
    purl = PackageURL.from_string(purl_str)
    if purl.version is None:
        raise ValueError()
    return purl.version


class SPDX_2_3:
    """
    Class containing methods for SPDX v2.3 SBOM manipulation.
    """

    supported_version = "SPDX-2.3"

    @classmethod
    def _make_purl_ref(cls, purl: str) -> dict[str, str]:
        """
        Create an SPDX externalRefs field from a PackageURL.
        """
        return {
            "referenceCategory": "PACKAGE-MANAGER",
            "referenceType": "purl",
            "referenceLocator": purl,
        }

    @classmethod
    def _get_updated_index_purl(cls, package: dict, repository: str, index_digest: str) -> str:
        """
        Constructs a PackageURL for an index image with updated information.
        """
        # TODO:
        # this assumes that index and image packages are in positions 0 and 1 respectively
        original_index_ref = package["externalRefs"][0]
        original_index_purl = original_index_ref["referenceLocator"]
        arch = get_purl_arch(original_index_purl)
        return construct_purl(repository, index_digest, arch)

    @classmethod
    def _get_updated_multiarch_image_purl(cls, package: dict, repository: str) -> str:
        """
        Constructs a PackageURL for an arch-specific image with updated information.
        """
        # TODO:
        # this assumes that index and image packages are in positions 0 and 1 respectively
        original_image_ref = package["externalRefs"][1]
        original_image_purl = original_image_ref["referenceLocator"]
        digest = get_purl_digest(original_image_purl)
        return construct_purl(repository, digest)

    @classmethod
    def _get_updated_image_purl(cls, package: dict, repository: str) -> str:
        """
        Constructs a PackageURL for a single-arch image with updated information.
        """
        original_image_ref = package["externalRefs"][0]
        original_image_purl = original_image_ref["referenceLocator"]
        digest = get_purl_digest(original_image_purl)
        return construct_purl(repository, digest)

    @classmethod
    def _find_image_package(cls, sbom: dict, digest: str) -> Optional[dict]:
        """
        Find the SPDX package for a digest, based on the package checksum.
        """
        for package in sbom.get("packages", []):
            checksums = package.get("checksums")
            if checksums is None:
                continue

            package_digest = None
            for checksum in checksums:
                if checksum.get("algorithm") == "SHA256":
                    package_digest = checksum.get("checksumValue")
                    break

            if without_sha_header(digest) == package_digest:
                return package

        return None

    @classmethod
    def _extract_sha256_checksum(cls, package: dict) -> Optional[str]:
        """
        Extracts a sha256 checksum from an SPDX package. Returns None if no such
        checksum is found.
        """
        checksums = package.get("checksums")
        if checksums is None:
            return None

        for checksum in checksums:
            if checksum.get("algorithm") == "SHA256":
                return checksum.get("checksumValue")

        return None

    @classmethod
    def _is_relevant(cls, package: dict, index: IndexImage) -> bool:
        """
        Determines whether an SPDX package should be updated based on its
        checksum. If the checksum is found in the child digests of the index
        image, the package should be updated.
        """
        sha256_checksum = cls._extract_sha256_checksum(package)
        if sha256_checksum is None:
            return False

        child_digests = [image.digest for image in index.children]
        digest = f"sha256:{sha256_checksum}"
        return digest in child_digests

    @classmethod
    def update_index_image_sbom(cls, repository: str, index: IndexImage, sbom: dict) -> None:
        """
        Update the SBOM of an index image in a repository.
        """
        version = sbom["spdxVersion"]
        if version != cls.supported_version:
            raise ValueError(
                f"Called update on unsupported version {version}, "
                f"supported version is {cls.supported_version}"
            )

        sbom["name"] = make_reference(repository, index.digest)

        index_package = cls._find_image_package(sbom, index.digest)
        if not index_package:
            raise SBOMError(f"Could not find SPDX package for index {index}")

        index_purl = construct_purl(repository, index.digest)
        index_package["externalRefs"] = [cls._make_purl_ref(index_purl)]

        for package in sbom["packages"]:
            if not cls._is_relevant(package, index):
                continue

            index_purl = cls._get_updated_index_purl(package, repository, index.digest)
            image_purl = cls._get_updated_multiarch_image_purl(package, repository)

            package["externalRefs"] = [
                cls._make_purl_ref(index_purl),
                cls._make_purl_ref(image_purl),
            ]

    @classmethod
    def update_image_sbom(cls, repository: str, image: Image, sbom: dict) -> None:
        """
        Update the SBOM of an arch-specific image in a repository.
        """
        version = sbom["spdxVersion"]
        if version != cls.supported_version:
            raise ValueError(
                f"Called update on unsupported version {version}, "
                f"supported version is {cls.supported_version}"
            )

        sbom["name"] = make_reference(repository, image.digest)

        image_package = cls._find_image_package(sbom, image.digest)
        if not image_package:
            raise SBOMError(f"Could not find SPDX package in SBOM for image {image}")

        image_purl = cls._get_updated_image_purl(image_package, repository)
        image_package["externalRefs"] = [cls._make_purl_ref(image_purl)]


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
    repository: str, image: Union[IndexImage, Image], destination: Path
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

    reference = f"{repository}@{image.digest}"
    sbom, sbom_path = await load_sbom(reference, destination)

    if sbom.get("spdxVersion") == "SPDX-2.3":
        if isinstance(image, IndexImage):
            SPDX_2_3.update_index_image_sbom(repository, image, sbom)
        else:
            SPDX_2_3.update_image_sbom(repository, image, sbom)
    elif sbom.get("bomFormat") == "CycloneDX":
        # TODO: implement
        pass
    else:
        raise SBOMError(f"Unsupported SBOM format for image {reference}.")

    await write_sbom(sbom, sbom_path)


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
            update_sbom(component.repository, index, destination),
        ]
        for child in index.children:
            update_tasks.append(update_sbom(component.repository, child, destination))

        await asyncio.gather(*update_tasks)
        return

    # Single arch image
    await update_sbom(component.repository, component.image, destination)


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
    parser = argparse.ArgumentParser(
        prog="update-component-sbom",
        description="Update component SBOM purls with release info.",
    )
    parser.add_argument(
        "--data-path", required=True, type=Path, help="Path to the data (RPA) in JSON format."
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

    snapshot = await sbomlib.make_snapshot(args.snapshot_path, args.data_path)
    asyncio.run(update_sboms(snapshot, args.output_path))


if __name__ == "__main__":
    asyncio.run(main())
