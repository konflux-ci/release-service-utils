"""
This module contains the SBOM handler for SPDX-2.3.
"""

from typing import Optional, Union

from sbom.handlers.abstract import SBOMHandler
from sbom.sbomlib import (
    Component,
    Image,
    IndexImage,
    SBOMError,
    construct_purl,
    get_purl_arch,
    get_purl_digest,
    make_reference,
    without_sha_header,
)


class SPDXVersion23(SBOMHandler):  # pylint: disable=too-few-public-methods
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
    def _find_purl_in_refs(cls, package: dict, digest: str) -> Optional[str]:
        """
        Tries and to find a purl in the externalRefs of a package the version of
        which is equal to the digest provided.
        """
        for ref in filter(lambda rf: rf["referenceType"] == "purl", package["externalRefs"]):
            purl = ref["referenceLocator"]
            if digest == get_purl_digest(purl):
                return purl

        return None

    @classmethod
    def _get_updated_external_refs(
        cls, digest: str, repository: str, tags: list[str], arch: Optional[str] = None
    ) -> list[dict]:
        """
        Gets new externalRefs value based on input information.
        """
        purls = (construct_purl(repository, digest, tag=tag, arch=arch) for tag in tags)
        return [cls._make_purl_ref(purl) for purl in purls]

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
    def _update_index_image_sbom(
        cls, component: Component, index: IndexImage, sbom: dict
    ) -> None:
        """
        Update the SBOM of an index image in a repository.
        """
        version = sbom["spdxVersion"]
        if version != cls.supported_version:
            raise ValueError(
                f"Called update on unsupported version {version}, "
                f"supported version is {cls.supported_version}"
            )

        sbom["name"] = make_reference(component.repository, index.digest)

        index_package = cls._find_image_package(sbom, index.digest)
        if not index_package:
            raise SBOMError(f"Could not find SPDX package for index {index}")

        index_package["externalRefs"] = cls._get_updated_external_refs(
            index.digest,
            component.repository,
            component.tags,
        )

        for package in sbom["packages"]:
            if not cls._is_relevant(package, index):
                continue

            original_purl = cls._find_purl_in_refs(package, index.digest)
            if original_purl is None:
                continue

            arch = get_purl_arch(original_purl)
            package["externalRefs"] = cls._get_updated_external_refs(
                index.digest, component.repository, component.tags, arch
            )

    @classmethod
    def _update_image_sbom(cls, component: Component, image: Image, sbom: dict) -> None:
        """
        Update the SBOM of single-arch image in a repository.
        """
        version = sbom["spdxVersion"]
        if version != cls.supported_version:
            raise ValueError(
                f"Called update on unsupported version {version}, "
                f"supported version is {cls.supported_version}"
            )

        sbom["name"] = make_reference(component.repository, image.digest)

        image_package = cls._find_image_package(sbom, image.digest)
        if not image_package:
            raise SBOMError(f"Could not find SPDX package in SBOM for image {image}")

        image_package["externalRefs"] = cls._get_updated_external_refs(
            image.digest, component.repository, component.tags
        )

    @classmethod
    def update_sbom(
        cls, component: Component, image: Union[IndexImage, Image], sbom: dict
    ) -> None:
        if isinstance(image, IndexImage):
            cls._update_index_image_sbom(component, image, sbom)
        elif isinstance(image, Image):
            cls._update_image_sbom(component, image, sbom)
