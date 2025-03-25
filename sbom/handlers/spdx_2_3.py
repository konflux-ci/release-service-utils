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


class SPDX_2_3(SBOMHandler):
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
    def _get_updated_index_purl(
        cls, package: dict, repository: str, index_digest: str, tag: Optional[str]
    ) -> str:
        """
        Constructs a PackageURL for an index image with updated information.
        """
        # TODO:
        # this assumes that index and image packages are in positions 0 and 1 respectively
        original_index_ref = package["externalRefs"][0]
        original_index_purl = original_index_ref["referenceLocator"]
        arch = get_purl_arch(original_index_purl)
        return construct_purl(repository, index_digest, arch, tag=tag)

    @classmethod
    def _get_updated_multiarch_image_purl(
        cls, package: dict, repository: str, tag: Optional[str]
    ) -> str:
        """
        Constructs a PackageURL for an arch-specific image with updated information.
        """
        # TODO:
        # this assumes that index and image packages are in positions 0 and 1 respectively
        original_image_ref = package["externalRefs"][1]
        original_image_purl = original_image_ref["referenceLocator"]
        digest = get_purl_digest(original_image_purl)
        return construct_purl(repository, digest, tag=tag)

    @classmethod
    def _get_updated_image_purl(
        cls, package: dict, repository: str, tag: Optional[str]
    ) -> str:
        """
        Constructs a PackageURL for a single-arch image with updated information.
        """
        original_image_ref = package["externalRefs"][0]
        original_image_purl = original_image_ref["referenceLocator"]
        digest = get_purl_digest(original_image_purl)
        return construct_purl(repository, digest, tag=tag)

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

        index_purl = construct_purl(
            component.repository, index.digest, tag=component.unique_tag
        )
        index_package["externalRefs"] = [cls._make_purl_ref(index_purl)]

        for package in sbom["packages"]:
            if not cls._is_relevant(package, index):
                continue

            index_purl = cls._get_updated_index_purl(
                package, component.repository, index.digest, component.unique_tag
            )
            image_purl = cls._get_updated_multiarch_image_purl(
                package, component.repository, component.unique_tag
            )

            package["externalRefs"] = [
                cls._make_purl_ref(index_purl),
                cls._make_purl_ref(image_purl),
            ]

    @classmethod
    def _update_image_sbom(cls, component: Component, image: Image, sbom: dict) -> None:
        """
        Update the SBOM of an arch-specific image in a repository.
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

        image_purl = cls._get_updated_image_purl(
            image_package, component.repository, component.unique_tag
        )
        image_package["externalRefs"] = [cls._make_purl_ref(image_purl)]

    @classmethod
    def update_sbom(
        cls, component: Component, image: Union[IndexImage, Image], sbom: dict
    ) -> None:
        if isinstance(image, IndexImage):
            cls._update_index_image_sbom(component, image, sbom)
        elif isinstance(image, Image):
            cls._update_image_sbom(component, image, sbom)
