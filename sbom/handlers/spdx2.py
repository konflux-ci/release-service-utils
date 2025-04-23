"""
This module contains the SBOM handler for SPDX version 2 SBOMs.
"""

from enum import Enum
from typing import Optional, Union, Any

from packageurl import PackageURL

from sbom.logging import get_sbom_logger
from sbom.sbomlib import (
    Component,
    Image,
    IndexImage,
    SBOMError,
    SBOMHandler,
    construct_purl,
    get_purl_arch,
    get_purl_digest,
    make_reference,
    without_sha_header,
)

logger = get_sbom_logger()


class SPDXPackage:
    """
    Wrapper class for easier SPDX package manipulation.
    """

    def __init__(self, package: Any) -> None:
        self.package = package

    @property
    def external_refs(self) -> list[dict[str, Any]]:
        """
        Get the externalRefs field of the package.
        """
        return self.package.get("externalRefs", [])

    @external_refs.setter
    def external_refs(self, value: list[Any]) -> None:
        self.package["externalRefs"] = value

    @property
    def spdxid(self) -> str:
        return self.package.get("SPDXID", "UNKNOWN")

    @property
    def checksums(self) -> list[dict[str, Any]]:
        """
        Get the checksums field of the package.
        """
        return self.package.get("checksums", [])

    @property
    def sha256_checksum(self) -> Optional[str]:
        """
        Extracts a sha256 checksum from an SPDX package. Returns None if no such
        checksum is found.
        """
        checksums = self.checksums
        if checksums is None:
            return None

        for checksum in checksums:
            if checksum.get("algorithm") == "SHA256":
                return checksum.get("checksumValue")

        return None

    def update_external_refs(
        self,
        digest: str,
        repository: str,
        tags: list[str],
        arch: Optional[str] = None,
    ) -> None:
        """
        Update the external refs of an SPDX package by creating new OCI PURL
        references and stripping all old OCI PURL references. Other types of
        externalRefs are preserved.
        """
        new_oci_refs = SPDXPackage._get_updated_oci_purl_external_refs(
            digest,
            repository,
            tags,
            arch=arch,
        )

        self._strip_oci_purls_external_refs()
        self.external_refs[:0] = new_oci_refs

    def _strip_oci_purls_external_refs(self) -> None:
        """
        Remove all OCI purl externalRefs from a package.
        """

        def is_oci_purl_ref(ref: dict) -> bool:
            ptype = ref.get("referenceType")
            if ptype != "purl":
                return False
            purl_str = ref.get("referenceLocator")
            if purl_str is None:
                return False

            purl = PackageURL.from_string(purl_str)
            return purl.type == "oci"

        new_external_refs = [ref for ref in self.external_refs if not is_oci_purl_ref(ref)]
        self.external_refs = new_external_refs

    @staticmethod
    def _get_updated_oci_purl_external_refs(
        digest: str, repository: str, tags: list[str], arch: Optional[str] = None
    ) -> list[dict]:
        """
        Gets new oci purl externalRefs value based on input information.
        """
        purls = (construct_purl(repository, digest, tag=tag, arch=arch) for tag in tags)
        return [SPDXPackage._make_purl_ref(purl) for purl in purls]

    @staticmethod
    def _make_purl_ref(purl: str) -> dict[str, str]:
        """
        Create an SPDX externalRefs field from a PackageURL.
        """
        return {
            "referenceCategory": "PACKAGE-MANAGER",
            "referenceType": "purl",
            "referenceLocator": purl,
        }


class SPDXSpec(Enum):
    """
    Enum containing all recognized SPDX versions.
    """

    SPDX_2_0 = "SPDX-2.0"
    SPDX_2_1 = "SPDX-2.1"
    SPDX_2_2 = "SPDX-2.2"
    SPDX_2_2_1 = "SPDX-2.2.1"
    SPDX_2_2_2 = "SPDX-2.2.2"
    SPDX_2_3 = "SPDX-2.3"


class SPDXVersion2(SBOMHandler):  # pylint: disable=too-few-public-methods
    """
    Class containing methods for SPDX v2.x SBOM manipulation.
    """

    supported_versions = [
        SPDXSpec.SPDX_2_0,
        SPDXSpec.SPDX_2_1,
        SPDXSpec.SPDX_2_2,
        SPDXSpec.SPDX_2_2_1,
        SPDXSpec.SPDX_2_2_2,
        SPDXSpec.SPDX_2_3,
    ]

    @classmethod
    def supports(cls, sbom: dict) -> bool:
        raw = sbom.get("spdxVersion")
        if raw is None:
            return False

        try:
            spec = SPDXSpec(raw)
        except ValueError:
            logger.warning("SPDX spec %s not recognized.")
            return False

        return spec in cls.supported_versions

    @classmethod
    def _find_purl_in_refs(cls, package: SPDXPackage, digest: str) -> Optional[str]:
        """
        Tries to find a purl in the externalRefs of a package the version of
        which matches the passed digest.
        """
        for ref in filter(lambda rf: rf["referenceType"] == "purl", package.external_refs):
            purl = ref["referenceLocator"]
            if digest == get_purl_digest(purl):
                return purl

        return None

    @classmethod
    def _find_image_package(cls, sbom: dict, digest: str) -> Optional[SPDXPackage]:
        """
        Find the SPDX package for a digest, based on the package checksum.
        """
        for package in map(SPDXPackage, sbom.get("packages", [])):
            if without_sha_header(digest) == package.sha256_checksum:
                return package

        return None

    @classmethod
    def _update_index_image_sbom(
        cls, component: Component, index: IndexImage, sbom: dict
    ) -> None:
        """
        Update the SBOM of an index image in a repository.
        """
        sbom["name"] = make_reference(component.repository, index.digest)

        index_package = cls._find_image_package(sbom, index.digest)
        if not index_package:
            raise SBOMError(f"Could not find SPDX package for index {index}")

        index_package.update_external_refs(
            index.digest,
            component.repository,
            component.tags,
        )

        for image in index.children:
            package = cls._find_image_package(sbom, image.digest)
            if package is None:
                logger.warning("Could not find SPDX package for %s.", image.digest)
                continue

            original_purl = cls._find_purl_in_refs(package, image.digest)
            if original_purl is None:
                logger.warning(
                    "Could not find OCI PURL for %s in package %s for index %s.",
                    image.digest,
                    package.spdxid,
                    index.digest,
                )
                continue

            arch = get_purl_arch(original_purl)
            package.update_external_refs(
                image.digest,
                component.repository,
                component.tags,
                arch=arch,
            )

    @classmethod
    def _update_image_sbom(cls, component: Component, image: Image, sbom: dict) -> None:
        """
        Update the SBOM of single-arch image in a repository.
        """
        sbom["name"] = make_reference(component.repository, image.digest)

        image_package = cls._find_image_package(sbom, image.digest)
        if not image_package:
            raise SBOMError(f"Could not find SPDX package in SBOM for image {image.digest}")

        image_package.update_external_refs(
            image.digest,
            component.repository,
            component.tags,
        )

    def update_sbom(
        self, component: Component, image: Union[IndexImage, Image], sbom: dict
    ) -> None:
        if isinstance(image, IndexImage):
            # breakpoint()
            SPDXVersion2._update_index_image_sbom(component, image, sbom)
        elif isinstance(image, Image):
            SPDXVersion2._update_image_sbom(component, image, sbom)
