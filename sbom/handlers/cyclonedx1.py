from functools import total_ordering
from typing import Union, Optional
from enum import Enum
from packaging.version import Version


from sbom.logging import get_sbom_logger
from sbom.sbomlib import (
    Component,
    IndexImage,
    Image,
    SBOMHandler,
    construct_purl,
    get_purl_arch,
    get_purl_digest,
)


logger = get_sbom_logger()


@total_ordering
class CDXSpec(Enum):
    """
    Enum containing all recognized CycloneDX versions.
    """

    v1_4 = "1.4"
    v1_5 = "1.5"
    v1_6 = "1.6"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, CDXSpec):
            return Version(self.value) == Version(other.value)

        return NotImplemented

    def __lt__(self, other: object):
        if isinstance(other, CDXSpec):
            return Version(self.value) < Version(other.value)

        return NotImplemented


class CycloneDXVersion1(SBOMHandler):
    supported_versions = [
        CDXSpec.v1_4,
        CDXSpec.v1_5,
        CDXSpec.v1_6,
    ]

    def __init__(self, version: str) -> None:
        self.version = CDXSpec(version)

    @classmethod
    def supports(cls, sbom: dict) -> bool:
        if "bomFormat" not in sbom:
            return False

        raw = sbom.get("specVersion")
        if raw is None:
            return False

        try:
            spec = CDXSpec(raw)
        except ValueError:
            logger.warning("CDX spec %s not recognized.")
            return False

        return spec in cls.supported_versions

    def update_sbom(
        self, component: Component, image: Union[IndexImage, Image], sbom: dict
    ) -> None:
        self._update_sbom(component, image, sbom)

    def _update_component_purl_identity(
        self,
        kflx_component: Component,
        arch: Optional[str],
        cdx_component: dict,
    ) -> None:
        if self.version < CDXSpec.v1_6:
            logger.warning(
                "Updating the evidence.identity field is only supported for CDX version 1.6."
            )
            return

        if len(kflx_component.tags) <= 1:
            return

        for tag in kflx_component.tags:
            purl = construct_purl(
                kflx_component.repository, kflx_component.image.digest, arch=arch, tag=tag
            )
            purl_identity = {"field": "purl", "concludedValue": purl}

        if cdx_component.get("evidence") is None:
            cdx_component["evidence"] = {}

        evidence = cdx_component["evidence"]
        identity = evidence.get("identity", [])

        # The identity can either be an array or a single object. In both cases
        # we preserve the original identity.
        if isinstance(identity, list):
            identity.extend(purl_identity)
        else:
            evidence["identity"] = [identity, *purl_identity]

    def _update_container_component(
        self, kflx_component: Component, cdx_component: dict
    ) -> None:
        if cdx_component.get("type") != "container":
            logger.warning(
                'Called update method on CDX package with type %s instead of "container".'
            )
            return

        purl = cdx_component.get("purl")
        if not purl:
            return

        arch = get_purl_arch(purl)
        digest = get_purl_digest(purl)
        tag = kflx_component.tags[0] if kflx_component.tags else None
        new_purl = construct_purl(kflx_component.repository, digest, arch=arch, tag=tag)
        cdx_component["purl"] = new_purl

        # Only CDX 1.6 supports multiple identity objects
        if self.version >= CDXSpec.v1_6:
            self._update_component_purl_identity(kflx_component, arch, cdx_component)

        if isinstance(kflx_component.image, IndexImage):
            variants = cdx_component.get("pedigree", {}).get("variants", [])
            child_digests = [img.digest for img in kflx_component.image.children]
            for component in variants:
                purl = component.get("purl")
                if purl is None or get_purl_digest(purl) not in child_digests:
                    continue

                self._update_container_component(kflx_component, component)

    def _update_metadata_component(self, kflx_component: Component, sbom: dict) -> None:
        component = sbom.get("metadata", {}).get("component", {})
        self._update_container_component(kflx_component, component)

        if "metadata" in sbom:
            sbom["metadata"]["component"] = component
        else:
            metadata = {"component": component}
            sbom["metadata"] = metadata

    def _update_sbom(
        self, kflx_component: Component, image: Union[IndexImage, Image], sbom: dict
    ) -> None:
        self._update_metadata_component(kflx_component, sbom)

        for cdx_component in sbom.get("components", []):
            if cdx_component.get("type") != "container":
                continue

            purl = cdx_component.get("purl")
            if purl is None or get_purl_digest(purl) != image.digest:
                continue

            self._update_container_component(kflx_component, cdx_component)
