from typing import Union, Optional
from enum import Enum


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


class CDXSpec(Enum):
    """
    Enum containing all recognized CycloneDX versions.
    """

    v1_4 = "1.4"
    v1_5 = "1.5"
    v1_6 = "1.6"


class CycloneDXVersion1(SBOMHandler):
    supported_versions = [
        CDXSpec.v1_4,
        CDXSpec.v1_5,
        CDXSpec.v1_6,
    ]

    def __init__(self) -> None:
        pass

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
            logger.warning("CDX spec %s not recognized.", raw)
            return False

        return spec in cls.supported_versions

    def update_sbom(
        self, component: Component, image: Union[IndexImage, Image], sbom: dict
    ) -> None:
        self._bump_version(sbom)
        self._update_metadata_component(component, sbom)

        for cdx_component in sbom.get("components", []):
            if cdx_component.get("type") != "container":
                continue

            purl = cdx_component.get("purl")
            if purl is None or get_purl_digest(purl) != image.digest:
                continue

            self._update_container_component(component, cdx_component)

    def _bump_version(self, sbom: dict) -> None:
        """
        Bump the CDX version to 1.6, so we can populate the fields relevant to tags.
        This is legal, because CycloneDX v1.X is forward-compatible.
        """
        sbom["$schema"] = "http://cyclonedx.org/schema/bom-1.6.schema.json"
        sbom["specVersion"] = "1.6"

    def _update_component_purl_identity(
        self,
        kflx_component: Component,
        arch: Optional[str],
        cdx_component: dict,
    ) -> None:
        if len(kflx_component.tags) <= 1:
            return

        new_identity = []
        for tag in kflx_component.tags:
            purl = construct_purl(
                kflx_component.repository, kflx_component.image.digest, arch=arch, tag=tag
            )
            new_identity.append({"field": "purl", "concludedValue": purl})

        if cdx_component.get("evidence") is None:
            cdx_component["evidence"] = {}

        evidence = cdx_component["evidence"]
        identity = evidence.get("identity", [])

        # The identity can either be an array or a single object. In both cases
        # we preserve the original identity.
        if isinstance(identity, list):
            identity.extend(new_identity)
            evidence["identity"] = identity
        else:
            evidence["identity"] = [identity, *new_identity]

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

        self._update_component_purl_identity(kflx_component, arch, cdx_component)

        if isinstance(kflx_component.image, IndexImage):
            variants = cdx_component.get("pedigree", {}).get("variants", [])
            child_digests = [img.digest for img in kflx_component.image.children]
            for variant in variants:
                purl = variant.get("purl")
                if purl is None or get_purl_digest(purl) not in child_digests:
                    continue

                self._update_container_component(kflx_component, variant)

    def _update_metadata_component(self, kflx_component: Component, sbom: dict) -> None:
        component = sbom.get("metadata", {}).get("component", {})
        self._update_container_component(kflx_component, component)

        if "metadata" in sbom:
            sbom["metadata"]["component"] = component
        else:
            metadata = {"component": component}
            sbom["metadata"] = metadata
