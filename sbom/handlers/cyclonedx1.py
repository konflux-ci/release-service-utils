from enum import Enum

from packageurl import PackageURL

from sbom.logging import get_sbom_logger
from sbom.sbomlib import (
    Component,
    Image,
    SBOMError,
    SBOMHandler,
    construct_purl_object,
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

    def update_sbom(self, component: Component, image: Image, sbom: dict) -> None:
        self._bump_version(sbom)
        self._update_metadata_component(component, sbom)

        for cdx_component in sbom.get("components", []):
            if cdx_component.get("type") != "container":
                continue

            purl = cdx_component.get("purl")
            if purl is None or get_purl_digest(purl) != image.digest:
                continue

            self._update_container_component(component, cdx_component, update_tags=True)

    def _bump_version(self, sbom: dict) -> None:
        """
        Bump the CDX version to 1.6, so we can populate the fields relevant to
        tags. This is legal, because CycloneDX v1.X is forward-compatible (all
        1.4 and 1.5 boms are valid 1.6 boms).
        """
        # This is here to make sure an error is raised if this class is
        # updated for CDX 1.7.
        if sbom["specVersion"] not in ["1.4", "1.5", "1.6"]:
            raise SBOMError("Attempted to downgrade an SBOM.")

        sbom["$schema"] = "http://cyclonedx.org/schema/bom-1.6.schema.json"
        sbom["specVersion"] = "1.6"

    def _update_component_purl_identity(
        self,
        kflx_component: Component,
        cdx_component: dict,
        base_purl: PackageURL,
    ) -> None:
        """
        Update the evidence.identity field of a CDX component to contain PURLs
        with all tags.

        Args:
            kflx_component (Component): associated Konflux component
            cdx_component (dict): parsed CDX component object
        """
        if len(kflx_component.tags) <= 1:
            return

        new_identity = []
        for tag in kflx_component.tags:
            if isinstance(base_purl.qualifiers, dict):
                base_purl.qualifiers["tag"] = tag

            new_identity.append({"field": "purl", "concludedValue": base_purl.to_string()})

        if "evidence" not in cdx_component:
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
        self, kflx_component: Component, cdx_component: dict, update_tags: bool
    ) -> None:
        """
        Update a CDX component with the container type in-situ based on the
        passed Konflux component.

        Args:
            kflx_component (Component): associated Konflux component
            cdx_component (dict): parsed CDX component object
            update_tags (bool): flag determining whether to add the
                evidence.identity field to the component
        """
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
        new_purl = construct_purl_object(kflx_component.repository, digest, arch=arch, tag=tag)

        cdx_component["purl"] = new_purl.to_string()

        if update_tags:
            self._update_component_purl_identity(kflx_component, cdx_component, new_purl)

    def _update_metadata_component(self, kflx_component: Component, sbom: dict) -> None:
        """
        Updates the metadata component of the SBOM in-situ based on the Konflux component.

        Args:
            kflx_component (Component): associated Konflux component
            sbom (dict): the parsed CDX sbom to update
        """
        component = sbom.get("metadata", {}).get("component", {})
        self._update_container_component(kflx_component, component, update_tags=False)

        if "metadata" in sbom:
            sbom["metadata"]["component"] = component
        else:
            metadata = {"component": component}
            sbom["metadata"] = metadata
