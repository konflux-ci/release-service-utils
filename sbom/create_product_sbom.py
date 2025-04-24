#!/usr/bin/env python3
"""
This script creates a product-level SBOM based on the merged data file and a
mapped snapshot spec.

Example usage:
$ create_product_sbom --data-path data.json --snapshot-path snapshot.json \
    --output-path product_sbom.json
"""
import uuid
from datetime import datetime, timezone
import argparse
from typing import List, Union
from pathlib import Path
import asyncio

import pydantic as pdc

from spdx_tools.spdx.model.actor import Actor, ActorType
from spdx_tools.spdx.model.checksum import Checksum, ChecksumAlgorithm
from spdx_tools.spdx.model.document import CreationInfo, Document
from spdx_tools.spdx.model.package import (
    ExternalPackageRef,
    ExternalPackageRefCategory,
    Package,
)
from spdx_tools.spdx.model.relationship import Relationship, RelationshipType
from spdx_tools.spdx.model.spdx_no_assertion import SpdxNoAssertion
from spdx_tools.spdx.writer.write_anything import write_file

from sbom import sbomlib
from sbom.logging import get_sbom_logger, setup_sbom_logger
from sbom.sbomlib import Component, Snapshot, construct_purl

logger = get_sbom_logger()


class ReleaseNotes(pdc.BaseModel):
    """
    Pydantic model representing the release notes.
    """

    product_name: str
    product_version: str
    cpe: Union[str, List[str]] = pdc.Field(union_mode="left_to_right")


class ReleaseData(pdc.BaseModel):
    """
    Pydantic model representing the merged data file.
    """

    release_notes: ReleaseNotes = pdc.Field(alias="releaseNotes")


def create_product_package(product_elem_id: str, release_notes: ReleaseNotes) -> Package:
    """Create SPDX package corresponding to the product."""
    if isinstance(release_notes.cpe, str):
        cpes = [release_notes.cpe]
    else:
        cpes = release_notes.cpe

    refs = [
        ExternalPackageRef(
            category=ExternalPackageRefCategory.SECURITY,
            reference_type="cpe22Type",
            locator=cpe,
        )
        for cpe in cpes
    ]

    return Package(
        spdx_id=product_elem_id,
        name=release_notes.product_name,
        version=release_notes.product_version,
        download_location=SpdxNoAssertion(),
        supplier=Actor(ActorType.ORGANIZATION, "Red Hat"),
        license_declared=SpdxNoAssertion(),
        files_analyzed=False,
        external_references=refs,
    )


def create_product_relationship(doc_elem_id: str, product_elem_id: str) -> Relationship:
    """Create SPDX relationship corresponding to the product SPDX package."""
    return Relationship(
        spdx_element_id=doc_elem_id,
        relationship_type=RelationshipType.DESCRIBES,
        related_spdx_element_id=product_elem_id,
    )


def get_component_packages(components: List[Component]) -> List[Package]:
    """
    Get a list of SPDX packages - one per each component.

    Each component can have multiple external references - purls.
    """
    packages = []
    for component in components:
        checksum = component.image.digest.split(":", 1)[1]

        purls = [
            construct_purl(component.repository, component.image.digest, tag=tag)
            for tag in component.tags
        ]

        packages.append(
            Package(
                spdx_id=f"SPDXRef-component-{component.name}",
                name=component.name,
                license_declared=SpdxNoAssertion(),
                download_location=SpdxNoAssertion(),
                files_analyzed=False,
                supplier=Actor(ActorType.ORGANIZATION, "Red Hat"),
                external_references=[
                    ExternalPackageRef(
                        category=ExternalPackageRefCategory.PACKAGE_MANAGER,
                        reference_type="purl",
                        locator=purl,
                    )
                    for purl in purls
                ],
                checksums=[Checksum(algorithm=ChecksumAlgorithm.SHA256, value=checksum)],
            )
        )

    return packages


def get_component_relationships(
    product_elem_id: str, packages: List[Package]
) -> List[Relationship]:
    """Get SPDX relationship for each SPDX component package."""
    return [
        Relationship(
            spdx_element_id=package.spdx_id,
            relationship_type=RelationshipType.PACKAGE_OF,
            related_spdx_element_id=product_elem_id,
        )
        for package in packages
    ]


def create_sbom(release_notes: ReleaseNotes, snapshot: Snapshot) -> Document:
    """
    Create an SPDX document based on release notes and a snapshot.
    """
    doc_elem_id = "SPDXRef-DOCUMENT"
    product_elem_id = "SPDXRef-product"

    creation_info = CreationInfo(
        spdx_version="SPDX-2.3",
        spdx_id=doc_elem_id,
        name=f"{release_notes.product_name} {release_notes.product_version}",
        data_license="CC0-1.0",
        document_namespace=f"https://redhat.com/{uuid.uuid4()}.spdx.json",
        creators=[
            Actor(ActorType.ORGANIZATION, "Red Hat"),
            Actor(ActorType.TOOL, "Konflux CI"),
        ],
        created=datetime.now(timezone.utc),
    )

    product_package = create_product_package(product_elem_id, release_notes)
    product_relationship = create_product_relationship(doc_elem_id, product_elem_id)

    component_packages = get_component_packages(snapshot.components)
    component_relationships = get_component_relationships(product_elem_id, component_packages)

    return Document(
        creation_info=creation_info,
        packages=[product_package, *component_packages],
        relationships=[product_relationship, *component_relationships],
    )


def parse_release_notes(raw_json: str) -> ReleaseNotes:
    return ReleaseData.model_validate_json(raw_json).release_notes


def main() -> None:  # pragma: nocover
    """
    Script entrypoint.
    """
    parser = argparse.ArgumentParser(
        prog="create-product-sbom",
        description="Create product-level SBOM from merged data file"
        " and mapped snapshot spec.",
    )
    parser.add_argument(
        "--data-path",
        required=True,
        type=Path,
        help="Path to the merged data file in JSON format.",
    )
    parser.add_argument(
        "--snapshot-path",
        required=True,
        type=Path,
        help="Path to the input data in JSON format.",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        type=Path,
        help="Path to save the output SBOM in JSON format.",
    )

    args = parser.parse_args()
    setup_sbom_logger()

    try:
        snapshot = asyncio.run(sbomlib.make_snapshot(args.snapshot_path))
        with open(args.data_path, "r", encoding="utf-8") as fp:
            raw_json = fp.read()
            release_notes = parse_release_notes(raw_json)

            sbom = create_sbom(release_notes, snapshot)

        write_file(document=sbom, file_name=str(args.output_path), validate=True)
    except Exception:  # pylint: disable=broad-except
        logger.exception("Creation of the product-level SBOM failed.")
        raise


if __name__ == "__main__":  # pragma: nocover
    main()
