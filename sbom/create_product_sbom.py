#!/usr/bin/env python3
"""
This script creates a product-level SBOM based on releaseNotes in the merged
data.json file.
"""
import json
import uuid
from datetime import datetime, timezone
import argparse
from collections import defaultdict
from typing import DefaultDict, Dict, List


def create_product_package(name: str, version: str, cpe: str) -> Dict:
    """Create SPDX package corresponding to the product."""
    return {
        "SPDXID": "SPDXRef-product",
        "name": name,
        "versionInfo": version,
        "supplier": "Organization: Red Hat",
        "downloadLocation": "NOASSERTION",
        "externalRefs": [
            {
                "referenceCategory": "SECURITY",
                "referenceType": "cpe22Type",
                "referenceLocator": cpe,
            }
        ],
    }


def create_product_relationship() -> Dict:
    """Create SPDX relationship corresponding to the product SPDX package."""
    return {
        "spdxElementId": "SPDXRef-DOCUMENT",
        "relationshipType": "DESCRIBES",
        "relatedSpdxElement": "SPDXRef-product",
    }


def get_component_packages(images: List[Dict]) -> List[Dict]:
    """
    Get a list of SPDX packages - one per each component.

    Each component can have multiple external references - purls.
    """
    packages = []
    component_to_purls_map = get_component_to_purls_map(images)

    for i, (component, purls) in enumerate(component_to_purls_map.items()):
        SPDXID = f"SPDXRef-component-{i}"
        external_refs = [
            {
                "referenceCategory": "PACKAGE-MANAGER",
                "referenceType": "purl",
                "referenceLocator": purl,
            }
            for purl in purls
        ]

        package = {
            "SPDXID": SPDXID,
            "name": component,
            "downloadLocation": "NOASSERTION",
            "externalRefs": external_refs,
        }
        packages.append(package)

    return packages


def get_component_to_purls_map(images: List[Dict]) -> Dict[str, List[str]]:
    """Get dictionary mapping component names to list of image purls."""
    component_purls: DefaultDict[str, List[str]] = defaultdict(list)

    for image in images:
        component = image["component"]
        purl = image["purl"]
        component_purls[component].append(purl)

    return dict(component_purls)


def get_component_relationships(packages: List[Dict]):
    """Get SPDX relationship for each SPDX component package."""
    return [
        {
            "spdxElementId": package["SPDXID"],
            "relationshipType": "PACKAGE_OF",
            "relatedSpdxElement": "SPDXRef-product",
        }
        for package in packages
    ]


def create_sbom(data_path: str) -> Dict:
    with open(data_path, "r") as fp:
        data = json.load(fp)
        release_notes = data["releaseNotes"]

    product_name = release_notes["product_name"]
    product_version = release_notes["product_version"]
    cpe = release_notes["cpe"]

    # per SPDX spec, this URI does not have to be accessible, it's only used to
    # uniquely identify this SPDX document.
    # https://spdx.github.io/spdx-spec/v2.3/document-creation-information/#65-spdx-document-namespace-field
    document_namespace = f"https://redhat.com/{uuid.uuid4()}.spdx.json"

    packages = [create_product_package(product_name, product_version, cpe)]
    relationships = [create_product_relationship()]

    component_packages = get_component_packages(release_notes["content"].get("images", []))
    component_relationships = get_component_relationships(component_packages)

    packages.extend(component_packages)
    relationships.extend(component_relationships)

    sbom = {
        "spdxVersion": "SPDX-2.3",
        "SPDXID": "SPDXRef-DOCUMENT",
        "dataLicense": "CC-BY-4.0",
        "documentNamespace": document_namespace,
        "creationInfo": {
            # E.g. 2024-10-21T20:52:36+00:00
            "created": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "creators": ["Organization: Red Hat", "Tool: Konflux CI"],
        },
        "name": f"{product_name} {product_version}",
        "packages": packages,
        "relationships": relationships,
    }

    return sbom


def main():
    parser = argparse.ArgumentParser(
        prog="create-product-sbom", description="Create product-level SBOM from releaseNotes."
    )

    parser.add_argument(
        "--data-path", required=True, type=str, help="Path to the input data in JSON format."
    )
    parser.add_argument(
        "--output-path",
        required=True,
        type=str,
        help="Path to save the output SBOM in JSON format.",
    )

    args = parser.parse_args()

    sbom = create_sbom(args.data_path)
    with open(args.output_path, "w") as fp:
        json.dump(sbom, fp)


if __name__ == "__main__":
    main()
