#!/usr/bin/env python3
"""
This script updates the purls in component-level SBOMs with release time info.
"""
import argparse
import glob
import json
import logging
import os
from collections import defaultdict
from typing import DefaultDict, Dict, List
import re

from packageurl import PackageURL

LOG = logging.getLogger("update_component_sbom")


def get_component_to_purls_map(images: List[Dict]) -> Dict[str, List[str]]:
    """
        Get dictionary mapping component names to list of image purls.

        If the image is single arch, just use the existing purls.
        If the image is multi-arch, the purl formats are as follows (SPDX only):
        - The index package has one purl with the index sha, and no arch info.
        - The child packages have one purl with the index sha and arch info, and one purl with
          the child image sha and no arch info.

    Args:
        images: List of image metadata from the given data.json.

    Returns:
        Dictionary mapping of component names to list of purls.
    """
    component_purls: DefaultDict[str, List[str]] = defaultdict(list)

    for image in images:
        component = image["component"]
        purl = image["purl"]
        arch = image.get("arch")
        multiarch = image.get("multiarch", False)

        if multiarch and arch:
            # replace sha for index purl
            index_sha = image.get("imageSha")
            if index_sha:
                index_purl = re.sub("sha256%3A.*\\?", f"sha256%3A{index_sha}?", purl)

            # the index purl needs no arch info
            component_purls[component] = [re.sub("arch=.*&|&arch=.*$", "", index_purl)]

            component_purls[f"{component}_{arch}"].append(index_purl)
            # remove arch from child image digest, since it's already in index purl
            component_purls[f"{component}_{arch}"].append(
                re.sub("arch=.*&|&arch=.*$", "", purl)
            )
        else:
            component_purls[component].append(purl)

    LOG.debug("Component to purl mapping: %s", component_purls)
    return dict(component_purls)


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


def get_image_pullspec_from_purl(purl: str) -> str:
    """
    Parse the image purl to get the image pullspec.

    The pullspec is made of the repository URL and digest that is available as
    the version in the purl.

    Args:
        purl (str): A package URL for an image.

    Returns:
        str: Image pullspec with repository URL and digest.
    """
    parsed_purl = PackageURL.from_string(purl)
    repository = parsed_purl.qualifiers["repository_url"]
    return f"{repository}@{parsed_purl.version}"


def update_spdx_sbom(sbom: Dict, component_to_purls_map: Dict[str, List[str]]) -> None:
    """
    Update the purl in an SBOM with SPDX format and set the SBOM document name
    Args:
        sbom: SPDX SBOM file to update.
        component_to_purls_map: dictionary mapping of component names to list of purls.
    """
    LOG.info("Updating SPDX sbom")
    for package in sbom["packages"]:
        if package["name"] in component_to_purls_map:
            # Remove existing purls that contain internal repo info
            package["externalRefs"] = list(
                filter(lambda n: n.get("referenceType") != "purl", package["externalRefs"])
            )
            purls = component_to_purls_map[package["name"]]
            purl_external_refs = [
                {
                    "referenceCategory": "PACKAGE-MANAGER",
                    "referenceType": "purl",
                    "referenceLocator": purl,
                }
                for purl in purls
            ]
            package["externalRefs"].extend(purl_external_refs)

            # Set the SBOM document name to the first image pullspec
            sbom["name"] = get_image_pullspec_from_purl(purls[0])


def update_sboms(data_path: str, input_path: str, output_path: str) -> None:
    """
    Update all SBOM files in the given input_path directory, and save the updated SBOMs to the
    output_path directory
    Args:
        data_path: path to data.json file containing image metadata.
        input_path: path to directory holding SBOMs files to be updated.
        output_path: path to directory to save updated SBOMs.
    """
    with open(data_path, "r") as data_file:
        data = json.load(data_file)

    component_to_purls_map = get_component_to_purls_map(
        data["releaseNotes"]["content"].get("images", [])
    )
    # get all json files in input dir
    input_jsons = glob.glob(os.path.join(input_path, "*.json"))
    # loop through files
    LOG.info("Found %s json files in input directory: %s", len(input_jsons), input_jsons)
    for i in input_jsons:
        with open(i, "r") as input_file:
            sbom = json.load(input_file)

        if sbom.get("bomFormat") == "CycloneDX":
            update_cyclonedx_sbom(sbom, component_to_purls_map)
        elif "spdxVersion" in sbom:
            update_spdx_sbom(sbom, component_to_purls_map)
        else:
            continue

        output_filename = os.path.join(output_path, os.path.basename(i))
        LOG.info("Saving updated SBOM to %s", output_filename)
        with open(output_filename, "w") as output_file:
            json.dump(sbom, output_file)


def main():
    parser = argparse.ArgumentParser(
        prog="update-component-sbom",
        description="Update component SBOM purls with release info.",
    )
    parser.add_argument(
        "--data-path", required=True, type=str, help="Path to the input data in JSON format."
    )
    parser.add_argument(
        "--input-path",
        required=True,
        type=str,
        help="Path to the directory holding the SBOM files to be updated.",
    )
    parser.add_argument(
        "--output-path",
        required=True,
        type=str,
        help="Path to the directory to save the updated SBOM files.",
    )

    args = parser.parse_args()

    update_sboms(args.data_path, args.input_path, args.output_path)


if __name__ == "__main__":
    main()
