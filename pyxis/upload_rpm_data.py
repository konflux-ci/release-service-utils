#!/usr/bin/env python3
"""Upload rpm manifest to Pyxis

NOTE: Any change to this file that changes functionality should be posted for review in
#forum-metadata-guild on slack. No PR changing functionality should be removed without
approval from someone on that team.

This script will take Pyxis image ID and an sbom spdx file
on the input. It will inspect the sbom for the rpms and then push
data into Pyxis. There are two separate items that will be pushed:

1. RPM Manifest object
If an RPM Manifest already exists for the container
image, nothing is done as we assume it was already pushed by this
script.

2. content_sets field of ContainerImage object

Required env vars:
PYXIS_KEY_PATH
PYXIS_CERT_PATH

Optional env vars:
PYXIS_GRAPHQL_API
"""
import argparse
import json
import logging
import string
import os
from pathlib import Path
import time
from urllib.error import HTTPError
from packageurl import PackageURL

import pyxis

LOGGER = logging.getLogger("upload_rpm_data")
IGNORED_PACKAGES = ["gpg-pubkey"]


def upload_container_rpm_data_with_retry(
    graphql_api: str,
    image_id: str,
    sbom_path: str,
    retries: int = 3,
    backoff_factor: float = 5.0,
):
    """Call the upload_container_rpm_data function with retries"""
    last_err = RuntimeError()
    for attempt in range(retries):
        try:
            time.sleep(backoff_factor * attempt)
            upload_container_rpm_data(graphql_api, image_id, sbom_path)
            return
        except RuntimeError as e:
            LOGGER.warning(f"Attempt {attempt+1} failed.")
            last_err = e
        except HTTPError as e:
            if e.code == 504:
                LOGGER.warning(f"Attempt {attempt+1} failed with HTTPError code 504.")
                last_err = e
            else:
                raise e
    LOGGER.error("Out of attempts. Raising the error.")
    raise last_err


def upload_container_rpm_data(graphql_api: str, image_id: str, sbom_path: str):
    """Upload a Container Image RPM Manifest and content sets to Pyxis"""

    sbom_packages = load_sbom_packages(sbom_path)
    LOGGER.info(f"Loaded {len(sbom_packages)} packages from sbom file.")

    rpms, content_sets = construct_rpm_items_and_content_sets(sbom_packages)

    image = get_image_rpm_data(graphql_api, image_id)

    if image["rpm_manifest"] is not None and "_id" in image["rpm_manifest"]:
        # We assume that if the RPM Manifest already exists, it is accurate as the
        # entire object is added in one request.
        LOGGER.info("RPM manifest already exists for ContainerImage. Skipping...")
        rpm_manifest_id = image["rpm_manifest"]["_id"]
    else:
        rpm_manifest_id = create_image_rpm_manifest(graphql_api, image_id, rpms)
    LOGGER.info(f"RPM manifest ID: {rpm_manifest_id}")

    if image["content_sets"] is not None:
        LOGGER.info(
            f"Content sets for the image are already set, skipping: {image['content_sets']}"
        )
    elif not content_sets:
        LOGGER.info(
            "No content sets found in the sbom, skipping update of "
            "ContainerImage.content_sets field in Pyxis"
        )
    else:
        LOGGER.info(f"Updating ContainerImage.content_sets field in Pyxis to: {content_sets}")
        update_container_content_sets(graphql_api, image_id, content_sets)


def parse_arguments() -> argparse.Namespace:  # pragma: no cover
    """Parse CLI arguments

    :return: Dictionary of parsed arguments
    """

    parser = argparse.ArgumentParser(description="Upload RPM data to Pyxis via graphql")

    parser.add_argument(
        "--pyxis-graphql-api",
        default=os.environ.get("PYXIS_GRAPHQL_API", "https://graphql-pyxis.api.redhat.com/"),
        help="Pyxis Graphql endpoint.",
    )
    parser.add_argument(
        "--image-id",
        help="Pyxis container image ID. If omitted, sbom filename is used",
    )
    parser.add_argument("--sbom-path", help="Path to the sbom file", required=True)
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--retry",
        "-r",
        action="store_true",
        help="If set, retry the upload in case it fails",
    )
    return parser.parse_args()


def get_image_rpm_data(graphql_api: str, image_id: str) -> dict:
    """Get the Image's RPM Manifest id and content sets from Pyxis using GraphQL API

    This function uses the get_image graphql query to get the rpm_manifest
    id and content sets.
    """
    query = """
query ($id: ObjectIDFilterScalar!) {
    get_image(id: $id) {
        data {
            _id
            rpm_manifest {
                _id
            }
            content_sets
        }
        error {
            status
            detail
        }
    }
}
    """
    variables = {"id": image_id}
    body = {"query": query, "variables": variables}

    data = pyxis.graphql_query(graphql_api, body)
    image = data["get_image"]["data"]

    return image


def create_image_rpm_manifest(graphql_api: str, image_id: str, rpms: list) -> str:
    """Create ContainerImageRPMManifest object in Pyxis using GraphQL API"""
    mutation = """
mutation ($id: String!, $input: ContainerImageRPMManifestInput!) {
    create_image_rpm_manifest(id: $id, input: $input) {
        data {
            _id
        }
        error {
            detail
        }
    }
}
"""
    variables = {"id": "konflux-" + image_id, "input": {"image_id": image_id, "rpms": rpms}}
    body = {"query": mutation, "variables": variables}

    data = pyxis.graphql_query(graphql_api, body)

    return data["create_image_rpm_manifest"]["data"]["_id"]


def update_container_content_sets(graphql_api: str, image_id: str, content_sets: list):
    """Update ContainerImage.content_sets field in Pyxis using GraphQL API"""
    mutation = """
mutation ($id: ObjectIDFilterScalar!, $input: ContainerImageInput!) {
    update_image(id: $id, input: $input) {
        data {
            _id
        }
        error {
            detail
        }
    }
}
"""
    variables = {"id": image_id, "input": {"content_sets": content_sets}}
    body = {"query": mutation, "variables": variables}

    data = pyxis.graphql_query(graphql_api, body)

    return data["update_image"]["data"]["_id"]


def load_sbom_packages(sbom_path: str) -> list[dict]:
    """Open sbom file, load packages and return them

    If unable to open and load the json, raise an exception.
    """
    try:
        with open(sbom_path) as f:
            sbom = json.load(f)
        packages = sbom["packages"] if "packages" in sbom else []
    except Exception:
        LOGGER.error("Unable to load packages from sbom file")
        raise

    return packages


def construct_rpm_items_and_content_sets(
    packages: list[dict],
) -> tuple[list[dict], list[str]]:
    """Create RpmsItems object and content_sets from packages for Pyxis.

    This function creates two items:

    1. A list of RpmsItems dicts. There will be
    one RpmsItems per rpm package. A list is then formed of them
    and returned to be used in a containerImageRPMManifest.

    2. A list of unique content set strings to be saved in the ContainerImage.content_sets
    field in Pyxis
    """
    rpms_items = []
    content_sets = set()
    for package in packages:
        # First check if there is an rpm summary to use with a potential rpm item
        rpm_summary = get_rpm_summary(package)
        for externalRef in package.get("externalRefs", []):
            if externalRef.get("referenceType") != "purl":
                continue
            type = get_purl_type(externalRef["referenceLocator"])
            if type != "rpm":
                continue
            purl_dict = PackageURL.from_string(externalRef["referenceLocator"]).to_dict()
            if purl_dict["name"] in IGNORED_PACKAGES:
                continue
            if "qualifiers" not in purl_dict or purl_dict["qualifiers"] is None:
                continue
            rpm_item = {
                "name": purl_dict["name"],
                "summary": purl_dict["name"],
                "architecture": purl_dict["qualifiers"].get("arch", "noarch"),
            }
            if purl_dict["version"] is not None:
                rpm_item["version"] = purl_dict["version"].split("-")[0]
                rpm_item["release"] = purl_dict["version"].split("-")[1]
                rpm_item["nvra"] = (
                    f"{rpm_item['name']}-{purl_dict['version']}.{rpm_item['architecture']}"
                )
                rpm_item["summary"] = rpm_item["nvra"]
            if "upstream" in purl_dict["qualifiers"]:
                rpm_item["srpm_name"] = purl_dict["qualifiers"]["upstream"]

            if rpm_summary != "":
                rpm_item["summary"] = rpm_summary

            # XXX - temporary https://issues.redhat.com/browse/KONFLUX-4292
            # Undo this in https://issues.redhat.com/browse/KONFLUX-4175
            if (
                package.get("supplier") == "Organization: Red Hat, Inc."
                or purl_dict["namespace"] == "redhat"
            ):
                rpm_item["gpg"] = "199e2f91fd431d51"

            rpms_items.append(rpm_item)

            if "repository_id" in purl_dict["qualifiers"]:
                content_sets.add(purl_dict["qualifiers"]["repository_id"])

    return rpms_items, sorted(content_sets)


def get_rpm_summary(package: dict) -> str:
    """Get RPM summary from the package"""
    for annotation in package.get("annotations", []):
        if annotation.get("annotator") != "Tool: cachi2:jsonencoded":
            continue
        comment = annotation.get("comment", "")
        try:
            comment_dict = json.loads(comment)
        except json.JSONDecodeError:
            LOGGER.warning(f"Failed to decode JSON from annotation comment: {comment}")
            continue
        if comment_dict.get("name") == "cachi2:rpm_summary":
            summary = comment_dict.get("value", "")
            return summary
    return ""


def get_purl_type(purl: str):
    """
    Return purl type parsed from a purl string.

    Copied and adapted from packageurl package. The reason we need this function
    and cannot simply use the type component of
    packageurl.PackageURL.from_string(purl) is that there can be invalid non-rpm
    purls generated by syft. By getting just the type first and skipping those
    purls, we avoid failing on those invalid purls.

    Raise ValueError on errors.
    """
    scheme, sep, remainder = purl.partition(":")
    if not sep or scheme != "pkg":
        raise ValueError(f'purl is missing the required "pkg" scheme component: {repr(purl)}.')

    # this strip '/, // and /// as possible in :// or :///
    remainder = remainder.strip().lstrip("/")

    type, sep, remainder = remainder.partition("/")  # NOQA
    if not type or not sep:
        raise ValueError(f"purl is missing the required type component: {repr(purl)}.")

    type = type.lower()

    return type


def main():  # pragma: no cover
    """Main func"""
    args = parse_arguments()
    log_level = logging.DEBUG if args.verbose else logging.INFO
    pyxis.setup_logger(level=log_level)

    if not os.path.isfile(args.sbom_path):
        msg = f"sbom file does not exist: {args.sbom_path}"
        LOGGER.error(msg)
        raise RuntimeError(msg)

    # Use sbom filename (minus extension) for image_id if not provided
    if args.image_id is None:
        image_id = Path(args.sbom_path).stem
    else:
        image_id = args.image_id
    if not all(c in string.hexdigits for c in image_id):
        raise ValueError(f"image-id is invalid, hexadecimal value is expected: {image_id}")
    LOGGER.debug(f"Image ID: {image_id}")

    LOGGER.debug(f"Pyxis GraphQL API: {args.pyxis_graphql_api}")

    if args.retry:
        upload_container_rpm_data_with_retry(args.pyxis_graphql_api, image_id, args.sbom_path)
    else:
        upload_container_rpm_data(args.pyxis_graphql_api, image_id, args.sbom_path)


if __name__ == "__main__":  # pragma: no cover
    main()
