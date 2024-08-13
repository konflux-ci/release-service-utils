#!/usr/bin/env python3
"""Upload rpm manifest to Pyxis

This script will take Pyxis image ID and an sbom cyclonedx file
on the input. It will inspect the sbom for the rpms and then push
data into Pyxis. If an RPM Manifest already exists for the container
image, nothing is done as we assume it was already pushed by this
script.

Required env vars:
PYXIS_KEY_PATH
PYXIS_CERT_PATH

Optional env vars:
PYXIS_GRAPHQL_API
"""
import logging
import string
import os
from pathlib import Path
import time
from urllib.error import HTTPError
from packageurl import PackageURL
from upload_sbom import load_sbom_components, parse_arguments

import pyxis

LOGGER = logging.getLogger("upload_rpm_manifest")


def upload_container_rpm_manifest_with_retry(
    graphql_api: str,
    image_id: str,
    sbom_path: str,
    retries: int = 3,
    backoff_factor: float = 5.0,
):
    """Call the upload_container_rpm_manifest function with retries"""
    last_err = RuntimeError()
    for attempt in range(retries):
        try:
            time.sleep(backoff_factor * attempt)
            upload_container_rpm_manifest(graphql_api, image_id, sbom_path)
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


def upload_container_rpm_manifest(graphql_api: str, image_id: str, sbom_path: str):
    """Upload a Container Image RPM Manifest to Pyxis"""
    if get_rpm_manifest_id(graphql_api, image_id) != "":
        # We assume that if the RPM Manifest already exists, it is accurate as the
        # entire object is added in one request.
        LOGGER.info("RPM manifest already exists for ContainerImage. Exiting...")
        return

    sbom_components = load_sbom_components(sbom_path)
    LOGGER.info(f"Loaded {len(sbom_components)} components from sbom file.")

    rpms = construct_rpm_items(sbom_components)

    rpm_manifest_id = create_image_rpm_manifest(graphql_api, image_id, rpms)
    LOGGER.info(f"RPM manifest ID: {rpm_manifest_id}")


def get_rpm_manifest_id(graphql_api: str, image_id: str) -> str:
    """Get RPM Manifest id from Pyxis using GraphQL API

    This function uses the get_image graphql query to get the rpm_manifest
    id. This will be the empty string if no rpm_manifest exists for the
    ContainerImage or the id if one does exist.
    """
    query = """
query ($id: ObjectIDFilterScalar!) {
    get_image(id: $id) {
        data {
            _id
            rpm_manifest {
                _id
            }
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
    if image["rpm_manifest"] is not None and "_id" in image["rpm_manifest"]:
        return image["rpm_manifest"]["_id"]

    return ""


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


def construct_rpm_items(components: list[dict]) -> list[dict]:
    """Create RpmsItems object from component for Pyxis.

    This function creates a list of RpmsItems dicts. There will be
    one RpmsItems per rpm component. A list is then formed of them
    and returned to be used in a containerImageRPMManifest.
    """

    rpms_items = []
    for component in components:
        if "purl" in component:
            purl_dict = PackageURL.from_string(component["purl"]).to_dict()
            if purl_dict["type"] == "rpm":
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
                rpms_items.append(rpm_item)
    return rpms_items


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
        upload_container_rpm_manifest_with_retry(
            args.pyxis_graphql_api, image_id, args.sbom_path
        )
    else:
        upload_container_rpm_manifest(args.pyxis_graphql_api, image_id, args.sbom_path)


if __name__ == "__main__":  # pragma: no cover
    main()
