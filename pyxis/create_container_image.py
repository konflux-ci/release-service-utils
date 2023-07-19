#!/usr/bin/env python3
import argparse
from urllib.parse import quote
from datetime import datetime
import json
import logging
from typing import Any, Dict
from urllib.parse import urljoin

import pyxis

LOGGER = logging.getLogger("create_container_image")


def setup_argparser() -> Any:  # pragma: no cover
    """Setup argument parser

    :return: Initialized argument parser
    """

    parser = argparse.ArgumentParser(description="ContainerImage resource creator.")

    parser.add_argument(
        "--pyxis-url",
        default="https://pyxis.com",
        help="Base URL for Pyxis container metadata API",
    )
    parser.add_argument("--certified", help="Is the ContainerImage certified?", required=True)
    parser.add_argument(
        "--tag",
        help="The ContainerImage tag name to upload",
        required=True,
    )
    parser.add_argument(
        "--skopeo-result",
        help="File with result of `skopeo inspect` running against image"
        " represented by ContainerImage to be created",
        required=True,
    )
    parser.add_argument(
        "--is-latest",
        help="Should the `latest` tag of the ContainerImage be overwritten?",
        required=True,
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    return parser


def image_already_exists(args, digest: str) -> bool:
    """Function to check if a containerImage with the given digest
    already exists in the pyxis instance

    :return: True if one exists, else false
    """

    # quote is needed to urlparse the quotation marks
    filter_str = quote(f'docker_image_digest=="{digest}";' f"not(deleted==true)")

    check_url = urljoin(args.pyxis_url, f"v1/images?page_size=1&filter={filter_str}")

    # Get the list of the ContainerImages with given parameters
    rsp = pyxis.get(check_url)
    rsp.raise_for_status()

    query_results = rsp.json()["data"]

    if len(query_results) == 0:
        LOGGER.info("Image with given docker_image_digest doesn't exist yet")
        return False

    LOGGER.info(
        "Image with given docker_image_digest already exists." "Skipping the image creation."
    )
    if "_id" in query_results[0]:
        LOGGER.info(f"The image id is: {query_results[0]['_id']}")
    else:
        raise Exception("Image metadata was found in Pyxis, but the id key was missing.")

    return True


def prepare_parsed_data(skopeo_result: Dict[str, Any]) -> Dict[str, Any]:
    """Function to extract the data this script needs from provided skopeo inspect output

    :return: Dict of tuples containing pertinent data
    """

    return {
        "digest": skopeo_result.get("Digest", ""),
        "docker_version": skopeo_result.get("DockerVersion", ""),
        "layers": skopeo_result.get("Layers", []),
        "name": skopeo_result.get("Name", ""),
        "architecture": skopeo_result.get("Architecture", ""),
        "env_variables": skopeo_result.get("Env", []),
    }


def create_container_image(args, parsed_data: Dict[str, Any]):
    """Function to create a new containerImage entry in a pyxis instance"""

    LOGGER.info("Creating new container image")

    date_now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")

    if "digest" not in parsed_data:
        raise Exception("Digest was not found in the passed skopeo inspect json")
    if "name" not in parsed_data:
        raise Exception("Name was not found in the passed skopeo inspect json")
    docker_image_digest = parsed_data["digest"]
    # digest isn't accepted in the parsed_data payload to pyxis
    del parsed_data["digest"]
    docker_image_registry = parsed_data["name"].split("/")[0]
    docker_image_repo = parsed_data["name"].split("/", 1)[1]
    # name isn't accepted in the parsed_data payload to pyxis
    del parsed_data["name"]

    upload_url = urljoin(args.pyxis_url, "v1/images")
    container_image_payload = {
        "repositories": [
            {
                "published": False,
                "registry": docker_image_registry,
                "repository": docker_image_repo,
                "push_date": date_now,
                "tags": [
                    {
                        "added_date": date_now,
                        "name": args.tag,
                    },
                ],
            }
        ],
        "certified": json.loads(args.certified.lower()),
        "docker_image_digest": docker_image_digest,
        "image_id": docker_image_digest,
        "architecture": parsed_data["architecture"],
        "parsed_data": parsed_data,
    }

    if args.is_latest == "true":
        container_image_payload["repositories"][0]["tags"].append(
            {
                "added_date": date_now,
                "name": "latest",
            }
        )

    rsp = pyxis.post(upload_url, container_image_payload).json()

    # Make sure container metadata was successfully added to Pyxis
    if "_id" in rsp:
        LOGGER.info(f"The image id is: {rsp['_id']}")
    else:
        raise Exception("Image metadata was not successfully added to Pyxis.")


def main():  # pragma: no cover
    """Main func"""

    parser = setup_argparser()
    args = parser.parse_args()
    log_level = logging.DEBUG if args.verbose else logging.INFO
    pyxis.setup_logger(level=log_level)

    with open(args.skopeo_result) as json_file:
        skopeo_result = json.load(json_file)

    parsed_data = prepare_parsed_data(skopeo_result)

    if not image_already_exists(args, parsed_data["digest"]):
        create_container_image(args, parsed_data)


if __name__ == "__main__":  # pragma: no cover
    main()
