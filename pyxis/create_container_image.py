#!/usr/bin/env python3
"""
Python script to create a Container Image object in Pyxis

Note about releasing to registry.redhat.io (using `--rh-push true` CLI argument):

Our goal is to be able to download images from registry.redhat.io. For that to happen,
an image needs to be pushed to quay.io/redhat-prod/$PRODUCT----$IMAGE, e.g.
quay.io/redhat-prod/rhtas-tech-preview----tuf-server-rhel9. When creating
the Container Image object in Pyxis, we need a second repository item
under `repositories` where the registry needs to be set to
registry.access.redhat.com and the repository would be rhtas-tech-preview/tuf-server-rhel9
in the example above ("----" converted to "/"). This also requires a corresponding
Container Repository object to exist in Pyxis. This will typically be created as part
of product onboarding to RHTAP.

For stage, if you want to be able to pull an image from registry.stage.redhat.io,
the image is pushed to quay.io/redhat-pending, the Container Image is created
in stage Pyxis, but the registry value in Pyxis is still set to registry.access.redhat.com.

Why is the registry set to registry.access.redhat.com and not registry.redhat.io?
Mostly for historical reasons.

When Red Hat started releasing container images, they were all available
in a publicly available registry: registry.access.redhat.com .
Later, Red Hat introduced the so called "terms based registry": registry.redhat.io
The new registry requires users to agree to terms and the access is authenticated.
At first, all images were available in both registries. Nowaways, most images that
are released are only available from registry.redhat.io. This is controlled by
the `requires_terms` flag in the Pyxis Container Repository object:
https://pyxis.api.redhat.com/docs/objects/ContainerRepository.html?tab=Fields

For RHTAP, we currently expect to only release to registry.redhat.io and not the public
registry. But if we did want to release to registry.access.redhat.com, there would
likely be no change required in our pipeline - only the Container Repository
object in Pyxis would need to have `requires_terms` set to false.
"""
import argparse
from urllib.parse import quote
from datetime import datetime
import json
import logging
from typing import Any, Dict
from urllib.parse import urljoin

import pyxis

LOGGER = logging.getLogger("create_container_image")

# Media types that are used for multi arch images
MANIFEST_LIST_TYPES = [
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
]


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
        "--tags",
        help="Tags to include in the ContainerImage object. It can be a single tag "
        "or multiple tags separated by space",
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
    parser.add_argument(
        "--media-type",
        help="The mediaType string returned by `skopeo inspect --raw`. "
        "Used to determine if it's a single arch or multiarch image.",
        required=True,
    )
    parser.add_argument(
        "--rh-push",
        help="If set to true, a second item will be created in ContainerImage.repositories "
        "with the registry and repository entries converted to use Red Hat's official "
        "registry. E.g. a mapped repository of "
        "quay.io/redhat-pending/product---my-image will be converted to use "
        "registry registry.access.redhat.com and repository product/my-image. Also, "
        "the image will be marked as published.",
        default="false",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    return parser


def image_already_exists(args, digest: str) -> bool:
    """Function to check if a containerImage with the given digest
    already exists in the pyxis instance

    :return: True if one exists, else false
    """
    digest_field = get_digest_field(args.media_type)

    # quote is needed to urlparse the quotation marks
    filter_str = quote(f'repositories.{digest_field}=="{digest}";not(deleted==true)')

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

    image_name = parsed_data["name"]
    image_registry = image_name.split("/")[0]
    image_repo = image_name.split("/", 1)[1]
    # name isn't accepted in the parsed_data payload to pyxis
    del parsed_data["name"]

    upload_url = urljoin(args.pyxis_url, "v1/images")

    tags = args.tags.split()
    if args.is_latest == "true":
        tags.append("latest")
    pyxis_tags = [
        {
            "added_date": date_now,
            "name": tag,
        }
        for tag in tags
    ]

    container_image_payload = {
        "repositories": [
            {
                "published": False,
                "registry": image_registry,
                "repository": image_repo,
                "push_date": date_now,
                "tags": pyxis_tags,
            }
        ],
        "certified": json.loads(args.certified.lower()),
        "image_id": docker_image_digest,
        "architecture": parsed_data["architecture"],
        "parsed_data": parsed_data,
    }

    digest_field = get_digest_field(args.media_type)
    container_image_payload["repositories"][0][digest_field] = docker_image_digest

    # For images released to registry.redhat.io we need a second repository item
    # with published=true and registry and repository converted.
    # E.g. if the name in the skopeo inspect result is
    # "quay.io/redhat-prod/rhtas-tech-preview----cosign-rhel9",
    # repository will be "rhtas-tech-preview/cosign-rhel9"
    if args.rh_push == "true":
        repo = container_image_payload["repositories"][0].copy()
        repo["published"] = True
        repo["registry"] = "registry.access.redhat.com"
        repo["repository"] = image_name.split("/")[-1].replace("----", "/")
        container_image_payload["repositories"].append(repo)

    rsp = pyxis.post(upload_url, container_image_payload).json()

    # Make sure container metadata was successfully added to Pyxis
    if "_id" in rsp:
        LOGGER.info(f"The image id is: {rsp['_id']}")
    else:
        raise Exception("Image metadata was not successfully added to Pyxis.")


def get_digest_field(media_type: str) -> str:
    """This will return one of the two possible digest fields
    to use in the repository object which is embedded in the
    ContainerImage object.

    manifest_schema2_digest is used for single arch images,
    manifest_list_digest is used for multi arch images
    """
    if media_type in MANIFEST_LIST_TYPES:
        return "manifest_list_digest"
    else:
        return "manifest_schema2_digest"


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
