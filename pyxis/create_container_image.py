#!/usr/bin/env python3
"""
Python script to create a Container Image object in Pyxis

NOTE: Any change to this file that changes functionality should be posted for review in
#forum-metadata-guild on slack. No PR changing functionality should be removed without
approval from someone on that team.

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

In Konflux, by default we release to registry.redhat.io and not the public
registry. But we do also release images with `requires_terms` set to false (e.g. ubi)
so these images are available from both registries
(registry.redhat.io, registry.access.redhat.com).
"""
import argparse
from urllib.parse import quote
from datetime import datetime
import json
import logging
from typing import Any, Dict, List, Optional
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
        "--oras-manifest-fetch",
        help="File with result of `oras manifest fetch` running against image"
        " represented by ContainerImage to be created",
        required=True,
    )
    parser.add_argument(
        "--is-latest",
        help="Should the `latest` tag of the ContainerImage be overwritten?",
        required=True,
    )
    parser.add_argument(
        "--name",
        help='The "name" of the image: the registry/repository-name.',
        required=True,
    )
    parser.add_argument(
        "--digest",
        help="The digest of the pullspec, without regard to platform. "
        "Could be digest of either single or multiarch image.",
        required=True,
    )
    parser.add_argument(
        "--architecture-digest",
        help="The digest of the specific architecture of the image, regardless "
        "of whether it is a single or multiarch image.",
        required=True,
    )
    parser.add_argument(
        "--architecture",
        help="The architecture of the image.",
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
    parser.add_argument(
        "--dockerfile",
        help="Path to the Dockerfile to be included in the ContainerImage.parsed_data field",
        default="",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    return parser


def emit_id(identifier):
    """Emit the image identifier in a standard way.

    The formatting of this log line is important.
    It is parsed by the tekton task that calls us.
    """
    LOGGER.info(f"The image id is: {identifier}")


def proxymap(repository: str) -> str:
    """Map a backend repo name to its proxy equivalent.

    i.e., map quay.io/redhat-pending/foo----bar to foo/bar
    """
    return repository.split("/")[-1].replace("----", "/")


def find_image(pyxis_url, digest: str) -> Any:
    """Function to find a containerImage with the given digest.

    :return: the image, if one exists, else None if none found
    """

    raw_filter = f'repositories.manifest_schema2_digest=="{digest}";not(deleted==true)'
    # quote is needed to urlparse the quotation marks
    filter_str = quote(raw_filter)
    check_url = urljoin(pyxis_url, f"v1/images?page_size=1&filter={filter_str}")

    # Get the list of the ContainerImages with given parameters
    rsp = pyxis.get(check_url)
    rsp.raise_for_status()

    query_results = rsp.json()["data"]

    if len(query_results) == 0:
        return None

    if "_id" in query_results[0]:
        emit_id(query_results[0]["_id"])
    else:
        raise RuntimeError("Image metadata was found in Pyxis, but the id key was missing.")

    return query_results[0]


def find_repo_in_image(repository_str: str, image: Dict[str, Any]) -> Optional[int]:
    """Check if a repository already exists in the ContainerImage

    :return: index of repository if repository_str string is found in the ContainerImage
        repositories, None otherwise
    """
    for index, repository in enumerate(image["repositories"]):
        if repository["repository"] == repository_str:
            return index
    return None


def prepare_parsed_data(args) -> Dict[str, Any]:
    """Function to extract the data this script needs from provided oras manifest fetch output

    :return: Dict of tuples containing pertinent data
    """

    with open(args.oras_manifest_fetch) as json_file:
        oras_manifest_fetch = json.load(json_file)

    parsed_data = {
        "architecture": args.architecture,
        "layers": [
            layer["digest"] for layer in reversed(oras_manifest_fetch.get("layers", []))
        ],
        "uncompressed_layer_sizes": [
            {"layer_id": layer["digest"], "size_bytes": layer["size"]}
            for layer in reversed(oras_manifest_fetch.get("uncompressed_layers", []))
        ],
        "uncompressed_size_bytes": sum(
            [
                layer.get("size", 0)
                for layer in oras_manifest_fetch.get("uncompressed_layers", [])
            ]
        ),
        "sum_layer_size_bytes": sum(
            [layer.get("size", 0) for layer in oras_manifest_fetch.get("layers", [])]
        ),
        "top_layer_id": None,
        "uncompressed_top_layer_id": None,
    }
    if parsed_data["layers"]:
        parsed_data["top_layer_id"] = parsed_data["layers"][0]
    if parsed_data["uncompressed_layer_sizes"]:
        parsed_data["uncompressed_top_layer_id"] = parsed_data["uncompressed_layer_sizes"][0][
            "layer_id"
        ]

    if args.dockerfile != "":
        with open(args.dockerfile) as f:
            dockerfile_content = f.read()
        parsed_data["files"] = [
            {"key": "buildfile", "content": dockerfile_content, "filename": "Dockerfile"}
        ]

    return parsed_data


def pyxis_tags(tags, date_now):
    """Return list of tags formatted for pyxis"""
    return [
        {
            "added_date": date_now,
            "name": tag,
        }
        for tag in tags
    ]


def repository_digest_values(args):
    """Return digest values for the repository entry in the image entity"""
    result = {"manifest_schema2_digest": args.architecture_digest}
    if args.media_type in MANIFEST_LIST_TYPES:
        result["manifest_list_digest"] = args.digest
    return result


def create_container_image(args, parsed_data: Dict[str, Any], tags: List[str]):
    """Function to create a new containerImage entry in a pyxis instance"""

    LOGGER.info("Creating new container image")

    repository = construct_repository(args, tags)

    # sum_layer_size_bytes isn't accepted in the parsed_data payload to pyxis
    sum_layer_size_bytes = parsed_data.pop("sum_layer_size_bytes", 0)

    # top_layer_id isn't accepted in the parsed_data payload to pyxis
    top_layer_id = parsed_data.pop("top_layer_id", None)

    # uncompressed_top_layer_id isn't accepted in the parsed_data payload to pyxis
    uncompressed_top_layer_id = parsed_data.pop("uncompressed_top_layer_id", None)

    upload_url = urljoin(args.pyxis_url, "v1/images")

    container_image_payload = {
        "repositories": [repository],
        "certified": json.loads(args.certified.lower()),
        "image_id": args.architecture_digest,
        "architecture": parsed_data["architecture"],
        "parsed_data": parsed_data,
        "sum_layer_size_bytes": sum_layer_size_bytes,
    }

    # Only supply these ids if they are not "None"
    if top_layer_id:
        container_image_payload["top_layer_id"] = top_layer_id
    if uncompressed_top_layer_id:
        container_image_payload["uncompressed_top_layer_id"] = uncompressed_top_layer_id

    rsp = pyxis.post(upload_url, container_image_payload).json()

    # Make sure container metadata was successfully added to Pyxis
    if "_id" in rsp:
        emit_id(rsp["_id"])
    else:
        raise Exception("Image metadata was not successfully added to Pyxis.")


def update_container_image_repositories(
    pyxis_url, image_id: str, repositories: List[Dict[str, Any]]
):
    LOGGER.info(f"Updating repositories for container image {image_id}")

    patch_url = urljoin(pyxis_url, f"v1/images/id/{image_id}")

    payload = {"repositories": repositories}

    rsp = pyxis.patch(patch_url, payload).json()

    # Make sure container metadata was successfully added to Pyxis
    if "_id" in rsp:
        emit_id(rsp["_id"])
    else:
        raise Exception("Image metadata was not successfully added to Pyxis.")


def construct_repository(args, tags):
    image_name = args.name
    image_registry = image_name.split("/")[0]
    image_repo = image_name.split("/", 1)[1]

    date_now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f+00:00")

    # For images released to registry.redhat.io we need a special repository item
    # with published=true and registry and repository converted.
    # E.g. if the name in the oras manifest result is
    # "quay.io/redhat-prod/rhtas-tech-preview----cosign-rhel9",
    # repository will be "rhtas-tech-preview/cosign-rhel9"
    if args.rh_push == "true":
        LOGGER.info("--rh-push is true. Associating registry.access.redhat.com repository.")
        repo = {
            "published": True,
            "registry": "registry.access.redhat.com",
            "repository": proxymap(image_name),
            "push_date": date_now,
            "tags": pyxis_tags(tags, date_now),
        }
    else:
        repo = {
            "published": False,
            "registry": image_registry,
            "repository": image_repo,
            "push_date": date_now,
            "tags": pyxis_tags(tags, date_now),
        }

    repo.update(repository_digest_values(args))

    return repo


def main():  # pragma: no cover
    """Main func"""

    parser = setup_argparser()
    args = parser.parse_args()
    log_level = logging.DEBUG if args.verbose else logging.INFO
    pyxis.setup_logger(level=log_level)

    parsed_data = prepare_parsed_data(args)

    if args.rh_push == "true":
        image_repo = proxymap(args.name)
    else:
        image_repo = args.name.split("/", 1)[1]

    tags = args.tags.split()
    if args.is_latest == "true":
        tags.append("latest")

    # First check if it exists at all. If not, create it
    LOGGER.info(f"Checking to see if digest {args.architecture_digest} exists in pyxis")
    image = find_image(args.pyxis_url, args.architecture_digest)
    if image is None:
        LOGGER.info("Image with given docker_image_digest doesn't exist yet.")
        create_container_image(args, parsed_data, tags)
        return

    identifier = image["_id"]
    # Then, check if it already references the given repository. If not, add the repo
    repo_index = find_repo_in_image(image_repo, image)
    repositories = image["repositories"]
    if repo_index is None:
        LOGGER.info(
            f"Image with given docker_image_digest exists as {identifier}, but "
            f"is not yet associated with repository {args.name}."
        )
        repositories.append(construct_repository(args, tags))
        update_container_image_repositories(args.pyxis_url, identifier, repositories)
        return

    # Then, check if the tags are different. If they are, update them
    existing_tags = [tag["name"] for tag in repositories[repo_index].get("tags") or []]
    if existing_tags != tags:
        LOGGER.info(
            f"Image with given docker_image_digest exists as {identifier} and "
            f"is associated with repository {args.name}, but the tags differ."
        )
        repositories[repo_index] = construct_repository(args, tags)
        update_container_image_repositories(args.pyxis_url, identifier, repositories)
        return

    LOGGER.info(
        f"Image with given docker_image_digest already exists as {identifier} "
        f"and is associated with repository {args.name} and tags {args.tags}. "
        "Skipping the image creation."
    )


if __name__ == "__main__":  # pragma: no cover
    main()
