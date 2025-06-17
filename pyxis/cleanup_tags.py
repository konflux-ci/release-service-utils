#!/usr/bin/env python3
"""
Python script to clean up tags from previous Container Image objects in Pyxis

It takes a Pyxis image id for input and it will check the tags this image has
and then it will look for all other images with the same arch and repo
and it will remove all the tags that are present in the image provided as input.

Required env vars:
PYXIS_KEY_PATH
PYXIS_CERT_PATH

Optional env vars:
PYXIS_GRAPHQL_API
"""
import argparse
import logging
import string
import os
import time
from typing import List, Dict, Any

import pyxis

LOGGER = logging.getLogger("cleanup_tags")


def parse_arguments() -> argparse.Namespace:  # pragma: no cover
    """Parse CLI arguments

    :return: Dictionary of parsed arguments
    """

    parser = argparse.ArgumentParser(description="Clean up tags from previous images in Pyxis")

    parser.add_argument(
        "--pyxis-graphql-api",
        default=os.environ.get(
            "PYXIS_GRAPHQL_API", "https://graphql-pyxis.api.redhat.com/graphql/"
        ),
        help="Pyxis Graphql endpoint",
    )
    parser.add_argument("--repository", required=True, help="Repository to cleanup tags from")
    parser.add_argument(
        "image_id",
        help="Pyxis Container Image ID",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--retry",
        "-r",
        action="store_true",
        help="If set, retry the upload in case it fails",
    )

    return parser.parse_args()


def cleanup_tags_with_retry(
    graphql_api: str,
    image_id: str,
    target_repository: str,
    retries: int = 3,
    backoff_factor: float = 5.0,
):
    last_err = RuntimeError()
    for attempt in range(retries):
        try:
            time.sleep(backoff_factor * attempt)
            cleanup_tags(graphql_api, image_id, target_repository)
            return
        except RuntimeError as e:
            LOGGER.warning(f"Attempt {attempt+1} failed.")
            last_err = e
    LOGGER.error("Out of attempts. Raising the error.")
    raise last_err


def cleanup_tags(graphql_api, image_id: str, target_repository: str):
    image = get_image(graphql_api, image_id)

    registry, repository, tags = get_rh_registry_image_properties(image, target_repository)

    LOGGER.info(f"Image id: {image['_id']}")
    LOGGER.info(f"Image architecture: {image['architecture']}")
    LOGGER.info(f"Repository: {repository}")
    LOGGER.info(f"Image tags: {tags}")

    images_for_cleanup = {}

    for tag in tags:
        candidates = get_candidates_for_cleanup(
            graphql_api,
            registry,
            repository,
            tag,
        )
        for candidate in candidates:
            id = candidate["_id"]
            if (
                id != image["_id"]
                and id not in images_for_cleanup
                and candidate["architecture"] == image["architecture"]
            ):
                images_for_cleanup[id] = candidate

    LOGGER.info(f"Found {len(images_for_cleanup)} images for cleanup.")
    update_images(graphql_api, tags, images_for_cleanup, repository)


def get_image(graphql_api: str, image_id: str) -> dict:
    """Get ContainerImage object from Pyxis using GraphQL API"""
    query = """
query ($id: ObjectIDFilterScalar!) {
    get_image(id: $id) {
        data {
            _id
            architecture
            repositories {
                registry
                repository
                tags {
                    name
                }
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

    return image


def get_rh_registry_image_properties(image: Dict, target_repository: str):
    """Get the registry.access.redhat.com repository properties of the image
    needed to search for related images.

    :return: (registry, repository, tags)
    """
    for repo in image["repositories"]:
        if (
            repo["registry"] == "registry.access.redhat.com"
            and repo["repository"] == target_repository
        ):
            if repo["tags"] is None:
                tags = []
            else:
                tags = [tag["name"] for tag in repo["tags"]]
            return repo["registry"], repo["repository"], tags

    raise RuntimeError(
        "Cannot find the registry.access.redhat.com repository entry for the image"
    )


def get_candidates_for_cleanup(
    graphql_api, registry, repository, tag: str, page_size: int = 50
):
    """Get ContainerImage objects from Pyxis using GraphQL API

    The function will get all the images based on the registry, repository and tag
    and it will return a list of them.
    """
    # Get all ContainerImage.Repositories fields
    # See https://catalog.redhat.com/api/containers/docs/objects/ContainerImageRepo.html
    query = """
query (
    $registry: String!, $repository: String!, $tag: String!,
    $page: Int!, $page_size: Int!) {
    find_repository_images_by_registry_path_tag(
        registry: $registry,
        repository: $repository,
        tag: $tag,
        page: $page,
        page_size: $page_size) {
        data {
            _id
            architecture
            repositories {
                published
                registry
                repository
                tags {
                    name
                    added_date
                    manifest_schema1_digest
                    removed_date
                }
                comparison {
                    advisory_rpm_mapping {
                        advisory_ids
                        nvra
                    }
                    reason
                    reason_text
                    rpms {
                        downgrade
                        new
                        remove
                        upgrade
                    }
                    with_nvr
                }
                content_advisory_ids
                image_advisory_id
                manifest_list_digest
                manifest_schema2_digest
                published_date
                push_date
                signatures {
                    key_long_id
                    tags
                }
            }
        }
        error {
            status
            detail
        }
    }
}
    """
    has_more = True
    page = 0
    images = []

    while has_more:
        variables = {
            "registry": registry,
            "repository": repository,
            "tag": tag,
            "page": page,
            "page_size": page_size,
        }
        body = {"query": query, "variables": variables}

        data = pyxis.graphql_query(graphql_api, body)
        images_batch = data["find_repository_images_by_registry_path_tag"]["data"]
        images.extend(images_batch)

        has_more = len(images_batch) == page_size
        page += 1

    return images


def update_images(graphql_api: str, tags: List[str], images: Dict, target_repository: str):
    """Update images to remove unwanted tags from them

    For each image in `images` it will remove all `tags`
    from its repositories and then it will update the image
    using graphql mutation update_image.
    """
    for image in images.values():
        LOGGER.info(f"Updating image {image['_id']} with architecture {image['architecture']}")
        LOGGER.info("Repositories and tags before update:")
        for repository in image["repositories"]:
            repo_tags = [tag["name"] for tag in repository.get("tags") or []]
            LOGGER.info(f"  {repository['registry']}/{repository['repository']}: {repo_tags}")
        for i in range(len(image["repositories"])):
            # clean up tags only only from the given repository
            if image["repositories"][i]["repository"] == target_repository:
                repo_tags = image["repositories"][i]["tags"]
                image["repositories"][i]["tags"] = [
                    tag for tag in repo_tags if tag["name"] not in tags
                ]

        # When we load the images for patching, we request all fields of
        # the ContainerRepository objects because otherwise we might remove some data with
        # the update. But that means that fields that are not used will be null/None and
        # if you try to include those in the update request, Pyxis will fail with errors like:
        # 'signatures': ['Field may not be null.']
        # So these null items must be removed before making the update request.
        image = remove_none_values(image)
        updated_image = update_image(graphql_api, image)
        LOGGER.info("Repositories and tags after update:")
        for repository in updated_image["repositories"]:
            repo_tags = (
                [tag["name"] for tag in repository["tags"]]
                if repository["tags"] is not None
                else []
            )
            LOGGER.info(f"  {repository['registry']}/{repository['repository']}: {repo_tags}")


def update_image(graphql_api: str, image: Dict):
    """Update image using Pyxis GraphQL API"""
    mutation = """
mutation ($id: ObjectIDFilterScalar!, $input: ContainerImageInput!) {
    update_image(id: $id, input: $input) {
        data {
            _id
            architecture
            repositories {
                registry
                repository
                tags {
                    name
                }
            }
        }
        error {
            status
            detail
        }
    }
}
    """
    variables = {"id": image["_id"], "input": image}
    body = {"query": mutation, "variables": variables}

    data = pyxis.graphql_query(graphql_api, body)
    updated_image = data["update_image"]["data"]

    return updated_image


def remove_none_values(d: Any):
    """
    Recursively remove all items from the dictionary (or nested dictionaries) which are None.
    """
    if not isinstance(d, dict):
        return d

    cleaned_dict = {}
    for key, value in d.items():
        if isinstance(value, dict):
            # Recursively clean the nested dictionary
            cleaned_value = remove_none_values(value)
            if cleaned_value:  # Add only if the nested dictionary is not empty
                cleaned_dict[key] = cleaned_value
        elif isinstance(value, list):
            # Recursively clean the list
            cleaned_list = [remove_none_values(item) for item in value if item is not None]
            cleaned_dict[key] = cleaned_list  # The list can be empty, we add it anyway
        elif value is not None:
            cleaned_dict[key] = value

    return cleaned_dict


def main():  # pragma: no cover
    """Main func"""
    args = parse_arguments()
    log_level = logging.DEBUG if args.verbose else logging.INFO
    pyxis.setup_logger(level=log_level)

    if not all(c in string.hexdigits for c in args.image_id):
        raise ValueError(
            f"image-id is invalid, hexadecimal value is expected: {args.image_id}"
        )
    LOGGER.debug(f"Image ID: {args.image_id}")

    LOGGER.debug(f"Pyxis GraphQL API: {args.pyxis_graphql_api}")

    if args.retry:
        cleanup_tags_with_retry(args.pyxis_graphql_api, args.image_id, args.repository)
    else:
        cleanup_tags(args.pyxis_graphql_api, args.image_id, args.repository)


if __name__ == "__main__":  # pragma: no cover
    main()
