#!/usr/bin/env python3
"""
Python script to check if signature exists

Required env vars:
PYXIS_KEY_PATH
PYXIS_CERT_PATH

Optional env vars:
PYXIS_GRAPHQL_API
"""
import argparse
import logging
import os
import sys

import pyxis

LOGGER = logging.getLogger("find_signature")


def parse_arguments() -> argparse.Namespace:  # pragma: no cover
    """Parse CLI arguments

    :return: Dictionary of parsed arguments
    """

    parser = argparse.ArgumentParser(description="find signature for image in Pyxis")

    parser.add_argument(
        "--pyxis-graphql-api",
        default=os.environ.get(
            "PYXIS_GRAPHQL_API", "https://graphql-pyxis.api.redhat.com/graphql/"
        ),
        help="Pyxis Graphql endpoint",
    )
    parser.add_argument(
        "--pyxis-api",
        default=os.environ.get("PYXIS_API", "https://pyxis.api.redhat.com/"),
        help="Pyxis API endpoint",
    )
    parser.add_argument(
        "--reference",
        help="image reference",
    )
    parser.add_argument(
        "--repository",
        help="image repository",
    )
    parser.add_argument(
        "--manifest_digest",
        help="image manifest digest",
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--retry",
        "-r",
        action="store_true",
        help="If set, retry the query in case it fails",
    )

    return parser.parse_args()


def find_signature_using_reference(
    pyxis_api, graphql_api, reference: str, manifest_digest: str
) -> bool:
    LOGGER.info(f"reference: {reference}")
    LOGGER.info(f"manifest_digest: {manifest_digest}")

    image_registry = reference.split("/")[0]
    repository = reference.split("/", 1)[1].split(":")[0]
    LOGGER.debug(f"image_registry: {image_registry}")
    LOGGER.debug(f"repository: {repository}")

    current_index = 1
    while True:
        LOGGER.debug(f"current_index {current_index}")
        query = """
query (
    $repository: String!, $manifest_digest: String!, $index: Int!) {
    find_signature_data_by_index(non_zero_index: $index,
        repository: $repository,
        manifest_digest: $manifest_digest,
        sort_by: [{ field: "creation_date", order: DESC }]) {
            error {
                detail
                status
            }

            data {
                _id
            }
    }
}
        """
        variables = {
            "repository": repository,
            "manifest_digest": manifest_digest,
            "index": current_index,
        }
        body = {"query": query, "variables": variables}

        data = pyxis.graphql_query(graphql_api, body)
        signatures = data["find_signature_data_by_index"]["data"]
        LOGGER.debug(f"Found {len(signatures)} signatures.")
        if len(signatures) == 1:
            LOGGER.debug(f"{signatures}")
            id = signatures[0]["_id"]
            LOGGER.debug(f"id: {id}")
            signatureurl = f"{pyxis_api}/v1/signatures/id/{id}"
            response = pyxis.get(url=signatureurl)
            LOGGER.debug(f"response: {response}")
            json = response.json()
            LOGGER.debug(f"json: {json}")
            reference_from_signature = json["reference"]
            LOGGER.debug(f"reference_from_signature: {reference_from_signature}")
            if reference_from_signature == reference:
                LOGGER.info(f"{reference_from_signature} matches")
                return True
            else:
                current_index += 1
        else:
            LOGGER.info("signature not found")
            return False


def main():  # pragma: no cover
    """Main func"""
    args = parse_arguments()
    log_level = logging.DEBUG if args.verbose else logging.INFO
    pyxis.setup_logger(level=log_level)

    LOGGER.debug(f"Pyxis GraphQL API: {args.pyxis_graphql_api}")
    LOGGER.debug(f"Pyxis API: {args.pyxis_api}")

    found = find_signature_using_reference(
        args.pyxis_api, args.pyxis_graphql_api, args.reference, args.manifest_digest
    )
    if found:
        sys.exit(0)
    sys.exit(1)


if __name__ == "__main__":  # pragma: no cover
    main()
