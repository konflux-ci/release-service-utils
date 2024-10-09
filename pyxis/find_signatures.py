#!/usr/bin/env python3
"""
Python script to find all signatures in Pyxis for a given repository and manifest_digest
and save a list of references that are signed in a text file, one reference per line

Required env vars:
PYXIS_KEY_PATH
PYXIS_CERT_PATH

Optional env vars:
PYXIS_GRAPHQL_API
"""
import argparse
import logging
import os

import pyxis

LOGGER = logging.getLogger("find_signatures")


def parse_arguments() -> argparse.Namespace:  # pragma: no cover
    """Parse CLI arguments

    :return: Dictionary of parsed arguments
    """

    parser = argparse.ArgumentParser(
        description="find all signatures in Pyxis for a given repository and manifest_digest"
        "and save a list of references that are signed in a text file, one reference"
        "per line"
    )

    parser.add_argument(
        "--pyxis-graphql-api",
        default=os.environ.get(
            "PYXIS_GRAPHQL_API", "https://graphql-pyxis.api.redhat.com/graphql/"
        ),
        help="Pyxis Graphql endpoint",
        required=True,
    )
    parser.add_argument(
        "--repository",
        help="image repository",
        required=True,
    )
    parser.add_argument(
        "--manifest_digest",
        help="image manifest digest",
        required=True,
    )
    parser.add_argument(
        "--output_file",
        help="output file of references found",
        required=True,
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--retry",
        "-r",
        action="store_true",
        help="If set, retry the query in case it fails",
    )

    return parser.parse_args()


def find_signatures_for_repository(graphql_api, repository: str, manifest_digest: str) -> set:
    LOGGER.info(f"repository: {repository}")
    LOGGER.info(f"manifest_digest: {manifest_digest}")

    references = set()
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

    signature_query = """
query (
    $id: ObjectIDFilterScalar!) {
    get_signature(id: $id) {
        error {
            detail
            status
        }

        data {
            _id
            reference
        }
    }
}
    """

    current_index = 1
    while True:
        LOGGER.debug(f"current_index {current_index}")
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
            signature_variables = {
                "id": id,
            }
            signature_body = {"query": signature_query, "variables": signature_variables}
            signature_data = pyxis.graphql_query(graphql_api, signature_body)
            LOGGER.debug(f"signature_data: {signature_data}")
            reference_from_signature = signature_data["get_signature"]["data"]["reference"]
            LOGGER.debug(f"reference_from_signature: {reference_from_signature}")
            references.add(reference_from_signature)
            current_index += 1
        else:
            LOGGER.debug("no more signatures")
            break
    LOGGER.info(f"Found {len(references)} references.")
    return references


def main():  # pragma: no cover
    """Main func"""
    args = parse_arguments()
    log_level = logging.DEBUG if args.verbose else logging.INFO
    pyxis.setup_logger(level=log_level)

    LOGGER.debug(f"Pyxis GraphQL API: {args.pyxis_graphql_api}")

    references = find_signatures_for_repository(
        args.pyxis_graphql_api, args.repository, args.manifest_digest
    )
    with open(args.output_file, "w") as f:
        for line in references:
            f.write(f"{line}\n")
    LOGGER.info(f"Writing references to {args.output_file}")


if __name__ == "__main__":  # pragma: no cover
    main()
