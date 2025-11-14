#!/usr/bin/env python3
"""
Python script to find all signatures in Pyxis for a given repository and manifest_digest
and save a list of references with keys that are signed in a text file, one reference per line

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
        "and save a list of references with keys that are signed in a text file, one reference"
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
        help="output file of references with keys found",
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


def find_signatures_for_repository(
    graphql_api, repository: str, manifest_digest: str, page_size: int = 50
) -> set:
    LOGGER.info(f"repository: {repository}")
    LOGGER.info(f"manifest_digest: {manifest_digest}")
    references_with_keys = set()
    query = """
query ($repository: String!, $manifest_digest: String!, $page: Int!, $page_size: Int!) {
    find_signatures(
        page: $page
        page_size: $page_size
        sort_by: [{field: "last_update_date", order: DESC}]
        filter: {and: [{manifest_digest: {eq: $manifest_digest}},
            {repository: {eq: $repository}}]}
    ) {
        error {
            detail
            status
        }
        data {
            _id
            reference
            sig_key_id
        }
    }
}
    """
    has_more = True
    page = 0
    while has_more:
        LOGGER.debug(f"current page: {page}")
        variables = {
            "repository": repository,
            "manifest_digest": manifest_digest,
            "page": page,
            "page_size": page_size,
        }
        body = {"query": query, "variables": variables}
        data = pyxis.graphql_query(graphql_api, body)
        signatures = data["find_signatures"]["data"]
        LOGGER.debug(f"Found {len(signatures)} signatures.")
        references_with_keys.update(
            [f"{signature['reference']} {signature['sig_key_id']}" for signature in signatures]
        )
        has_more = len(signatures) == page_size
        page += 1
    LOGGER.info(f"Found {len(references_with_keys)} references.")
    return references_with_keys


def main():  # pragma: no cover
    """Main func"""
    args = parse_arguments()
    log_level = logging.DEBUG if args.verbose else logging.INFO
    pyxis.setup_logger(level=log_level)

    LOGGER.debug(f"Pyxis GraphQL API: {args.pyxis_graphql_api}")

    references_with_keys = find_signatures_for_repository(
        args.pyxis_graphql_api, args.repository, args.manifest_digest
    )
    with open(args.output_file, "w") as f:
        for ref_with_key in references_with_keys:
            f.write(f"{ref_with_key}\n")
    LOGGER.info(f"Writing references to {args.output_file}")


if __name__ == "__main__":  # pragma: no cover
    main()
