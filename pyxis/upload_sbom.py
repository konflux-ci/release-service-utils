#!/usr/bin/env python3
"""Upload sbom to Pyxis

This script will take Pyxis image ID and an sbom cyclonedx file
on the input and it will push the sbom data into Pyxis.
This consists of two steps:
1. Create a ContentManifest in Pyxis, referencing the image ID provided
   If a manifest already exists, creation is skipped.
2. For each sbom component, create a ContentManifestComponent in Pyxis,
   referencing the ContentManifest created in step 1. above.
   If some components already exists, their creation is skipped.

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
import json
import re
from pathlib import Path
import time
from typing import Any
from jinja2 import Template

import pyxis

LOGGER = logging.getLogger("upload_sbom")


# Fields not implemented in Pyxis
# See https://issues.redhat.com/browse/ISV-3376
UNSUPPORTED_FIELDS = [
    "pedigree",
    "signature",
    "components",
]
TEMPLATE_FILE = "create_content_manifest_components.graphql.jinja"


def parse_arguments() -> argparse.Namespace:  # pragma: no cover
    """Parse CLI arguments

    :return: Dictionary of parsed arguments
    """

    parser = argparse.ArgumentParser(description="Upload sbom metadata to Pyxis")

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
        help="If set, retry the whole sbom uploading in case it fails",
    )
    return parser.parse_args()


def upload_sbom_with_retry(
    graphql_api: str,
    image_id: str,
    sbom_path: str,
    retries: int = 3,
    backoff_factor: float = 5.0,
):
    last_err = RuntimeError()
    for attempt in range(retries):
        try:
            time.sleep(backoff_factor * attempt)
            upload_sbom(graphql_api, image_id, sbom_path)
            return
        except RuntimeError as e:
            LOGGER.warning(f"Attempt {attempt+1} failed.")
            last_err = e
    LOGGER.error("Out of attempts. Raising the error.")
    raise last_err


def upload_sbom(graphql_api: str, image_id: str, sbom_path: str):
    image = get_image(graphql_api, image_id)

    if image["content_manifest"] is not None and "_id" in image["content_manifest"]:
        content_manifest_id = image["content_manifest"]["_id"]
        LOGGER.info("Content manifest already exists. Skipping creation.")
    else:
        content_manifest_id = create_content_manifest(graphql_api, image_id)
    LOGGER.info(f"Content manifest ID: {content_manifest_id}")

    existing_component_count = len(image["components"])

    sbom_components = load_sbom_components(sbom_path)
    sbom_component_count = len(sbom_components)
    LOGGER.info(f"Loaded {sbom_component_count} components from sbom file.")

    if existing_component_count >= sbom_component_count:
        LOGGER.info(
            f"Pyxis already contains {existing_component_count} components."
            " Skipping component creation."
        )
        return

    if existing_component_count > 0:
        existing_bom_refs = get_existing_bom_refs(image["components"])
        LOGGER.info(
            f"Skipping {existing_component_count} components already present in Pyxis."
        )
    else:
        existing_bom_refs = set()

    components = []

    for i in range(existing_component_count, sbom_component_count):
        LOGGER.info(f"Processing component {i+1}/{sbom_component_count}")

        component = sbom_components[i]
        component = convert_keys(component)
        remove_unsupported_fields(component)

        # bom-ref is not required, but has to be unique for
        # a given sbom. In most cases it is defined.
        # Pyxis team suggested we at least check this,
        # since Pyxis has no checks for component uniqueness.
        if component.get("bom_ref") is not None:
            if component["bom_ref"] in existing_bom_refs:
                LOGGER.info("Skipping component - bom_ref already exists in Pyxis")
                continue
            else:
                existing_bom_refs.add(component["bom_ref"])

        components.append(component)

    create_content_manifest_components(graphql_api, content_manifest_id, components)


def get_image(graphql_api: str, image_id: str, page_size: int = 50) -> dict:
    """Get ContainerImage object from Pyxis using GraphQL API

    This will also include the content manifest id and all the components
    via edges. The edges are paged, so the whole query is run repeatedly
    until there are no more components.
    """
    query = """
query ($id: ObjectIDFilterScalar!, $page: Int!, $page_size: Int!) {
    get_image(id: $id) {
        data {
            _id
            content_manifest {
                _id
            }
            edges {
                content_manifest_components(page: $page, page_size: $page_size) {
                    data {
                        _id
                        bom_ref
                    }
                }
            }
        }
        error {
            detail
        }
    }
}
    """
    has_more = True
    page = 0
    components = []
    image = {}
    while has_more:
        variables = {"id": image_id, "page": page, "page_size": page_size}
        body = {"query": query, "variables": variables}

        data = pyxis.graphql_query(graphql_api, body)
        image = data["get_image"]["data"]

        components_batch = image["edges"]["content_manifest_components"]["data"]
        components.extend(components_batch)
        has_more = len(components_batch) == page_size
        page += 1
    image["components"] = components

    return image


def create_content_manifest(graphql_api: str, image_id: str) -> str:
    """Create ContentManifest object in Pyxis using GraphQL API"""
    mutation = """
mutation ($input: ContentManifestInput! ) {
    create_content_manifest(input: $input) {
        data {
            _id
        }
        error {
            detail
        }
    }
}
"""
    variables = {"input": {"image": {"_id": image_id}}}
    body = {"query": mutation, "variables": variables}

    data = pyxis.graphql_query(graphql_api, body)

    return data["create_content_manifest"]["data"]["_id"]


def get_existing_bom_refs(components: list) -> set[str]:
    bom_refs = [c["bom_ref"] for c in components if c.get("bom_ref") is not None]
    return set(bom_refs)


def create_content_manifest_components(
    graphql_api: str, content_manifest_id: str, components: list[dict], batch_size: int = 5
):
    """Create ContentManifestComponent objects in Pyxis using GraphQL API.

    Components will be created in batches.
    """
    if not components:
        LOGGER.info("No components to be created - skipping component creation")
        return
    LOGGER.info(f"Creating {len(components)} components in Pyxis...")
    template = get_template()
    for i in range(0, len(components), batch_size):
        batch = components[i : i + batch_size]
        LOGGER.info(f"Adding component {i+1} to {i+len(batch)}")
        mutation = template.render(components=batch)

        variables = {f"input{j}": component for j, component in enumerate(batch)}
        variables["id"] = content_manifest_id

        body = {"query": mutation, "variables": variables}

        pyxis.graphql_query(graphql_api, body)


def get_template() -> Template:
    script_dir = os.path.dirname(__file__)
    template_path = os.path.join(script_dir, "../templates/", TEMPLATE_FILE)
    with open(template_path) as t:
        template = Template(t.read())
    return template


def load_sbom_components(sbom_path: str) -> list[dict]:
    """Open sbom file, load components and return them

    If unable to open and load the json, raise an exception.
    If there are duplicate bom-ref strings in the components,
    raise an exception.
    """
    try:
        with open(sbom_path) as f:
            sbom = json.load(f)
        components = sbom["components"]
    except Exception:
        LOGGER.error("Unable to load components from sbom file")
        raise

    check_bom_ref_duplicates(components)

    return components


def check_bom_ref_duplicates(components: list[dict]):
    """Check if any two components use the same bom-ref string

    bom-ref is not required, but has to be unique for
    a given sbom. In most cases it is defined.
    Pyxis team suggested we at least check this,
    since Pyxis has no checks for component uniqueness.
    """
    bom_refs = [c["bom-ref"] for c in components if c.get("bom-ref") is not None]
    seen = set()
    for x in bom_refs:
        if x in seen:
            LOGGER.error(f"Duplicate bom-ref detected: {x}")
            msg = "Invalid sbom file. bom-ref must to be unique."
            LOGGER.error(msg)
            raise ValueError(msg)
        else:
            seen.add(x)


def convert_keys(item: Any) -> Any:
    """Transform component keys to what's used in Pyxis

    Some of the CycloneDX component field names use characters that are not allowed
    as field names in Pyxis (GraphQL). Namely these two conversions are made recursively:
    1. Use _ instead of camel case, e.g. camelCase -> camel_case
    2. Use _ instead of -, e.g. key-with-dash -> key_with_dash
    """
    if isinstance(item, list):
        return [convert_keys(sub) for sub in item]
    elif isinstance(item, dict):
        d = {}
        for k, v in item.items():
            k = k.replace("-", "_")
            k = re.sub("([A-Z]+)", r"_\1", k).lower()
            d[k] = convert_keys(v)
        return d
    else:
        return item


def remove_unsupported_fields(component: dict):
    """Remove component fields that are not supported in Pyxis"""
    for key in UNSUPPORTED_FIELDS:
        if key in component:
            del component[key]


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
        upload_sbom_with_retry(args.pyxis_graphql_api, image_id, args.sbom_path)
    else:
        upload_sbom(args.pyxis_graphql_api, image_id, args.sbom_path)


if __name__ == "__main__":  # pragma: no cover
    main()
