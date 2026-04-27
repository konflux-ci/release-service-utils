#!/usr/bin/env python3
"""Publish content to CGW using idempotent create/update behavior."""

import os
import sys
import yaml
import hashlib
import argparse
import logging
import importlib
from pathlib import Path

import requests
from requests.auth import HTTPBasicAuth


def _load_cgw_idempotency():
    """Load shared idempotency helpers when run as script or module."""
    try:
        return importlib.import_module("utils.cgw_idempotency")
    except ModuleNotFoundError:
        root_dir = Path(__file__).resolve().parent.parent
        if str(root_dir) not in sys.path:
            sys.path.insert(0, str(root_dir))
        return importlib.import_module("utils.cgw_idempotency")


cgw_idempotency = _load_cgw_idempotency()

LOG = logging.getLogger("developer-portal-wrapper")
DEFAULT_LOG_FMT = "%(asctime)s [%(levelname)-8s] %(message)s"
DEFAULT_DATE_FMT = "%Y-%m-%d %H:%M:%S %z"
CGW_ENV_VARS_STRICT = (
    "CGW_USERNAME",
    "CGW_PASSWORD",
)

WORKSPACE_DIR = "/tmp"
METADATA_FILE_PATH = f"{WORKSPACE_DIR}/cgw_metadata.yaml"

os.makedirs(WORKSPACE_DIR, exist_ok=True)


def generate_download_url(content_dir, file_name):
    """
    Generate a download URL in this format:
    /content/origin/files/sha256/{checksum[:2]}{checksum}/{file_name}
    """
    prefix = "/content/origin/files/sha256"
    sha256_hash = hashlib.sha256()
    with open(content_dir + "/" + file_name, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    checksum = sha256_hash.hexdigest()
    return f"{prefix}/{checksum[:2]}/{checksum}/{file_name}"


def generate_metadata(
    product_name, product_code, product_version_name, content_dir, content_list, file_prefix
):
    """
    Generate metadata for each file in
    content_list that starts with the component name
    """
    metadata = []
    short_url_prefix = f"/cgw/{product_code}/{product_version_name}"

    for order, file in enumerate(content_list, start=1):
        if not file.startswith(file_prefix):
            LOG.warning(
                "Skipping file: %s as it does not start with the expected prefix", file
            )
            continue

        LOG.info("Processing file: %s", file)
        metadata.append(
            {
                "shortURL": f"{short_url_prefix}/{file}",
                "downloadURL": generate_download_url(content_dir, file),
                "label": file,
                "type": "FILE",
                "hidden": False,
                "invisible": False,
                "order": order,
            }
        )

    return metadata


def validate_env_vars():
    assert all(
        [os.getenv(item) for item in CGW_ENV_VARS_STRICT]
    ), f"Provide all required CGW environment variables: {', '.join(CGW_ENV_VARS_STRICT)}"


def parse_args():
    parser = argparse.ArgumentParser(
        prog="developer_portal_wrapper",
        description="Add binaries to Developer Portal.",
    )

    parser.add_argument("--dry-run", action="store_true", help="Log command to be executed")
    parser.add_argument(
        "--debug",
        "-d",
        action="count",
        default=0,
        help=("Show debug logs; can be provided up to three times to enable more logs"),
    )

    parser.add_argument(
        "--product-name",
        help="Product Name in Developer Portal",
        required=True,
    )
    parser.add_argument(
        "--product-code",
        help="Product Code in Developer Portal",
        required=True,
    )
    parser.add_argument(
        "--product-version-name",
        help="Product Version Name in Developer Portal",
        required=True,
    )
    parser.add_argument(
        "--cgw-hostname",
        help=(
            "Content Gateway base URL in Developer Portal, "
            "for example: https://developers.qa.redhat.com/content-gateway/rest/admin"
        ),
        required=True,
    )
    parser.add_argument(
        "--content-directory",
        help="Content Directory to use for CGW operations",
        required=True,
    )
    parser.add_argument(
        "--file-prefix",
        help="Prefix to use when searching for file in Content Directory",
        required=True,
    )

    return parser.parse_args()


def publish_metadata(
    *,
    cgw_hostname,
    product_name,
    product_code,
    product_version_name,
    metadata,
    dry_run=False,
):
    username = os.getenv("CGW_USERNAME")
    password = os.getenv("CGW_PASSWORD")

    session = requests.Session()
    session.auth = HTTPBasicAuth(username, password)
    session.headers.update(
        {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
    )

    if dry_run:
        product_id = 999999
        product_version_id = 999999
        created_ids = [999999 for _ in metadata]
        updated_ids = []
        skipped_ids = []
    else:
        product_id = cgw_idempotency.get_product_id(
            host=cgw_hostname,
            session=session,
            product_name=product_name,
            product_code=product_code,
        )
        product_version_id = cgw_idempotency.get_version_id(
            host=cgw_hostname,
            session=session,
            product_id=product_id,
            version_name=product_version_name,
        )
        for item in metadata:
            item["productVersionId"] = product_version_id

        created_ids, updated_ids, skipped_ids = cgw_idempotency.create_files(
            host=cgw_hostname,
            session=session,
            product_id=product_id,
            version_id=product_version_id,
            metadata=metadata,
        )

    LOG.info(
        "CGW publish summary: created=%s updated=%s skipped=%s",
        len(created_ids),
        len(updated_ids),
        len(skipped_ids),
    )


def main():
    args = parse_args()

    loglevel = logging.DEBUG if args.debug else logging.INFO

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(loglevel)

    logging.basicConfig(
        level=loglevel,
        format=DEFAULT_LOG_FMT,
        datefmt=DEFAULT_DATE_FMT,
        handlers=[stream_handler],
    )
    validate_env_vars()

    if args.dry_run:
        LOG.info("This is a dry-run!")

    product_name = args.product_name
    product_code = args.product_code
    product_version_name = args.product_version_name
    cgw_hostname = args.cgw_hostname

    content_dir = args.content_directory
    file_prefix = args.file_prefix
    content_list = os.listdir(content_dir)

    metadata = generate_metadata(
        product_name,
        product_code,
        product_version_name,
        content_dir,
        content_list,
        file_prefix,
    )
    LOG.info("%s files will be published to CGW", len(metadata))

    with open(METADATA_FILE_PATH, "w") as file:
        yaml.dump(metadata, file, default_flow_style=False, sort_keys=False)

    LOG.info(f"YAML content dumped to {METADATA_FILE_PATH}")
    if args.dry_run:
        LOG.info(
            "Would idempotently publish %s file(s) to CGW host %s for product %s/%s",
            len(metadata),
            cgw_hostname,
            product_code,
            product_version_name,
        )
        publish_metadata(
            cgw_hostname=cgw_hostname,
            product_name=product_name,
            product_code=product_code,
            product_version_name=product_version_name,
            metadata=metadata,
            dry_run=True,
        )
    else:
        try:
            LOG.info("Publishing metadata to CGW using idempotent API flow")
            publish_metadata(
                cgw_hostname=cgw_hostname,
                product_name=product_name,
                product_code=product_code,
                product_version_name=product_version_name,
                metadata=metadata,
            )
        except RuntimeError:
            LOG.exception("CGW publish failed")
            raise
        except Exception as exc:
            LOG.exception("Unknown error occurred")
            raise RuntimeError from exc


if __name__ == "__main__":
    main()
