#!/usr/bin/env python3
"""
Run push-cgw-metadata command to push metadata to CGW:
1. Extract all components under contentGateway key from dataPath
2. Find all the files in contentDir that starts with the component name
4. Generate necessary metadata for each file
5. Dump the metadata to a YAML file
6. Run push-cgw-metadata to push the metadata
"""
import os
import sys
import yaml
import hashlib
import subprocess
import argparse
import logging

LOG = logging.getLogger("developer-portal-wrapper")
DEFAULT_LOG_FMT = "%(asctime)s [%(levelname)-8s] %(message)s"
DEFAULT_DATE_FMT = "%Y-%m-%d %H:%M:%S %z"
CGW_ENV_VARS_STRICT = (
    "CGW_USERNAME",
    "CGW_TOKEN",
)

WORKSPACE_DIR = "/tmp"
METADATA_FILE_PATH = f"{WORKSPACE_DIR}/cgw_metadata.yaml"
RESULT_FILE_JSON_PATH = f"{WORKSPACE_DIR}/results.json"

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
    for file in content_list:
        matching_component = file.startswith(file_prefix)

        if matching_component:
            LOG.info("processing file: %s", file)
            component_item = {
                "productName": product_name,
                "productCode": product_code,
                "productVersionName": product_version_name,
                "downloadURL": generate_download_url(content_dir, file),
                "shortURL": f"{short_url_prefix}/{file}",
                "label": file,
                "type": "FILE",
                "hidden": False,
                "invisible": False,
            }
            metadata.append(
                {
                    "type": "file",
                    "action": "create",
                    "metadata": component_item,
                }
            )
        else:
            LOG.warning(f"Skipping file: {file} as it does not start with any component name")

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
        help="Content Gateway Hostname in Developer Portal",
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
    cgw_username = os.getenv("CGW_USERNAME")
    cgw_token = os.getenv("CGW_TOKEN")
    main_command = "push-cgw-metadata"
    common_args = [
        "--CGW_filepath",
        METADATA_FILE_PATH,
        "--CGW_hostname",
        cgw_hostname,
    ]
    cred_args = [
        "--CGW_username",
        cgw_username,
        "--CGW_password",
        cgw_token,
    ]
    command = [main_command] + common_args
    cmd_str = " ".join(command)
    command += cred_args

    if args.dry_run:
        LOG.info("Would have run: %s", cmd_str)
    else:
        try:
            LOG.info("Running %s", cmd_str)
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError:
            LOG.exception("Command %s failed, check exception for details", cmd_str)
            raise
        except Exception as exc:
            LOG.exception("Unknown error occurred")
            raise RuntimeError from exc


if __name__ == "__main__":
    main()
