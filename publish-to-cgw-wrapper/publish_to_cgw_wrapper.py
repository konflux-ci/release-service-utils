#!/usr/bin/env python3
"""
This script interacts with the Content Gateway (CGW) API to create and manage content files.
It ensures each file is checked before creation and skips files that already exist.
The script is idempotent, it can be executed multiple times as long as the label,
short URL, and download URL remain unchanged.

### **Functionality:**
1. Reads a JSON snapshot containing data that has been injected with contentGateway,
   files and contentDir.
2. Validates that all required fields are present and non-empty.
3. For each `component` entry:
    - Retrieves the product ID and version ID
    - Generates metadata for each file listed in `files` and located in the
      content directory.
    - Checks for existing files and skips them if they match the label,
      short URL, and downloadURL.
    - Creates new files using the metadata.
    - Rolls back created files if an error occurs during execution.
4. Output all `result_data`.
"""

import os
import argparse
import json
import hashlib
import logging
import importlib
import sys
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        prog="publish_to_cgw_wrapper", description="Publish content to the Content Gateway"
    )
    parser.add_argument(
        "--cgw_host",
        required=True,
        help="The hostname of the content-gateway to publish the metadata to",
    )
    parser.add_argument(
        "--data_json",
        required=True,
        help="JSON string containing snapshot merged data",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        help="Simulate the script without API calls",
    )

    return parser.parse_args()


def load_data(data_arg):
    """Load JSON from string"""
    try:
        return json.loads(data_arg)
    except json.JSONDecodeError:
        raise ValueError("Invalid 'data_json' must be a valid JSON string")


def validate_components(data):
    """
    Validates snapshot component data. Skips components without a 'contentGateway'
    and fails if required fields are missing in either the 'contentGateway'
    or any listed files. Returns only the valid components.

    Note: Filename is always derived from the 'source' field using basename.
    """
    required_cg_keys = ["productCode", "productName", "productVersionName", "contentDir"]
    errors = []
    valid_components = []

    components = data.get("components")
    if not components:
        raise ValueError("Missing or empty 'components' in data.")

    for c_num, component in enumerate(components, start=1):
        if "contentGateway" not in component:
            logging.warning(
                f"Configuration is not defined for publishing... "
                f"skipping component {c_num}"
            )
            continue

        error = False

        if not component.get("name"):
            errors.append(f"Component {c_num} is missing 'name'")
            error = True

        for param in required_cg_keys:
            if not component["contentGateway"].get(param):
                errors.append(f"Component {c_num} is missing '{param}'")
                error = True

        # Validate files array - require source field
        for f_num, file in enumerate(component.get("files", []), start=0):
            if not file.get("source"):
                errors.append(
                    f"Component {c_num}, file {f_num} is missing or has empty 'source'"
                )
                error = True

        if not error:
            valid_components.append(component)

    if errors:
        raise ValueError("Validation failed with the following errors:\n" + "\n".join(errors))

    logging.info(
        f"Validation summary: {len(components)} total components, "
        f"{len(valid_components)} valid components, "
        f"{len(components) - len(valid_components)} skipped components"
    )
    return valid_components


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
    *,
    content_dir,
    component_name,
    files,
    product_code,
    version_id,
    version_name,
    mirror_openshift_Push,
    component_index,
):
    """
    Generate metadata for files listed in 'files' and present in the content_dir.
    Also includes metadata for checksum files starting with 'sha256' or component name.

    Ordering scheme:
      - Checksum files get fixed orders 1, 2, 3.
      - Regular files get order = component_index * 1000 + file_position.
        This keeps each component's files in a unique range.
    """

    logging.info(f"Generating metadata for files in {content_dir}")

    default_values_per_component = {
        "type": "FILE",
        "hidden": False,
        "invisible": False,
    }
    shortURL_base = ""
    if mirror_openshift_Push:
        shortURL_base = "/pub/cgw"

    dir_contents = set(os.listdir(content_dir))

    # Checksum files always get fixed orders 1, 2, 3
    checksum_files = [
        ("sha256sum.txt", "Checksum", 1),
        ("sha256sum.txt.gpg", "Checksum - GPG", 2),
        ("sha256sum.txt.sig", "Checksum - Signature", 3),
    ]

    # Regular files get order = component_index * 1000 + position
    rpa_files = [
        (os.path.basename(f["source"]), os.path.basename(f["source"]))
        for f in files
        if os.path.basename(f["source"]) in dir_contents
    ]

    metadata = []
    for name, label, order in checksum_files:
        if name not in dir_contents:
            continue
        logging.info(f"Processing file: {name} (order={order}, label={label})")
        metadata.append(
            {
                **default_values_per_component,
                "shortURL": f"{shortURL_base}/{product_code}/{version_name}/{name}",
                "productVersionId": version_id,
                "downloadURL": generate_download_url(content_dir, name),
                "label": label,
                "order": order,
            }
        )

    for i, (file_name, label) in enumerate(rpa_files):
        order = component_index * 1000 + i
        logging.info(f"Processing file: {file_name} (order={order}, label={label})")
        metadata.append(
            {
                **default_values_per_component,
                "shortURL": f"{shortURL_base}/{product_code}/{version_name}/{file_name}",
                "productVersionId": version_id,
                "downloadURL": generate_download_url(content_dir, file_name),
                "label": label,
                "order": order,
            }
        )

    return metadata


def process_component(*, host, session, component, dry_run=False, component_index):
    """
    Process a component retrieve product/version ID,
    generate metadata, create files, and return the result
    data (per component)
    """
    productName = component["contentGateway"]["productName"]
    productCode = component["contentGateway"]["productCode"]
    productVersionName = component["contentGateway"]["productVersionName"]
    mirror_openshift_Push = component["contentGateway"].get("mirrorOpenshiftPush")
    contentDir = component["contentGateway"]["contentDir"]
    componentName = component["name"]
    files = component["files"]

    if dry_run:
        product_id = 999999
        product_version_id = 999999
    else:
        product_id = cgw_idempotency.get_product_id(
            host=host,
            session=session,
            product_name=productName,
            product_code=productCode,
        )

        product_version_id = cgw_idempotency.get_version_id(
            host=host,
            session=session,
            product_id=product_id,
            version_name=productVersionName,
        )

    metadata = generate_metadata(
        content_dir=contentDir,
        component_name=componentName,
        files=files,
        product_code=productCode,
        version_id=product_version_id,
        version_name=productVersionName,
        mirror_openshift_Push=mirror_openshift_Push,
        component_index=component_index,
    )

    if dry_run:
        created = [999999 for _ in metadata]
        updated = []
        skipped = []
    else:
        created, updated, skipped = cgw_idempotency.create_files(
            host=host,
            session=session,
            product_id=product_id,
            version_id=product_version_id,
            metadata=metadata,
        )

    logging.info(
        f"Created {len(created)} files, "
        f"Updated {len(updated)} files, "
        f"Skipped {len(skipped)} files."
    )

    result_data = {
        "product_id": product_id,
        "product_version_id": product_version_id,
        "created_file_ids": created,
        "updated_file_ids": updated,
        "no_of_files_processed": len(metadata),
        "no_of_files_created": len(created),
        "no_of_files_updated": len(updated),
        "no_of_files_skipped": len(skipped),
        "metadata": metadata,
    }

    return result_data


def main():
    try:
        args = parse_args()

        USERNAME = os.getenv("CGW_USERNAME")
        PASSWORD = os.getenv("CGW_PASSWORD")

        if not USERNAME or not PASSWORD:
            raise ValueError(
                "CGW_USERNAME and CGW_PASSWORD environment variables are required"
            )

        session = requests.Session()
        session.auth = HTTPBasicAuth(USERNAME, PASSWORD)
        session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        data = load_data(args.data_json)
        components = validate_components(data)
        if not components:
            # Exit without error if there are no valid components to publish
            logging.warning("No components eligible for publishing")
            exit(0)

        all_results = []
        for num, component in enumerate(components, start=1):
            content_gateway = component["contentGateway"]
            logging.info(
                f"Processing component: {num}/{len(components)} "
                f"(productName: {content_gateway['productName']} "
                f"productVersionName: {content_gateway['productVersionName']})"
            )
            try:
                result_data = process_component(
                    host=args.cgw_host,
                    session=session,
                    component=component,
                    dry_run=args.dry_run,
                    component_index=num,
                )
                if result_data is None:
                    continue

                all_results.append(result_data)

            except Exception as e:
                if all_results:
                    logging.warning("Rolling back all created files due to error.")
                    for result in all_results:
                        cgw_idempotency.rollback_files(
                            host=args.cgw_host,
                            session=session,
                            product_id=result["product_id"],
                            version_id=result["product_version_id"],
                            created_file_ids=result["created_file_ids"],
                        )
                raise RuntimeError(
                    f"Error processing component {num} "
                    f"(productName: {content_gateway.get('productName')}, "
                    f"productVersionName: {content_gateway.get('productVersionName')}): {e}"
                )

        logging.info("Processed result:\n%s", json.dumps(all_results, indent=2))
        logging.info("All files processed successfully.")
        return all_results
    except Exception as e:
        logging.error(e)
        exit(1)


if __name__ == "__main__":
    main()
