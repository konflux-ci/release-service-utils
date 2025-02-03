#!/usr/bin/env python3
"""
This script interacts with the Content Gateway (CGW) API to create and manage content files.
It ensures each file is checked before creation and skips files that already exist.
The script is idempotent,it can be executed multiple times as long as the label,
short URL, and download URL remain unchanged.

### **Functionality:**
1. Reads a JSON metadata file and a directory containing content files.
2. Retrieves the product ID using the provided product name and product code.
3. Retrieves the version ID using the product version name.
4. Generates metadata for each file in the content directory.
5. Checks for existing files and skips them if they match the label, short URL, and download
URL.
6. Creates new files using the metadata.
7. Rolls back created files if an error occurs during execution.
8. Writes the final result, including processed, created, and skipped files, to a JSON file.
9. Outputs the path of the generated result.json file to an output file.
"""

import os
import argparse
import json
import hashlib
import logging
import requests
from requests.auth import HTTPBasicAuth

# Default values for each component,
# values from data_file takes precedence over these
default_values_per_component = {
    "type": "FILE",
    "hidden": False,
    "invisible": False,
}

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
        "--data_file",
        required=True,
        help="Path to the JSON file containing merged data",
    )
    parser.add_argument(
        "--content_dir",
        required=True,
        help="Path to the directory containing content to push",
    )
    parser.add_argument(
        "--output_file",
        required=True,
        help="Path to the file which write the result.json file path",
    )

    return parser.parse_args()


def call_cgw_api(*, host, method, endpoint, session, data=None):
    """Make an API call to the Content Gateway service."""
    try:
        response = session.request(
            method=method.upper(),
            url=f"{host}{endpoint}",
            json=data,
        )

        if not response.ok:
            error_message = (
                response.text.strip() or f"HTTP {response.status_code}:{response.reason}"
            )
            raise RuntimeError(f"API call failed: {error_message}")

        return response
    except requests.RequestException as e:
        raise RuntimeError(f"API call failed: {e}")


def get_product_id(*, host, session, product_name, product_code):
    """Retrieve the product ID by name and product code."""
    products = call_cgw_api(host=host, method="GET", endpoint="/products", session=session)
    products = products.json()
    for product in products:
        if product.get("name") == product_name and product.get("productCode") == product_code:
            logging.info(f"Found product: {product_name} with ID {product.get('id')}")
            return product.get("id")
    raise ValueError(f"Product {product_name} not found with product code {product_code}")


def get_version_id(*, host, session, product_id, version_name):
    """Retrieve the version ID for a specific product."""
    versions = call_cgw_api(
        host=host, method="GET", endpoint=f"/products/{product_id}/versions", session=session
    )
    versions = versions.json()
    for version in versions:
        if version.get("versionName") == version_name:
            logging.info(f"Found version: {version_name} with ID {version.get('id')}")
            return version.get("id")
    raise ValueError(f"Version not found: {version_name}")


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
    *, content_dir, components, product_Code, version_id, version_name, mirror_openshift_Push
):
    """
    Generate metadata for each file in
    content_list that starts with the component name
    """
    shortURL_base = "/pub/"
    if mirror_openshift_Push:
        shortURL_base = "/pub/cgw"
    metadata = []
    shasum_files_processed = []
    logging.info(f"Generating metadata for files in {content_dir}")
    for file in os.listdir(content_dir):
        matching_component = None
        for component in components:
            if file.startswith(component["name"]):
                matching_component = component.copy()
                break

        if matching_component:
            logging.info(f"Processing file: {file}")
            matching_component.update(
                {
                    "productVersionId": version_id,
                    "downloadURL": generate_download_url(content_dir, file),
                    "shortURL": f"{shortURL_base}/{product_Code}/{version_name}/{file}",
                    "label": file,
                }
            )
            del matching_component["name"]
            metadata.append(
                {"type": "file", **default_values_per_component, **matching_component}
            )
        else:
            if file.startswith("sha256") and file not in shasum_files_processed:
                shasum_files_processed.append(file)
                logging.info(f"Processing file: {file}")
                if file.endswith(".gpg"):
                    label = "Checksum - GPG"
                elif file.endswith(".sig"):
                    label = "Checksum - Signature"
                elif file.endswith(".txt"):
                    label = "Checksum"

                metadata.append(
                    {
                        "productVersionId": version_id,
                        "downloadURL": generate_download_url(content_dir, file),
                        "shortURL": f"{shortURL_base}/{product_Code}/{version_name}/{file}",
                        "label": label,
                        **default_values_per_component,
                    }
                )
            else:
                # Skip files that do not start with any component name or
                # sha256
                logging.info(
                    f"Skipping file: {file} as it does not start with any component name"
                )
                continue

    return metadata


def file_already_exists(existing_files, new_file):
    """Check if a file already exists"""
    for file in existing_files:
        if all(
            file.get(key) == new_file.get(key) for key in ["label", "downloadURL", "shortURL"]
        ):
            return file
    return None


def rollback_files(*, host, session, product_id, version_id, created_file_ids):
    """Rollback created files by listing and deleting them."""
    if created_file_ids:
        logging.warning("Rolling back created files due to failure")

    for file_id in created_file_ids:
        try:
            call_cgw_api(
                host=host,
                method="DELETE",
                endpoint=f"/products/{product_id}/versions/{version_id}/files/{file_id}",
                session=session,
            )
        except Exception as e:
            raise RuntimeError(f"Failed to rollback file: {e}")


def create_files(*, host, session, product_id, version_id, metadata):
    """Create files using the metadata created and rollback on failure."""
    created_file_ids = []
    skipped_files_ids = []
    try:
        existing_files = call_cgw_api(
            host=host,
            method="GET",
            endpoint=f"/products/{product_id}/versions/{version_id}/files",
            session=session,
        )
        existing_files = existing_files.json()

        for file_metadata in metadata:
            file_check = file_already_exists(existing_files, file_metadata)
            if file_check:
                skipped_files_ids.append(file_check.get("id"))
                logging.info(
                    "Skipping creation: File {} already exists with ShortURL {}".format(
                        file_check["label"], file_check["shortURL"]
                    )
                )
                continue
            logging.info(
                "Creating file: {} with ShortURL {}".format(
                    file_metadata["label"], file_metadata["shortURL"]
                )
            )
            created_file_id = call_cgw_api(
                host=host,
                method="POST",
                endpoint=f"/products/{product_id}/versions/{version_id}/files",
                session=session,
                data=file_metadata,
            )
            created_file_id = created_file_id.json()
            logging.info(f"Succesfully created file with ID: {created_file_id}")
            created_file_ids.append(created_file_id)
        return created_file_ids, skipped_files_ids
    except Exception as e:
        rollback_files(
            host=host,
            session=session,
            product_id=product_id,
            version_id=version_id,
            created_file_ids=created_file_ids,
        )
        raise RuntimeError(f"Failed to create file: {e}")


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

        with open(args.data_file, "r") as file:
            data = json.load(file)

        productName = data["contentGateway"]["productName"]
        productCode = data["contentGateway"]["productCode"]
        productVersionName = data["contentGateway"]["productVersionName"]
        mirrorOpenshiftPush = data["contentGateway"].get("mirrorOpenshiftPush")
        components = data["contentGateway"]["components"]

        product_id = get_product_id(
            host=args.cgw_host,
            session=session,
            product_name=productName,
            product_code=productCode,
        )
        product_version_id = get_version_id(
            host=args.cgw_host,
            session=session,
            product_id=product_id,
            version_name=productVersionName,
        )
        metadata = generate_metadata(
            content_dir=args.content_dir,
            components=components,
            product_Code=productCode,
            version_id=product_version_id,
            version_name=productVersionName,
            mirror_openshift_Push=mirrorOpenshiftPush,
        )
        created, skipped = create_files(
            host=args.cgw_host,
            session=session,
            product_id=product_id,
            version_id=product_version_id,
            metadata=metadata,
        )
        logging.info(f"Created {len(created)} files and skipped {len(skipped)} files")

        result_data = {
            "no_of_files_processed": len(metadata),
            "no_of_files_created": len(created),
            "no_of_files_skipped": len(skipped),
            "metadata": metadata,
        }
        result_file = os.path.join(os.path.dirname(args.data_file), "result.json")
        with open(result_file, "w") as f:
            json.dump(result_data, f)
        with open(args.output_file, "w") as f:
            f.write(result_file)

    except Exception as e:
        logging.error(e)
        exit(1)


if __name__ == "__main__":
    main()
