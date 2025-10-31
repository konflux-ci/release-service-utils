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
import requests
from requests.auth import HTTPBasicAuth

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
    """
    required_cg_keys = ["productCode", "productName", "productVersionName", "contentDir"]
    required_file_keys = ["filename"]
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

        for f_num, file in enumerate(component.get("files"), start=0):
            for param in required_file_keys:
                if not file.get(param):
                    errors.append(
                        f"Component {c_num}, file {f_num} is missing or has empty '{param}'"
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
    *,
    content_dir,
    component_name,
    files,
    product_code,
    version_id,
    version_name,
    mirror_openshift_Push,
):
    """
    Generate metadata for files listed in 'files' and present in the content_dir.
    Also includes metadata for checksum files starting with 'sha256' or component name.
    """

    logging.info(f"Generating metadata for files in {content_dir}")

    default_values_per_component = {
        "type": "FILE",
        "hidden": False,
        "invisible": False,
    }
    shortURL_base = "/cgw"
    if mirror_openshift_Push:
        shortURL_base = "/pub/cgw"

    file_lookup = {file["filename"] for file in files}
    metadata = []

    for file_name in os.listdir(content_dir):
        if file_name in file_lookup:
            logging.info(f"Processing file: {file_name}")
            metadata.append(
                {
                    **default_values_per_component,
                    "shortURL": f"{shortURL_base}/{product_code}/{version_name}/{file_name}",
                    "productVersionId": version_id,
                    "downloadURL": generate_download_url(content_dir, file_name),
                    "label": file_name,
                }
            )
        elif file_name.startswith("sha256") or file_name.startswith(component_name):
            logging.info(f"Processing file: {file_name}")
            label = None
            if file_name.endswith(".gpg"):
                label = "Checksum - GPG"
            elif file_name.endswith(".sig"):
                label = "Checksum - Signature"
            elif file_name.endswith(".txt"):
                label = "Checksum"

            if label:
                metadata.append(
                    {
                        **default_values_per_component,
                        "productVersionId": version_id,
                        "downloadURL": generate_download_url(content_dir, file_name),
                        "shortURL": (
                            f"{shortURL_base}/{product_code}/{version_name}/{file_name}"
                        ),
                        "label": label,
                    }
                )
        else:
            # Skip files that arent listed in files and aren't a checksum.
            logging.warning(
                f"Skipping file: {file_name} "
                "as it's not listed in component 'files' and not a checksum."
            )
            continue

    return metadata


def find_existing_file(existing_files, new_file):
    """Find a file with matching shortURL, regardless of downloadURL"""
    for file in existing_files:
        if file.get("shortURL") == new_file.get("shortURL"):
            return file
    return None


def update_file(*, host, session, product_id, version_id, file_id, file_metadata):
    """Update an existing file using POST with ID in body"""
    # Add the file ID to the metadata for update
    update_data = {**file_metadata, "id": file_id}

    response = call_cgw_api(
        host=host,
        method="POST",
        endpoint=f"/products/{product_id}/versions/{version_id}/files",
        session=session,
        data=update_data,
    )
    return response


def rollback_files(*, host, session, product_id, version_id, created_file_ids):
    """Rollback created files by listing and deleting them."""
    if created_file_ids:
        logging.warning(
            f"Rolling back created files due to failure "
            f"(productId: {product_id}, productVersionId: {version_id})"
        )

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
    updated_file_ids = []
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
            existing_file = find_existing_file(existing_files, file_metadata)

            if existing_file:
                # Check if downloadURL is different (needs update)
                if existing_file.get("downloadURL") != file_metadata.get("downloadURL"):
                    logging.info(
                        f"Updating file: {file_metadata['label']} "
                        f"(ID: {existing_file['id']}) with new downloadURL"
                    )
                    update_file(
                        host=host,
                        session=session,
                        product_id=product_id,
                        version_id=version_id,
                        file_id=existing_file["id"],
                        file_metadata=file_metadata,
                    )
                    updated_file_ids.append(existing_file["id"])
                    logging.info(f"Successfully updated file ID: {existing_file['id']}")
                else:
                    # File exists with same downloadURL - skip
                    skipped_files_ids.append(existing_file.get("id"))
                    logging.info(
                        f"Skipping: File {existing_file['label']} already exists "
                        f"with same content (ID: {existing_file['id']})"
                    )
            else:
                # File doesn't exist - create new
                logging.info(
                    f"Creating file: {file_metadata['label']} "
                    f"with ShortURL {file_metadata['shortURL']}"
                )
                created_file_id = call_cgw_api(
                    host=host,
                    method="POST",
                    endpoint=f"/products/{product_id}/versions/{version_id}/files",
                    session=session,
                    data=file_metadata,
                )
                created_file_id = created_file_id.json()
                logging.info(f"Successfully created file with ID: {created_file_id}")
                created_file_ids.append(created_file_id)
        return created_file_ids, updated_file_ids, skipped_files_ids
    except Exception as e:
        rollback_files(
            host=host,
            session=session,
            product_id=product_id,
            version_id=version_id,
            created_file_ids=created_file_ids,
        )
        raise RuntimeError(f"Failed to create file: {e}")


def process_component(*, host, session, component, dry_run=False):
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
        product_id = get_product_id(
            host=host,
            session=session,
            product_name=productName,
            product_code=productCode,
        )

        product_version_id = get_version_id(
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
    )

    if dry_run:
        created = [999999 for _ in metadata]
        updated = []
        skipped = []
    else:
        created, updated, skipped = create_files(
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
                )
                if result_data is None:
                    continue

                all_results.append(result_data)

            except Exception as e:
                if all_results:
                    logging.warning("Rolling back all created files due to error.")
                    for result in all_results:
                        rollback_files(
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
