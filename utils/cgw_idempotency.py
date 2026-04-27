import logging
import re
from urllib.parse import urlsplit, urlunsplit

import requests

TIMESTAMP_TOKEN_RE = re.compile(r"-(\d{10})(?=-)")


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
    products = call_cgw_api(
        host=host, method="GET", endpoint="/products", session=session
    ).json()
    for product in products:
        if product.get("name") == product_name and product.get("productCode") == product_code:
            logging.info("Found product: %s with ID %s", product_name, product.get("id"))
            return product.get("id")
    raise ValueError(f"Product {product_name} not found with product code {product_code}")


def get_version_id(*, host, session, product_id, version_name):
    """Retrieve the version ID for a specific product."""
    versions = call_cgw_api(
        host=host,
        method="GET",
        endpoint=f"/products/{product_id}/versions",
        session=session,
    ).json()
    for version in versions:
        if version.get("versionName") == version_name:
            logging.info("Found version: %s with ID %s", version_name, version.get("id"))
            return version.get("id")
    raise ValueError(f"Version not found: {version_name}")


def find_existing_file(existing_files, new_file):
    """Find a file with matching logical shortURL, regardless of downloadURL."""
    new_shorturl = normalize_shorturl_for_matching(new_file.get("shortURL", ""))
    for file in existing_files:
        if normalize_shorturl_for_matching(file.get("shortURL", "")) == new_shorturl:
            return file
    return None


def normalize_shorturl_for_matching(shorturl):
    """
    Normalize shortURL for idempotent matching.

    For timestamped filenames such as
    `...-1777494747-x86_64-boot.iso.gz`, strip the epoch token so
    subsequent runs with a new timestamp still match the same logical file.
    """
    if not shorturl:
        return shorturl

    parts = urlsplit(shorturl)
    segments = parts.path.split("/")
    if segments:
        segments[-1] = TIMESTAMP_TOKEN_RE.sub("", segments[-1])
    normalized_path = "/".join(segments)
    return urlunsplit(
        (parts.scheme, parts.netloc, normalized_path, parts.query, parts.fragment)
    )


def remove_duplicate_entries(*, host, session, product_id, version_id, duplicates):
    """Delete duplicate CGW file entries and keep one canonical entry."""
    if not duplicates:
        return

    for dup in duplicates:
        call_cgw_api(
            host=host,
            method="DELETE",
            endpoint=f"/products/{product_id}/versions/{version_id}/files/{dup['id']}",
            session=session,
        )
        logging.info("Removed duplicate CGW file ID: %s (%s)", dup["id"], dup.get("shortURL"))


def update_file(*, host, session, product_id, version_id, file_id, file_metadata):
    """Update an existing file using POST with ID in body."""
    update_data = {**file_metadata, "id": file_id}
    return call_cgw_api(
        host=host,
        method="POST",
        endpoint=f"/products/{product_id}/versions/{version_id}/files",
        session=session,
        data=update_data,
    )


def rollback_files(*, host, session, product_id, version_id, created_file_ids):
    """Rollback created files by listing and deleting them."""
    if created_file_ids:
        logging.warning(
            "Rolling back created files due to failure (productId: %s, productVersionId: %s)",
            product_id,
            version_id,
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
    """
    Create or update files idempotently.

    Existing files are matched by shortURL:
    - same shortURL + same downloadURL -> skip
    - same shortURL + different downloadURL -> update
    - no match -> create
    """
    created_file_ids = []
    updated_file_ids = []
    skipped_files_ids = []

    try:
        existing_files = call_cgw_api(
            host=host,
            method="GET",
            endpoint=f"/products/{product_id}/versions/{version_id}/files",
            session=session,
        ).json()

        for file_metadata in metadata:
            matching_files = [
                file
                for file in existing_files
                if normalize_shorturl_for_matching(file.get("shortURL", ""))
                == normalize_shorturl_for_matching(file_metadata.get("shortURL", ""))
            ]
            existing_file = matching_files[0] if matching_files else None

            if len(matching_files) > 1:
                remove_duplicate_entries(
                    host=host,
                    session=session,
                    product_id=product_id,
                    version_id=version_id,
                    duplicates=matching_files[1:],
                )
                existing_files = [
                    f
                    for f in existing_files
                    if f.get("id") not in {d["id"] for d in matching_files[1:]}
                ]

            if existing_file:
                if existing_file.get("downloadURL") != file_metadata.get(
                    "downloadURL"
                ) or existing_file.get("shortURL") != file_metadata.get("shortURL"):
                    logging.info(
                        "Updating file: %s (ID: %s) to latest metadata",
                        file_metadata["label"],
                        existing_file["id"],
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
                    logging.info("Successfully updated file ID: %s", existing_file["id"])
                else:
                    skipped_files_ids.append(existing_file.get("id"))
                    logging.info(
                        "Skipping: File %s already exists with same content (ID: %s)",
                        existing_file["label"],
                        existing_file["id"],
                    )
            else:
                logging.info(
                    "Creating file: %s with ShortURL %s",
                    file_metadata["label"],
                    file_metadata["shortURL"],
                )
                created_file_id = call_cgw_api(
                    host=host,
                    method="POST",
                    endpoint=f"/products/{product_id}/versions/{version_id}/files",
                    session=session,
                    data=file_metadata,
                ).json()
                logging.info("Successfully created file with ID: %s", created_file_id)
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
