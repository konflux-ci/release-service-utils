#!/usr/bin/env python3
"""Publish Index Image - Copies an index image from source to target registry.

This script implements idempotent image publishing with digest-based deduplication.
"""

import argparse
import logging
import re
import sys
from pathlib import Path
from typing import Optional

from logger import setup_logger
from rsmodels.secret import Secret
from rsmodels.container_image import ContainerImage
from skopeo import SkopeoClient, SkopeoClientError


def load_credential(file_path: str, logger: logging.Logger) -> Secret:
    """Load credential from file and return as Secret.

    Args:
        file_path: Path to the credential file.
        logger: Logger instance for logging.

    Returns:
        Secret object containing the credential.

    Raises:
        Exception if there is an error reading the credential file.

    """
    try:
        cred_text = Path(file_path).read_text().strip()
        return Secret(cred_text, name=Path(file_path).name)
    except Exception as e:
        logger.error(f"Error reading credential from {file_path}: {e}")
        raise


def extract_source_digest(source_index: str) -> str:
    """Try to extract digest from source pull spec (everything after @).

    If no digest is found, return the whole string.

    Args:
        source_index: The source image pull spec (e.g., registry.io/image@sha256:...).

    Returns:
        The extracted digest string if found, otherwise the whole source_index string.

    """
    if "@" not in source_index:
        return (
            source_index  # If no digest, return the whole string (though this is unexpected)
        )
    return source_index.split("@", 1)[1]


def needs_source_auth(source_index: str) -> bool:
    """Check if source registry requires authentication.

    Red Hat internal proxy registries don't require authentication.
    Pattern: registry-proxy.engineering.redhat.com or
    registry-proxy-stage.engineering.redhat.com

    Args:
        source_index: The source image pull spec (e.g., registry.io/image@sha256

    Returns:
        True if authentication is needed, False otherwise.

    """
    pattern = r"^registry-proxy(-stage)?\.engineering\.redhat\.com"
    return not re.match(pattern, source_index)


def inspect_image(
    client: SkopeoClient,
    index: str,
    credential: Secret,
    logger: logging.Logger,
    config: bool = False,
    retry_times: Optional[int] = None,
) -> ContainerImage | None:
    """Inspect target image and return its digest if it exists.

    Args:
        client: SkopeoClient instance for interacting with registries.
        target_index: The target image pull spec (e.g., registry.io/image:tag).
        target_credential: Credential for authenticating to the target registry.
        logger: Logger instance for logging.
        retry_times: Optional number of retry attempts for network operations.

    Returns:
        The digest string of the target image if it exists, otherwise None.

    """
    logger.info(f"Getting target image digest: {index}")
    try:
        digest = client.inspect(
            f"docker://{index}",
            config=config,
            creds=credential,
            retry_times=retry_times,
        )
        logger.info("Target image exists.")
        return digest
    except SkopeoClientError:
        logger.info("Target image does not exist. Proceeding to copy the image.")
        return None


def copy_image(
    client: SkopeoClient,
    source_index: str,
    target_index: str,
    source_credential: Optional[Secret],
    target_credential: Secret,
    logger: logging.Logger,
    retry_times: Optional[int] = None,
) -> tuple[bool, str]:
    """Copy image from source to target registry.

    Args:
        client: SkopeoClient instance for interacting with registries.
        source_index: The source image pull spec (e.g., registry.io/image@sha256
        target_index: The target image pull spec (e.g., registry.io/image:tag).
        source_credential: Credential for authenticating to the source registry (if needed).
        target_credential: Credential for authenticating to the target registry.
        logger: Logger instance for logging.
        retry_times: Optional number of retry attempts for network operations.

    Returns:
        A tuple containing a boolean indicating success and a message string.

    """
    logger.info(f"Copying image from {source_index} to {target_index}")
    try:
        client.copy(
            f"docker://{source_index}",
            f"docker://{target_index}",
            all=True,
            preserve_digests=True,
            src_tls_verify=False,
            src_creds=source_credential,
            dest_creds=target_credential,
            retry_times=retry_times,
        )
        return True, "Index Image Published successfully"
    except SkopeoClientError as e:
        logger.error(f"Error details: {e}")
        return False, "Error: Failed publishing Index Image"


def write_result(message: str) -> None:
    """Write result message to stdout.

    Args:
        message: The message to write to stdout.

    """
    print(message)


def parse_arguments() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Publish index image from source to target registry"
    )
    parser.add_argument(
        "--source-index",
        required=True,
        help="Source image pull spec with digest (e.g., registry.io/image@sha256:...)",
    )
    parser.add_argument(
        "--target-index",
        required=True,
        help="Target image pull spec (e.g., registry.io/image:tag)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of retry attempts for network operations",
    )
    parser.add_argument(
        "--source-credential-path",
        default="/mnt/publishingCredentials/sourceIndexCredential",
        help="Path to source registry credential file",
    )
    parser.add_argument(
        "--target-credential-path",
        default="/mnt/publishingCredentials/targetIndexCredential",
        help="Path to target registry credential file",
    )
    parser.add_argument(
        "--target-ocp-version",
        default="OCP Version this image was built to",

    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose (debug) logging"
    )

    return parser.parse_args()


def main() -> int:
    """Run main workflow execution."""
    args = parse_arguments()

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logger = setup_logger(level=log_level, name="publish_index_image")

    try:
        # Step 1: Initialize skopeo client
        client = SkopeoClient(logger=logger)

        # Step 2: Load credentials
        logger.info(f"Loading source credential from: {args.source_credential_path}")
        source_credential = load_credential(args.source_credential_path, logger=logger)
        logger.info(f"Loading target credential from: {args.target_credential_path}")
        target_credential = load_credential(args.target_credential_path, logger=logger)

        # Step 3: Extract source digest
        logger.info(f"Extracting source digest from: {args.source_index}")
        source_digest = extract_source_digest(args.source_index)

        # Step 4: Determine if source needs authentication
        source_creds = source_credential if needs_source_auth(args.source_index) else None

        # Step 5: Inspect source image to get ocp version
        source_container_image = inspect_image(
            client,
            args.source_index,
            source_creds,
            config=True,
            logger=logger,
            retry_times=args.retries,
        )
        source_ocp_version = source_container_image.config.Labels.get("com.redhat.component.ocp-version") \
                if source_container_image else None
        if source_ocp_version != args.target_ocp_version:
            message = (f"The source index does not "
                       f"match its targetOcpVersion ({source_ocp_version} != {args.target_ocp_version})")
            logger.error(message)
            write_result(message)
            sys.exit(1)

        # Step 6: Check target image existence
        target_container_image = inspect_image(
            client,
            args.target_index,
            target_credential,
            config=True,
            logger=logger,
            retry_times=args.retries,
        )
        target_digest = target_container_image.digest if target_container_image else None

        # hotfix and pre-ga targetIndex should be skip the next check, as they don't exist in the upstream quay
        # until skopeo copy runs.
        if re.match(r".*\:v[0-9]{1}\.[0-9]{2}$", args.target_index):
            real_target_ocp_version = target_container_image.config.Labels.get("com.redhat.component.ocp-version")
            # check if both indexes are of the same OCP version and exit in case they mismatch.
            if source_ocp_version != real_target_ocp_version:
                message = (f"The indexes versions does not match"
                           f"({source_ocp_version} != {real_target_ocp_version})")
                logger.error(message)
                write_result(message)

        # Step 7 & 8: Compare digests if target exists
        if target_digest is not None:
            logger.debug(f"Source Digest - {source_digest}")
            logger.debug(f"Target Digest - {target_digest}")

            if source_digest == target_digest:
                # Digests match - skip copy
                message = "Image already exists with the same digest, skipping copy."
                write_result(message)
                return 0
            else:
                logger.info(
                    "Image exists in target registry but digests do not match. "
                    "Proceeding to copy the image."
                )

        # Step 8: Execute copy operation
        success, message = copy_image(
            client,
            args.source_index,
            args.target_index,
            source_creds,
            target_credential,
            logger=logger,
            retry_times=args.retries,
        )

        # Step 9: Write result
        write_result(message)

        return 0 if success else 1

    except Exception as e:
        logger.exception(f"Failed to publish index image: {e}")
        write_result(str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
