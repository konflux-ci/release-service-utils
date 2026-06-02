#!/usr/bin/env python3
"""Publish Index Image - Copies an index image from source to target registry.

This script implements idempotent image publishing with digest-based deduplication.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from typing import Any

from logger import setup_logger
from skopeo import inspect, copy


def load_credential(file_path: str, logger: logging.Logger) -> str:
    """Load credential from file and return as string.

    Args:
        file_path: Path to the credential file.
        logger: Logger instance for logging.

    Returns:
        Credential string read from the file.

    Raises:
        Exception if there is an error reading the credential file.

    """
    try:
        return Path(file_path).read_text().strip()
    except Exception as e:
        logger.error(f"Error reading credential from {file_path}: {e}")
        raise


def extract_source_digest(source_index: str, logger: logging.Logger) -> str:
    """Try to extract digest from source pull spec (everything after @).

    If no digest is found, return the whole string.

    Args:
        source_index: The source image pull spec (e.g., registry.io/image@sha256:...).
        logger: Logger instance for logging.

    Returns:
        The extracted digest string if found, otherwise the whole source_index string.

    """
    if "@" not in source_index:
        logger.warning(
            f"No digest found in source index '{source_index}'. "
            "This is unexpected; proceeding with the whole string."
        )
        return source_index
    return source_index.split("@", 1)[1]


def needs_source_auth(source_index: str) -> bool:
    """Check if source registry requires authentication.

    Red Hat internal proxy registries don't require authentication.
    Pattern: registry-proxy.engineering.redhat.com or
    registry-proxy-stage.engineering.redhat.com

    Args:
        source_index: The source image pull spec.

    Returns:
        True if authentication is needed, False otherwise.

    """
    pattern = r"^registry-proxy(-stage)?\.engineering\.redhat\.com"
    return not re.match(pattern, source_index)


def inspect_image(
    index: str,
    credential: str | None,
    logger: logging.Logger,
    config: bool = False,
    retry_times: int | None = None,
) -> dict[str, Any] | None:
    """Inspect an image and return parsed JSON output.

    Args:
        index: The image pull spec (e.g., registry.io/image:tag).
        credential: Credential for authenticating to the registry.
        logger: Logger instance for logging.
        config: Whether to inspect the image config.
        retry_times: Optional number of retry attempts for network operations.

    Returns:
        Parsed JSON dict from skopeo inspect, or None if image does not exist.

    """
    logger.info(f"Inspecting image: {index}")
    kwargs: dict[str, Any] = {"config": config}
    if credential:
        kwargs["creds"] = credential
    if retry_times is not None:
        kwargs["retry_times"] = retry_times

    ret = inspect(index, **kwargs)
    if ret.returncode != 0:
        logger.info("Image does not exist or inspect failed.")
        return None

    logger.info("Image exists.")
    return json.loads(ret.stdout)


def copy_image(
    source_index: str,
    target_index: str,
    source_credential: str | None,
    target_credential: str,
    logger: logging.Logger,
    retry_times: int | None = None,
) -> tuple[bool, str]:
    """Copy image from source to target registry.

    Args:
        source_index: The source image pull spec.
        target_index: The target image pull spec.
        source_credential: Credential for authenticating to the source registry.
        target_credential: Credential for authenticating to the target registry.
        logger: Logger instance for logging.
        retry_times: Optional number of retry attempts for network operations.

    Returns:
        A tuple containing a boolean indicating success and a message string.

    """
    logger.info(f"Copying image from {source_index} to {target_index}")
    kwargs: dict[str, Any] = {
        "all": True,
        "preserve_digests": True,
        "src_tls_verify": False,
        "dest_creds": target_credential,
    }
    if source_credential:
        kwargs["src_creds"] = source_credential
    if retry_times is not None:
        kwargs["retry_times"] = retry_times

    ret = copy(
        f"docker://{source_index}",
        f"docker://{target_index}",
        **kwargs,
    )
    if ret.returncode == 0:
        return True, "Index Image Published successfully"
    else:
        logger.error(f"Error details: {ret.stderr}")
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

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logger = setup_logger(level=log_level, name="publish_index_image")

    try:
        # Step 1: Load credentials
        logger.info(f"Loading source credential from: {args.source_credential_path}")
        source_credential = load_credential(args.source_credential_path, logger=logger)
        logger.info(f"Loading target credential from: {args.target_credential_path}")
        target_credential = load_credential(args.target_credential_path, logger=logger)

        # Step 2: Extract source digest
        logger.info(f"Extracting source digest from: {args.source_index}")
        source_digest = extract_source_digest(args.source_index, logger)

        # Step 3: Determine if source needs authentication
        source_creds = source_credential if needs_source_auth(args.source_index) else None

        # Step 4: Inspect source image config to get ocp version
        source_config = inspect_image(
            args.source_index,
            source_creds,
            config=True,
            logger=logger,
            retry_times=args.retries,
        )
        source_ocp_version = (
            source_config["config"]["Labels"].get("com.redhat.component.ocp-version")
            if source_config
            else None
        )
        if source_ocp_version != args.target_ocp_version:
            message = (
                f"The source index does not match its targetOcpVersion "
                f"({source_ocp_version} != {args.target_ocp_version})"
            )
            logger.error(message)
            write_result(message)
            sys.exit(1)

        # Step 5: Inspect target image (non-config to get Digest)
        target_image_info = inspect_image(
            args.target_index,
            target_credential,
            logger=logger,
            retry_times=args.retries,
        )
        target_digest = target_image_info["Digest"] if target_image_info else None

        # Hotfix and pre-ga targetIndex should skip the next check
        if target_image_info and re.match(r".*:v[0-9]{1}\.[0-9]{2}$", args.target_index):
            real_target_ocp_version = target_image_info["Labels"].get(
                "com.redhat.component.ocp-version"
            )
            if source_ocp_version != real_target_ocp_version:
                message = (
                    f"The indexes versions do not match "
                    f"({source_ocp_version} != {real_target_ocp_version})"
                )
                logger.error(message)
                write_result(message)

        # Step 6: Compare digests if target exists
        if target_digest is not None:
            logger.debug(f"Source Digest - {source_digest}")
            logger.debug(f"Target Digest - {target_digest}")

            if source_digest == target_digest:
                message = "Image already exists with the same digest, skipping copy."
                write_result(message)
                return 0
            else:
                logger.info(
                    "Image exists in target registry but digests do not match. "
                    "Proceeding to copy the image."
                )

        # Step 7: Execute copy operation
        success, message = copy_image(
            args.source_index,
            args.target_index,
            source_creds,
            target_credential,
            logger=logger,
            retry_times=args.retries,
        )

        # Step 8: Write result
        write_result(message)

        return 0 if success else 1

    except Exception as e:
        logger.exception(f"Failed to publish index image: {e}")
        write_result(str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
