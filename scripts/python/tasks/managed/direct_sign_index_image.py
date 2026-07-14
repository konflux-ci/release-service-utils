#!/usr/bin/env python3
"""Sign FBC index images via the container-signing pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import tempfile
from pathlib import Path
from typing import Any

from kubectl import get_configmap
from logger import logger as LOGGER
from rh_direct_sign_image import (
    PYXIS_INSTANCE_MAP,
    SigningItem,
    batch_signing_items,
    filter_already_signed,
    get_signing_keys,
    get_submit_config,
    submit_batches,
    validate_file,
    write_batches,
)
from subprocess_cmd import run_cmd


def translate_reference(target_index: str) -> str:
    """Translate a quay.io internal reference to a public registry.redhat.io reference.

    Args:
        target_index: Internal quay.io image reference
            (e.g. quay.io/redhat/redhat----fbc-target-index:v4.23).

    Returns:
        The public registry.redhat.io URL for the image.

    Raises:
        ValueError: If no redhat.io entry is found in the translation output.

    """
    result = run_cmd(["translate-delivery-repo", target_index])
    entries = json.loads(result.stdout)
    for entry in entries:
        if entry.get("repo") == "redhat.io":
            return entry["url"]
    raise ValueError(
        f"No redhat.io entry in translate-delivery-repo output for {target_index}"
    )


def collect_fbc_signing_items(
    fbc_results: dict[str, Any], signing_keys: list[str]
) -> list[SigningItem]:
    """Build signing items from FBC results.

    For each component, translates the target_index (and target_index_with_timestamp
    when present and different) to public references and creates a SigningItem for
    every (reference, digest, key) combination.

    Args:
        fbc_results: Parsed FBC results JSON containing components.
        signing_keys: List of signing key IDs.

    Returns:
        List of SigningItem objects covering all signing candidates.

    """
    items: list[SigningItem] = []

    for component in fbc_results.get("components", []):
        target_index = component["target_index"]

        rh_registry_repo = component.get("rh-registry-repo", "")
        repository = (
            rh_registry_repo.split("/", 1)[1] if "/" in rh_registry_repo else rh_registry_repo
        )

        target_indexes = [target_index]
        ts_index = component.get("target_index_with_timestamp", "")
        if ts_index and ts_index != target_index:
            target_indexes.append(ts_index)

        for idx in target_indexes:
            reference = translate_reference(idx)
            LOGGER.info("Translated %s -> %s", idx, reference)
            for digest in component.get("image_digests", []):
                for key in signing_keys:
                    items.append(SigningItem(reference, digest, repository, key))

    return items


def setup_argparser() -> argparse.ArgumentParser:
    """Build and return the CLI argument parser.

    Returns:
        Configured argument parser.

    """
    parser = argparse.ArgumentParser(description="Sign FBC index images.")
    parser.add_argument(
        "--fbc-results",
        required=True,
        type=validate_file,
        help="Path to the FBC results JSON file",
    )
    parser.add_argument(
        "--pyxis-server",
        required=True,
        choices=PYXIS_INSTANCE_MAP.keys(),
        help="Pyxis server instance to use",
    )
    parser.add_argument(
        "--data-file",
        required=True,
        type=validate_file,
        help="Path to the merged release data JSON file",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Directory where batch files are written (default: temp dir)",
    )
    parser.add_argument(
        "--batch-max-size",
        type=int,
        default=14 * 1024,
        help="Maximum size in bytes of each base64-encoded batch (default: %(default)s)",
    )
    parser.add_argument(
        "--fail-on-lookup-error",
        default="true",
        help="Fail when Pyxis lookups fail; set to 'false' to skip filtering on error",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Maximum concurrent Pyxis lookup threads (default: %(default)s)",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )

    submit = parser.add_argument_group("request submission")
    submit.add_argument(
        "--pipeline",
        default="container-signing",
        help="Internal pipeline name for signing (default: %(default)s)",
    )
    submit.add_argument(
        "--pipeline-image",
        required=True,
        help="Container image override for the signing pipeline",
    )
    submit.add_argument(
        "--requester",
        required=True,
        help="Name of the user requesting signing, for auditing",
    )
    submit.add_argument(
        "--request-timeout",
        default="1800",
        help="InternalRequest timeout in seconds (default: %(default)s)",
    )
    submit.add_argument(
        "--pipeline-timeout",
        default="0h30m0s",
        help="Pipeline timeout (default: %(default)s)",
    )
    submit.add_argument(
        "--task-timeout",
        default="0h25m0s",
        help="Task timeout (default: %(default)s)",
    )
    submit.add_argument(
        "--service-account",
        default="signing-pipeline-sa",
        help="Service account for the signing pipeline (default: %(default)s)",
    )
    submit.add_argument(
        "--task-id",
        default="",
        help="Task run UID used as a label on internal requests",
    )
    submit.add_argument(
        "--pipelinerun-uid",
        default="",
        help="Pipeline run UID used as a label on internal requests",
    )
    submit.add_argument(
        "--signing-repo",
        default="https://gitlab.cee.redhat.com/signing/signing.git",
        help="Git repository URL for signing tasks (default: %(default)s)",
    )
    submit.add_argument(
        "--signing-revision",
        default="main",
        help="Git revision in the signing repository (default: %(default)s)",
    )
    submit.add_argument(
        "--concurrent-limit",
        type=int,
        default=8,
        help="Maximum number of parallel signing requests (default: %(default)s)",
    )

    return parser


def main() -> int:
    """Entry point for FBC index image signing."""
    parser = setup_argparser()

    args = parser.parse_args()
    LOGGER.setLevel(logging.DEBUG if args.verbose else logging.INFO)

    pyxis_url = PYXIS_INSTANCE_MAP[args.pyxis_server]
    LOGGER.info("Using Pyxis instance: %s", pyxis_url)

    data_file = json.loads(args.data_file.read_text())
    config_map_name = data_file.get("sign", {}).get("configMapName", "signing-config-map")
    configmap = get_configmap(config_map_name)
    signing_keys = get_signing_keys(configmap)
    LOGGER.info("Signing keys: %s", signing_keys)

    fbc_results = json.loads(args.fbc_results.read_text())

    all_items = collect_fbc_signing_items(fbc_results, signing_keys)
    LOGGER.info("Total signing candidates: %d", len(all_items))

    if not all_items:
        LOGGER.info("No signing candidates found")
        return 0

    fail_on_error = args.fail_on_lookup_error.lower() != "false"
    try:
        to_sign = filter_already_signed(all_items, pyxis_url, max_workers=args.max_workers)
    except Exception:
        if fail_on_error:
            raise
        LOGGER.warning(
            "Pyxis lookup failed; failOnSignatureLookupError=false."
            " Submitting all %d items without filtering.",
            len(all_items),
        )
        to_sign = all_items

    LOGGER.info("Items to sign after filtering: %d", len(to_sign))

    if not to_sign:
        LOGGER.info("All items already signed, nothing to submit")
        return 0

    batches = batch_signing_items(to_sign, max_batch_bytes=args.batch_max_size)
    batch_dir = args.output if args.output else Path(tempfile.mkdtemp())
    write_batches(batches, batch_dir)
    LOGGER.info("Wrote %d batch(es) to '%s'", len(batches), batch_dir)

    submit_config = get_submit_config(configmap, args, data_file)
    submit_batches(batch_dir, submit_config)

    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
