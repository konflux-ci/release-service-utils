#!/usr/bin/env python3
"""Collect Konflux signing configuration parameters from a Kubernetes ConfigMap.

Retrieve keyless signing parameters from the cluster-config ConfigMap and write
them to Tekton result files. If the ConfigMap is not found, output empty strings
for all parameters except enableKeylessSigning which defaults to "false".
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from release_service_utils.helpers import kubectl
from release_service_utils.helpers import tekton
from release_service_utils.helpers.logger import logger

RESULT_KEYS = (
    "enableKeylessSigning",
    "defaultOIDCIssuer",
    "rekorExternalUrl",
    "rekorUrl",
    "fulcioExternalUrl",
    "fulcioUrl",
    "tufExternalUrl",
    "tufUrl",
    "buildIdentityRegexp",
    "tektonChainsIdentity",
)

USAGE = """\
usage: python3 -m collect_signing_params [--config-map-name NAME] \
[--config-map-namespace NAMESPACE]

Collect signing parameters from a Kubernetes ConfigMap and write them to
Tekton result files. All result file path environment variables are required;
the script exits with code 1 if any are missing.

environment variables (all required):
  RESULT_ENABLE_KEYLESS_SIGNING   Path to enableKeylessSigning result file
  RESULT_DEFAULT_OIDC_ISSUER      Path to defaultOIDCIssuer result file
  RESULT_REKOR_EXTERNAL_URL       Path to rekorExternalUrl result file
  RESULT_REKOR_URL                Path to rekorUrl result file
  RESULT_FULCIO_EXTERNAL_URL      Path to fulcioExternalUrl result file
  RESULT_FULCIO_URL               Path to fulcioUrl result file
  RESULT_TUF_EXTERNAL_URL         Path to tufExternalUrl result file
  RESULT_TUF_URL                  Path to tufUrl result file
  RESULT_BUILD_IDENTITY_REGEXP    Path to buildIdentityRegexp result file
  RESULT_TEKTON_CHAINS_IDENTITY   Path to tektonChainsIdentity result file

optional arguments:
  --config-map-name NAME      ConfigMap name (default: cluster-config)
  --config-map-namespace NS   ConfigMap namespace (default: konflux-info)
"""


def get_empty_signing_params() -> dict[str, str]:
    """Return signing parameters with empty values, enableKeylessSigning as 'false'."""
    params = {key: "" for key in RESULT_KEYS}
    params["enableKeylessSigning"] = "false"
    return params


def _prefer_internal_or_external(
    data: dict[str, Any], internal_key: str, external_key: str
) -> str:
    """Return internal URL if set, otherwise fall back to external URL.

    Args:
        data: The ConfigMap data dictionary.
        internal_key: The key for the internal URL.
        external_key: The key for the external URL.

    Returns:
        The internal URL if non-empty, otherwise the external URL (or empty string).

    """
    internal = data.get(internal_key, "")
    if internal:
        return str(internal)
    external = data.get(external_key, "")
    return str(external) if external else ""


def extract_signing_params_from_configmap(
    configmap_data: dict[str, Any],
) -> dict[str, str]:
    """Extract signing parameters from ConfigMap data.

    Args:
        configmap_data: The full ConfigMap object as returned by kubectl.

    Returns:
        A dictionary mapping result keys to their string values.

    """
    data = configmap_data.get("data", {})
    params: dict[str, str] = {}

    params["enableKeylessSigning"] = str(data.get("enableKeylessSigning", "")) or "false"
    params["defaultOIDCIssuer"] = str(data.get("defaultOIDCIssuer", "")) or ""
    params["rekorExternalUrl"] = str(data.get("rekorExternalUrl", "")) or ""
    params["fulcioExternalUrl"] = str(data.get("fulcioExternalUrl", "")) or ""
    params["tufExternalUrl"] = str(data.get("tufExternalUrl", "")) or ""
    params["buildIdentityRegexp"] = str(data.get("buildIdentityRegexp", "")) or ""
    params["tektonChainsIdentity"] = str(data.get("tektonChainsIdentity", "")) or ""

    params["rekorUrl"] = _prefer_internal_or_external(
        data, "rekorInternalUrl", "rekorExternalUrl"
    )
    params["fulcioUrl"] = _prefer_internal_or_external(
        data, "fulcioInternalUrl", "fulcioExternalUrl"
    )
    params["tufUrl"] = _prefer_internal_or_external(data, "tufInternalUrl", "tufExternalUrl")

    return params


def write_result_files(params: dict[str, str], result_paths: dict[str, Path]) -> None:
    """Write signing parameters to Tekton result files.

    Args:
        params: A dictionary of result keys to their string values.
        result_paths: A dictionary mapping result keys to their file paths.

    """
    for key in RESULT_KEYS:
        result_path = result_paths[key]
        value = params.get(key, "")
        result_path.write_text(value, encoding="utf-8")
        logger.info("Wrote result %s = %s", key, value if value else "(empty)")


def collect_signing_params(
    config_map_name: str,
    config_map_namespace: str,
    result_paths: dict[str, Path],
) -> dict[str, str]:
    """Collect signing parameters from a ConfigMap and write to result files.

    Args:
        config_map_name: The name of the ConfigMap to read.
        config_map_namespace: The namespace where the ConfigMap is located.
        result_paths: A dictionary mapping result keys to their file paths.

    Returns:
        The collected signing parameters as a dictionary.

    """
    logger.info(
        "Collecting signing params from ConfigMap %s/%s",
        config_map_namespace,
        config_map_name,
    )

    try:
        configmap = kubectl.get_configmap(config_map_name, namespace=config_map_namespace)
        params = extract_signing_params_from_configmap(configmap)
        logger.info("ConfigMap found, extracted signing parameters")
    except kubectl.ConfigMapNotFoundError as e:
        logger.warning("ConfigMap not found: %s", e)
        logger.info("Using empty signing parameters with keyless signing disabled")
        params = get_empty_signing_params()

    write_result_files(params, result_paths)
    return params


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Command-line arguments (defaults to sys.argv[1:]).

    Returns:
        Parsed arguments namespace.

    """
    parser = tekton.tekton_argument_parser("collect_signing_params")
    parser.add_argument("--config-map-name", default="cluster-config")
    parser.add_argument("--config-map-namespace", default="konflux-info")
    parser.add_argument("-h", "--help", action="store_true")

    args = parser.parse_args(argv)

    if args.help:
        tekton.exit_with_usage(USAGE)

    return args


def main(argv: list[str] | None = None) -> int:
    """Parse arguments, collect signing parameters, and write Tekton results."""
    args = parse_args(argv)

    (
        enable_keyless_signing_path,
        default_oidc_issuer_path,
        rekor_external_url_path,
        rekor_url_path,
        fulcio_external_url_path,
        fulcio_url_path,
        tuf_external_url_path,
        tuf_url_path,
        build_identity_regexp_path,
        tekton_chains_identity_path,
    ) = tekton.result_paths_from_env(
        "RESULT_ENABLE_KEYLESS_SIGNING",
        "RESULT_DEFAULT_OIDC_ISSUER",
        "RESULT_REKOR_EXTERNAL_URL",
        "RESULT_REKOR_URL",
        "RESULT_FULCIO_EXTERNAL_URL",
        "RESULT_FULCIO_URL",
        "RESULT_TUF_EXTERNAL_URL",
        "RESULT_TUF_URL",
        "RESULT_BUILD_IDENTITY_REGEXP",
        "RESULT_TEKTON_CHAINS_IDENTITY",
    )

    result_paths = {
        "enableKeylessSigning": enable_keyless_signing_path,
        "defaultOIDCIssuer": default_oidc_issuer_path,
        "rekorExternalUrl": rekor_external_url_path,
        "rekorUrl": rekor_url_path,
        "fulcioExternalUrl": fulcio_external_url_path,
        "fulcioUrl": fulcio_url_path,
        "tufExternalUrl": tuf_external_url_path,
        "tufUrl": tuf_url_path,
        "buildIdentityRegexp": build_identity_regexp_path,
        "tektonChainsIdentity": tekton_chains_identity_path,
    }

    collect_signing_params(
        config_map_name=args.config_map_name,
        config_map_namespace=args.config_map_namespace,
        result_paths=result_paths,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
