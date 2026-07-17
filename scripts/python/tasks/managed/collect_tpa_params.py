#!/usr/bin/env python3
"""Collect TPA (Trusted Profile Analyzer) parameters from cluster config or data file.

This script collects the TPA server configuration either from a Kubernetes
cluster ConfigMap (TSF workflow) or from a JSON data file. It outputs the
configuration values to Tekton result files.

The script first attempts to read configuration from a ConfigMap in the
specified namespace. If that fails or is incomplete, it falls back to
reading from a data file and determining stage/production configuration.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import file
import kubectl
import retry
import tekton
from logger import logger


@dataclass(frozen=True)
class TPAParams:
    """Container for TPA configuration parameters."""

    atlas_api_url: str
    sso_token_url: str
    secret_name: str
    retry_aws_secret_name: str
    retry_s3_bucket: str


STAGE_DEFAULTS = TPAParams(
    atlas_api_url="https://atlas.release.stage.devshift.net",
    sso_token_url=(
        "https://auth.stage.redhat.com/auth/realms/EmployeeIDP/protocol/openid-connect/token"
    ),
    secret_name="atlas-staging-sso-secret",
    retry_aws_secret_name="atlas-retry-s3-staging-secret",
    retry_s3_bucket="mpp-e1-preprod-sbom-29093454-2ea7-4fd0-b4cf-dc69a7529ee0",
)

PROD_DEFAULTS = TPAParams(
    atlas_api_url="https://atlas.release.devshift.net",
    sso_token_url=(
        "https://auth.redhat.com/auth/realms/EmployeeIDP/protocol/openid-connect/token"
    ),
    secret_name="atlas-prod-sso-secret",
    retry_aws_secret_name="atlas-retry-s3-production-secret",
    retry_s3_bucket="mpp-e1-prod-sbom-e02138d3-5c5c-4d90-a38f-6c54f658604d",
)

TSF_SECRET_NAME = "release-sso-secret"
TSF_RETRY_AWS_SECRET_NAME = "secret-not-present"


def try_tsf_config(
    configmap_name: str,
    configmap_namespace: str,
    *,
    get_configmap: Any = kubectl.get_configmap,
    sleep_fn: Any = None,
) -> TPAParams | None:
    """Attempt to read TSF configuration from cluster ConfigMap.

    Args:
        configmap_name: Name of the ConfigMap to read.
        configmap_namespace: Namespace where the ConfigMap is located.
        get_configmap: Callable to retrieve ConfigMap (for testing).
        sleep_fn: Callable for sleep during retries (for testing).

    Returns:
        TPAParams if valid TSF config found, None otherwise.

    """
    logger.info(
        "Checking for cluster configuration in %s/%s...",
        configmap_namespace,
        configmap_name,
    )
    try:
        cm = retry.retry_with_exponential_backoff(
            lambda: get_configmap(configmap_name, namespace=configmap_namespace),
            max_attempts=3,
            retry_on=RuntimeError,
            base_sleep_seconds=2,
            sleep_fn=sleep_fn,
        )
    except RuntimeError as e:
        logger.info("Could not retrieve ConfigMap after retries: %s", e)
        return None

    data = cm.get("data", {})
    atlas_api_url = data.get("trustifyServerExternalUrl", "")
    sso_token_base_url = data.get("trustifyOIDCIssuerUrl", "")

    if not atlas_api_url or not sso_token_base_url:
        logger.info("ConfigMap missing trustifyServerExternalUrl or trustifyOIDCIssuerUrl")
        return None

    logger.info("Detected cluster-config with TSF configuration")
    sso_token_url = f"{sso_token_base_url}/protocol/openid-connect/token"
    return TPAParams(
        atlas_api_url=atlas_api_url,
        sso_token_url=sso_token_url,
        secret_name=TSF_SECRET_NAME,
        retry_aws_secret_name=TSF_RETRY_AWS_SECRET_NAME,
        retry_s3_bucket="",
    )


def get_tpa_config(tpa_data: dict[str, Any]) -> dict[str, Any]:
    """Extract TPA/atlas configuration from the data object.

    The data file may contain either 'atlas' or 'tpa' key with the configuration.
    This function returns the nested object, preferring 'atlas' over 'tpa'.

    Args:
        tpa_data: The parsed JSON data object.

    Returns:
        The TPA configuration dict (may be empty).

    """
    return tpa_data.get("atlas") or tpa_data.get("tpa") or {}


def params_from_data_file(
    data_dir: Path,
    data_path: str,
    *,
    fail_on_missing: bool = True,
) -> TPAParams | None:
    """Read TPA parameters from a JSON data file.

    Args:
        data_dir: Base directory where data is stored.
        data_path: Relative path to the JSON data file.
        fail_on_missing: If True, raise error when server value is missing/invalid.

    Returns:
        TPAParams if valid configuration found, None if fail_on_missing is False
        and no valid config exists.

    Raises:
        FileNotFoundError: If the data file does not exist.
        ValueError: If fail_on_missing is True and server value is missing/invalid.

    """
    data_file = data_dir / data_path
    logger.info("Loading data from %s", data_file)
    data = file.load_json_dict(data_file)
    tpa_config = get_tpa_config(data)
    server = tpa_config.get("server")

    if server == "stage":
        defaults = STAGE_DEFAULTS
    elif server == "production":
        defaults = PROD_DEFAULTS
    else:
        defaults = None

    if defaults is not None:
        return replace(
            defaults,
            secret_name=tpa_config.get("atlas-sso-secret-name", defaults.secret_name),
            retry_aws_secret_name=tpa_config.get(
                "atlas-retry-aws-secret-name", defaults.retry_aws_secret_name
            ),
        )
    elif fail_on_missing:
        if server is None:
            msg = (
                ".(tpa/atlas).server value is missing from the data file. "
                "This field is mandatory. Consult with your release engineering "
                "contact to ask why you are missing this value."
            )
        else:
            msg = (
                f"Unknown .(tpa/atlas).server value '{server}'. "
                "Expected 'stage' or 'production'."
            )
        raise ValueError(msg)

    return None


def write_results(params: TPAParams | None, result_paths: dict[str, Path]) -> None:
    """Write TPA parameters to Tekton result files.

    Args:
        params: TPAParams to write, or None for empty results.
        result_paths: Dict mapping result name to file path.

    """
    if params is None:
        params = TPAParams(
            atlas_api_url="",
            sso_token_url="",
            secret_name="",
            retry_aws_secret_name="",
            retry_s3_bucket="",
        )

    result_paths["atlasApiUrl"].write_text(params.atlas_api_url, encoding="utf-8")
    result_paths["ssoTokenUrl"].write_text(params.sso_token_url, encoding="utf-8")
    result_paths["secretName"].write_text(params.secret_name, encoding="utf-8")
    result_paths["retryAWSSecretName"].write_text(
        params.retry_aws_secret_name, encoding="utf-8"
    )
    result_paths["retryS3Bucket"].write_text(params.retry_s3_bucket, encoding="utf-8")


def run_collect_tpa_params(
    *,
    data_dir: Path,
    data_path: str,
    configmap_name: str,
    configmap_namespace: str,
    fail_on_missing: bool,
    result_paths: dict[str, Path],
    get_configmap: Any = None,
    sleep_fn: Any = None,
) -> None:
    """Collect TPA parameters and write them to result files.

    First attempts to read TSF configuration from a cluster ConfigMap.
    If that fails, falls back to reading from the data file.

    Args:
        data_dir: Base directory where data is stored.
        data_path: Relative path to the JSON data file.
        configmap_name: Name of the ConfigMap to read.
        configmap_namespace: Namespace where the ConfigMap is located.
        fail_on_missing: If True, fail when required values are missing.
        result_paths: Dict mapping result name to file path.
        get_configmap: Callable to retrieve ConfigMap (for testing).
        sleep_fn: Callable for sleep during retries (for testing).

    """
    if get_configmap is None:
        get_configmap = kubectl.get_configmap

    params = try_tsf_config(
        configmap_name, configmap_namespace, get_configmap=get_configmap, sleep_fn=sleep_fn
    )

    if params is not None:
        logger.info("Using TSF configuration from cluster ConfigMap")
        write_results(params, result_paths)
        return

    logger.info("Falling back to data file configuration")
    if not data_path:
        if fail_on_missing:
            msg = "No dataPath provided and cluster ConfigMap not available"
            raise ValueError(msg)
        write_results(None, result_paths)
        return

    params = params_from_data_file(data_dir, data_path, fail_on_missing=fail_on_missing)
    write_results(params, result_paths)
    if params:
        logger.info("TPA parameters collected successfully")
    else:
        logger.info("No TPA parameters found, writing empty results")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Command-line arguments (defaults to sys.argv[1:]).

    Returns:
        Parsed arguments namespace.

    """
    parser = tekton.tekton_argument_parser("collect-tpa-params")
    parser.add_argument(
        "--data-dir",
        required=True,
        help="Directory where data is stored",
    )
    parser.add_argument(
        "--data-path",
        default="",
        help="Path to the JSON data file (relative to data-dir)",
    )
    parser.add_argument(
        "--configmap-name",
        default="cluster-config",
        help="Name of the ConfigMap to read TPA parameters from",
    )
    parser.add_argument(
        "--configmap-namespace",
        default="konflux-info",
        help="Namespace where the ConfigMap is located",
    )
    parser.add_argument(
        "--fail-on-missing",
        type=lambda v: v.lower() == "true",
        default=True,
        help="Fail if TPA parameters are missing (true/false)",
    )
    parser.add_argument(
        "-h",
        "--help",
        action="store_true",
        help="Show this help message",
    )

    args = parser.parse_args(argv)

    if args.help:
        tekton.exit_with_usage(
            "Usage: collect-tpa-params --data-dir DIR [--data-path PATH] "
            "[--configmap-name NAME] [--configmap-namespace NS] "
            "[--fail-on-missing true|false]\n"
        )

    return args


def main(argv: list[str] | None = None) -> int:
    """Run the collect-tpa-params workflow."""
    args = parse_args(argv)

    (
        result_atlas_api_url,
        result_sso_token_url,
        result_secret_name,
        result_retry_aws,
        result_retry_s3,
    ) = tekton.result_paths_from_env(
        "RESULT_ATLASAPIURL",
        "RESULT_SSOTOKENURL",
        "RESULT_SECRETNAME",
        "RESULT_RETRYAWSSECRETNAME",
        "RESULT_RETRYS3BUCKET",
    )

    result_paths = {
        "atlasApiUrl": result_atlas_api_url,
        "ssoTokenUrl": result_sso_token_url,
        "secretName": result_secret_name,
        "retryAWSSecretName": result_retry_aws,
        "retryS3Bucket": result_retry_s3,
    }

    run_collect_tpa_params(
        data_dir=Path(args.data_dir),
        data_path=args.data_path,
        configmap_name=args.configmap_name,
        configmap_namespace=args.configmap_namespace,
        fail_on_missing=args.fail_on_missing,
        result_paths=result_paths,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
