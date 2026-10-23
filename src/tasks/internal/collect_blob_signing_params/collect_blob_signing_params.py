#!/usr/bin/env python3
"""Collect blob-signing parameters from a Kubernetes ConfigMap."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from release_service_utils.helpers import kubectl
from release_service_utils.helpers import tekton
from release_service_utils.helpers.logger import logger

RESULT_KEYS = (
    "pyxis_url",
    "sig_key_id",
    "sig_key_name",
    "umb_url",
    "umb_listen_topic",
    "umb_publish_topic",
    "umb_client_name",
    "pyxis_ssl_cert_secret_name",
    "umb_ssl_cert_secret_name",
)


def _required(data: dict[str, Any], cm_key: str) -> str:
    """Return a stripped ConfigMap value, raising if it is missing or blank."""
    value = str(data[cm_key]).strip()
    if not value:
        raise ValueError(f"ConfigMap key {cm_key!r} is empty")
    return value


def extract_blob_signing_params(configmap: dict[str, Any]) -> dict[str, str]:
    """Return required blob-signing values from ConfigMap data."""
    data = configmap["data"]
    return {key: _required(data, key.upper()) for key in RESULT_KEYS}


def collect_blob_signing_params(
    config_map_name: str,
    result_paths: dict[str, Path],
) -> dict[str, str]:
    """Fetch the ConfigMap, extract params, and write result files."""
    logger.info("Collecting blob signing params from ConfigMap %s", config_map_name)
    params = extract_blob_signing_params(kubectl.get_configmap(config_map_name))
    for key in RESULT_KEYS:
        path = result_paths[key]
        path.write_text(params[key], encoding="utf-8")
        logger.info("Wrote result %s = %s", key, params[key])
    return params


def main() -> int:
    """Read Tekton env vars, collect params, and write results."""
    config_map_name = tekton.require_env("PARAM_CONFIG_MAP_NAME")
    env_vars = tuple(f"RESULT_{key.upper()}" for key in RESULT_KEYS)
    result_paths = dict(zip(RESULT_KEYS, tekton.result_paths_from_env(*env_vars), strict=True))
    collect_blob_signing_params(
        config_map_name=config_map_name,
        result_paths=result_paths,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
