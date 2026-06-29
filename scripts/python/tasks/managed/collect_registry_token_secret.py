#!/usr/bin/env python3
"""Collect the registry token secret name from the release data file."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import file
import tekton
from logger import logger

PROG = "collect_registry_token_secret.py"


def _is_public(value: Any) -> bool:
    """Return True when a mapping ``public`` flag is enabled."""
    if value is True:
        return True
    return str(value).lower() == "true"


def is_secret_required(data: dict[str, Any]) -> bool:
    """Return True when defaults or any component require making repos public.

    Mirrors the bash task: ``mapping.defaults.public`` or any
    ``mapping.components[*].public`` set to true.
    """
    mapping = data.get("mapping") or {}
    defaults = mapping.get("defaults") or {}
    if _is_public(defaults.get("public", False)):
        return True

    for component in mapping.get("components") or []:
        if isinstance(component, dict) and _is_public(component.get("public", False)):
            return True

    return False


def collect_registry_token_secret(data: dict[str, Any]) -> str:
    """Return the registry secret name, or an empty string when not required.

    Raises:
        ValueError: When a secret is required but ``mapping.registrySecret``
            is absent from the data file.

    """
    if not is_secret_required(data):
        logger.info("No repos to make public, so no secret is required. Exiting...")
        return ""

    mapping = data.get("mapping") or {}
    if "registrySecret" not in mapping:
        raise ValueError("Registry secret missing in data JSON file")

    secret = mapping["registrySecret"]
    if secret is None:
        raise ValueError("Registry secret missing in data JSON file")

    return str(secret)


def run(data_file: Path) -> str:
    """Load *data_file* and return the registry secret name (or empty)."""
    if not data_file.is_file():
        raise FileNotFoundError("No valid data file was provided.")

    data = file.load_json_dict(data_file)
    return collect_registry_token_secret(data)


def main() -> int:
    """Read Tekton env vars, resolve the secret name, and write the result."""
    data_dir = Path(tekton.require_env("PARAM_DATA_DIR"))
    data_path = Path(tekton.require_env("PARAM_DATA_PATH"))
    (result_path,) = tekton.result_paths_from_env("RESULT_REGISTRY_SECRET")

    secret = run(data_dir / data_path)
    result_path.write_text(secret, encoding="utf-8")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
