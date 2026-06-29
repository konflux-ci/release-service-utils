#!/usr/bin/env python3
"""Collect the registry token secret name from the release data file."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import file
import snapshot
import tekton
from logger import logger


def is_secret_required(data: dict[str, Any]) -> bool:
    """Return True when defaults or any component require making repos public.

    An empty component (no ``public`` override) resolves to the mapping-level
    default, so this also covers the case of ``mapping.defaults.public`` being
    true with no components at all.
    """
    if snapshot.component_public(data, {}):
        return True

    mapping = data.get("mapping") or {}
    for component in mapping.get("components") or []:
        if isinstance(component, dict) and snapshot.component_public(data, component):
            return True

    return False


def collect_registry_token_secret(data: dict[str, Any]) -> str:
    """Return the registry secret name, or an empty string when not required.

    Raises:
        KeyError: When a secret is required but ``mapping.registrySecret``
            is absent.

    """
    if not is_secret_required(data):
        logger.info("No repos to make public, so no secret is required. Exiting...")
        return ""

    mapping = data.get("mapping") or {}
    return str(mapping["registrySecret"]).strip()


def run(data_file: Path) -> str:
    """Load *data_file* and return the registry secret name (or empty)."""
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
