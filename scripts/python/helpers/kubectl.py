"""Helpers for interacting with Kubernetes via kubectl."""

from __future__ import annotations

import json
from typing import Any

from subprocess_cmd import run_cmd


def get_configmap(name: str) -> dict[str, Any]:
    """Fetch a Kubernetes ConfigMap by name and return its parsed JSON.

    Args:
        name: The ConfigMap resource name to retrieve.

    Returns:
        The full ConfigMap object as a parsed dictionary.

    Raises:
        RuntimeError: If kubectl exits with a non-zero return code.

    """
    result = run_cmd(["kubectl", "get", f"cm/{name}", "-ojson"], check=False)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to retrieve ConfigMap '{name}': {result.stderr.strip()}")
    return json.loads(result.stdout)
