"""Helpers for interacting with Kubernetes via kubectl."""

from __future__ import annotations

import json
from typing import Any

from subprocess_cmd import run_cmd


class ConfigMapNotFoundError(RuntimeError):
    """Raised specifically when kubectl reports the ConfigMap does not exist.

    Callers that want to treat "genuinely absent" differently from other kubectl
    failures (RBAC denied, API server unreachable, network errors, etc.) should
    catch this subclass rather than the base RuntimeError, so infra hiccups aren't
    silently mistaken for an intentional absence of configuration.
    """


def get_configmap(name: str, *, namespace: str | None = None) -> dict[str, Any]:
    """Fetch a Kubernetes ConfigMap by name and return its parsed JSON.

    Args:
        name: The ConfigMap resource name to retrieve.
        namespace: Optional namespace to retrieve the ConfigMap from.

    Returns:
        The full ConfigMap object as a parsed dictionary.

    Raises:
        ConfigMapNotFoundError: If kubectl reports the ConfigMap does not exist.
        RuntimeError: If kubectl exits with a non-zero return code for any other reason.

    """
    cmd = ["kubectl", "get", f"cm/{name}", "-ojson"]
    if namespace:
        cmd.extend(["-n", namespace])
    result = run_cmd(cmd, check=False)
    if result.returncode != 0:
        stderr = result.stderr.strip()
        if f'configmaps "{name}" not found' in stderr:
            raise ConfigMapNotFoundError(f"ConfigMap '{name}' not found: {stderr}")
        raise RuntimeError(f"Failed to retrieve ConfigMap '{name}': {stderr}")
    return json.loads(result.stdout)
