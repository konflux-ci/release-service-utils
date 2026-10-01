"""Helpers for interacting with Kubernetes via kubectl."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from release_service_utils.helpers.subprocess_cmd import run_cmd


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


def auth_can_i(
    verb: str,
    resource: str,
    *,
    name: str | None = None,
    namespace: str | None = None,
) -> bool:
    """Check whether the current service account can perform an action.

    Args:
        verb: The API verb to check (e.g. "get", "create").
        resource: The Kubernetes resource type (e.g. "release", "snapshot").
        name: Optional resource name to check access for.
        namespace: Optional namespace to check access in.

    Returns:
        True if the action is allowed, False otherwise.

    """
    target = f"{resource}/{name}" if name else resource
    cmd = ["kubectl", "auth", "can-i", verb, target]
    if namespace:
        cmd.extend(["-n", namespace])
    result = run_cmd(cmd, check=False)
    if result.returncode != 0:
        ns_flag = f" -n {namespace}" if namespace else ""
        raise RuntimeError(
            f"Failed to run 'kubectl auth can-i {verb} {target}{ns_flag}': "
            f"{(result.stderr or result.stdout or '').strip()}"
        )
    return result.stdout.strip() == "yes"


def patch_resource(
    resource_type: str,
    name: str,
    *,
    namespace: str,
    patch_file: Path,
    subresource: str | None = None,
    patch_type: str = "merge",
    warnings_as_errors: bool = False,
) -> None:
    """Patch a Kubernetes resource using the contents of a patch file.

    Args:
        resource_type: The Kubernetes resource type (e.g. "release").
        name: The resource name to patch.
        namespace: The namespace the resource lives in.
        patch_file: Path to a file containing the patch (JSON or YAML).
        subresource: Optional subresource to patch (e.g. "status").
        patch_type: The patch strategy passed to ``--type`` (default "merge").
        warnings_as_errors: Fail the command when the API server returns warnings.

    Raises:
        RuntimeError: If kubectl exits with a non-zero return code.

    """
    cmd = ["kubectl"]
    if warnings_as_errors:
        cmd.append("--warnings-as-errors=true")
    cmd.extend(["patch", resource_type, "-n", namespace, name, f"--type={patch_type}"])
    if subresource:
        cmd.extend(["--subresource", subresource])
    cmd.extend(["--patch-file", str(patch_file)])
    result = run_cmd(cmd, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to patch {resource_type}/{name} in namespace '{namespace}': "
            f"{(result.stderr or result.stdout or '').strip()}"
        )
