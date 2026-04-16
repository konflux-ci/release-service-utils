#!/usr/bin/env python3
"""
Retrieve a Kubernetes resource via kubectl, with optional KubeArchive fallback.

Usage: get-resource <resource_type> <namespace/name> [jsonpath]

When a jsonpath is supplied and retrieval fails, prints '{}' and exits 0.
When no jsonpath is supplied and retrieval fails, prints the kubectl error
to stderr and exits with kubectl's exit code.

For eligible resource types (snapshots), falls back to KubeArchive when
kubectl fails.
"""

import argparse
import json
import logging
import os
import subprocess
import sys

LOGGER = logging.getLogger("get_resource")

KA_RESOURCE_TYPES = {"snapshot", "snapshots"}
_KUBECTL_KA_CONFIG_PATH_DEFAULT = "/tmp/kubectl-ka-config"


def _run(cmd):
    """Run a command and return (exit_code, stdout, stderr)."""
    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode, result.stdout, result.stderr


def extract_jsonpath(data, jsonpath):
    """Extract a value from a dict using a kubectl-style jsonpath.

    Handles simple dot-separated field access and a single [*] wildcard.
    Returns the extracted value, or None if the path doesn't resolve.
    """
    path = jsonpath.strip().strip("{}").lstrip(".")

    def _walk(obj, keys):
        for key in keys:
            if not isinstance(obj, dict) or key not in obj:
                return None
            obj = obj[key]
        return obj

    if "[*]" not in path:
        return _walk(data, path.split("."))

    before, after = path.split("[*]", 1)
    before_keys = [k for k in before.rstrip(".").split(".") if k]
    after_keys = [k for k in after.lstrip(".").split(".") if k]

    items = _walk(data, before_keys)
    if not isinstance(items, list):
        return None
    if not after_keys:
        return items
    return [v for item in items if (v := _walk(item, after_keys)) is not None]


def format_jsonpath_result(value):
    """Format an extracted jsonpath value for output, matching kubectl behavior."""
    if isinstance(value, list):
        parts = (
            json.dumps(v, separators=(",", ":")) if isinstance(v, (dict, list)) else str(v)
            for v in value
        )
        return " ".join(parts)
    if isinstance(value, dict):
        return json.dumps(value)
    return str(value)


def ka_enabled(resource_type):
    return resource_type in KA_RESOURCE_TYPES


def _ka_config_path():
    return os.environ.get("KUBECTL_KA_CONFIG_PATH", _KUBECTL_KA_CONFIG_PATH_DEFAULT)


def ensure_ka_config():
    """Discover and write KubeArchive config. Raises RuntimeError on failure."""
    config_path = _ka_config_path()
    if os.path.isfile(config_path):
        return

    rc, stdout, _ = _run(
        [
            "kubectl",
            "get",
            "configmap",
            "kubearchive-api-url",
            "-n",
            "product-kubearchive",
            "-o",
            "jsonpath={.data.URL}",
        ]
    )

    ka_url = stdout.strip() if rc == 0 else ""
    if not ka_url:
        raise RuntimeError(
            "KubeArchive not available: kubearchive-api-url ConfigMap not found"
        )

    rc, _, stderr = _run(["kubectl", "ka", "config", "set", "host", ka_url])
    if rc != 0:
        raise RuntimeError(f"Failed to set KubeArchive host: {stderr.strip()}")

    ssl_cert = os.environ.get("SSL_CERT_FILE", "")
    if ssl_cert:
        rc, _, stderr = _run(["kubectl", "ka", "config", "set", "ca", ssl_cert])
        if rc != 0:
            raise RuntimeError(f"Failed to set KubeArchive CA: {stderr.strip()}")

    LOGGER.info("KubeArchive configured: url=%s", ka_url)


def _resource_version(item):
    try:
        return int(item.get("metadata", {}).get("resourceVersion", 0))
    except (ValueError, TypeError):
        return 0


def get_from_ka(resource_type, namespace, name):
    """Retrieve a resource from KubeArchive. Returns JSON string.

    Raises RuntimeError if the resource cannot be retrieved.
    """
    ensure_ka_config()

    LOGGER.info("KubeArchive fallback: type=%s name=%s ns=%s", resource_type, name, namespace)

    rc, stdout, stderr = _run(
        [
            "kubectl",
            "ka",
            "get",
            resource_type,
            name,
            "-n",
            namespace,
            "--archived",
            "-o",
            "json",
        ]
    )

    if rc != 0:
        LOGGER.warning(
            "Named get failed (exit=%d): %s, trying list fallback", rc, stderr.strip()
        )
        rc, stdout, _ = _run(
            [
                "kubectl",
                "ka",
                "get",
                resource_type,
                "-n",
                namespace,
                "--archived",
                "--limit",
                "1000",
                "-o",
                "json",
            ]
        )
        if rc != 0:
            raise RuntimeError(
                f"KubeArchive get and list both failed for {resource_type} {name}"
            )

    try:
        data = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Invalid JSON from KubeArchive for {resource_type} {name}"
        ) from exc

    items = [
        item for item in data.get("items", []) if item.get("metadata", {}).get("name") == name
    ]

    if not items:
        raise RuntimeError(f"Resource {name} not found in KubeArchive")

    LOGGER.info("Found %d item(s) in KubeArchive", len(items))
    return json.dumps(max(items, key=_resource_version), indent=2)


def setup_argparser():
    parser = argparse.ArgumentParser(
        description="Retrieve a Kubernetes resource via kubectl, "
        "with optional KubeArchive fallback.",
    )
    parser.add_argument(
        "resource_type", help="Kubernetes resource type (e.g. snapshot, release)"
    )
    parser.add_argument(
        "namespaced_name",
        help="Resource in namespace/name format",
    )
    parser.add_argument(
        "jsonpath", nargs="?", default=None, help="kubectl-style jsonpath filter"
    )
    return parser


def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s %(message)s"
    )

    parser = setup_argparser()
    args = parser.parse_args()

    parts = args.namespaced_name.split("/", 1)
    if len(parts) != 2:
        parser.error(f"expected namespace/name, got '{args.namespaced_name}'")
    namespace, name = parts

    resource_type = args.resource_type
    jsonpath = args.jsonpath

    os.environ.setdefault("KUBECTL_KA_CONFIG_PATH", _KUBECTL_KA_CONFIG_PATH_DEFAULT)

    if jsonpath:
        rc, stdout, _ = _run(
            [
                "kubectl",
                "get",
                resource_type,
                "-n",
                namespace,
                name,
                "-o",
                f"jsonpath={jsonpath}",
                "--allow-missing-template-keys=false",
            ]
        )
        if rc == 0:
            print(stdout, end="")
            sys.exit(0)

        if ka_enabled(resource_type):
            try:
                ka_json_str = get_from_ka(resource_type, namespace, name)
                value = extract_jsonpath(json.loads(ka_json_str), jsonpath)
                if value is not None:
                    print(format_jsonpath_result(value))
                    sys.exit(0)
            except Exception:
                LOGGER.warning("KubeArchive fallback failed", exc_info=True)

        print("{}")
    else:
        rc, stdout, stderr = _run(
            [
                "kubectl",
                "get",
                resource_type,
                "-n",
                namespace,
                name,
                "-o",
                "json",
            ]
        )
        if rc == 0:
            print(stdout, end="")
            sys.exit(0)

        if ka_enabled(resource_type):
            try:
                ka_json_str = get_from_ka(resource_type, namespace, name)
                print(ka_json_str)
                sys.exit(0)
            except Exception:
                LOGGER.warning("KubeArchive fallback failed", exc_info=True)

        print(stdout + stderr, file=sys.stderr, end="")
        sys.exit(rc)


if __name__ == "__main__":
    main()
