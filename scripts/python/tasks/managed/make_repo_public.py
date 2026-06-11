#!/usr/bin/env python3
"""Make Quay repositories public using the Quay API."""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import requests

import file
import http_client
from logger import logger

PROG = "make_repo_public.py"

SYSTEM_CA_BUNDLE = "/etc/pki/tls/certs/ca-bundle.crt"


def setup_ca_bundle(ca_cert_path: Path) -> None:
    """Combine system and custom CA bundles if the custom cert exists."""
    if not ca_cert_path.is_file():
        return

    system_bundle = Path(SYSTEM_CA_BUNDLE)
    parts: list[bytes] = []
    if system_bundle.is_file():
        parts.append(system_bundle.read_bytes())
    parts.append(ca_cert_path.read_bytes())
    combined = file.make_tempfile_path(prefix="combined-ca-bundle-", data=b"\n".join(parts))

    os.environ["SSL_CERT_FILE"] = str(combined)
    os.environ["CURL_CA_BUNDLE"] = str(combined)
    os.environ["REQUESTS_CA_BUNDLE"] = str(combined)


def is_quay_registry(
    registry: str,
    session: requests.Session,
    cache: dict[str, bool],
) -> bool:
    """Return True if ``registry`` exposes a Quay-compatible discovery endpoint.

    Issue a GET to ``https://{registry}/api/v1/discovery``; HTTP 200 means
    Quay, any other status means not Quay. Results are cached in ``cache``
    so each registry is probed only once.
    """
    if registry in cache:
        return cache[registry]

    url = f"https://{registry}/api/v1/discovery"
    try:
        resp = session.get(url, timeout=30)
        result = resp.status_code == 200
    except requests.RequestException as exc:
        logger.warning(
            "Failed to probe discovery endpoint for %s: %s",
            registry,
            exc,
        )
        result = False

    cache[registry] = result
    return result


def make_repo_public(
    registry: str,
    repo_path: str,
    token: str,
    session: requests.Session,
) -> None:
    """POST to the Quay API to change repository visibility to public.

    Raises ``RuntimeError`` on non-2xx responses. When ``REGISTRY_SECRET_NAME``
    is set in the environment, the error message includes a hint about the
    expected secret key and permissions.
    """
    url = f"https://{registry}/api/v1/repository/{repo_path}/changevisibility"
    try:
        resp = session.post(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={"visibility": "public"},
            timeout=30,
        )
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Failed to connect to {registry} to make {repo_path} public: {exc}"
        ) from exc
    if resp.ok:
        logger.info("Repository %s/%s is now public", registry, repo_path)
        return

    msg = (
        f"Failed to make repo {registry}/{repo_path} public"
        f" (HTTP {resp.status_code}: {resp.text})."
    )
    secret_name = os.environ.get("REGISTRY_SECRET_NAME", "").strip()
    if secret_name:
        msg += (
            f" Make sure the secret {secret_name} contains"
            ' the "token" key with token that has permission to'
            " Administer Repositories."
        )
    raise RuntimeError(msg)


def run(
    data_file: Path,
    snapshot_file: Path,
    secret_path: Path,
    ca_cert_path: Path,
) -> None:
    """Orchestrate making repositories public based on data and snapshot files.

    Reads the merged data JSON and snapshot JSON, then for each component with
    ``public: true`` calls the Quay API to change visibility. Only a single
    Quay registry is supported per invocation; encountering repos on two
    different Quay registries raises ``RuntimeError``.
    """
    if not data_file.is_file():
        raise RuntimeError("No valid data file was provided.")
    if not snapshot_file.is_file():
        raise RuntimeError("No valid snapshot file was provided.")

    setup_ca_bundle(ca_cert_path)

    token_path = secret_path / "token"
    if not token_path.is_file():
        raise RuntimeError(f"Registry secret token file not found at {token_path}")
    token = token_path.read_text(encoding="utf-8").strip()

    try:
        data: dict[str, Any] = json.loads(data_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON in data file {data_file}: {exc}") from exc
    try:
        snapshot: dict[str, Any] = json.loads(snapshot_file.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON in snapshot file {snapshot_file}: {exc}") from exc

    default_public = data.get("mapping", {}).get("defaults", {}).get("public", False)

    session = http_client.get_session()
    quay_cache: dict[str, bool] = {}

    target_registry: str | None = None

    components = snapshot.get("components", [])
    for component in components:
        public = component.get("public", default_public)
        if str(public).lower() != "true":
            continue

        repositories = component.get("repositories", [])
        for repo_entry in repositories:
            repo_url = repo_entry.get("url", "")
            if not repo_url:
                raise RuntimeError(
                    f"Repository entry in component {component.get('name', '?')}"
                    " is missing the 'url' field"
                )

            logger.info("Making repository %s public...", repo_url)

            registry = repo_url.split("/")[0]
            repo_path = "/".join(repo_url.split("/")[1:]).rstrip("/")

            if not is_quay_registry(registry, session, quay_cache):
                logger.warning("Registry %s is not a Quay instance. Skipping.", registry)
                continue

            if target_registry is None:
                target_registry = registry
            elif target_registry != registry:
                raise RuntimeError(
                    f"Multiple Quay registries found ({target_registry} and"
                    f" {registry}). Only a single Quay registry is supported"
                    " because the registrySecret contains a token for one"
                    " registry."
                )

            make_repo_public(registry, repo_path, token, session)


def main() -> int:
    """Read environment variables and call ``run()``.

    Return 0 on success, 1 on missing env vars or ``RuntimeError`` from
    ``run()``.
    """
    data_file_str = os.environ.get("DATA_FILE", "").strip()
    snapshot_file_str = os.environ.get("SNAPSHOT_FILE", "").strip()

    if not data_file_str:
        print(f"{PROG}: DATA_FILE must be set", file=sys.stderr)
        return 1
    if not snapshot_file_str:
        print(f"{PROG}: SNAPSHOT_FILE must be set", file=sys.stderr)
        return 1

    secret_path = file.path_from_env_variable("REGISTRY_SECRET_PATH", "/etc/secrets")
    ca_cert_path = file.path_from_env_variable("CA_CERT_PATH", "/mnt/trusted-ca/ca-bundle.crt")

    try:
        run(
            Path(data_file_str),
            Path(snapshot_file_str),
            secret_path,
            ca_cert_path,
        )
    except RuntimeError as e:
        print(f"{PROG}: {e}", file=sys.stderr)
        return 1

    logger.info("make-repo-public completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
