"""Pyxis URL mapping and repository GET/PATCH helpers."""

from __future__ import annotations

import json
from typing import Any

import http_client
import requests

FLATPAK_QUAY_PREFIXES = (
    "quay.io/rh-flatpaks-prod/",
    "quay.io/rh-flatpaks-stage/",
)

PROD_CATALOG_QUAY_PREFIXES = (
    "quay.io/redhat-prod/",
    "quay.io/rh-flatpaks-prod/",
)

STAGE_CATALOG_QUAY_PREFIXES = (
    "quay.io/redhat-pending/",
    "quay.io/rh-flatpaks-stage/",
)

PYXIS_BASE_URL_BY_SERVER: dict[str, str] = {
    "production": "https://pyxis.api.redhat.com",
    "stage": "https://pyxis.preprod.api.redhat.com",
    "production-internal": "https://pyxis.engineering.redhat.com",
    "stage-internal": "https://pyxis.stage.engineering.redhat.com",
}

INVALID_SERVER_MESSAGE = (
    "Invalid server parameter. Only 'production','production-internal',"
    "'stage-internal' and 'stage' allowed."
)


def pyxis_api_url_for_server(server: str) -> str:
    """Return the Pyxis v1 API base URL for a Tekton `server` param value."""
    base = PYXIS_BASE_URL_BY_SERVER.get(server)
    if base is None:
        raise ValueError(INVALID_SERVER_MESSAGE)
    return f"{base.rstrip('/')}/v1"


def pyxis_registry_for_quay_url(repository_url: str) -> str:
    """Return the Pyxis registry name for a mapped Quay repository URL."""
    if repository_url.startswith(FLATPAK_QUAY_PREFIXES):
        return "flatpaks.registry.redhat.io"
    return "registry.access.redhat.com"


def pyxis_repository_from_quay_url(repository_url: str) -> str:
    """Convert the Quay repo path suffix to a Pyxis repository name."""
    repository_name = repository_url.rsplit("/", 1)[-1]
    return repository_name.replace("----", "/")


def catalog_base_url_for_quay_url(repository_url: str) -> str:
    """Return the Red Hat catalog base URL for a mapped Quay repository URL."""
    if repository_url.startswith(PROD_CATALOG_QUAY_PREFIXES):
        return "https://catalog.redhat.com/software/containers"
    if repository_url.startswith(STAGE_CATALOG_QUAY_PREFIXES):
        return "https://catalog.stage.redhat.com/software/containers"
    msg = f"Unknown repository prefix for {repository_url!r}"
    raise ValueError(msg)


def catalog_url_for_repository(
    repository_url: str,
    pyxis_repository: str,
    repository_id: str,
) -> str:
    """Build a catalog page URL for a published Pyxis repository."""
    base = catalog_base_url_for_quay_url(repository_url)
    return f"{base}/{pyxis_repository}/{repository_id}"


def repository_lookup_url(
    pyxis_api_url: str,
    registry: str,
    repository: str,
) -> str:
    """Return the Pyxis GET URL for a registry/repository pair."""
    return (
        f"{pyxis_api_url.rstrip('/')}/repositories/registry/"
        f"{registry}/repository/{repository}"
    )


def get_repository_json(
    pyxis_api_url: str,
    registry: str,
    repository: str,
    *,
    cert: tuple[str, str],
) -> dict[str, Any]:
    """GET a Pyxis container repository record and return the JSON body."""
    url = repository_lookup_url(pyxis_api_url, registry, repository)
    raw = http_client.get_text(
        url,
        cert=cert,
        timeout=120,
        allow_error_status=True,
    )
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        preview = raw[:100] if raw else "(empty)"
        msg = f"invalid JSON from Pyxis GET {url}: {preview}"
        raise ValueError(msg) from exc


def patch_repository_json(
    pyxis_api_url: str,
    repository_id: str,
    payload: dict[str, Any],
    *,
    cert: tuple[str, str],
) -> None:
    """PATCH a Pyxis container repository by id.

    Publishing is idempotent, so transient 5xx responses are retried.
    """
    url = f"{pyxis_api_url.rstrip('/')}/repositories/id/{repository_id}"
    session = http_client.get_retry_session(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=5.0,
        allowed_methods=frozenset({"PATCH"}),
    )
    session.cert = cert
    response = session.patch(
        url,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=120,
    )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise requests.RequestException(
            f"Pyxis PATCH failed for {url}: {response.status_code} {response.text}"
        ) from exc
