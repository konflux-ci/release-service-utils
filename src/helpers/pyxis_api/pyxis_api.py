"""Pyxis URL mapping and repository GET/PATCH helpers."""

from __future__ import annotations

import json
from typing import Any

import requests

from release_service_utils.helpers import http_client

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

PYXIS_GRAPHQL_URL_BY_SERVER: dict[str, str] = {
    "production": "https://graphql-pyxis.api.redhat.com/graphql/",
    "stage": "https://graphql-pyxis.preprod.api.redhat.com/graphql/",
    "production-internal": "https://graphql.pyxis.engineering.redhat.com/graphql/",
    "stage-internal": "https://graphql.pyxis.stage.engineering.redhat.com/graphql/",
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


def pyxis_graphql_url_for_server(server: str) -> str:
    """Return the Pyxis GraphQL API URL for a Tekton `server` param value."""
    url = PYXIS_GRAPHQL_URL_BY_SERVER.get(server)
    if url is None:
        raise ValueError(INVALID_SERVER_MESSAGE)
    return url


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


def get_advisory(
    advisory_api_url: str,
    advisory_id: str,
    *,
    cert: tuple[str, str],
) -> dict[str, Any] | None:
    """GET a Pyxis advisory by ID, return JSON or None if not found."""
    url = f"{advisory_api_url.rstrip('/')}/id/{advisory_id}"
    try:
        text = http_client.get_text(
            url,
            cert=cert,
            timeout=120,
        )
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            return None
        raise requests.RequestException(f"Pyxis advisory GET failed for {url}") from exc

    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        preview = text[:100] if text else "(empty)"
        msg = f"invalid JSON from Pyxis advisory GET {url}: {preview}"
        raise ValueError(msg) from exc


def create_or_update_advisory(
    advisory_api_url: str,
    advisory_id: str,
    payload: dict[str, Any],
    *,
    cert: tuple[str, str],
) -> None:
    """Create or update Pyxis advisory (GET to check, then POST or PATCH).

    Retries the GET-then-POST/PATCH sequence up to 3 times so idempotent
    upserts recover from transient failures.
    """
    existing = get_advisory(advisory_api_url, advisory_id, cert=cert)
    session = http_client.get_retry_session(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=2.0,
        allowed_methods=frozenset({"POST", "PATCH"}),
    )
    session.cert = cert
    if existing is not None:
        url = f"{advisory_api_url.rstrip('/')}/id/{advisory_id}"
        response = session.patch(
            url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=120,
        )
        response.raise_for_status()
    else:
        url = advisory_api_url.rstrip("/")
        response = session.post(
            url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=120,
        )
        response.raise_for_status()


def get_image_by_digest(
    images_api_url: str,
    digest: str,
    *,
    cert: tuple[str, str],
) -> dict[str, Any]:
    """GET Pyxis image by manifest digest (sha256, no prefix)."""
    url = (
        f"{images_api_url.rstrip('/')}?page_size=1&"
        f"filter=repositories.manifest_schema2_digest%3D%3D%22sha256%3A{digest}%22%3B"
        f"not%28deleted%3D%3Dtrue%29"
    )
    try:
        text = http_client.get_text(
            url,
            cert=cert,
            timeout=120,
        )
    except requests.HTTPError as exc:
        raise requests.RequestException(f"Pyxis image GET failed for {url}") from exc

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        preview = text[:100] if text else "(empty)"
        msg = f"invalid JSON from Pyxis image GET {url}: {preview}"
        raise ValueError(msg) from exc

    images = data.get("data", [])
    if not images:
        msg = f"No Pyxis image found for digest sha256:{digest}"
        raise ValueError(msg)
    return images[0]


def link_image_to_advisory(
    images_api_url: str,
    image_id: str,
    repo_path: str,
    advisory_id: str,
    *,
    cert: tuple[str, str],
) -> None:
    """PATCH Pyxis image to link advisory ID to repository entry."""
    url = f"{images_api_url.rstrip('/')}/id/{image_id}"
    try:
        text = http_client.get_text(
            url=url,
            timeout=120,
        )
    except requests.HTTPError as exc:
        raise requests.RequestException(f"Pyxis image GET failed for {url}") from exc

    try:
        image_data = json.loads(text)
    except json.JSONDecodeError as exc:
        preview = text[:100] if text else "(empty)"
        msg = f"invalid JSON from Pyxis image GET {url}: {preview}"
        raise ValueError(msg) from exc

    # Update image_advisory_id for matching repository
    repositories = image_data.get("repositories", [])
    for repo in repositories:
        if (
            repo.get("registry") == "registry.access.redhat.com"
            and repo.get("repository") == repo_path
        ):
            repo["image_advisory_id"] = advisory_id
            break
    else:
        msg = (
            f"Pyxis image {image_id} has no repository entry for "
            f"registry.access.redhat.com/{repo_path}"
        )
        raise ValueError(msg)

    patch_session = http_client.get_retry_session(
        total=3,
        connect=3,
        read=3,
        status=3,
        backoff_factor=2.0,
        allowed_methods=frozenset({"PATCH"}),
    )
    patch_session.cert = cert
    patch_response = patch_session.patch(
        url,
        json=image_data,
        headers={"Content-Type": "application/json"},
        timeout=120,
    )
    try:
        patch_response.raise_for_status()
    except requests.HTTPError as exc:
        raise requests.RequestException(
            f"Pyxis image PATCH failed for {url}: "
            f"{patch_response.status_code} {patch_response.text}"
        ) from exc
