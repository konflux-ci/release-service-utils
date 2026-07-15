"""Pulp REST API client with TOML-based configuration and auth.

Parse ``cli.toml`` files, authenticate via Basic or OAuth2
client-credentials, and query the Pulp REST API for distributions,
repository versions, and RPM content digests.
"""

from __future__ import annotations

from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
import requests.auth
import tomllib

from release_service_utils.helpers import retry

TOKEN_URL = "https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token"


class PulpDigestStatus(Enum):
    """Outcome of a Pulp digest check."""

    MATCH = "match"
    NOT_FOUND = "not_found"
    MISMATCH = "mismatch"


def parse_pulp_config(config_path: Path) -> dict[str, str]:
    """Parse a Pulp cli.toml file and return the ``[cli]`` section values.

    Return a flat dict with keys: ``base_url``, ``client_id``,
    ``client_secret``, ``username``, ``password``.  Credentials default
    to empty strings; ``base_url`` is required and raises
    ``RuntimeError`` when missing or blank.
    """
    raw = config_path.read_text(encoding="utf-8")
    if not raw.strip():
        raise RuntimeError(f"Missing cli.toml content in {config_path}")
    parsed = tomllib.loads(raw)
    cli = parsed.get("cli", {})
    base_url = str(cli.get("base_url", "")).rstrip("/")
    if not base_url:
        raise RuntimeError(f"Missing required 'base_url' in [cli] section of {config_path}")
    return {
        "base_url": base_url,
        "client_id": str(cli.get("client_id", "")),
        "client_secret": str(cli.get("client_secret", "")),
        "username": str(cli.get("username", "")),
        "password": str(cli.get("password", "")),
    }


def _get_access_token(
    client_id: str,
    client_secret: str,
) -> str:
    """Fetch an OAuth2 access token from the Red Hat SSO endpoint."""

    def _fetch() -> str:
        resp = requests.post(
            TOKEN_URL,
            auth=(client_id, client_secret),
            data={"grant_type": "client_credentials", "scope": "api.console"},
            timeout=30,
        )
        resp.raise_for_status()
        return str(resp.json()["access_token"])

    return retry.retry_with_exponential_backoff(
        _fetch,
        max_attempts=3,
        retry_on=requests.RequestException,
    )


class PulpAuth(requests.auth.AuthBase):
    """Attach Pulp credentials to every request on a ``Session``.

    Prefer Basic auth when username/password are present; fall back to
    OAuth2 client-credentials.  OAuth2 tokens are fetched on every
    request so that long-running tasks never hit an expired token
    (matches the bash ``curl_auth`` -> ``get_auth_header`` behaviour).
    """

    def __init__(
        self,
        config: dict[str, str],
    ) -> None:
        """Initialize auth from *config*; validate credentials eagerly."""
        username = config.get("username", "")
        password = config.get("password", "")
        if username and password:
            self._basic_auth: requests.auth.HTTPBasicAuth | None = requests.auth.HTTPBasicAuth(
                username, password
            )
            self._client_id = ""
            self._client_secret = ""
            return

        client_id = config.get("client_id", "")
        client_secret = config.get("client_secret", "")
        if client_id and client_secret:
            self._basic_auth = None
            self._client_id = client_id
            self._client_secret = client_secret
            _get_access_token(client_id, client_secret)
            return

        raise RuntimeError(
            "No valid credentials in cli.toml "
            "(need username/password or client_id/client_secret)"
        )

    def __call__(self, r: requests.PreparedRequest) -> requests.PreparedRequest:
        """Set the Authorization header on the outgoing request."""
        if self._basic_auth:
            return self._basic_auth(r)
        token = _get_access_token(self._client_id, self._client_secret)
        r.headers["Authorization"] = f"Bearer {token}"
        return r


class PulpClient:
    """Thin wrapper around Pulp REST API calls."""

    def __init__(self, session: requests.Session, base_url: str, domain: str) -> None:
        """Create a client bound to *base_url* and Pulp *domain*."""
        self._session = session
        self._base_url = base_url
        self._domain = domain

    def get_json(self, url: str) -> dict[str, Any]:
        """GET *url* and return parsed JSON."""
        resp = self._session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def get_published_version_href(self, repo_name: str) -> str:
        """Return the published repository-version href for *repo_name*.

        Check the distribution's ``repository_version``, then its
        ``publication``, then the repository's ``latest_version_href``.
        Return an empty string when nothing is published.
        """
        dist_url = (
            f"{self._base_url}/api/pulp/{self._domain}"
            f"/api/v3/distributions/rpm/rpm/?name={repo_name}"
        )
        dist_data = self.get_json(dist_url)

        results = dist_data.get("results") or []
        if not results:
            return ""
        dist = results[0]

        rv = dist.get("repository_version")
        if rv:
            return str(rv)

        pub_href = dist.get("publication")
        if pub_href:
            rv = self.get_json(f"{self._base_url}{pub_href}").get("repository_version")
            if rv:
                return str(rv)

        repo_href = dist.get("repository")
        if repo_href:
            rv = self.get_json(f"{self._base_url}{repo_href}").get("latest_version_href")
            if rv:
                return str(rv)

        return ""

    def check_digest(
        self,
        repo_name: str,
        name: str,
        epoch: str,
        version: str,
        release: str,
        arch: str,
        expected_sha: str,
    ) -> PulpDigestStatus:
        """Check Pulp for an RPM digest in published content.

        Return ``MATCH`` when the digest matches, ``NOT_FOUND`` when
        no published version or no matching RPM exists, and ``MISMATCH``
        when a different digest is found.

        Raise ``requests.RequestException`` on API failures.
        """
        from logger import logger

        rv_href = self.get_published_version_href(repo_name)

        if not rv_href:
            logger.info(
                "  -> No published version for %s. RPM not accessible to users.",
                repo_name,
            )
            return PulpDigestStatus.NOT_FOUND

        query_url = (
            f"{self._base_url}/api/pulp/{self._domain}"
            f"/api/v3/content/rpm/packages/"
            f"?repository_version={quote(rv_href, safe='')}"
            f"&name={quote(name, safe='')}"
            f"&epoch={quote(epoch, safe='')}"
            f"&version={quote(version, safe='')}"
            f"&release={quote(release, safe='')}"
            f"&arch={quote(arch, safe='')}"
        )

        resp = self._session.get(query_url, timeout=60)
        resp.raise_for_status()

        data = resp.json()
        if data.get("count", 0) == 0:
            return PulpDigestStatus.NOT_FOUND

        for result in data.get("results", []):
            chref = result.get("pulp_href")
            if not chref:
                continue
            content_data = self.get_json(f"{self._base_url}{chref}")

            artifact_href = content_data.get("artifact") or (
                (content_data.get("artifacts") or [None])[0]
            )
            if not artifact_href:
                continue

            server_sha = self.get_json(f"{self._base_url}{artifact_href}").get("sha256", "")
            if server_sha and server_sha == expected_sha:
                return PulpDigestStatus.MATCH

        return PulpDigestStatus.MISMATCH
