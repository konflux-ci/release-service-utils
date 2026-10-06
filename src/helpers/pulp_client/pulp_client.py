"""Pulp REST API client with TOML-based configuration and auth.

Parse ``cli.toml`` files, authenticate via Basic or OAuth2
client-credentials, and query the Pulp REST API for domains,
distributions, repository versions, and RPM content digests.
Chunked RPM content upload goes through the ``pulp`` CLI.
"""

from __future__ import annotations

import json
import time
from collections.abc import Sequence
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
import requests.auth
import tomllib

from release_service_utils.helpers import http_client
from release_service_utils.helpers import retry
from release_service_utils.helpers import subprocess_cmd
from release_service_utils.helpers.logger import logger

TOKEN_URL = "https://sso.redhat.com/auth/realms/redhat-external/protocol/openid-connect/token"
RPM_REPO_LIST_LIMIT = 100
UPLOAD_ATTEMPTS = 5
UPLOAD_BASE_SLEEP_SECONDS = 2
# pulp-cli ``domain show`` lists domains through this slug when DOMAIN_ENABLED.
PULP_LISTING_DOMAIN = "default"


class PulpDigestStatus(Enum):
    """Outcome of a Pulp digest check."""

    MATCH = "match"
    NOT_FOUND = "not_found"
    MISMATCH = "mismatch"


def parse_pulp_config(config_path: Path) -> dict[str, str]:
    """Parse a Pulp cli.toml file and return credential fields.

    Prefer the ``[cli]`` table when present; otherwise read top-level
    scalar keys.  Return a flat dict with keys: ``base_url``,
    ``client_id``, ``client_secret``, ``username``, ``password``.
    Credentials default to empty strings; ``base_url`` is required and
    raises ``RuntimeError`` when missing or blank.
    """
    raw = config_path.read_text(encoding="utf-8")
    if not raw.strip():
        raise RuntimeError(f"Missing cli.toml content in {config_path}")
    parsed = tomllib.loads(raw)
    if "cli" in parsed and isinstance(parsed["cli"], dict):
        cli = parsed["cli"]
    else:
        cli = {k: v for k, v in parsed.items() if not isinstance(v, dict)}
    base_url = str(cli.get("base_url", "")).rstrip("/")
    if not base_url:
        raise RuntimeError(f"Missing required 'base_url' in {config_path}")
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
    OAuth2 client-credentials. OAuth2 tokens are fetched on every
    request so long-running tasks never hit an expired token.
    """

    auth_method: str

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
            self.auth_method = "basic"
            return

        client_id = config.get("client_id", "")
        client_secret = config.get("client_secret", "")
        if client_id and client_secret:
            self._basic_auth = None
            self._client_id = client_id
            self._client_secret = client_secret
            self.auth_method = "oauth2-bearer"
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
    """Thin wrapper around Pulp REST API calls and CLI RPM uploads."""

    def __init__(self, session: requests.Session, base_url: str, domain: str) -> None:
        """Create a client bound to *base_url* and Pulp *domain*."""
        self._session = session
        self._base_url = base_url
        self._domain = domain

    @classmethod
    def from_config(cls, config: dict[str, str], domain: str) -> PulpClient:
        """Build a client with a retrying session and *config* credentials."""
        session = http_client.get_retry_session(
            total=3,
            connect=3,
            read=3,
            status=2,
            backoff_factor=0.4,
            allowed_methods=frozenset({"GET", "POST"}),
        )
        session.auth = PulpAuth(config)
        return cls(session, config["base_url"], domain)

    @property
    def auth_method(self) -> str:
        """Return ``basic`` or ``oauth2-bearer`` from the attached authenticator."""
        auth = self._session.auth
        if isinstance(auth, PulpAuth):
            return auth.auth_method
        return "unknown"

    def get_json(self, url: str) -> dict[str, Any]:
        """GET *url* and return parsed JSON."""
        resp = self._session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.json()

    def _absolute_url(self, href: str) -> str:
        """Return *href* as an absolute URL against this client's base."""
        if href.startswith(("http://", "https://")):
            return href
        return f"{self._base_url}{href}"

    def _paged_results(self, url: str) -> list[dict[str, Any]]:
        """Follow Pulp list pagination and return every result object."""
        items: list[dict[str, Any]] = []
        next_url: str | None = url
        while next_url:
            data = self.get_json(next_url)
            for item in data.get("results") or []:
                if isinstance(item, dict):
                    items.append(item)
            nxt = data.get("next") or ""
            next_url = self._absolute_url(str(nxt)) if nxt else None
        return items

    def ensure_domain_exists(self) -> None:
        """Raise if this client's Pulp domain is not present.

        Domain-enabled Pulp requires a domain slug in the API path.
        Query ``/api/pulp/default/api/v3/domains/`` to match
        ``pulp domain show --name``. The unscoped path
        ``/api/pulp/api/v3/domains/`` 404s on packages.redhat.com.
        """
        url = (
            f"{self._base_url}/api/pulp/{PULP_LISTING_DOMAIN}/api/v3/domains/"
            f"?name={quote(self._domain, safe='')}"
        )
        data = self.get_json(url)
        results = data.get("results") or []
        name = ""
        if results and isinstance(results[0], dict):
            name = str(results[0].get("name") or "")
        if name != self._domain:
            raise RuntimeError(f"Domain '{self._domain}' not found.")
        logger.info("Domain '%s' exists", self._domain)

    def list_rpm_repository_names(self) -> set[str]:
        """Return RPM repository names in this domain."""
        url = (
            f"{self._base_url}/api/pulp/{self._domain}"
            f"/api/v3/repositories/rpm/rpm/?fields=name&limit={RPM_REPO_LIST_LIMIT}"
        )
        return {str(item["name"]) for item in self._paged_results(url) if item.get("name")}

    def ensure_repos_exist(self, names: Sequence[str]) -> None:
        """Raise if any of *names* is missing from this domain."""
        if not names:
            return
        existing = self.list_rpm_repository_names()
        missing = [name for name in names if name not in existing]
        if missing:
            raise RuntimeError(
                f"Missing repos in Pulp domain {self._domain}: {' '.join(missing)}"
            )

    def get_published_version_href(
        self, repo_name: str, *, fallback_to_latest: bool = False
    ) -> str:
        """Return the published repository-version href for *repo_name*.

        Check the distribution's ``repository_version``, then its
        ``publication``. When *fallback_to_latest* is True, also try the
        repository's unpublished ``latest_version_href``. Return an empty
        string when nothing is published (and, by default, when content
        exists only in an unpublished latest version).
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

        if not fallback_to_latest:
            return ""

        repo_href = dist.get("repository")
        if repo_href:
            rv = self.get_json(f"{self._base_url}{repo_href}").get("latest_version_href")
            if rv:
                return str(rv)

        return ""

    def get_repo_href(self, repo_name: str) -> str:
        """Return the Pulp href for the RPM repository named *repo_name*."""
        url = (
            f"{self._base_url}/api/pulp/{self._domain}"
            f"/api/v3/repositories/rpm/rpm/?name={quote(repo_name, safe='')}"
        )
        data = self.get_json(url)
        results = data.get("results") or []
        href = results[0].get("pulp_href") if results else None
        if not href:
            raise RuntimeError(f"Could not find repository href for {repo_name}")
        return str(href)

    def wait_for_task(
        self,
        task_url: str,
        timeout_seconds: int,
        poll_interval: int = 5,
    ) -> None:
        """Poll *task_url* until the Pulp task completes, fails, or times out."""
        elapsed = 0
        while elapsed < timeout_seconds:
            data = self.get_json(task_url)
            state = data.get("state") or ""
            if state == "completed":
                logger.info("Pulp task completed: %s", task_url)
                return
            if state == "failed":
                detail = data.get("error") or data.get("description") or "No details"
                raise RuntimeError(f"Pulp task failed: {task_url}: {detail}")
            logger.info(
                "Waiting for Pulp task (state=%s, %ss elapsed)...",
                state,
                elapsed,
            )
            time.sleep(poll_interval)
            elapsed += poll_interval
        raise RuntimeError(f"Timeout waiting for Pulp task: {task_url}")

    def add_content(
        self,
        repo_name: str,
        hrefs: list[str],
        timeout_seconds: int,
    ) -> None:
        """Add content units identified by *hrefs* to *repo_name* and wait."""
        if not hrefs:
            return
        repo_href = self.get_repo_href(repo_name)
        url = f"{self._base_url}{repo_href}modify/"
        logger.info("Calling API: POST %s", url)
        resp = self._session.post(
            url,
            json={"add_content_units": hrefs},
            timeout=60,
        )
        if resp.status_code not in (200, 202):
            raise RuntimeError(f"modify returned HTTP {resp.status_code}: {resp.text}")
        body = resp.json() if resp.content else {}
        task_href = body.get("task") or ""
        if not task_href:
            return
        self.wait_for_task(self._absolute_url(str(task_href)), timeout_seconds)

    def upload_rpm(self, file_path: Path, chunk_size: str, config_file: Path) -> str:
        """Upload *file_path* with retries and return the resulting pulp_href.

        Uses the ``pulp`` CLI for chunked RPM content upload, authenticated
        via the ``cli.toml`` at *config_file*.
        """
        relpath = file_path.name

        def _do_upload() -> str:
            logger.info("Upload start: %s", relpath)
            started = time.monotonic()
            result = subprocess_cmd.run_cmd(
                [
                    "pulp",
                    "--config",
                    str(config_file),
                    "--domain",
                    self._domain,
                    "rpm",
                    "content",
                    "upload",
                    "--chunk-size",
                    chunk_size,
                    "--file",
                    str(file_path),
                    "--relative-path",
                    relpath,
                ],
                check=False,
            )
            elapsed = int(time.monotonic() - started)
            cli_error = (result.stderr or "").strip() or (result.stdout or "").strip()
            if result.returncode != 0:
                suffix = f": {cli_error}" if cli_error else ""
                logger.error(
                    "Upload failed: %s (rc=%s, took %ss)%s",
                    relpath,
                    result.returncode,
                    elapsed,
                    suffix,
                )
                raise RuntimeError(
                    f"Upload failed for {file_path} (rc={result.returncode}){suffix}"
                )
            logger.info("Upload end: %s (took %ss)", relpath, elapsed)
            try:
                data = json.loads(result.stdout or "{}")
            except json.JSONDecodeError as exc:
                suffix = f": {cli_error}" if cli_error else ""
                raise RuntimeError(
                    f"Upload failed for {file_path}: invalid JSON{suffix}"
                ) from exc
            href = data.get("pulp_href") or ""
            if not href:
                suffix = f": {cli_error}" if cli_error else ""
                raise RuntimeError(f"Upload failed for {file_path} (href missing){suffix}")
            return str(href)

        return retry.retry_with_exponential_backoff(
            _do_upload,
            max_attempts=UPLOAD_ATTEMPTS,
            retry_on=RuntimeError,
            base_sleep_seconds=UPLOAD_BASE_SLEEP_SECONDS,
        )

    def check_digest(
        self,
        repo_name: str,
        name: str,
        epoch: str,
        version: str,
        release: str,
        arch: str,
        expected_sha: str,
        *,
        fallback_to_latest: bool = False,
    ) -> PulpDigestStatus:
        """Check Pulp for an RPM digest in published content.

        Return ``MATCH`` when the digest matches, ``NOT_FOUND`` when
        no published version or no matching RPM exists, and ``MISMATCH``
        when a different digest is found.

        By default only published distribution/publication content is
        searched. Pass *fallback_to_latest* True to also search the
        repository's unpublished ``latest_version_href``.

        Raise ``requests.RequestException`` on API failures.
        """
        rv_href = self.get_published_version_href(
            repo_name, fallback_to_latest=fallback_to_latest
        )

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
