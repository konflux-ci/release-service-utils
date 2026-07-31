"""Jira REST API helpers shared across task scripts.

Provides constants and functions for interacting with Jira Cloud and legacy
issue tracker APIs: server normalization, API URL construction, credential
reading, and authenticated JSON requests.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import authentication
import requests
from requests.auth import HTTPBasicAuth

SUPPORTED_JIRA_SERVER = "redhat.atlassian.net"
LEGACY_JIRA_SERVER = "issues.redhat.com"

ISSUE_TRACKERS: dict[str, dict[str, Any]] = {
    "Jira": {
        "api": "rest/api/2/issue",
        "servers": [
            LEGACY_JIRA_SERVER,
            "jira.atlassian.com",
            SUPPORTED_JIRA_SERVER,
        ],
    },
    "bugzilla": {
        "api": "rest/bug",
        "servers": ["bugzilla.redhat.com"],
    },
}


def normalize_issue_server(source: str) -> str:
    """Map legacy issue tracker hostnames to the current Jira server."""
    if source == LEGACY_JIRA_SERVER:
        return SUPPORTED_JIRA_SERVER
    return source


def api_path_for_server(server: str) -> str:
    """Return the REST API prefix for *server*."""
    for tracker in ISSUE_TRACKERS.values():
        servers = tracker.get("servers")
        if isinstance(servers, list) and server in servers:
            return str(tracker["api"])
    msg = f"no API mapping for server: {server}"
    raise ValueError(msg)


def read_jira_credentials(secret_path: Path) -> tuple[str, str]:
    """Read Jira basic-auth credentials from mounted secret files."""
    email = authentication.read_mounted_text(secret_path, "email")
    token = authentication.read_mounted_text(secret_path, "token")
    if not email or not token:
        msg = f"Jira secret at {secret_path} must include email and token"
        raise ValueError(msg)
    return email, token


def jira_issue_url(server: str, issue_id: str) -> str:
    """Build the Jira issue API URL for *issue_id* on *server*."""
    api_path = api_path_for_server(server)
    return f"https://{server}/{api_path}/{issue_id}"


def jira_get_json(
    session: requests.Session,
    url: str,
    auth: HTTPBasicAuth,
) -> dict[str, Any]:
    """Perform a GET request and return the parsed JSON object."""
    response = session.get(url, auth=auth, timeout=60.0)
    response.raise_for_status()
    data = response.json()
    if not isinstance(data, dict):
        msg = f"expected JSON object from {url}"
        raise ValueError(msg)
    return data


def jira_post_json(
    session: requests.Session,
    url: str,
    auth: HTTPBasicAuth,
    payload: dict[str, Any],
) -> None:
    """Perform a POST request and raise when the response is not successful."""
    response = session.post(
        url,
        auth=auth,
        json=payload,
        headers={"Content-Type": "application/json"},
        timeout=60.0,
    )
    response.raise_for_status()
