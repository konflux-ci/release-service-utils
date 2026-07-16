#!/usr/bin/env python3
"""Close Jira issues listed in releaseNotes after an advisory is published."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import advisory_data
import authentication
import file
import http_client
import requests
import tekton
from logger import logger
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

_VALID_JIRA_ISSUE_ID = re.compile(r"^[A-Za-z][A-Za-z0-9_]+-\d+$|^\d+$")


def normalize_issue_server(source: str) -> str:
    """Map legacy issue tracker hostnames to the current Jira server."""
    if source == LEGACY_JIRA_SERVER:
        return SUPPORTED_JIRA_SERVER
    return source


def is_jira_eligible_issue(issue: dict[str, Any]) -> bool:
    """Return whether *issue* will be processed against the Jira API."""
    issue_id = issue.get("id")
    source = issue.get("source")
    if not isinstance(issue_id, str) or not issue_id.strip():
        return False
    if not isinstance(source, str) or not source.strip():
        return False
    server = normalize_issue_server(source.strip())
    if server != SUPPORTED_JIRA_SERVER:
        return False
    return _VALID_JIRA_ISSUE_ID.fullmatch(issue_id.strip()) is not None


def api_path_for_server(server: str) -> str:
    """Return the REST API prefix for *server*."""
    for tracker in ISSUE_TRACKERS.values():
        servers = tracker.get("servers")
        if isinstance(servers, list) and server in servers:
            return str(tracker["api"])
    msg = f"no API mapping for server: {server}"
    raise ValueError(msg)


def load_fixed_issues(data: dict[str, Any]) -> list[dict[str, Any]]:
    """Return fixed issues from releaseNotes, or an empty list when absent."""
    fixed = advisory_data.content_array_from_decoded(
        data,
        ".releaseNotes.issues.fixed",
    )
    return [row for row in fixed if isinstance(row, dict)]


def close_comment(advisory_url: str) -> str:
    """Build the Jira comment posted when an issue is closed."""
    return f"Fixed in Konflux Advisory {advisory_url}"


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


def issue_status_name(issue: dict[str, Any]) -> str:
    """Return the human-readable Jira status name for *issue*."""
    fields = issue.get("fields")
    if not isinstance(fields, dict):
        return ""
    status = fields.get("status")
    if not isinstance(status, dict):
        return ""
    name = status.get("name")
    return name if isinstance(name, str) else ""


def closed_transition_id(transitions: dict[str, Any]) -> str | None:
    """Return the transition id for the Closed state, if present."""
    items = transitions.get("transitions")
    if not isinstance(items, list):
        return None
    for transition in items:
        if not isinstance(transition, dict):
            continue
        if transition.get("name") != "Closed":
            continue
        transition_id = transition.get("id")
        if transition_id is not None:
            return str(transition_id)
    return None


def close_issue_with_comment(
    session: requests.Session,
    issue_url: str,
    auth: HTTPBasicAuth,
    transition_id: str,
    comment: str,
) -> None:
    """Transition the issue to Closed and add *comment*."""
    payload = {
        "transition": {"id": transition_id},
        "update": {"comment": [{"add": {"body": comment}}]},
    }
    jira_post_json(session, f"{issue_url}/transitions", auth, payload)


def add_issue_comment(
    session: requests.Session,
    issue_url: str,
    auth: HTTPBasicAuth,
    comment: str,
) -> None:
    """Add *comment* to the issue without changing its state."""
    jira_post_json(session, f"{issue_url}/comment", auth, {"body": comment})


def process_fixed_issue(
    issue: dict[str, Any],
    *,
    advisory_url: str,
    auth: HTTPBasicAuth,
    session: requests.Session,
) -> None:
    """Close one fixed issue or add an advisory comment when closing fails."""
    issue_id = issue.get("id")
    source = issue.get("source")
    if not isinstance(issue_id, str) or not issue_id.strip():
        logger.warning("Skipping issue with missing id: %s", issue)
        return
    if not isinstance(source, str) or not source.strip():
        logger.warning("Skipping issue with missing source: %s", issue)
        return

    normalized_source = source.strip()
    server = normalize_issue_server(normalized_source)
    if server != SUPPORTED_JIRA_SERVER:
        logger.warning(
            "This task currently only supports closing issues on "
            "issues.redhat.com and redhat.atlassian.net. Skipping issue %s "
            "as it is on %s",
            issue,
            normalized_source,
        )
        return

    normalized_id = issue_id.strip()
    if _VALID_JIRA_ISSUE_ID.fullmatch(normalized_id) is None:
        logger.warning(
            "Skipping issue with invalid Jira id %r: %s",
            normalized_id,
            issue,
        )
        return

    issue_url = jira_issue_url(server, normalized_id)
    comment = close_comment(advisory_url)

    issue_data = jira_get_json(session, issue_url, auth)
    if issue_status_name(issue_data) == "Closed":
        logger.info("Issue %s is already in Closed state. Skipping it.", issue)
        return

    logger.info("Closing issue %s", issue)
    closing_failed = False
    try:
        transitions = jira_get_json(session, f"{issue_url}/transitions", auth)
        transition_id = closed_transition_id(transitions)
        if transition_id is None:
            logger.warning(
                "Warning: failed to fetch the closed state id for issue %s. "
                "We most likely do not have permission to close it. Will try "
                "to add a comment instead.",
                issue,
            )
            closing_failed = True
        else:
            close_issue_with_comment(session, issue_url, auth, transition_id, comment)
    except requests.RequestException as exc:
        logger.warning(
            "Warning: failed to close issue %s. Will try to add a comment " "instead. %s",
            issue,
            exc,
        )
        closing_failed = True

    if not closing_failed:
        return

    try:
        add_issue_comment(session, issue_url, auth, comment)
    except requests.RequestException as exc:
        logger.warning("Warning: failed to add comment to issue %s. %s", issue, exc)


def close_advisory_issues(
    *,
    data_dir: Path,
    data_path: Path,
    advisory_url: str,
    secret_path: Path,
) -> None:
    """Close fixed Jira issues referenced in the release data file."""
    data_file = data_dir / data_path
    logger.info("Loading release data from %s", data_file)
    data = file.load_json_dict(data_file)
    fixed_issues = load_fixed_issues(data)
    if not any(is_jira_eligible_issue(issue) for issue in fixed_issues):
        return

    email, token = read_jira_credentials(secret_path)
    auth = HTTPBasicAuth(email, token)
    session = http_client.get_retry_session(
        total=5,
        connect=3,
        read=3,
        status=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
    )

    first_error: Exception | None = None
    for issue in fixed_issues:
        try:
            process_fixed_issue(
                issue,
                advisory_url=advisory_url,
                auth=auth,
                session=session,
            )
        except Exception as exc:
            if first_error is None:
                first_error = exc
            logger.warning("Failed to process issue %s: %s", issue, exc)

    if first_error is not None:
        raise first_error


def main() -> int:
    """Run the close-advisory-issues workflow."""
    close_advisory_issues(
        data_dir=Path(tekton.require_env("PARAM_DATA_DIR")),
        data_path=Path(tekton.require_env("PARAM_DATA_PATH")),
        advisory_url=tekton.require_env("PARAM_ADVISORY_URL"),
        secret_path=file.path_from_env_variable("JIRA_SECRET_PATH", "/etc/secrets"),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
