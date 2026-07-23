#!/usr/bin/env python3
"""Check if issues or CVEs in releaseNotes are embargoed.

Validates Jira/Bugzilla issues from ``releaseNotes.issues.fixed`` by querying
their REST APIs, injects a ``public`` boolean into each issue entry, validates
that Vulnerability-type Jira issues have their CVE listed in the content section,
and delegates CVE embargo checking to an InternalRequest running
``check-embargoed-cves``.
"""

from __future__ import annotations

import json
import os
import random
import time
from pathlib import Path
from typing import Any

import file
import http_client
import requests
import tekton
from internal_request import SPAWN_OVERHEAD_SECONDS, InternalRequestWaitError, create
from internal_request.internal_request import PIPELINERUN_UID_LABEL
from jira import (
    SUPPORTED_JIRA_SERVER,
    jira_issue_url,
    normalize_issue_server,
    read_jira_credentials,
)
from logger import logger
from requests.auth import HTTPBasicAuth
from subprocess_cmd import run_cmd

PROG = "embargo_check.py"

CVE_FIELD = "customfield_10667"

MAX_JIRA_404_RETRIES = 3


def _get_with_jira_404_retry(
    session: requests.Session,
    url: str,
    auth: HTTPBasicAuth,
) -> dict[str, Any]:
    """GET a Jira URL, retrying on transient 404s (RELEASE-2386).

    Jira Cloud occasionally returns spurious 404 responses for issues that
    exist.  This wrapper retries those with exponential backoff and jitter,
    then returns the parsed JSON object (matching the ``jira_get_json``
    contract).  Rate-limit (429) and server-error retries are handled by the
    session's urllib3 Retry adapter.
    """
    retries = 0
    while True:
        resp = session.get(url, auth=auth, timeout=60.0)
        if resp.status_code == 404:
            retries += 1
            if retries < MAX_JIRA_404_RETRIES:
                delay = 1.0 * (2 ** (retries - 1)) + random.randint(0, 2)
                logger.warning(
                    "Received 404 for %s (attempt %d/%d), retrying in %.1fs",
                    url,
                    retries,
                    MAX_JIRA_404_RETRIES,
                    delay,
                )
                time.sleep(delay)
                continue
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            msg = f"expected JSON object from {url}"
            raise ValueError(msg)
        return data


def _get_content_items(data: dict[str, Any]) -> list[dict[str, Any]] | None:
    """Return the content items list from releaseNotes, or None if absent.

    Checks ``images`` first, then ``artifacts``.  Returns the list even when
    empty (an empty list is valid content); returns ``None`` only when neither
    key is present.
    """
    release_notes = data.get("releaseNotes", {})
    content = release_notes.get("content", {})
    if content.get("images") is not None:
        return content["images"]
    if content.get("artifacts") is not None:
        return content["artifacts"]
    return None


def _extract_cves(data: dict[str, Any]) -> list[str]:
    """Extract unique CVE IDs from releaseNotes.content."""
    items = _get_content_items(data)
    if items is None:
        return []
    cves: set[str] = set()
    for item in items:
        fixed = item.get("cves", {}).get("fixed", {})
        cves.update(fixed.keys())
    return sorted(cves)


def _check_issue_visibility(session: requests.Session, url: str) -> bool:
    """Return True if the issue is accessible without authentication."""
    try:
        resp = session.get(url, auth=None, timeout=60.0)
        resp.raise_for_status()
        return True
    except requests.RequestException:
        return False


def _process_issue(
    issue: dict[str, Any],
    *,
    session: requests.Session,
    auth: HTTPBasicAuth,
    content_items: list[dict[str, Any]] | None,
) -> str | None:
    """Validate a single issue and inject the ``public`` key.

    Return an error message if the issue fails validation, or ``None`` on
    success.  The *issue* dict is mutated in place to add the ``public`` key.
    """
    server = normalize_issue_server(issue.get("source", ""))
    issue_id = issue.get("id", "")

    try:
        api_url = jira_issue_url(server, issue_id)
    except ValueError:
        return (
            f"Error: {issue_id} uses unsupported issue tracker '{server}'. "
            "Cannot verify embargo status; assuming embargoed."
        )

    try:
        if server == SUPPORTED_JIRA_SERVER:
            output = _get_with_jira_404_retry(session, api_url, auth)
        else:
            resp = session.get(api_url, auth=None, timeout=60.0)
            resp.raise_for_status()
            output = resp.json()
            if not isinstance(output, dict):
                return (
                    f"Error: {issue_id} returned unexpected JSON from {api_url}. "
                    "Cannot verify embargo status; assuming embargoed."
                )
    except requests.RequestException:
        return (
            f"Error: {issue_id} is not visible. "
            "Assuming it is embargoed and stopping pipelineRun execution."
        )

    public = False
    security_value = output.get("fields", {}).get("security")
    if security_value is None:
        if server == SUPPORTED_JIRA_SERVER:
            public = _check_issue_visibility(session, api_url)
        else:
            public = True

    issue["public"] = public

    if server != SUPPORTED_JIRA_SERVER:
        return None

    issue_type = (output.get("fields") or {}).get("issuetype", {}).get("name", "")
    if issue_type != "Vulnerability":
        return None

    if content_items is None:
        logger.info("No content found under releaseNotes.content.images or .artifacts;")
        return None

    cve_id = (output.get("fields") or {}).get(CVE_FIELD, "")
    cve_found = any(
        cve_id in (item.get("cves", {}).get("fixed", {})) for item in content_items
    )
    if not cve_found:
        return (
            f"Error: Issue {issue_id} lists 'CVE ID' {cve_id} "
            f"but that CVE is not present in the releaseNotes.content section "
            f"for any image or artifact. This is likely due to CVE {cve_id} "
            f"not being provided in the releaseNotes.cves part of your Release object."
        )

    return None


def check_issues(
    data: dict[str, Any],
    *,
    secret_path: Path,
    session: requests.Session,
) -> tuple[dict[str, Any], list[str]]:
    """Validate issues and inject the ``public`` key.

    Return the modified data dict and a list of error messages.
    """
    issues = (data.get("releaseNotes") or {}).get("issues", {}).get("fixed", [])
    if not issues:
        return data, []

    email, token = read_jira_credentials(secret_path)
    auth = HTTPBasicAuth(email, token)
    content_items = _get_content_items(data)

    errors: list[str] = []
    for issue in issues:
        error = _process_issue(
            issue,
            session=session,
            auth=auth,
            content_items=content_items,
        )
        if error:
            errors.append(error)

    return data, errors


def _format_timeout(seconds: int) -> str:
    """Format seconds as HHhMMmSSs for Tekton timeouts."""
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    return f"{hours:02d}h{minutes:02d}m{secs:02d}s"


def check_cves(
    data: dict[str, Any],
    *,
    pipeline_run_uid: str,
    request_timeout: int,
    task_git_url: str,
    task_git_revision: str,
) -> list[str]:
    """Check CVEs for embargo status via an InternalRequest.

    Return a list of error messages (empty on success).
    """
    cves = _extract_cves(data)
    if not cves:
        logger.info("No CVEs found to check")
        return []

    cves_str = " ".join(cves)
    logger.info("Checking the following CVEs: %s", cves_str)

    pipeline_timeout = _format_timeout(request_timeout + SPAWN_OVERHEAD_SECONDS)
    task_timeout = _format_timeout(request_timeout)
    wait_timeout = request_timeout + SPAWN_OVERHEAD_SECONDS

    try:
        ir_name = create(
            "check-embargoed-cves",
            params={
                "cves": cves_str,
                "taskGitUrl": task_git_url,
                "taskGitRevision": task_git_revision,
            },
            labels={PIPELINERUN_UID_LABEL: pipeline_run_uid},
            sync=True,
            timeout=wait_timeout,
            pipeline_timeout=pipeline_timeout,
            task_timeout=task_timeout,
        )
    except InternalRequestWaitError as e:
        return [f"internal-request failed: {e}"]

    logger.info("done (%s)", ir_name)

    result = run_cmd(
        [
            "kubectl",
            "get",
            "internalrequest",
            ir_name,
            "-o=jsonpath={.status.results}",
        ],
        check=True,
    )

    try:
        results = json.loads(result.stdout)
    except json.JSONDecodeError:
        return [f"Could not parse InternalRequest results: {result.stdout.strip()}"]

    if results.get("result") == "Success":
        logger.info("No embargoed CVEs found")
        return []

    embargoed = results.get("embargoed_cves", "")
    return [f"The following CVEs are marked as embargoed: {embargoed}"]


def run(
    data_file: Path,
    *,
    secret_path: Path,
    pipeline_run_uid: str,
    request_timeout: int,
    task_git_url: str,
    task_git_revision: str,
) -> None:
    """Orchestrate the embargo check: validate issues and check CVEs."""
    if not data_file.is_file():
        raise RuntimeError("No data JSON was provided.")

    data = file.load_json_dict(data_file)

    session = http_client.get_retry_session(
        total=5,
        connect=3,
        read=3,
        status=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
    )

    data, issue_errors = check_issues(
        data,
        secret_path=secret_path,
        session=session,
    )

    data_file.write_text(json.dumps(data, indent=2), encoding="utf-8")

    if issue_errors:
        for err in issue_errors:
            logger.error(err)
        raise RuntimeError("\n".join(issue_errors))

    cve_errors = check_cves(
        data,
        pipeline_run_uid=pipeline_run_uid,
        request_timeout=request_timeout,
        task_git_url=task_git_url,
        task_git_revision=task_git_revision,
    )

    if cve_errors:
        for err in cve_errors:
            logger.error(err)
        raise RuntimeError("\n".join(cve_errors))


def main() -> int:
    """Read environment variables and execute the embargo check."""
    data_file_str = tekton.require_env("DATA_FILE")
    secret_path = file.path_from_env_variable("JIRA_SECRET_PATH", "/etc/secrets")
    pipeline_run_uid = tekton.require_env("PIPELINE_RUN_UID")
    request_timeout = int(os.environ.get("REQUEST_TIMEOUT", "2700").strip())
    task_git_url = tekton.require_env("TASK_GIT_URL")
    task_git_revision = tekton.require_env("TASK_GIT_REVISION")

    run(
        Path(data_file_str),
        secret_path=secret_path,
        pipeline_run_uid=pipeline_run_uid,
        request_timeout=request_timeout,
        task_git_url=task_git_url,
        task_git_revision=task_git_revision,
    )

    logger.info("embargo-check completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
