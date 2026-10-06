#!/usr/bin/env python3
"""Create or update the sticky ITS results comment on a pull request."""

from __future__ import annotations

import argparse
import base64
import binascii
import html
import json
import os
import sys
from pathlib import Path
from typing import Any

import requests

from release_service_utils.helpers import retry
from release_service_utils.helpers import tekton
from release_service_utils.helpers.logger import logger
from release_service_utils.helpers.redact import redact_secrets
from release_service_utils.helpers.vcs import github

MARKER = "<!-- release-service-catalog-its-results:v1 -->"
STATE_PREFIX = "<!-- release-service-catalog-its-results-state: "
STATE_SUFFIX = " -->"
MAX_UPDATE_ATTEMPTS = 5
FINAL_RESULTS = {"FAILURE", "SUCCESS", "SKIPPED"}


class RetryableCommentUpdateError(RuntimeError):
    """Retryable PR comment update failure."""


class UnreadableCommentStateError(ValueError):
    """Stored PR comment state exists but could not be read safely."""


def parse_args() -> tuple[str, int]:
    """Return ``(repo_name, pr_number)`` from ``sys.argv``."""
    parser = argparse.ArgumentParser(
        prog=Path(sys.argv[0]).name,
        description=__doc__,
    )
    parser.add_argument("repo_name")
    parser.add_argument("pr_number", type=int)
    parsed = parser.parse_args()
    return parsed.repo_name.strip(), parsed.pr_number


def load_metadata(metadata_json: str) -> dict[str, str] | None:
    """Return normalized run-test metadata, or ``None`` when it should be skipped."""
    try:
        metadata = json.loads(metadata_json)
    except json.JSONDecodeError:
        logger.warning("No valid run-test metadata found; skipping PR results comment update.")
        return None
    if not isinstance(metadata, dict):
        logger.warning(
            "run-test metadata is not an object; skipping PR results comment update."
        )
        return None

    its_name = str(metadata.get("its_name") or "").strip()
    its_key = str(metadata.get("its_key") or its_name).strip()
    result = str(metadata.get("result") or "").strip()
    if not its_name or not its_key or not result:
        logger.warning(
            "run-test metadata is missing its_name, its_key, or result; "
            "skipping PR results comment update."
        )
        return None
    if result not in FINAL_RESULTS:
        logger.warning(
            "run-test metadata result %s is not final; skipping PR results comment update.",
            result,
        )
        return None

    return {
        "its_name": its_name,
        "its_key": its_key,
        "result": result,
        "failure_label": str(metadata.get("failure_label") or "").strip(),
        "details_url": str(metadata.get("details_url") or "").strip(),
        "details_text": str(metadata.get("details_text") or "").strip(),
    }


def encode_state(rows: list[dict[str, str]]) -> str:
    """Return the hidden state marker payload for *rows*."""
    state_json = json.dumps(rows, separators=(",", ":")).encode("utf-8")
    return base64.standard_b64encode(state_json).decode("ascii")


def extract_state(body: str) -> list[dict[str, str]]:
    """Return the stored ITS state from *body*."""
    has_comment_marker = MARKER in body
    for line in body.splitlines():
        if line.startswith(STATE_PREFIX) and line.endswith(STATE_SUFFIX):
            encoded = line[len(STATE_PREFIX) : -len(STATE_SUFFIX)]
            try:
                decoded = base64.standard_b64decode(encoded).decode("utf-8")
                state = json.loads(decoded)
            except (ValueError, UnicodeDecodeError, binascii.Error) as exc:
                raise UnreadableCommentStateError(
                    "stored ITS results state is unreadable"
                ) from exc
            if not isinstance(state, list):
                raise UnreadableCommentStateError("stored ITS results state is not a list")
            normalized: list[dict[str, str]] = []
            for row in state:
                if not isinstance(row, dict):
                    continue
                normalized.append(
                    {
                        "its_key": str(
                            row.get("its_key") or row.get("its_name") or ""
                        ).strip(),
                        "its_name": str(row.get("its_name") or "").strip(),
                        "failure_label": str(row.get("failure_label") or "").strip(),
                        "details_url": str(row.get("details_url") or "").strip(),
                        "details_text": str(row.get("details_text") or "").strip(),
                    }
                )
            return normalized
    if has_comment_marker:
        raise UnreadableCommentStateError("stored ITS results state marker is missing")
    return []


def merge_state(
    existing_state: list[dict[str, str]],
    metadata: dict[str, str],
) -> list[dict[str, str]]:
    """Return the updated ITS state for one run-test result."""
    current_key = metadata["its_key"]
    current_name = metadata["its_name"]
    without_current = [
        row
        for row in existing_state
        if (row.get("its_key") or row.get("its_name") or "") != current_key
        and not (not row.get("its_key") and row.get("its_name") == current_name)
    ]
    if metadata["result"] == "FAILURE":
        without_current.append(
            {
                "its_key": current_key,
                "its_name": current_name,
                "failure_label": metadata["failure_label"],
                "details_url": metadata["details_url"],
                "details_text": metadata["details_text"],
            }
        )
    return sorted(without_current, key=lambda row: row["its_name"])


def render_row(row: dict[str, str]) -> str:
    """Render one ITS failure row as HTML."""
    link_cell = ""
    if row["details_url"].startswith(("http://", "https://")):
        escaped_url = html.escape(row["details_url"], quote=True)
        link_cell = f'<a href="{escaped_url}">Open</a>'

    details_cell = ""
    if row["details_text"]:
        details = html.escape(row["details_text"], quote=True).replace("\n", "<br>")
        details_cell = f"<details><summary>Show</summary>{details}</details>"

    its_name = html.escape(row["its_name"], quote=True)
    failure_label = html.escape(row["failure_label"], quote=True)
    return (
        f"<tr><td>{its_name}</td><td>{failure_label}</td><td>{link_cell}</td>"
        f"<td>{details_cell}</td></tr>"
    )


def render_comment_body(state: list[dict[str, str]]) -> str:
    """Render the full sticky PR comment body."""
    lines = [
        MARKER,
        f"{STATE_PREFIX}{encode_state(state)}{STATE_SUFFIX}",
        "## Release Service Catalog ITS failures",
        "",
    ]
    if not state:
        lines.append("No failing ITS rows in the latest PR-triggered runs.")
        return "\n".join(lines)

    lines.extend(
        [
            "<table>",
            "<thead><tr><th>ITS</th><th>Failure</th><th>PipelineRun</th>"
            "<th>Details</th></tr></thead>",
            "<tbody>",
            *[render_row(row) for row in state],
            "</tbody>",
            "</table>",
        ]
    )
    return "\n".join(lines)


def state_matches_expected(
    state: list[dict[str, str]],
    metadata: dict[str, str],
) -> bool:
    """Return whether *state* reflects *metadata*."""
    target_key = metadata["its_key"]
    if metadata["result"] == "FAILURE":
        for row in state:
            row_key = row.get("its_key") or row.get("its_name") or ""
            if row_key != target_key:
                continue
            return (
                row.get("failure_label", "") == metadata["failure_label"]
                and row.get("details_url", "") == metadata["details_url"]
                and row.get("details_text", "") == metadata["details_text"]
            )
        return False
    return all(
        (row.get("its_key") or row.get("its_name") or "") != target_key for row in state
    )


def find_existing_comment(
    session: github.GitHubAppSession,
    repo_name: str,
    pr_number: int,
) -> dict[str, Any] | None:
    """Return the existing sticky ITS results comment for this token, if any."""
    github_login = github.get_authenticated_user_login(session)
    page = 1
    while True:
        comments = github.list_issue_comments(session, repo_name, pr_number, page=page)
        matching_comment = None
        for comment in comments:
            user = comment.get("user") or {}
            if (user.get("login") or "") != github_login:
                continue
            if MARKER in str(comment.get("body") or ""):
                matching_comment = comment
        if matching_comment is not None:
            return matching_comment
        if len(comments) < 100:
            return None
        page += 1


def _github_error_text(exc: requests.HTTPError) -> str:
    """Return a short redacted error message for a GitHub API failure."""
    response = exc.response
    if response is None:
        return redact_secrets(str(exc))
    body = response.text.strip()
    if body:
        try:
            payload = response.json()
        except ValueError:
            return redact_secrets(body)
        if isinstance(payload, dict):
            for key in ("message", "error"):
                text = str(payload.get(key) or "").strip()
                if text:
                    return redact_secrets(text)
            errors = payload.get("errors")
            if isinstance(errors, list):
                for item in errors:
                    if isinstance(item, dict):
                        text = str(item.get("message") or "").strip()
                        if text:
                            return redact_secrets(text)
        return redact_secrets(body)
    return redact_secrets(str(exc))


def _upsert_comment_once(
    session: github.GitHubAppSession,
    repo_name: str,
    pr_number: int,
    metadata: dict[str, str],
    attempt_state: dict[str, int],
) -> None:
    """Run one read-merge-write-verify attempt."""
    attempt_state["count"] += 1
    attempt = attempt_state["count"]

    try:
        existing_comment = find_existing_comment(session, repo_name, pr_number)
    except (
        UnreadableCommentStateError,
        json.JSONDecodeError,
        requests.RequestException,
        RuntimeError,
        TypeError,
    ) as exc:
        message = redact_secrets(str(exc))
        logger.warning(
            "Failed to read existing ITS results comment state on attempt %s/%s: %s",
            attempt,
            MAX_UPDATE_ATTEMPTS,
            message,
        )
        raise RetryableCommentUpdateError(message) from exc

    try:
        existing_state = (
            extract_state(str(existing_comment.get("body") or "")) if existing_comment else []
        )
    except UnreadableCommentStateError as exc:
        message = redact_secrets(str(exc))
        logger.warning(
            "Failed to decode existing ITS results comment state on attempt %s/%s: %s",
            attempt,
            MAX_UPDATE_ATTEMPTS,
            message,
        )
        raise RetryableCommentUpdateError(message) from exc
    updated_state = merge_state(existing_state, metadata)
    if existing_comment is None and not updated_state:
        logger.info("No failing ITS rows and no existing results comment; nothing to do.")
        return

    updated_body = render_comment_body(updated_state)
    try:
        if existing_comment is not None:
            logger.info(
                "Updating ITS results comment on PR #%s (attempt %s/%s)",
                pr_number,
                attempt,
                MAX_UPDATE_ATTEMPTS,
            )
            github.update_issue_comment(session, int(existing_comment["id"]), updated_body)
        else:
            logger.info(
                "Creating ITS results comment on PR #%s (attempt %s/%s)",
                pr_number,
                attempt,
                MAX_UPDATE_ATTEMPTS,
            )
            github.create_issue_comment(session, repo_name, pr_number, updated_body)
    except requests.HTTPError as exc:
        status = exc.response.status_code if exc.response is not None else "unknown"
        message = _github_error_text(exc)
        logger.warning(
            "Failed to write ITS results comment on attempt %s/%s (status %s): %s",
            attempt,
            MAX_UPDATE_ATTEMPTS,
            status,
            message,
        )
        raise RetryableCommentUpdateError(message) from exc
    except json.JSONDecodeError as exc:
        message = redact_secrets(str(exc))
        logger.warning(
            "Failed to decode ITS results comment write response on attempt %s/%s: %s",
            attempt,
            MAX_UPDATE_ATTEMPTS,
            message,
        )
        raise RetryableCommentUpdateError(message) from exc
    except requests.RequestException as exc:
        message = redact_secrets(str(exc))
        logger.warning(
            "Failed to write ITS results comment on attempt %s/%s: %s",
            attempt,
            MAX_UPDATE_ATTEMPTS,
            message,
        )
        raise RetryableCommentUpdateError(message) from exc

    try:
        refreshed_comment = find_existing_comment(session, repo_name, pr_number)
    except (
        UnreadableCommentStateError,
        json.JSONDecodeError,
        requests.RequestException,
        RuntimeError,
        TypeError,
    ) as exc:
        message = redact_secrets(str(exc))
        logger.warning(
            "Failed to read ITS results comment after write on attempt %s/%s: %s",
            attempt,
            MAX_UPDATE_ATTEMPTS,
            message,
        )
        raise RetryableCommentUpdateError(message) from exc

    if refreshed_comment is None:
        message = "ITS results comment was not readable after update"
        logger.warning("%s on attempt %s/%s", message, attempt, MAX_UPDATE_ATTEMPTS)
        raise RetryableCommentUpdateError(message)

    try:
        refreshed_state = extract_state(str(refreshed_comment.get("body") or ""))
    except UnreadableCommentStateError as exc:
        message = redact_secrets(str(exc))
        logger.warning(
            "Failed to decode ITS results comment state after write on attempt %s/%s: %s",
            attempt,
            MAX_UPDATE_ATTEMPTS,
            message,
        )
        raise RetryableCommentUpdateError(message) from exc
    if state_matches_expected(refreshed_state, metadata):
        logger.info("ITS results comment is up to date.")
        return

    message = "ITS results comment state did not match expected content"
    logger.warning("%s on attempt %s/%s", message, attempt, MAX_UPDATE_ATTEMPTS)
    raise RetryableCommentUpdateError(message)


def upsert_comment(
    session: github.GitHubAppSession,
    repo_name: str,
    pr_number: int,
    metadata: dict[str, str],
) -> int:
    """Create or update the sticky ITS results comment."""
    attempt_state = {"count": 0}
    try:
        retry.retry_with_exponential_backoff(
            lambda: _upsert_comment_once(
                session,
                repo_name,
                pr_number,
                metadata,
                attempt_state,
            ),
            max_attempts=MAX_UPDATE_ATTEMPTS,
            retry_on=RetryableCommentUpdateError,
            base_sleep_seconds=1,
            max_sleep_seconds=1,
        )
    except RetryableCommentUpdateError:
        logger.warning(
            "Failed to update ITS results comment after %s attempts",
            MAX_UPDATE_ATTEMPTS,
        )
        return 1
    return 0


def main() -> int:
    """Read metadata and update the sticky ITS results comment."""
    repo_name, pr_number = parse_args()
    metadata_json = os.environ.get("RUN_TEST_METADATA_JSON", "")
    metadata = load_metadata(metadata_json)
    if metadata is None:
        return 0

    token_path = Path(tekton.require_env("GITHUB_TOKEN_PATH"))
    token = token_path.read_text(encoding="utf-8").strip()
    session = github.bearer_token_session(token)
    return upsert_comment(session, repo_name, pr_number, metadata)


if __name__ == "__main__":
    raise SystemExit(main())
