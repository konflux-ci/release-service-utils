#!/usr/bin/env python3
"""Validate that image security-tracker labels map to valid Jira projects.

For every container image component in the snapshot this script:

1. Reads the ``com.redhat.security-tracker-project`` label from the
   component's ``metadata.labels`` list (populated by apply-mapping)
   and confirms the Jira project exists.
2. Resolves a Jira component using a fallback chain of labels:

   a. ``name`` — the generic image name label
   b. ``com.redhat.component`` — the Bugzilla component label
   c. ``com.redhat.security-tracker-component`` — the explicit tracker
      component label

   The first label whose value matches a component of the Jira project
   wins.  This gives teams whose existing labels already map to Jira
   components an escape hatch: they do not need to add the explicit
   ``com.redhat.security-tracker-component`` label.

The Jira server is fixed (``redhat.atlassian.net``), so the label carries
only the project key (e.g. ``OCPBUGS``) rather than a full URL.

With ``--enforce true`` (the default) any validation failure causes a
non-zero exit.  Without it, failures are logged as warnings and the
script exits successfully.  A failure to reach or authenticate to Jira is
always fatal, regardless of the enforce setting.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
from release_service_utils.helpers import http_client
from release_service_utils.helpers.file import load_json_dict
from release_service_utils.helpers.jira import (
    SUPPORTED_JIRA_SERVER,
    read_jira_credentials,
)
from release_service_utils.helpers.logger import logger
from requests.auth import HTTPBasicAuth

PROG = "check_jira_project_component.py"

SECURITY_TRACKER_PROJECT_LABEL = "com.redhat.security-tracker-project"
SECURITY_TRACKER_COMPONENT_LABEL = "com.redhat.security-tracker-component"

# Fallback chain: labels tried in order to resolve a Jira component.
COMPONENT_FALLBACK_LABELS: list[str] = [
    "name",
    "com.redhat.component",
    SECURITY_TRACKER_COMPONENT_LABEL,
]

REQUEST_TIMEOUT = 60.0


class TrackerValidationError(Exception):
    """Raised when tracker/project/component validation fails."""


class JiraConnectionError(Exception):
    """Raised when Jira cannot be reached or authenticated to.

    This is always fatal, independent of the enforce setting, because
    the script cannot make any determination without a working
    connection.
    """


def _get_component_labels(component: dict[str, Any]) -> dict[str, str]:
    """Extract labels from a component's ``metadata.labels`` list.

    The snapshot metadata stores labels as a list of
    ``{"name": "<key>", "value": "<val>"}`` dicts.  Convert this to a
    flat ``{key: value}`` dict for easier lookup.
    """
    raw = component.get("metadata", {}).get("labels") or []
    return {
        entry["name"]: str(entry.get("value") or "").strip()
        for entry in raw
        if isinstance(entry, dict) and "name" in entry
    }


def verify_jira_connection(
    session: requests.Session,
    auth: HTTPBasicAuth,
    server: str,
) -> None:
    """Verify Jira is reachable and the credentials authenticate.

    Raise ``JiraConnectionError`` on any failure so the caller can
    treat it as fatal.
    """
    url = f"https://{server}/rest/api/2/myself"
    try:
        response = session.get(url, auth=auth, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        raise JiraConnectionError(f"unable to reach Jira at '{server}': {exc}") from exc
    if response.status_code in (401, 403):
        raise JiraConnectionError(
            f"Jira authentication failed " f"({response.status_code}) for '{server}'"
        )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise JiraConnectionError(
            f"Jira connection check failed for '{server}': {exc}"
        ) from exc


def fetch_project_components(
    session: requests.Session,
    auth: HTTPBasicAuth,
    server: str,
    project_key: str,
) -> list[str] | None:
    """Return the component names of a Jira project.

    Return ``None`` when the project does not exist (HTTP 404).
    Raise ``JiraConnectionError`` on auth/network/other errors.
    """
    url = f"https://{server}/rest/api/2/project/" f"{quote(project_key)}/components"
    try:
        response = session.get(url, auth=auth, timeout=REQUEST_TIMEOUT)
    except requests.RequestException as exc:
        raise JiraConnectionError(
            f"unable to query Jira project " f"'{project_key}': {exc}"
        ) from exc
    if response.status_code == 404:
        return None
    if response.status_code in (401, 403):
        raise JiraConnectionError(
            f"Jira authorization failed "
            f"({response.status_code}) for project "
            f"'{project_key}'"
        )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise JiraConnectionError(
            f"Jira query failed for project " f"'{project_key}': {exc}"
        ) from exc
    data = response.json()
    return [c["name"] for c in data if isinstance(c, dict) and c.get("name")]


def _classify_component(
    component: dict[str, Any],
) -> tuple[str, Any]:
    """Classify a snapshot component without contacting Jira.

    Return a ``(status, payload)`` tuple where ``status`` is one of:

    - ``"skip"``: nothing to validate (no project label); ``payload``
      is None.
    - ``"validate"``: needs a Jira lookup; ``payload`` is
      ``(comp_name, project_key, labels)`` where ``labels`` is the
      full label dict.
    """
    comp_name = component.get("name", "<unknown>")
    labels = _get_component_labels(component)
    project_key = (labels.get(SECURITY_TRACKER_PROJECT_LABEL) or "").strip().upper()
    if not project_key:
        logger.info(
            "Component '%s' has no '%s' label; skipping "
            "(label presence is enforced separately).",
            comp_name,
            SECURITY_TRACKER_PROJECT_LABEL,
        )
        return ("skip", None)

    return ("validate", (comp_name, project_key, labels))


def _validate_jira_component(
    session: requests.Session,
    auth: HTTPBasicAuth,
    comp_name: str,
    project_key: str,
    labels: dict[str, str],
    cache: dict[str, list[str] | None],
) -> str | None:
    """Look up a Jira project and try the fallback chain.

    Return a violation message, or None when the component passes.
    The fallback chain tries each label in
    ``COMPONENT_FALLBACK_LABELS`` and accepts the first whose value
    matches a component of the Jira project.

    ``cache`` stores the result of ``fetch_project_components()``
    keyed by ``project_key`` so repeated calls for the same project
    avoid duplicate Jira requests.
    """
    if project_key in cache:
        component_names = cache[project_key]
    else:
        component_names = fetch_project_components(
            session, auth, SUPPORTED_JIRA_SERVER, project_key
        )
        cache[project_key] = component_names
    if component_names is None:
        return (
            f"Jira project '{project_key}' does not exist on "
            f"{SUPPORTED_JIRA_SERVER} (required by snapshot "
            f"component '{comp_name}')."
        )
    if not component_names:
        return (
            f"Jira project '{project_key}' has no components "
            f"defined (required by snapshot component "
            f"'{comp_name}')."
        )

    jira_names_lower = {name.strip().lower() for name in component_names}

    # Try each label in fallback order.
    tried: list[str] = []
    for label_key in COMPONENT_FALLBACK_LABELS:
        candidate = (labels.get(label_key) or "").strip()
        if not candidate:
            tried.append(f"{label_key}=(not set)")
            continue
        if candidate.lower() in jira_names_lower:
            logger.info(
                "Jira project '%s' contains component '%s' "
                "(resolved from label '%s' on snapshot "
                "component '%s').",
                project_key,
                candidate,
                label_key,
                comp_name,
            )
            return None
        tried.append(f"{label_key}='{candidate}'")

    tried_str = ", ".join(tried)
    return (
        f"No label value matched a component of Jira project "
        f"'{project_key}' for snapshot component "
        f"'{comp_name}'. Tried: {tried_str}. Existing Jira "
        f"components: {sorted(component_names)}."
    )


def check_jira_project_component(
    snapshot_path: Path,
    secret_path: Path,
    enforce: bool,
) -> None:
    """Validate the security-tracker labels for all components.

    Raise ``JiraConnectionError`` if Jira cannot be reached.  Raise
    ``TrackerValidationError`` when validation fails and ``enforce``
    is True.
    """
    snapshot = load_json_dict(snapshot_path)
    components = snapshot.get("components") or []

    violations: list[str] = []
    to_validate: list[tuple[str, str, dict[str, str]]] = []
    for component in components:
        status, payload = _classify_component(component)
        if status == "validate":
            to_validate.append(payload)

    if to_validate:
        email, token = read_jira_credentials(secret_path)
        auth = HTTPBasicAuth(email, token)
        session = http_client.get_retry_session(
            total=5,
            connect=3,
            read=3,
            status=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
        )
        verify_jira_connection(session, auth, SUPPORTED_JIRA_SERVER)
        cache: dict[str, list[str] | None] = {}
        for comp_name, project_key, comp_labels in to_validate:
            error = _validate_jira_component(
                session,
                auth,
                comp_name,
                project_key,
                comp_labels,
                cache,
            )
            if error is not None:
                violations.append(error)

    if not violations:
        logger.info("All components passed Jira project/component " "validation.")
        return

    for violation in violations:
        if enforce:
            logger.error(violation)
        else:
            logger.warning(violation)

    if enforce:
        raise TrackerValidationError(
            f"{len(violations)} component(s) failed Jira " f"project/component validation."
        )


def parse_args(
    argv: list[str] | None,
) -> argparse.Namespace:
    """Parse CLI arguments."""
    p = argparse.ArgumentParser(prog=PROG, description=__doc__)
    p.add_argument(
        "--snapshot-file",
        required=True,
        help="Path to the mapped snapshot JSON file",
    )
    p.add_argument(
        "--jira-secret-path",
        required=True,
        help=(
            "Path to the mounted secret directory containing "
            "'email' and 'token' files for Jira basic "
            "authentication"
        ),
    )
    p.add_argument(
        "--enforce",
        type=lambda s: s.strip().lower() == "true",
        default=True,
        help=("Set to 'true' to treat validation failures as " "errors (default: 'true')"),
    )
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Parse arguments and run the Jira project/component checks."""
    args = parse_args(argv[1:] if argv is not None else None)
    check_jira_project_component(
        snapshot_path=Path(args.snapshot_file),
        secret_path=Path(args.jira_secret_path),
        enforce=args.enforce,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
