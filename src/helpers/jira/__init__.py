"""Jira REST API helpers shared across task scripts.

Provides constants and functions for interacting with Jira Cloud and legacy
issue tracker APIs: server normalization, API URL construction, credential
reading, and authenticated JSON requests.
"""

from .jira import (  # noqa: F401
    SUPPORTED_JIRA_SERVER,
    LEGACY_JIRA_SERVER,
    ISSUE_TRACKERS,
    normalize_issue_server,
    api_path_for_server,
    read_jira_credentials,
    jira_issue_url,
    jira_get_json,
    jira_post_json,
)
