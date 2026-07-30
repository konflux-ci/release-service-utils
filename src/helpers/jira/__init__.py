"""Jira REST API helpers shared across task scripts.

Provides constants and functions for interacting with Jira Cloud and legacy
issue tracker APIs: server normalization, API URL construction, credential
reading, and authenticated JSON requests.
"""

from .jira import (  # noqa: F401
    ISSUE_TRACKERS,
    JIRA_CVE_CUSTOM_FIELD_ID,
    LEGACY_JIRA_SERVER,
    SUPPORTED_JIRA_SERVER,
    api_path_for_server,
    jira_get_json,
    jira_issue_url,
    jira_post_json,
    normalize_issue_server,
    read_jira_credentials,
)
