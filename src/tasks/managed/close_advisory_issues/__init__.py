"""Close Jira issues listed in releaseNotes after an advisory is published."""

from .close_advisory_issues import (  # noqa: F401
    ISSUE_TRACKERS,
    LEGACY_JIRA_SERVER,
    SUPPORTED_JIRA_SERVER,
    add_issue_comment,
    api_path_for_server,
    close_advisory_issues,
    close_comment,
    close_issue_with_comment,
    closed_transition_id,
    is_jira_eligible_issue,
    issue_status_name,
    jira_get_json,
    jira_issue_url,
    jira_post_json,
    load_fixed_issues,
    main,
    normalize_issue_server,
    process_fixed_issue,
    read_jira_credentials,
)
