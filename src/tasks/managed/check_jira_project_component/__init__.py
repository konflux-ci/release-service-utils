"""Validate image security-tracker labels against Jira projects.

For each container image component the ``com.redhat.security-tracker-project``
label is read as a Jira project key, the project is confirmed to exist, and a
Jira component is resolved using a fallback chain:

1. ``name`` label
2. ``com.redhat.component`` label
3. ``com.redhat.security-tracker-component`` label

The first label whose value matches a component of the Jira project wins.  This
gives teams whose existing ``name`` or ``com.redhat.component`` labels already
map to Jira components an escape hatch — they do not need to add the explicit
``com.redhat.security-tracker-component`` label.

With ``--enforce true`` validation failures cause a non-zero exit. Without it,
failures are logged as warnings and the script exits successfully. A failure to
reach or authenticate to Jira is always fatal.
"""

from .check_jira_project_component import (  # noqa: F401
    COMPONENT_FALLBACK_LABELS,
    PROG,
    SECURITY_TRACKER_COMPONENT_LABEL,
    SECURITY_TRACKER_PROJECT_LABEL,
    JiraConnectionError,
    TrackerValidationError,
    fetch_project_components,
    main,
    parse_args,
    verify_jira_connection,
)
