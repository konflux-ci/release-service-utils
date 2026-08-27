"""Check if issues or CVEs in releaseNotes are embargoed.

Validates Jira/Bugzilla issues from ``releaseNotes.issues.fixed`` by querying
their REST APIs, injects a ``public`` boolean into each issue entry, validates
that Vulnerability-type Jira issues have their CVE listed in the content section,
and delegates CVE embargo checking to an InternalRequest running
``check-embargoed-cves``.
"""

from release_service_utils.helpers.jira import (  # noqa: F401
    JIRA_CVE_CUSTOM_FIELD_ID,
    SUPPORTED_JIRA_SERVER,
)

from .embargo_check import (  # noqa: F401
    check_cves,
    check_issues,
    create,
    main,
    run,
)
