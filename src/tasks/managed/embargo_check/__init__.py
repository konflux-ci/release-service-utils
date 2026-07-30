"""Check if issues or CVEs in releaseNotes are embargoed.

Validates Jira/Bugzilla issues from ``releaseNotes.issues.fixed`` by querying
their REST APIs, injects a ``public`` boolean into each issue entry, validates
that Vulnerability-type Jira issues have their CVE listed in the content section,
and delegates CVE embargo checking to an InternalRequest running
``check-embargoed-cves``.
"""

from .embargo_check import check_issues, check_cves, create, run, main  # noqa: F401
