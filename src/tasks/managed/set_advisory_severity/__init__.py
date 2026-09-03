"""Set releaseNotes.severity for RHSA advisories via InternalRequest.

For non-RHSA types the script removes a user-supplied severity key (if any)
and exits. RHSA releases with no fixed CVEs fail. Generic artifacts skip the
OSIDB lookup. Image releases submit ``content.images`` to the
``get-advisory-severity`` pipeline and write the returned severity.
"""

from .set_advisory_severity import (  # noqa: F401
    count_fixed_cves,
    main,
    run,
)
