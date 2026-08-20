"""Check whether CVEs are embargoed using the OSIDB API.

* Reads service account data from ``/mnt/osidb-service-account/`` (or
  ``OSIDB_SERVICE_ACCOUNT_MOUNT``): ``name``, ``base64_keytab``, ``osidb_url``.
* Authenticate with kinit (retried), then for each CVE obtain a token and GET
  ``/osidb/api/v2/flaws`` with the requested fields.
* Writes result paths from ``RESULT_RESULT`` and ``RESULT_EMBARGOED_CVES`` (set by
  the runner, e.g. a Tekton task).
* After a valid invocation with those env vars, always exits with status ``0``;
  success or failure is in the result files, including a short message that names
  the step that failed (e.g. ``kinit_with_retry``, ``get_access_token``) or that
  listed CVEs are not clearly public in OSIDB. Bad or missing/empty
  ``--cves`` exits before result handling (our checks use 1; argparse uses 2 for
  malformed argv).

``OSIDB_SERVICE_ACCOUNT_MOUNT`` can be overridden in tests to use a temp
directory with the same file layout.
"""

from .check_embargoed_cves import (  # noqa: F401
    parse_args,
    parse_cve_list,
    is_embargoed_flaw_response,
    fetch_flaw_state,
    run_check,
    main,
)
