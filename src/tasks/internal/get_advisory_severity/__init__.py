"""Compute advisory severity from release-notes images and OSIDB flaw data.

* Reads OSIDB credentials from `/mnt/osidb-service-account/` (or
  `OSIDB_SERVICE_ACCOUNT_MOUNT`): `name`, `base64_keytab`, `osidb_url`.
* Decodes `IMAGES_ENCODED` (base64+gzip JSON array of release-note images).
* Queries OSIDB for each fixed CVE, then returns the highest impact as a Tekton
  result (title-cased, e.g. `Critical`).
* Writes `RESULT_RESULT`, `RESULT_SEVERITY`, and internal-request result paths.
* After a valid invocation with those env vars, always exits with status `0`;
  success or failure is in the result files.
"""

from . import get_advisory_severity  # noqa: F401
